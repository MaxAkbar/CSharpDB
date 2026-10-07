using System.Buffers.Binary;
using System.Security.Cryptography;

namespace CSharpDB.WalPrototype.Tests;

internal enum ControlMode : uint { Active = 1, Empty = 2 }

// Opaque 128-bit identity, encoded as two little-endian words (not Guid byte order).
internal readonly record struct FileIdentity(ulong Low, ulong High)
{
    public static FileIdentity Create()
    {
        Span<byte> bytes = stackalloc byte[16];
        FileIdentity value;
        do
        {
            RandomNumberGenerator.Fill(bytes);
            value = new(BinaryPrimitives.ReadUInt64LittleEndian(bytes),
                BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]));
        } while (value == default);
        return value;
    }
}

internal readonly record struct GenerationSalt(uint First, uint Second)
{
    public static GenerationSalt Create(GenerationSalt previous)
    {
        Span<byte> bytes = stackalloc byte[8];
        GenerationSalt value;
        do
        {
            RandomNumberGenerator.Fill(bytes);
            value = new(BinaryPrimitives.ReadUInt32LittleEndian(bytes),
                BinaryPrimitives.ReadUInt32LittleEndian(bytes[4..]));
        } while (value.First == 0 || value.Second == 0 || value == previous);
        return value;
    }
}

internal sealed record ControlRecord(FileIdentity Identity, ulong Sequence, ulong Epoch,
    GenerationSalt Salt, ControlMode Mode, long Floor, uint PageCount)
{
    // Opt-in prototype feature. Null retains the original floor-only experiment.
    public long? CommittedTail { get; init; }
    public bool SameBoundary(ControlRecord other) => this with { Sequence = other.Sequence } == other;
}

internal sealed record ControlSelection(ControlRecord Current, int CurrentSlot,
    ControlRecord? First, ControlRecord? Second)
{
    public bool Mirrored => First is not null && Second is not null && First.SameBoundary(Second);
}

internal static class ControlCodec
{
    public const int SlotSize = 4096;
    public const int DataStart = 2 * SlotSize;
    public const int PageSize = 4096;
    public const int FrameSize = 4120;
    public const int DigestOffset = SlotSize - 32;
    public const uint CommittedTailFeature = 1;

    public static byte[] Encode(ControlRecord record)
    {
        Validate(record, long.MaxValue);
        byte[] bytes = new byte[SlotSize];
        "CWA2"u8.CopyTo(bytes);
        Write32(bytes, 4, 2);
        Write32(bytes, 8, record.CommittedTail.HasValue ? CommittedTailFeature : 0);
        Write32(bytes, 12, SlotSize);
        Write32(bytes, 16, PageSize);
        Write32(bytes, 20, FrameSize);
        Write64(bytes, 24, DataStart);
        Write64(bytes, 32, record.Identity.Low);
        Write64(bytes, 40, record.Identity.High);
        Write64(bytes, 48, record.Sequence);
        Write64(bytes, 56, record.Epoch);
        Write32(bytes, 64, record.Salt.First);
        Write32(bytes, 68, record.Salt.Second);
        Write32(bytes, 72, (uint)record.Mode);
        Write64(bytes, 80, checked((ulong)record.Floor));
        Write32(bytes, 88, record.PageCount);
        if (record.CommittedTail is long tail) Write64(bytes, 96, checked((ulong)tail));
        SHA256.HashData(bytes.AsSpan(0, DigestOffset), bytes.AsSpan(DigestOffset));
        return bytes;
    }

    // Null means unreadable/torn. Checksummed but unsupported/malformed data is fatal,
    // including in the older slot; callers must never catch it and choose a stale floor.
    public static ControlRecord? Decode(ReadOnlySpan<byte> bytes, long physicalLength)
    {
        if (bytes.Length != SlotSize)
            return null;
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(bytes[..DigestOffset], digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, bytes[DigestOffset..]))
            return null;
        Require(bytes[..4].SequenceEqual("CWA2"u8), "Unsupported magic.");
        Require(Read32(bytes, 4) == 2, "Unsupported version.");
        uint features = Read32(bytes, 8);
        Require((features & ~CommittedTailFeature) == 0, "Unsupported feature flags.");
        Require(Read32(bytes, 12) == SlotSize && Read32(bytes, 16) == PageSize &&
            Read32(bytes, 20) == FrameSize && Read64(bytes, 24) == DataStart, "Unsupported layout.");
        Require(bytes.Slice(76, 4).IndexOfAnyExcept((byte)0) < 0 &&
            bytes[92..96].IndexOfAnyExcept((byte)0) < 0 &&
            bytes[104..DigestOffset].IndexOfAnyExcept((byte)0) < 0, "Nonzero reserved bytes.");
        ulong tail = Read64(bytes, 96);
        bool hasTail = (features & CommittedTailFeature) != 0;
        Require(hasTail ? tail <= long.MaxValue : tail == 0, "Invalid committed-tail field.");
        ulong floor = Read64(bytes, 80);
        Require(floor <= long.MaxValue, "Replay floor overflows file offsets.");
        var record = new ControlRecord(new(Read64(bytes, 32), Read64(bytes, 40)),
            Read64(bytes, 48), Read64(bytes, 56), new(Read32(bytes, 64), Read32(bytes, 68)),
            (ControlMode)Read32(bytes, 72), (long)floor, Read32(bytes, 88))
        { CommittedTail = hasTail ? (long)tail : null };
        Validate(record, physicalLength);
        return record;
    }

    public static ControlSelection Select(ReadOnlySpan<byte> header, long physicalLength)
    {
        var first = Decode(header[..Math.Min(header.Length, SlotSize)], physicalLength);
        var second = header.Length < SlotSize ? null :
            Decode(header.Slice(SlotSize, Math.Min(header.Length - SlotSize, SlotSize)), physicalLength);
        Require(first is not null || second is not null, "No readable control record.");
        if (first is not null && second is not null)
        {
            Require(first.Identity == second.Identity, "Conflicting file identities.");
            Require(first.CommittedTail.HasValue == second.CommittedTail.HasValue, "Conflicting feature flags.");
            Require(first.Sequence != second.Sequence || first == second, "Conflicting equal sequences.");
            var older = first.Sequence <= second.Sequence ? first : second;
            var newer = first.Sequence <= second.Sequence ? second : first;
            Require(newer.Epoch >= older.Epoch, "Generation moved backwards.");
            if (newer.Epoch == older.Epoch)
            {
                Require(newer.Salt == older.Salt, "Conflicting salts in one generation.");
                Require(older.Mode != ControlMode.Active || newer.Mode == ControlMode.Active,
                    "EMPTY requires a new generation.");
                if (older.Mode == ControlMode.Active)
                {
                    Require(newer.Floor >= older.Floor, "Replay floor moved backwards.");
                    if (older.CommittedTail is long oldTail)
                        Require(newer.CommittedTail >= oldTail, "Committed tail moved backwards.");
                }
            }
            else
                Require(newer.Salt != older.Salt, "New generation reused salts.");
        }
        bool chooseFirst = second is null || (first is not null && first.Sequence >= second.Sequence);
        return new(chooseFirst ? first! : second!, chooseFirst ? 0 : 1, first, second);
    }

    public static void Validate(ControlRecord record, long physicalLength)
    {
        Require(record.Identity != default, "Missing file identity.");
        Require(record.Sequence != 0 && record.Epoch != 0, "Zero sequence or epoch.");
        Require(record.Salt.First != 0 && record.Salt.Second != 0, "Missing generation salts.");
        Require(record.Mode is ControlMode.Active or ControlMode.Empty, "Unknown mode.");
        Require(physicalLength >= DataStart, "Truncated control area.");
        Require(record.Floor >= DataStart && record.Floor <= physicalLength &&
            (record.Floor - DataStart) % FrameSize == 0, "Invalid replay floor.");
        Require(record.Mode != ControlMode.Empty || record.Floor == DataStart, "EMPTY floor must be H.");
        if (record.CommittedTail is long tail)
        {
            Require(tail >= record.Floor && tail <= physicalLength &&
                (tail - DataStart) % FrameSize == 0, "Invalid committed tail.");
            Require(record.Mode != ControlMode.Empty || tail == DataStart, "EMPTY committed tail must be H.");
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidDataException(message);
    }

    private static uint Read32(ReadOnlySpan<byte> bytes, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(bytes[offset..]);
    private static ulong Read64(ReadOnlySpan<byte> bytes, int offset) => BinaryPrimitives.ReadUInt64LittleEndian(bytes[offset..]);
    private static void Write32(Span<byte> bytes, int offset, uint value) => BinaryPrimitives.WriteUInt32LittleEndian(bytes[offset..], value);
    private static void Write64(Span<byte> bytes, int offset, ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(bytes[offset..], value);
}

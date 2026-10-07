using System.Buffers.Binary;
using CSharpDB.Storage.Integrity;

namespace CSharpDB.WalPrototype.Tests;

internal sealed record PageWrite(uint PageId, byte[] Data);
internal readonly record struct FrameHeader(uint PageId, uint PageCount);
internal sealed record RecoveredWal(Dictionary<uint, long> Pages, uint PageCount, long PublishedTail);

// Same 24-byte header, additive checksums and last-frame commit marker as WriteAheadLog.
// Only the enclosing control region and recovery policy differ. No production binaries run here.
internal static class WalFrameCodec
{
    public const int HeaderSize = 24;
    public const uint MaximumPageCount = 65536; // Bounded experiment, not a product limit.
    private static readonly AdditiveChecksumProvider Checksum = new();

    public static byte[] Encode(PageWrite page, uint pageCount, GenerationSalt salt)
    {
        if (page.Data.Length != ControlCodec.PageSize) throw new ArgumentException("Expected one full page.");
        byte[] bytes = new byte[ControlCodec.FrameSize];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, page.PageId);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(4), pageCount);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), salt.First);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(12), salt.Second);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(16), unchecked(page.PageId + pageCount + salt.First + salt.Second));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(20), Checksum.Compute(page.Data));
        page.Data.CopyTo(bytes, HeaderSize);
        return bytes;
    }

    public static FrameHeader Decode(ReadOnlySpan<byte> bytes, GenerationSalt salt)
    {
        if (bytes.Length != ControlCodec.FrameSize) throw new InvalidDataException("Incomplete active WAL frame.");
        uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(bytes);
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(bytes[4..]);
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes[8..]) != salt.First ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes[12..]) != salt.Second)
            throw new InvalidDataException("Active frame generation mismatch.");
        if (BinaryPrimitives.ReadUInt32LittleEndian(bytes[16..]) != unchecked(pageId + count + salt.First + salt.Second) ||
            BinaryPrimitives.ReadUInt32LittleEndian(bytes[20..]) != Checksum.Compute(bytes[HeaderSize..]))
            throw new InvalidDataException("Active frame checksum mismatch.");
        return new(pageId, count);
    }

    public static byte[] CommitHeader(ReadOnlySpan<byte> encodedFrame, uint pageCount)
    {
        if (encodedFrame.Length != ControlCodec.FrameSize || pageCount == 0) throw new ArgumentException("Invalid commit frame.");
        byte[] header = encodedFrame[..HeaderSize].ToArray();
        uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(header);
        uint first = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(8));
        uint second = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(12));
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4), pageCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(16), unchecked(pageId + pageCount + first + second));
        // Retain the checksum of the bytes already written, not a reread of caller-owned data.
        return header;
    }

    public static RecoveredWal Recover(ExperimentalWalFile file, ControlRecord control)
    {
        Dictionary<uint, long> pages = [], pending = [];
        uint pageCount = control.PageCount;
        long published = control.Floor;
        if (control.Mode == ControlMode.Empty) return new(pages, pageCount, ControlCodec.DataStart);
        long scanEnd = control.CommittedTail ?? file.Length;
        for (long offset = control.Floor; offset < scanEnd; offset = checked(offset + ControlCodec.FrameSize))
        {
            // Partial bytes or an invalid complete frame may be a damaged acknowledged commit.
            // Fail without changing either file; do not infer a safe tail from physical EOF.
            if (scanEnd - offset < ControlCodec.FrameSize)
                throw new InvalidDataException("Ambiguous partial active WAL tail.");
            var header = Decode(file.ReadAt(offset, ControlCodec.FrameSize), control.Salt);
            if (header.PageId >= MaximumPageCount || header.PageCount > MaximumPageCount)
                throw new InvalidDataException("Frame exceeds prototype page limit.");
            pending[header.PageId] = offset;
            if (header.PageCount == 0) continue;
            if (header.PageCount < pageCount || pending.Keys.Any(id => id >= header.PageCount))
                throw new InvalidDataException("Invalid committed page range; shrinking is not supported by this prototype.");
            for (uint id = pageCount; id < header.PageCount; id++)
                if (!pending.ContainsKey(id)) throw new InvalidDataException("New committed page has no image.");
            foreach (var entry in pending) pages[entry.Key] = entry.Value;
            pending.Clear();
            pageCount = header.PageCount;
            published = checked(offset + ControlCodec.FrameSize);
        }
        if (control.CommittedTail.HasValue && (pending.Count != 0 || published != scanEnd))
            throw new InvalidDataException("Declared committed boundary ends in an uncommitted batch.");
        // A durable tail proves later bytes unacknowledged. The original floor-only format
        // can discard only a fully validated marker-free suffix; ambiguous bytes remain fatal.
        return new(pages, pageCount, published);
    }
}

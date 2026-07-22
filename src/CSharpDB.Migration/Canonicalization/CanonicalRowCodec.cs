using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Canonicalization;

/// <summary>
/// Streaming encoder and SHA-256 implementation for <c>csharpdb-canon-v1</c>.
/// </summary>
public static class CanonicalRowCodec
{
    public const string CanonicalizationId = "csharpdb-canon-v1";

    public const string RowDomain = "CSDBCAN1";

    public const string KeyDomain = "CSDBKEY1";

    public const string ContractHashHex =
        "8a323b42ac39d6faa2a8609c88143f5e78f613fb2b73cb2947ac50bf35ee616a";

    private const int StreamingBufferSize = 4 * 1024;

    private static readonly byte[] RowMagic = Encoding.ASCII.GetBytes(RowDomain);
    private static readonly byte[] KeyMagic = Encoding.ASCII.GetBytes(KeyDomain);
    private static readonly byte[] ContractHash = Convert.FromHexString(ContractHashHex);
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static byte[] EncodeRow(IReadOnlyList<CanonicalValue> fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        var writer = new ArrayBufferWriter<byte>();
        WriteRow(writer, fields);
        return writer.WrittenSpan.ToArray();
    }

    public static byte[] ComputeRowHashBytes(IReadOnlyList<CanonicalValue> fields) =>
        ComputeHash(fields, keyDomain: false);

    public static byte[] ComputeKeyHashBytes(IReadOnlyList<CanonicalValue> keyFields) =>
        ComputeHash(keyFields, keyDomain: true);

    public static string ComputeRowHash(IReadOnlyList<CanonicalValue> fields) =>
        Convert.ToHexString(ComputeRowHashBytes(fields)).ToLowerInvariant();

    public static string ComputeKeyHash(IReadOnlyList<CanonicalValue> keyFields) =>
        Convert.ToHexString(ComputeKeyHashBytes(keyFields)).ToLowerInvariant();

    private static byte[] ComputeHash(IReadOnlyList<CanonicalValue> fields, bool keyDomain)
    {
        ArgumentNullException.ThrowIfNull(fields);
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var writer = new IncrementalHashBufferWriter(hash);
        if (keyDomain)
            WriteBytes(writer, KeyMagic);
        WriteRow(writer, fields);
        return hash.GetHashAndReset();
    }

    private static void WriteRow(IBufferWriter<byte> writer, IReadOnlyList<CanonicalValue> fields)
    {
        int fieldCount = fields.Count;
        WriteBytes(writer, RowMagic);
        WriteBytes(writer, ContractHash);
        WriteUInt32(writer, checked((uint)fieldCount));

        for (int index = 0; index < fieldCount; index++)
            WriteField(writer, fields[index]);
    }

    private static void WriteField(IBufferWriter<byte> writer, CanonicalValue field)
    {
        field.Validate();
        WriteByte(writer, (byte)field.Type);
        WriteByte(writer, (byte)field.State);

        if (field.State == CanonicalFieldState.Null)
        {
            WriteUInt64(writer, 0);
            return;
        }

        if (field.State == CanonicalFieldState.Excluded)
        {
            WriteUInt64(writer, 1);
            WriteByte(writer, (byte)field.Bits0);
            return;
        }

        switch (field.Type)
        {
            case CanonicalType.Boolean:
                WriteUInt64(writer, 1);
                WriteByte(writer, checked((byte)field.Bits0));
                return;
            case CanonicalType.Int64:
            case CanonicalType.UInt64:
                WriteUInt64(writer, sizeof(ulong));
                WriteUInt64(writer, field.Bits0);
                return;
            case CanonicalType.Decimal:
                WriteDecimal(writer, field.Reference<CanonicalDecimal>());
                return;
            case CanonicalType.Binary32:
                WriteUInt64(writer, sizeof(uint));
                WriteUInt32(writer, checked((uint)field.Bits0));
                return;
            case CanonicalType.Binary64:
                WriteUInt64(writer, sizeof(ulong));
                WriteUInt64(writer, field.Bits0);
                return;
            case CanonicalType.Text:
                WriteText(writer, field.Reference<string>());
                return;
            case CanonicalType.Blob:
                WriteBlob(writer, field.Reference<ReadOnlyMemory<byte>>());
                return;
            case CanonicalType.Guid:
                WriteGuid(writer, field.Reference<Guid>());
                return;
            case CanonicalType.Date:
                WriteUInt64(writer, sizeof(int));
                WriteUInt32(writer, checked((uint)field.Bits0));
                return;
            case CanonicalType.Time:
                WriteUInt64(writer, sizeof(ulong));
                WriteUInt64(writer, field.Bits0);
                return;
            case CanonicalType.WallDateTime:
                WriteUInt64(writer, sizeof(int) + sizeof(ulong));
                WriteUInt32(writer, checked((uint)field.Bits0));
                WriteUInt64(writer, field.Bits1);
                return;
            case CanonicalType.UtcInstant:
                WriteUInt64(writer, sizeof(long) + sizeof(uint));
                WriteUInt64(writer, field.Bits0);
                WriteUInt32(writer, checked((uint)field.Bits1));
                return;
            case CanonicalType.OffsetDateTime:
                WriteUInt64(writer, sizeof(int) + sizeof(ulong) + sizeof(short));
                WriteUInt32(writer, unchecked((uint)field.Bits0));
                WriteUInt64(writer, field.Bits1);
                WriteUInt16(writer, checked((ushort)(field.Bits0 >> 32)));
                return;
            default:
                throw new InvalidDataException($"Unknown canonical type tag 0x{(byte)field.Type:x2}.");
        }
    }

    private static void WriteDecimal(IBufferWriter<byte> writer, CanonicalDecimal value)
    {
        ReadOnlySpan<byte> coefficient = value.CoefficientBytes;
        WriteUInt64(writer, checked((ulong)(sizeof(uint) + coefficient.Length)));
        WriteUInt32(writer, value.Scale);
        WriteBytes(writer, coefficient);
    }

    private static void WriteText(IBufferWriter<byte> writer, string value)
    {
        int length;
        try
        {
            length = StrictUtf8.GetByteCount(value);
        }
        catch (EncoderFallbackException ex)
        {
            throw new InvalidDataException("Canonical TEXT must be valid strict UTF-8.", ex);
        }

        WriteUInt64(writer, checked((ulong)length));
        Encoder encoder = StrictUtf8.GetEncoder();
        ReadOnlySpan<char> remaining = value.AsSpan();
        bool completed;
        do
        {
            Span<byte> destination = writer.GetSpan(1);
            try
            {
                encoder.Convert(
                    remaining,
                    destination,
                    flush: true,
                    out int charsUsed,
                    out int bytesUsed,
                    out completed);
                writer.Advance(bytesUsed);
                remaining = remaining[charsUsed..];
            }
            catch (EncoderFallbackException ex)
            {
                throw new InvalidDataException("Canonical TEXT must be valid strict UTF-8.", ex);
            }
        }
        while (!completed);
    }

    private static void WriteBlob(IBufferWriter<byte> writer, ReadOnlyMemory<byte> value)
    {
        WriteUInt64(writer, checked((ulong)value.Length));
        WriteBytes(writer, value.Span);
    }

    private static void WriteGuid(IBufferWriter<byte> writer, Guid value)
    {
        WriteUInt64(writer, 16);
        Span<byte> destination = writer.GetSpan(16);
        if (!value.TryWriteBytes(destination, bigEndian: true, out int bytesWritten) || bytesWritten != 16)
            throw new InvalidOperationException("The GUID could not be written in RFC/network byte order.");
        writer.Advance(bytesWritten);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> destination = writer.GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(sizeof(ushort));
    }

    private static void WriteUInt32(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(sizeof(uint));
        BinaryPrimitives.WriteUInt32BigEndian(destination, value);
        writer.Advance(sizeof(uint));
    }

    private static void WriteUInt64(IBufferWriter<byte> writer, ulong value)
    {
        Span<byte> destination = writer.GetSpan(sizeof(ulong));
        BinaryPrimitives.WriteUInt64BigEndian(destination, value);
        writer.Advance(sizeof(ulong));
    }

    private static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        while (!value.IsEmpty)
        {
            Span<byte> destination = writer.GetSpan(1);
            int count = Math.Min(destination.Length, value.Length);
            value[..count].CopyTo(destination);
            writer.Advance(count);
            value = value[count..];
        }
    }

    private sealed class IncrementalHashBufferWriter : IBufferWriter<byte>
    {
        private readonly byte[] _buffer = new byte[StreamingBufferSize];
        private readonly IncrementalHash _hash;

        public IncrementalHashBufferWriter(IncrementalHash hash)
        {
            _hash = hash;
        }

        public void Advance(int count)
        {
            if ((uint)count > _buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));
            _hash.AppendData(_buffer.AsSpan(0, count));
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            ValidateSizeHint(sizeHint);
            return _buffer;
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            ValidateSizeHint(sizeHint);
            return _buffer;
        }

        private static void ValidateSizeHint(int sizeHint)
        {
            if (sizeHint < 0 || sizeHint > StreamingBufferSize)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(sizeHint),
                    $"Canonical streaming writes are limited to {StreamingBufferSize} bytes per segment.");
            }
        }
    }
}

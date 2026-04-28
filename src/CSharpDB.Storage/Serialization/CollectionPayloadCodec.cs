using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Binary payload codec for collection documents stored directly in table B+trees.
/// Supports both the legacy direct JSON wrapper and the newer binary document payload.
/// </summary>
public static class CollectionPayloadCodec
{
    internal const byte LegacyJsonFormatMarker = 0xC1;
    internal const byte BinaryFormatMarker = 0xC2;
    internal const byte BinaryFormatVersion = 0x01;
    internal const byte GeneratedRecordFormatMarker = 0xD0;
    internal const byte GeneratedRecordFormatMagic = 0xF0;
    internal const byte GeneratedRecordFormatVersion = 0x01;

    public static bool IsDirectPayload(ReadOnlySpan<byte> payload)
        => TryReadHeader(payload, out _);

    public static bool IsBinaryPayload(ReadOnlySpan<byte> payload)
        => TryReadHeader(payload, out var header) && header.Format == CollectionPayloadFormat.Binary;

    internal static bool TryReadValidatedHeader(ReadOnlySpan<byte> payload, out Header header)
        => TryReadHeader(payload, out header);

    internal static bool TryReadFastHeader(ReadOnlySpan<byte> payload, out Header header)
    {
        if (TryReadFastHeaderFields(
                payload,
                out CollectionPayloadFormat format,
                out int keyStart,
                out int keyByteCount,
                out int documentStart))
        {
            header = new Header(format, keyStart, keyByteCount, documentStart);
            return true;
        }

        header = default;
        return false;
    }

    internal static ReadOnlySpan<byte> GetKeyUtf8(ReadOnlySpan<byte> payload, Header header)
        => payload.Slice(header.KeyStart, header.KeyByteCount);

    internal static ReadOnlySpan<byte> GetDocumentPayload(ReadOnlySpan<byte> payload, Header header)
        => payload[header.DocumentStart..];

    public static byte[] Encode(ReadOnlySpan<byte> keyUtf8, ReadOnlySpan<byte> jsonUtf8)
    {
        int keyLengthSize = Varint.SizeOf((ulong)keyUtf8.Length);
        byte[] payload = GC.AllocateUninitializedArray<byte>(1 + keyLengthSize + keyUtf8.Length + jsonUtf8.Length);
        payload[0] = LegacyJsonFormatMarker;

        int position = 1;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyUtf8.Length);
        keyUtf8.CopyTo(payload.AsSpan(position));
        position += keyUtf8.Length;
        jsonUtf8.CopyTo(payload.AsSpan(position));
        return payload;
    }

    public static byte[] Encode(string key, ReadOnlySpan<byte> jsonUtf8)
    {
        int keyByteCount = Encoding.UTF8.GetByteCount(key);
        int keyLengthSize = Varint.SizeOf((ulong)keyByteCount);
        byte[] payload = GC.AllocateUninitializedArray<byte>(1 + keyLengthSize + keyByteCount + jsonUtf8.Length);
        payload[0] = LegacyJsonFormatMarker;

        int position = 1;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyByteCount);
        position += Encoding.UTF8.GetBytes(key.AsSpan(), payload.AsSpan(position, keyByteCount));
        jsonUtf8.CopyTo(payload.AsSpan(position));
        return payload;
    }

    public static byte[] EncodeBinary(ReadOnlySpan<byte> keyUtf8, ReadOnlySpan<byte> documentPayload)
    {
        int keyLengthSize = Varint.SizeOf((ulong)keyUtf8.Length);
        byte[] payload = GC.AllocateUninitializedArray<byte>(2 + keyLengthSize + keyUtf8.Length + documentPayload.Length);
        payload[0] = BinaryFormatMarker;
        payload[1] = BinaryFormatVersion;

        int position = 2;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyUtf8.Length);
        keyUtf8.CopyTo(payload.AsSpan(position));
        position += keyUtf8.Length;
        documentPayload.CopyTo(payload.AsSpan(position));
        return payload;
    }

    public static byte[] EncodeBinary(string key, ReadOnlySpan<byte> documentPayload)
    {
        int keyByteCount = Encoding.UTF8.GetByteCount(key);
        int keyLengthSize = Varint.SizeOf((ulong)keyByteCount);
        byte[] payload = GC.AllocateUninitializedArray<byte>(2 + keyLengthSize + keyByteCount + documentPayload.Length);
        payload[0] = BinaryFormatMarker;
        payload[1] = BinaryFormatVersion;

        int position = 2;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyByteCount);
        position += Encoding.UTF8.GetBytes(key.AsSpan(), payload.AsSpan(position, keyByteCount));
        documentPayload.CopyTo(payload.AsSpan(position));
        return payload;
    }

    public static byte[] EncodeBinary<TState>(
        string key,
        int documentPayloadLength,
        SpanAction<byte, TState> writeDocumentPayload,
        TState state)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(writeDocumentPayload);
        ArgumentOutOfRangeException.ThrowIfNegative(documentPayloadLength);

        int keyByteCount = Encoding.UTF8.GetByteCount(key);
        int keyLengthSize = Varint.SizeOf((ulong)keyByteCount);
        byte[] payload = GC.AllocateUninitializedArray<byte>(2 + keyLengthSize + keyByteCount + documentPayloadLength);
        payload[0] = BinaryFormatMarker;
        payload[1] = BinaryFormatVersion;

        int position = 2;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyByteCount);
        position += Encoding.UTF8.GetBytes(key.AsSpan(), payload.AsSpan(position, keyByteCount));
        writeDocumentPayload(payload.AsSpan(position, documentPayloadLength), state);
        return payload;
    }

    public static byte[] EncodeBinary(
        string key,
        int documentPayloadLength,
        out int documentPayloadStart)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfNegative(documentPayloadLength);

        int keyByteCount = Encoding.UTF8.GetByteCount(key);
        int keyLengthSize = Varint.SizeOf((ulong)keyByteCount);
        byte[] payload = GC.AllocateUninitializedArray<byte>(2 + keyLengthSize + keyByteCount + documentPayloadLength);
        payload[0] = BinaryFormatMarker;
        payload[1] = BinaryFormatVersion;

        int position = 2;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyByteCount);
        position += Encoding.UTF8.GetBytes(key.AsSpan(), payload.AsSpan(position, keyByteCount));
        documentPayloadStart = position;
        return payload;
    }

    public static bool TryDecodeDirectPayloadKey(ReadOnlySpan<byte> payload, out string key)
    {
        if (TryReadFastHeaderFields(
                payload,
                out _,
                out int keyStart,
                out int keyByteCount,
                out _))
        {
            key = Encoding.UTF8.GetString(payload.Slice(keyStart, keyByteCount));
            return true;
        }

        key = string.Empty;
        return false;
    }

    public static bool TryDirectPayloadKeyEquals(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<byte> expectedKeyUtf8,
        out bool equals)
    {
        if (TryReadFastHeaderFields(
                payload,
                out _,
                out int keyStart,
                out int keyByteCount,
                out _))
        {
            equals = payload.Slice(keyStart, keyByteCount).SequenceEqual(expectedKeyUtf8);
            return true;
        }

        equals = false;
        return false;
    }

    public static bool TryDirectPayloadKeyEquals(
        ReadOnlySpan<byte> payload,
        string expectedKey,
        out bool equals)
    {
        ArgumentNullException.ThrowIfNull(expectedKey);

        if (TryReadFastHeaderFields(
                payload,
                out _,
                out int keyStart,
                out int keyByteCount,
                out _))
        {
            equals = KeyUtf8EqualsString(payload.Slice(keyStart, keyByteCount), expectedKey);
            return true;
        }

        equals = false;
        return false;
    }

    public static bool TryGetBinaryDocumentPayload(
        ReadOnlySpan<byte> payload,
        out ReadOnlySpan<byte> documentPayload)
    {
        if (TryReadFastHeaderFields(
                payload,
                out CollectionPayloadFormat format,
                out _,
                out _,
                out int documentStart) &&
            format == CollectionPayloadFormat.Binary)
        {
            documentPayload = payload[documentStart..];
            return true;
        }

        documentPayload = default;
        return false;
    }

    public static bool KeyEquals(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> expectedKeyUtf8)
    {
        var header = ReadHeader(payload);
        return payload.Slice(header.KeyStart, header.KeyByteCount).SequenceEqual(expectedKeyUtf8);
    }

    public static string DecodeKey(ReadOnlySpan<byte> payload)
    {
        var header = ReadHeader(payload);
        return Encoding.UTF8.GetString(payload.Slice(header.KeyStart, header.KeyByteCount));
    }

    public static string DecodeJson(ReadOnlySpan<byte> payload)
    {
        var header = ReadHeader(payload);
        if (header.Format == CollectionPayloadFormat.LegacyJson)
            return Encoding.UTF8.GetString(payload[header.DocumentStart..]);

        ReadOnlySpan<byte> documentPayload = payload[header.DocumentStart..];
        if (IsGeneratedRecordDocumentPayload(documentPayload))
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Generated record collection payloads require their generated collection codec for JSON conversion.");
        }

        return CollectionBinaryDocumentCodec.DecodeJson(documentPayload);
    }

    public static bool JsonEquals(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> expectedUtf8)
    {
        var header = ReadHeader(payload);
        if (header.Format == CollectionPayloadFormat.LegacyJson)
            return payload[header.DocumentStart..].SequenceEqual(expectedUtf8);

        ReadOnlySpan<byte> documentPayload = payload[header.DocumentStart..];
        return !IsGeneratedRecordDocumentPayload(documentPayload) &&
               CollectionBinaryDocumentCodec.EncodeJsonUtf8(documentPayload).AsSpan().SequenceEqual(expectedUtf8);
    }

    public static ReadOnlySpan<byte> GetJsonUtf8(ReadOnlySpan<byte> payload)
    {
        var header = ReadHeader(payload);
        if (header.Format != CollectionPayloadFormat.LegacyJson)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Binary collection payloads do not expose a direct JSON UTF-8 span.");
        }

        return payload[header.DocumentStart..];
    }

    public static ReadOnlySpan<byte> GetBinaryDocumentPayload(ReadOnlySpan<byte> payload)
    {
        var header = ReadHeader(payload);
        if (header.Format != CollectionPayloadFormat.Binary)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Legacy collection payloads do not expose a binary document payload.");
        }

        return payload[header.DocumentStart..];
    }

    private static Header ReadHeader(ReadOnlySpan<byte> payload)
    {
        if (!TryReadHeader(payload, out var header))
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid collection payload header.");

        return header;
    }

    private static bool TryReadHeader(ReadOnlySpan<byte> payload, out Header header)
    {
        header = default;

        try
        {
            if (TryReadLegacyHeader(payload, out header))
                return HasPlausibleJsonPayload(payload[header.DocumentStart..]);

            if (TryReadBinaryHeader(payload, out header))
                return IsKnownBinaryDocumentPayload(payload[header.DocumentStart..]);

            return false;
        }
        catch (Exception ex) when (ex is CSharpDbException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            return false;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryReadFastHeaderFields(
        ReadOnlySpan<byte> payload,
        out CollectionPayloadFormat format,
        out int keyStart,
        out int keyByteCount,
        out int documentStart)
    {
        int lengthOffset;
        if (payload.Length >= 3 &&
            payload[0] == BinaryFormatMarker &&
            payload[1] == BinaryFormatVersion)
        {
            format = CollectionPayloadFormat.Binary;
            lengthOffset = 2;
        }
        else if (payload.Length >= 2 && payload[0] == LegacyJsonFormatMarker)
        {
            format = CollectionPayloadFormat.LegacyJson;
            lengthOffset = 1;
        }
        else
        {
            return FailFastHeaderFields(out format, out keyStart, out keyByteCount, out documentStart);
        }

        if (!TryReadInt32Varint(payload, lengthOffset, out keyByteCount, out int keyLengthBytes))
            return FailFastHeaderFields(out format, out keyStart, out keyByteCount, out documentStart);

        keyStart = lengthOffset + keyLengthBytes;
        if (keyByteCount > payload.Length - keyStart)
            return FailFastHeaderFields(out format, out keyStart, out keyByteCount, out documentStart);

        documentStart = keyStart + keyByteCount;
        if (documentStart >= payload.Length)
            return FailFastHeaderFields(out format, out keyStart, out keyByteCount, out documentStart);

        return format != CollectionPayloadFormat.LegacyJson ||
               HasPlausibleJsonPayload(payload[documentStart..]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool FailFastHeaderFields(
        out CollectionPayloadFormat format,
        out int keyStart,
        out int keyByteCount,
        out int documentStart)
    {
        format = default;
        keyStart = 0;
        keyByteCount = 0;
        documentStart = 0;
        return false;
    }

    private static bool TryReadLegacyHeader(ReadOnlySpan<byte> payload, out Header header)
    {
        header = default;
        if (payload.Length < 2 || payload[0] != LegacyJsonFormatMarker)
            return false;

        int keyByteCount = checked((int)Varint.Read(payload[1..], out int keyLengthBytes));
        int keyStart = 1 + keyLengthBytes;
        int documentStart = checked(keyStart + keyByteCount);
        if (keyByteCount < 0 || documentStart >= payload.Length)
            return false;

        header = new Header(CollectionPayloadFormat.LegacyJson, keyStart, keyByteCount, documentStart);
        return true;
    }

    private static bool TryReadBinaryHeader(ReadOnlySpan<byte> payload, out Header header)
    {
        header = default;
        if (payload.Length < 3 || payload[0] != BinaryFormatMarker || payload[1] != BinaryFormatVersion)
            return false;

        int keyByteCount = checked((int)Varint.Read(payload[2..], out int keyLengthBytes));
        int keyStart = 2 + keyLengthBytes;
        int documentStart = checked(keyStart + keyByteCount);
        if (keyByteCount < 0 || documentStart >= payload.Length)
            return false;

        header = new Header(CollectionPayloadFormat.Binary, keyStart, keyByteCount, documentStart);
        return true;
    }

    private static bool IsKnownBinaryDocumentPayload(ReadOnlySpan<byte> payload)
        => IsGeneratedRecordDocumentPayload(payload) ||
           CollectionBinaryDocumentCodec.IsValidDocument(payload);

    private static bool IsGeneratedRecordDocumentPayload(ReadOnlySpan<byte> payload)
        => payload.Length >= 3 &&
           payload[0] == GeneratedRecordFormatMarker &&
           payload[1] == GeneratedRecordFormatMagic &&
           payload[2] == GeneratedRecordFormatVersion;

    private static bool HasPlausibleJsonPayload(ReadOnlySpan<byte> jsonUtf8)
    {
        for (int i = 0; i < jsonUtf8.Length; i++)
        {
            byte b = jsonUtf8[i];
            if (b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n')
                continue;

            return b == (byte)'{' ||
                   b == (byte)'[' ||
                   b == (byte)'"' ||
                   b == (byte)'-' ||
                   (b >= (byte)'0' && b <= (byte)'9') ||
                   b == (byte)'t' ||
                   b == (byte)'f' ||
                   b == (byte)'n';
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryReadInt32Varint(
        ReadOnlySpan<byte> payload,
        int offset,
        out int value,
        out int bytesRead)
    {
        ulong result = 0;
        int shift = 0;

        for (int i = 0; i < 5; i++)
        {
            int index = offset + i;
            if ((uint)index >= (uint)payload.Length)
                break;

            byte b = payload[index];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                if (result > int.MaxValue)
                    break;

                value = (int)result;
                bytesRead = i + 1;
                return true;
            }

            shift += 7;
        }

        value = 0;
        bytesRead = 0;
        return false;
    }

    private static bool KeyUtf8EqualsString(ReadOnlySpan<byte> keyUtf8, string expectedKey)
    {
        if (keyUtf8.Length == expectedKey.Length)
        {
            bool ascii = true;
            for (int i = 0; i < expectedKey.Length; i++)
            {
                char c = expectedKey[i];
                if (c > 0x7F)
                {
                    ascii = false;
                    break;
                }

                if (keyUtf8[i] != (byte)c)
                    return false;
            }

            if (ascii)
                return true;
        }

        int byteCount = Encoding.UTF8.GetByteCount(expectedKey);
        if (byteCount != keyUtf8.Length)
            return false;

        const int StackallocKeyThreshold = 256;
        byte[]? rented = null;
        Span<byte> expectedKeyUtf8 = byteCount <= StackallocKeyThreshold
            ? stackalloc byte[StackallocKeyThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

        try
        {
            int written = Encoding.UTF8.GetBytes(expectedKey.AsSpan(), expectedKeyUtf8);
            return keyUtf8.SequenceEqual(expectedKeyUtf8[..written]);
        }
        finally
        {
            if (rented is not null)
            {
                expectedKeyUtf8[..byteCount].Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    internal enum CollectionPayloadFormat : byte
    {
        LegacyJson = 1,
        Binary = 2,
    }

    internal readonly record struct Header(
        CollectionPayloadFormat Format,
        int KeyStart,
        int KeyByteCount,
        int DocumentStart);
}

using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Binary payload codec for collection documents stored directly in table B+trees.
/// Supports both the legacy direct JSON wrapper and the newer binary document payload.
/// </summary>
internal static class CollectionPayloadCodec
{
    internal const byte LegacyJsonFormatMarker = 0xC1;
    internal const byte BinaryFormatMarker = 0xC2;
    internal const byte BinaryFormatVersion = 0x01;

    public static bool IsDirectPayload(ReadOnlySpan<byte> payload)
        => TryReadHeader(payload, out _);

    public static bool IsBinaryPayload(ReadOnlySpan<byte> payload)
        => TryReadHeader(payload, out var header) && header.Format == CollectionPayloadFormat.Binary;

    internal static bool TryReadValidatedHeader(ReadOnlySpan<byte> payload, out Header header)
        => TryReadHeader(payload, out header);

    internal static bool TryReadFastHeader(ReadOnlySpan<byte> payload, out Header header)
    {
        header = default;

        try
        {
            if (TryReadLegacyHeader(payload, out header))
                return HasPlausibleJsonPayload(payload[header.DocumentStart..]);

            return TryReadBinaryHeader(payload, out header);
        }
        catch (Exception ex) when (ex is CSharpDbException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            header = default;
            return false;
        }
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
        return header.Format == CollectionPayloadFormat.LegacyJson
            ? Encoding.UTF8.GetString(payload[header.DocumentStart..])
            : CollectionBinaryDocumentCodec.DecodeJson(payload[header.DocumentStart..]);
    }

    public static bool JsonEquals(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> expectedUtf8)
    {
        var header = ReadHeader(payload);
        if (header.Format == CollectionPayloadFormat.LegacyJson)
            return payload[header.DocumentStart..].SequenceEqual(expectedUtf8);

        return CollectionBinaryDocumentCodec.EncodeJsonUtf8(payload[header.DocumentStart..]).AsSpan().SequenceEqual(expectedUtf8);
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
                return CollectionBinaryDocumentCodec.IsValidDocument(payload[header.DocumentStart..]);

            return false;
        }
        catch (Exception ex) when (ex is CSharpDbException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            return false;
        }
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

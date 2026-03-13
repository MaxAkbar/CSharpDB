using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Binary payload codec for collection documents stored directly in table B+trees.
/// </summary>
internal static class CollectionPayloadCodec
{
    internal const byte FormatMarker = 0xC1;

    public static bool IsDirectPayload(ReadOnlySpan<byte> payload)
    {
        if (!TryReadHeader(payload, out var header))
            return false;

        return HasPlausibleJsonPayload(payload[header.JsonStart..]);
    }

    public static byte[] Encode(ReadOnlySpan<byte> keyUtf8, ReadOnlySpan<byte> jsonUtf8)
    {
        int keyLengthSize = Varint.SizeOf((ulong)keyUtf8.Length);
        byte[] payload = GC.AllocateUninitializedArray<byte>(1 + keyLengthSize + keyUtf8.Length + jsonUtf8.Length);
        payload[0] = FormatMarker;

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
        payload[0] = FormatMarker;

        int position = 1;
        position += Varint.Write(payload.AsSpan(position), (ulong)keyByteCount);
        position += Encoding.UTF8.GetBytes(key.AsSpan(), payload.AsSpan(position, keyByteCount));
        jsonUtf8.CopyTo(payload.AsSpan(position));
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
        return Encoding.UTF8.GetString(payload[header.JsonStart..]);
    }

    public static ReadOnlySpan<byte> GetJsonUtf8(ReadOnlySpan<byte> payload)
    {
        var header = ReadHeader(payload);
        return payload[header.JsonStart..];
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
            if (payload.Length < 2 || payload[0] != FormatMarker)
                return false;

            int keyByteCount = checked((int)Varint.Read(payload[1..], out int keyLengthBytes));
            int keyStart = 1 + keyLengthBytes;
            int jsonStart = checked(keyStart + keyByteCount);

            if (keyByteCount < 0 || jsonStart >= payload.Length)
                return false;

            header = new Header(keyStart, keyByteCount, jsonStart);
            return true;
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            return false;
        }
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

    private readonly record struct Header(int KeyStart, int KeyByteCount, int JsonStart);
}

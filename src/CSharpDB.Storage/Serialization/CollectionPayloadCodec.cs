using System.Text;
using CSharpDB.Core;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Binary payload codec for collection documents stored directly in table B+trees.
/// </summary>
internal static class CollectionPayloadCodec
{
    internal const byte FormatMarker = 0xC1;

    public static bool IsDirectPayload(ReadOnlySpan<byte> payload)
        => payload.Length != 0 && payload[0] == FormatMarker;

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
        try
        {
            if (payload.Length < 2 || payload[0] != FormatMarker)
                throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid collection payload header.");

            int keyByteCount = checked((int)Varint.Read(payload[1..], out int keyLengthBytes));
            int keyStart = 1 + keyLengthBytes;
            int jsonStart = checked(keyStart + keyByteCount);

            if (keyByteCount < 0 || jsonStart > payload.Length)
                throw new CSharpDbException(ErrorCode.CorruptDatabase, "Collection payload length exceeds buffer size.");

            return new Header(keyStart, keyByteCount, jsonStart);
        }
        catch (CSharpDbException)
        {
            throw;
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Malformed collection payload.", ex);
        }
    }

    private readonly record struct Header(int KeyStart, int KeyByteCount, int JsonStart);
}

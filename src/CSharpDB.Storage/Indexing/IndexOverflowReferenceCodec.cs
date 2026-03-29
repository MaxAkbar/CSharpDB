using System.Buffers.Binary;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Storage.Indexing;

internal static class IndexOverflowReferenceCodec
{
    private static ReadOnlySpan<byte> MagicBytes => "CSDBOVF1"u8;

    internal const int EncodedLength = 16;

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length == EncodedLength &&
           payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] Encode(uint firstPageId, int payloadLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(payloadLength);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId), "Overflow payload references must point at a real page.");

        byte[] encoded = GC.AllocateUninitializedArray<byte>(EncodedLength);
        MagicBytes.CopyTo(encoded);
        BinaryPrimitives.WriteUInt32LittleEndian(encoded.AsSpan(MagicBytes.Length, sizeof(uint)), firstPageId);
        BinaryPrimitives.WriteInt32LittleEndian(encoded.AsSpan(MagicBytes.Length + sizeof(uint), sizeof(int)), payloadLength);
        return encoded;
    }

    public static uint ReadFirstPageId(ReadOnlySpan<byte> payload)
    {
        ValidateEncoded(payload);
        return BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(MagicBytes.Length, sizeof(uint)));
    }

    public static int ReadPayloadLength(ReadOnlySpan<byte> payload)
    {
        ValidateEncoded(payload);
        return BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(MagicBytes.Length + sizeof(uint), sizeof(int)));
    }

    private static void ValidateEncoded(ReadOnlySpan<byte> payload)
    {
        if (!IsEncoded(payload))
            throw new InvalidOperationException("Payload is not an index overflow reference.");
    }
}

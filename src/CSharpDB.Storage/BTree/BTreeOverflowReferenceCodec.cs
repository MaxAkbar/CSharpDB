using System.Buffers.Binary;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.BTrees;

/// <summary>
/// Encodes an external B+tree payload. This marker is intentionally distinct
/// from index overflow references, whose lifecycle is owned by an index adapter.
/// </summary>
internal static class BTreeOverflowReferenceCodec
{
    private static ReadOnlySpan<byte> MagicBytes => "CSDBBOV1"u8;

    internal const int EncodedLength = 16;

    internal static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length == EncodedLength &&
           payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    internal static byte[] Encode(OverflowPageReference reference)
    {
        if (reference.FirstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(reference), "Overflow references must point at a real page.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(reference.PayloadLength);

        byte[] encoded = GC.AllocateUninitializedArray<byte>(EncodedLength);
        MagicBytes.CopyTo(encoded);
        BinaryPrimitives.WriteUInt32LittleEndian(
            encoded.AsSpan(MagicBytes.Length, sizeof(uint)),
            reference.FirstPageId);
        BinaryPrimitives.WriteInt32LittleEndian(
            encoded.AsSpan(MagicBytes.Length + sizeof(uint), sizeof(int)),
            reference.PayloadLength);
        return encoded;
    }

    internal static OverflowPageReference Decode(ReadOnlySpan<byte> payload)
    {
        if (!IsEncoded(payload))
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Payload is not a B+tree overflow reference.");
        }

        uint firstPageId = BinaryPrimitives.ReadUInt32LittleEndian(
            payload.Slice(MagicBytes.Length, sizeof(uint)));
        int payloadLength = BinaryPrimitives.ReadInt32LittleEndian(
            payload.Slice(MagicBytes.Length + sizeof(uint), sizeof(int)));
        if (firstPageId == PageConstants.NullPageId || payloadLength <= 0)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "B+tree overflow reference does not point to a valid payload.");
        }

        return new OverflowPageReference(firstPageId, payloadLength);
    }
}

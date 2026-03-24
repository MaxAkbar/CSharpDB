using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextMetaPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'M', (byte)'1'];

    public static byte[] Encode(long documentCount, long totalTokenCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(documentCount);
        ArgumentOutOfRangeException.ThrowIfNegative(totalTokenCount);

        int size = MagicBytes.Length + Varint.SizeOf(documentCount) + Varint.SizeOf(totalTokenCount);
        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);

        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), documentCount);
        _ = Varint.Write(payload.AsSpan(offset), totalTokenCount);
        return payload;
    }

    public static bool TryDecode(ReadOnlySpan<byte> payload, out long documentCount, out long totalTokenCount)
    {
        documentCount = 0;
        totalTokenCount = 0;

        if (payload.Length < MagicBytes.Length || !payload[..MagicBytes.Length].SequenceEqual(MagicBytes))
            return false;

        int offset = MagicBytes.Length;
        documentCount = Varint.ReadSigned(payload[offset..], out int docCountBytes);
        offset += docCountBytes;
        totalTokenCount = Varint.ReadSigned(payload[offset..], out int tokenCountBytes);
        offset += tokenCountBytes;

        return offset == payload.Length && documentCount >= 0 && totalTokenCount >= 0;
    }
}

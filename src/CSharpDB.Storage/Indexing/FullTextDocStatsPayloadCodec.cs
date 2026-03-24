using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextDocStatsPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'D', (byte)'1'];

    public static byte[] Encode(int docLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(docLength);

        int size = MagicBytes.Length + Varint.SizeOf(docLength);
        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        _ = Varint.Write(payload.AsSpan(MagicBytes.Length), docLength);
        return payload;
    }

    public static bool TryDecode(ReadOnlySpan<byte> payload, out int docLength)
    {
        docLength = 0;
        if (payload.Length < MagicBytes.Length || !payload[..MagicBytes.Length].SequenceEqual(MagicBytes))
            return false;

        int value = checked((int)Varint.Read(payload[MagicBytes.Length..], out int bytesRead));
        docLength = value;
        return MagicBytes.Length + bytesRead == payload.Length && docLength >= 0;
    }
}

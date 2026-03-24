using System.Text;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal readonly record struct FullTextTermStat(string Term, int DocumentFrequency);

internal static class FullTextTermStatsPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'T', (byte)'1'];

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= MagicBytes.Length && payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] CreateSingle(string term, int documentFrequency)
        => EncodeEntries([new FullTextTermStat(term, documentFrequency)]);

    public static byte[] CreateFromSorted(IReadOnlyDictionary<string, int> documentFrequencyByTerm)
    {
        ArgumentNullException.ThrowIfNull(documentFrequencyByTerm);

        var entries = new List<FullTextTermStat>(documentFrequencyByTerm.Count);
        foreach ((string term, int df) in documentFrequencyByTerm)
            entries.Add(new FullTextTermStat(term, df));

        return EncodeEntries(entries);
    }

    public static bool TryGetDocumentFrequency(ReadOnlySpan<byte> payload, string term, out int documentFrequency)
    {
        documentFrequency = 0;
        if (!TryDecodeEntries(payload, out var entries))
            return false;

        int index = FindEntryIndex(entries, term);
        if (index < 0)
            return false;

        documentFrequency = entries[index].DocumentFrequency;
        return true;
    }

    public static byte[]? Adjust(ReadOnlySpan<byte> payload, string term, int delta, out bool changed)
    {
        if (delta == 0)
        {
            changed = false;
            return payload.ToArray();
        }

        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text term-stats format.");

        int insertIndex = FindInsertIndex(entries, term, out bool exists);
        if (!exists)
        {
            if (delta < 0)
                throw new InvalidOperationException("Cannot decrement a missing term-stat entry.");

            entries.Insert(insertIndex, new FullTextTermStat(term, delta));
            changed = true;
            return EncodeEntries(entries);
        }

        int updated = checked(entries[insertIndex].DocumentFrequency + delta);
        if (updated < 0)
            throw new InvalidOperationException("Document frequency cannot become negative.");

        if (updated == 0)
            entries.RemoveAt(insertIndex);
        else
            entries[insertIndex] = new FullTextTermStat(term, updated);

        changed = true;
        return entries.Count == 0 ? null : EncodeEntries(entries);
    }

    private static bool TryDecodeEntries(ReadOnlySpan<byte> payload, out List<FullTextTermStat> entries)
    {
        entries = [];
        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        int count = checked((int)Varint.Read(payload[offset..], out int countBytes));
        offset += countBytes;
        if (count < 0)
            return false;

        entries = new List<FullTextTermStat>(count);
        for (int i = 0; i < count; i++)
        {
            int encodedLength = checked((int)Varint.Read(payload[offset..], out int termLengthBytes));
            offset += termLengthBytes;
            if (encodedLength <= 0)
                return false;

            int termLength = encodedLength - 1;
            if (offset + termLength > payload.Length)
                return false;

            string term = Encoding.UTF8.GetString(payload.Slice(offset, termLength));
            offset += termLength;

            int documentFrequency = checked((int)Varint.Read(payload[offset..], out int dfBytes));
            offset += dfBytes;
            if (documentFrequency <= 0)
                return false;

            entries.Add(new FullTextTermStat(term, documentFrequency));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeEntries(IReadOnlyList<FullTextTermStat> entries)
    {
        int size = MagicBytes.Length + Varint.SizeOf(entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            int termByteLength = Encoding.UTF8.GetByteCount(entries[i].Term);
            size += Varint.SizeOf(termByteLength + 1) + termByteLength + Varint.SizeOf(entries[i].DocumentFrequency);
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), entries.Count);

        for (int i = 0; i < entries.Count; i++)
        {
            FullTextTermStat entry = entries[i];
            int termByteLength = Encoding.UTF8.GetByteCount(entry.Term);
            offset += Varint.Write(payload.AsSpan(offset), termByteLength + 1);
            offset += Encoding.UTF8.GetBytes(entry.Term, payload.AsSpan(offset, termByteLength));
            offset += Varint.Write(payload.AsSpan(offset), entry.DocumentFrequency);
        }

        return payload;
    }

    private static int FindInsertIndex(IReadOnlyList<FullTextTermStat> entries, string term, out bool exists)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = string.Compare(entries[mid].Term, term, StringComparison.Ordinal);
            if (comparison == 0)
            {
                exists = true;
                return mid;
            }

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        exists = false;
        return low;
    }

    private static int FindEntryIndex(IReadOnlyList<FullTextTermStat> entries, string term)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = string.Compare(entries[mid].Term, term, StringComparison.Ordinal);
            if (comparison == 0)
                return mid;

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return -1;
    }
}

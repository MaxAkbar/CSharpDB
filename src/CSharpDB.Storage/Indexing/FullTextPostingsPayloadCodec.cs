using System.Text;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal readonly record struct FullTextPosting(long DocId, int[] Positions)
{
    public int TermFrequency => Positions.Length;
}

internal static class FullTextPostingsListCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'P', (byte)'1'];

    public static byte[] EncodeSorted(IReadOnlyList<FullTextPosting> postings)
    {
        ArgumentNullException.ThrowIfNull(postings);

        int size = MagicBytes.Length + Varint.SizeOf(postings.Count);
        long previousDocId = 0;
        for (int i = 0; i < postings.Count; i++)
        {
            FullTextPosting posting = postings[i];
            long docDelta = posting.DocId - previousDocId;
            previousDocId = posting.DocId;

            size += Varint.SizeOf(docDelta);
            size += Varint.SizeOf(posting.TermFrequency);

            int previousPosition = 0;
            for (int j = 0; j < posting.Positions.Length; j++)
            {
                int delta = posting.Positions[j] - previousPosition;
                previousPosition = posting.Positions[j];
                size += Varint.SizeOf(delta);
            }
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), postings.Count);

        previousDocId = 0;
        for (int i = 0; i < postings.Count; i++)
        {
            FullTextPosting posting = postings[i];
            offset += Varint.Write(payload.AsSpan(offset), posting.DocId - previousDocId);
            previousDocId = posting.DocId;

            offset += Varint.Write(payload.AsSpan(offset), posting.TermFrequency);
            int previousPosition = 0;
            for (int j = 0; j < posting.Positions.Length; j++)
            {
                offset += Varint.Write(payload.AsSpan(offset), posting.Positions[j] - previousPosition);
                previousPosition = posting.Positions[j];
            }
        }

        return payload;
    }

    public static bool TryDecode(ReadOnlySpan<byte> payload, out List<FullTextPosting> postings)
    {
        postings = [];
        if (payload.Length < MagicBytes.Length || !payload[..MagicBytes.Length].SequenceEqual(MagicBytes))
            return false;

        int offset = MagicBytes.Length;
        int count = checked((int)Varint.Read(payload[offset..], out int countBytes));
        offset += countBytes;
        if (count < 0)
            return false;

        postings = new List<FullTextPosting>(count);
        long docId = 0;
        for (int i = 0; i < count; i++)
        {
            docId += Varint.ReadSigned(payload[offset..], out int docDeltaBytes);
            offset += docDeltaBytes;

            int termFrequency = checked((int)Varint.Read(payload[offset..], out int tfBytes));
            offset += tfBytes;
            if (termFrequency <= 0)
                return false;

            var positions = new int[termFrequency];
            int position = 0;
            for (int j = 0; j < termFrequency; j++)
            {
                position += checked((int)Varint.ReadSigned(payload[offset..], out int positionBytes));
                offset += positionBytes;
                positions[j] = position;
            }

            postings.Add(new FullTextPosting(docId, positions));
        }

        return offset == payload.Length;
    }
}

internal readonly record struct FullTextPostingsBucket(string Term, byte[] PostingsPayload);

internal static class FullTextPostingsPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'B', (byte)'1'];

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= MagicBytes.Length && payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] CreateSingle(string term, IReadOnlyList<FullTextPosting> postings)
        => EncodeEntries([new FullTextPostingsBucket(term, FullTextPostingsListCodec.EncodeSorted(postings))]);

    public static byte[] CreateFromSorted(IReadOnlyDictionary<string, List<FullTextPosting>> postingsByTerm)
    {
        ArgumentNullException.ThrowIfNull(postingsByTerm);

        var entries = new List<FullTextPostingsBucket>(postingsByTerm.Count);
        foreach ((string term, List<FullTextPosting> postings) in postingsByTerm)
            entries.Add(new FullTextPostingsBucket(term, FullTextPostingsListCodec.EncodeSorted(postings)));

        return EncodeEntries(entries);
    }

    public static bool TryGetMatchingPostings(ReadOnlySpan<byte> payload, string term, out byte[] postingsPayload)
    {
        postingsPayload = [];
        if (!TryDecodeEntries(payload, out var entries))
            return false;

        int index = FindEntryIndex(entries, term);
        if (index < 0)
            return false;

        postingsPayload = entries[index].PostingsPayload;
        return true;
    }

    public static bool TryDecodeBuckets(ReadOnlySpan<byte> payload, out List<FullTextPostingsBucket> entries)
        => TryDecodeEntries(payload, out entries);

    public static byte[] Insert(ReadOnlySpan<byte> payload, string term, long docId, IReadOnlyList<int> positions, out bool changed)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text postings bucket format.");

        int insertIndex = FindInsertIndex(entries, term, out bool exists);
        if (exists)
        {
            if (!FullTextPostingsListCodec.TryDecode(entries[insertIndex].PostingsPayload, out var postings))
                throw new InvalidOperationException("Term postings payload is invalid.");

            int postingIndex = FindPostingIndex(postings, docId);
            if (postingIndex >= 0)
            {
                postings[postingIndex] = new FullTextPosting(docId, positions.ToArray());
            }
            else
            {
                postings.Insert(~postingIndex, new FullTextPosting(docId, positions.ToArray()));
            }

            entries[insertIndex] = new FullTextPostingsBucket(term, FullTextPostingsListCodec.EncodeSorted(postings));
            changed = true;
            return EncodeEntries(entries);
        }

        entries.Insert(insertIndex, new FullTextPostingsBucket(term, FullTextPostingsListCodec.EncodeSorted(
        [
            new FullTextPosting(docId, positions.ToArray()),
        ])));
        changed = true;
        return EncodeEntries(entries);
    }

    public static byte[]? Remove(ReadOnlySpan<byte> payload, string term, long docId, out bool changed)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text postings bucket format.");

        int entryIndex = FindEntryIndex(entries, term);
        if (entryIndex < 0)
        {
            changed = false;
            return null;
        }

        if (!FullTextPostingsListCodec.TryDecode(entries[entryIndex].PostingsPayload, out var postings))
            throw new InvalidOperationException("Term postings payload is invalid.");

        int postingIndex = FindPostingIndex(postings, docId);
        if (postingIndex < 0)
        {
            changed = false;
            return null;
        }

        postings.RemoveAt(postingIndex);
        if (postings.Count == 0)
            entries.RemoveAt(entryIndex);
        else
            entries[entryIndex] = new FullTextPostingsBucket(term, FullTextPostingsListCodec.EncodeSorted(postings));

        changed = true;
        return entries.Count == 0 ? null : EncodeEntries(entries);
    }

    private static bool TryDecodeEntries(ReadOnlySpan<byte> payload, out List<FullTextPostingsBucket> entries)
    {
        entries = [];
        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        int count = checked((int)Varint.Read(payload[offset..], out int countBytes));
        offset += countBytes;
        if (count < 0)
            return false;

        entries = new List<FullTextPostingsBucket>(count);
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

            int postingsLength = checked((int)Varint.Read(payload[offset..], out int postingsLengthBytes));
            offset += postingsLengthBytes;
            if (postingsLength <= 0 || offset + postingsLength > payload.Length)
                return false;

            byte[] postingsPayload = payload.Slice(offset, postingsLength).ToArray();
            offset += postingsLength;

            entries.Add(new FullTextPostingsBucket(term, postingsPayload));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeEntries(IReadOnlyList<FullTextPostingsBucket> entries)
    {
        int size = MagicBytes.Length + Varint.SizeOf(entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            int termByteLength = Encoding.UTF8.GetByteCount(entries[i].Term);
            size += Varint.SizeOf(termByteLength + 1) + termByteLength;
            size += Varint.SizeOf(entries[i].PostingsPayload.Length) + entries[i].PostingsPayload.Length;
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), entries.Count);

        for (int i = 0; i < entries.Count; i++)
        {
            FullTextPostingsBucket entry = entries[i];
            int termByteLength = Encoding.UTF8.GetByteCount(entry.Term);
            offset += Varint.Write(payload.AsSpan(offset), termByteLength + 1);
            offset += Encoding.UTF8.GetBytes(entry.Term, payload.AsSpan(offset, termByteLength));
            offset += Varint.Write(payload.AsSpan(offset), entry.PostingsPayload.Length);
            entry.PostingsPayload.CopyTo(payload.AsSpan(offset));
            offset += entry.PostingsPayload.Length;
        }

        return payload;
    }

    private static int FindInsertIndex(IReadOnlyList<FullTextPostingsBucket> entries, string term, out bool exists)
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

    private static int FindEntryIndex(IReadOnlyList<FullTextPostingsBucket> entries, string term)
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

    private static int FindPostingIndex(IReadOnlyList<FullTextPosting> postings, long docId)
    {
        int low = 0;
        int high = postings.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            long current = postings[mid].DocId;
            if (current == docId)
                return mid;

            if (current < docId)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return ~low;
    }
}

internal readonly record struct FullTextPostingChunkDescriptor(long FirstDocId, long LastDocId, int PostingCount);

internal static class FullTextPostingChunkKeyCodec
{
    public static long ComputeKey(string term, long firstDocId)
    {
        ArgumentNullException.ThrowIfNull(term);

        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        foreach (byte b in Encoding.UTF8.GetBytes(term))
        {
            hash ^= b;
            hash *= prime;
        }

        unchecked
        {
            ulong docBits = (ulong)firstDocId;
            for (int i = 0; i < sizeof(long); i++)
            {
                hash ^= (byte)(docBits >> (i * 8));
                hash *= prime;
            }
        }

        return unchecked((long)hash);
    }
}

internal readonly record struct FullTextPostingChunkManifestBucket(
    string Term,
    FullTextPostingChunkDescriptor[] Chunks);

internal static class FullTextPostingChunkManifestCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'M', (byte)'1'];

    public static bool IsEncoded(ReadOnlySpan<byte> payload)
        => payload.Length >= MagicBytes.Length && payload[..MagicBytes.Length].SequenceEqual(MagicBytes);

    public static byte[] CreateSingle(string term, IReadOnlyList<FullTextPostingChunkDescriptor> chunks)
        => EncodeEntries([new FullTextPostingChunkManifestBucket(term, chunks.ToArray())]);

    public static bool TryGetChunks(ReadOnlySpan<byte> payload, string term, out FullTextPostingChunkDescriptor[] chunks)
    {
        chunks = [];
        if (!TryDecodeEntries(payload, out var entries))
            return false;

        int index = FindEntryIndex(entries, term);
        if (index < 0)
            return false;

        chunks = entries[index].Chunks;
        return true;
    }

    public static byte[]? Upsert(
        ReadOnlySpan<byte> payload,
        string term,
        IReadOnlyList<FullTextPostingChunkDescriptor> chunks)
    {
        if (chunks.Count == 0)
            return Remove(payload, term);

        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text posting chunk manifest format.");

        int index = FindInsertIndex(entries, term, out bool exists);
        var bucket = new FullTextPostingChunkManifestBucket(term, chunks.ToArray());
        if (exists)
            entries[index] = bucket;
        else
            entries.Insert(index, bucket);

        return EncodeEntries(entries);
    }

    public static byte[]? Remove(ReadOnlySpan<byte> payload, string term)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text posting chunk manifest format.");

        int index = FindEntryIndex(entries, term);
        if (index < 0)
            return payload.ToArray();

        entries.RemoveAt(index);
        return entries.Count == 0 ? null : EncodeEntries(entries);
    }

    private static bool TryDecodeEntries(ReadOnlySpan<byte> payload, out List<FullTextPostingChunkManifestBucket> entries)
    {
        entries = [];
        if (!IsEncoded(payload))
            return false;

        int offset = MagicBytes.Length;
        int count = checked((int)Varint.Read(payload[offset..], out int countBytes));
        offset += countBytes;
        if (count < 0)
            return false;

        entries = new List<FullTextPostingChunkManifestBucket>(count);
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

            int chunkCount = checked((int)Varint.Read(payload[offset..], out int chunkCountBytes));
            offset += chunkCountBytes;
            if (chunkCount < 0)
                return false;

            var chunks = new FullTextPostingChunkDescriptor[chunkCount];
            for (int j = 0; j < chunkCount; j++)
            {
                long firstDocId = checked((long)Varint.Read(payload[offset..], out int firstBytes));
                offset += firstBytes;
                long lastDocId = checked((long)Varint.Read(payload[offset..], out int lastBytes));
                offset += lastBytes;
                int postingCount = checked((int)Varint.Read(payload[offset..], out int postingCountBytes));
                offset += postingCountBytes;
                if (postingCount <= 0 || lastDocId < firstDocId)
                    return false;

                chunks[j] = new FullTextPostingChunkDescriptor(firstDocId, lastDocId, postingCount);
            }

            entries.Add(new FullTextPostingChunkManifestBucket(term, chunks));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeEntries(IReadOnlyList<FullTextPostingChunkManifestBucket> entries)
    {
        int size = MagicBytes.Length + Varint.SizeOf(entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            int termByteLength = Encoding.UTF8.GetByteCount(entries[i].Term);
            size += Varint.SizeOf(termByteLength + 1) + termByteLength;
            size += Varint.SizeOf(entries[i].Chunks.Length);
            for (int j = 0; j < entries[i].Chunks.Length; j++)
            {
                FullTextPostingChunkDescriptor chunk = entries[i].Chunks[j];
                size += Varint.SizeOf(chunk.FirstDocId);
                size += Varint.SizeOf(chunk.LastDocId);
                size += Varint.SizeOf(chunk.PostingCount);
            }
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), entries.Count);

        for (int i = 0; i < entries.Count; i++)
        {
            FullTextPostingChunkManifestBucket entry = entries[i];
            int termByteLength = Encoding.UTF8.GetByteCount(entry.Term);
            offset += Varint.Write(payload.AsSpan(offset), termByteLength + 1);
            offset += Encoding.UTF8.GetBytes(entry.Term, payload.AsSpan(offset, termByteLength));
            offset += Varint.Write(payload.AsSpan(offset), entry.Chunks.Length);
            for (int j = 0; j < entry.Chunks.Length; j++)
            {
                FullTextPostingChunkDescriptor chunk = entry.Chunks[j];
                offset += Varint.Write(payload.AsSpan(offset), chunk.FirstDocId);
                offset += Varint.Write(payload.AsSpan(offset), chunk.LastDocId);
                offset += Varint.Write(payload.AsSpan(offset), chunk.PostingCount);
            }
        }

        return payload;
    }

    private static int FindInsertIndex(
        IReadOnlyList<FullTextPostingChunkManifestBucket> entries,
        string term,
        out bool exists)
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

    private static int FindEntryIndex(IReadOnlyList<FullTextPostingChunkManifestBucket> entries, string term)
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

internal readonly record struct FullTextPostingChunkBucket(string Term, long FirstDocId, byte[] PostingsPayload);

internal static class FullTextPostingChunkPayloadCodec
{
    private static readonly byte[] MagicBytes = [(byte)'F', (byte)'T', (byte)'C', (byte)'1'];

    public static byte[] CreateSingle(string term, long firstDocId, IReadOnlyList<FullTextPosting> postings)
        => EncodeEntries([new FullTextPostingChunkBucket(term, firstDocId, FullTextPostingsListCodec.EncodeSorted(postings))]);

    public static bool TryGetPostings(
        ReadOnlySpan<byte> payload,
        string term,
        long firstDocId,
        out byte[] postingsPayload)
    {
        postingsPayload = [];
        if (!TryDecodeEntries(payload, out var entries))
            return false;

        int index = FindEntryIndex(entries, term, firstDocId);
        if (index < 0)
            return false;

        postingsPayload = entries[index].PostingsPayload;
        return true;
    }

    public static byte[] Upsert(
        ReadOnlySpan<byte> payload,
        string term,
        long firstDocId,
        IReadOnlyList<FullTextPosting> postings)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text posting chunk format.");

        int index = FindInsertIndex(entries, term, firstDocId, out bool exists);
        var bucket = new FullTextPostingChunkBucket(term, firstDocId, FullTextPostingsListCodec.EncodeSorted(postings));
        if (exists)
            entries[index] = bucket;
        else
            entries.Insert(index, bucket);

        return EncodeEntries(entries);
    }

    public static byte[]? Remove(ReadOnlySpan<byte> payload, string term, long firstDocId)
    {
        if (!TryDecodeEntries(payload, out var entries))
            throw new InvalidOperationException("Payload is not in full-text posting chunk format.");

        int index = FindEntryIndex(entries, term, firstDocId);
        if (index < 0)
            return payload.ToArray();

        entries.RemoveAt(index);
        return entries.Count == 0 ? null : EncodeEntries(entries);
    }

    private static bool TryDecodeEntries(ReadOnlySpan<byte> payload, out List<FullTextPostingChunkBucket> entries)
    {
        entries = [];
        if (payload.Length < MagicBytes.Length || !payload[..MagicBytes.Length].SequenceEqual(MagicBytes))
            return false;

        int offset = MagicBytes.Length;
        int count = checked((int)Varint.Read(payload[offset..], out int countBytes));
        offset += countBytes;
        if (count < 0)
            return false;

        entries = new List<FullTextPostingChunkBucket>(count);
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

            long firstDocId = checked((long)Varint.Read(payload[offset..], out int firstDocIdBytes));
            offset += firstDocIdBytes;

            int postingsLength = checked((int)Varint.Read(payload[offset..], out int postingsLengthBytes));
            offset += postingsLengthBytes;
            if (postingsLength <= 0 || offset + postingsLength > payload.Length)
                return false;

            byte[] postingsPayload = payload.Slice(offset, postingsLength).ToArray();
            offset += postingsLength;

            entries.Add(new FullTextPostingChunkBucket(term, firstDocId, postingsPayload));
        }

        return offset == payload.Length;
    }

    private static byte[] EncodeEntries(IReadOnlyList<FullTextPostingChunkBucket> entries)
    {
        int size = MagicBytes.Length + Varint.SizeOf(entries.Count);
        for (int i = 0; i < entries.Count; i++)
        {
            int termByteLength = Encoding.UTF8.GetByteCount(entries[i].Term);
            size += Varint.SizeOf(termByteLength + 1) + termByteLength;
            size += Varint.SizeOf(entries[i].FirstDocId);
            size += Varint.SizeOf(entries[i].PostingsPayload.Length) + entries[i].PostingsPayload.Length;
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(size);
        MagicBytes.CopyTo(payload, 0);
        int offset = MagicBytes.Length;
        offset += Varint.Write(payload.AsSpan(offset), entries.Count);

        for (int i = 0; i < entries.Count; i++)
        {
            FullTextPostingChunkBucket entry = entries[i];
            int termByteLength = Encoding.UTF8.GetByteCount(entry.Term);
            offset += Varint.Write(payload.AsSpan(offset), termByteLength + 1);
            offset += Encoding.UTF8.GetBytes(entry.Term, payload.AsSpan(offset, termByteLength));
            offset += Varint.Write(payload.AsSpan(offset), entry.FirstDocId);
            offset += Varint.Write(payload.AsSpan(offset), entry.PostingsPayload.Length);
            entry.PostingsPayload.CopyTo(payload.AsSpan(offset));
            offset += entry.PostingsPayload.Length;
        }

        return payload;
    }

    private static int FindInsertIndex(
        IReadOnlyList<FullTextPostingChunkBucket> entries,
        string term,
        long firstDocId,
        out bool exists)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = Compare(entries[mid], term, firstDocId);
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

    private static int FindEntryIndex(
        IReadOnlyList<FullTextPostingChunkBucket> entries,
        string term,
        long firstDocId)
    {
        int low = 0;
        int high = entries.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            int comparison = Compare(entries[mid], term, firstDocId);
            if (comparison == 0)
                return mid;

            if (comparison < 0)
                low = mid + 1;
            else
                high = mid - 1;
        }

        return -1;
    }

    private static int Compare(FullTextPostingChunkBucket entry, string term, long firstDocId)
    {
        int comparison = string.Compare(entry.Term, term, StringComparison.Ordinal);
        return comparison != 0 ? comparison : entry.FirstDocId.CompareTo(firstDocId);
    }
}

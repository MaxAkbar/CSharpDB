using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Indexing;

namespace CSharpDB.Engine;

internal static class FullTextIndexReader
{
    public static async ValueTask<IReadOnlyList<FullTextSearchHit>> SearchAsync(
        SchemaCatalog catalog,
        IndexSchema logicalIndex,
        string query,
        CancellationToken ct = default)
    {
        var tokenizer = new FullTextTokenizer(FullTextIndexOptionsCodec.Deserialize(logicalIndex.OptionsJson));
        string[] terms = tokenizer.Tokenize(query)
            .Select(static token => token.Text)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        if (terms.Length == 0)
            return Array.Empty<FullTextSearchHit>();

        IIndexStore postingsStore = catalog.GetIndexStore(FullTextIndexNaming.GetPostingsIndexName(logicalIndex.IndexName));
        IIndexStore? postingChunksStore = TryGetPostingChunksStore(catalog, logicalIndex);
        var postingsByTerm = new List<List<FullTextPosting>>(terms.Length);
        for (int i = 0; i < terms.Length; i++)
        {
            long key = FullTextTermKeyCodec.ComputeKey(terms[i]);
            byte[]? bucketPayload = await postingsStore.FindAsync(key, ct);
            if (bucketPayload == null)
            {
                return Array.Empty<FullTextSearchHit>();
            }

            if (postingChunksStore != null &&
                FullTextPostingChunkManifestCodec.IsEncoded(bucketPayload))
            {
                var chunkedPostings = await TryReadChunkedPostingsAsync(postingChunksStore, bucketPayload, terms[i], ct);
                if (chunkedPostings == null)
                    return Array.Empty<FullTextSearchHit>();

                postingsByTerm.Add(chunkedPostings);
                continue;
            }

            if (!FullTextPostingsPayloadCodec.TryGetMatchingPostings(bucketPayload, terms[i], out byte[] postingsPayload) ||
                !FullTextPostingsListCodec.TryDecode(postingsPayload, out var postings))
            {
                return Array.Empty<FullTextSearchHit>();
            }

            postingsByTerm.Add(postings);
        }

        postingsByTerm.Sort(static (left, right) => left.Count.CompareTo(right.Count));
        var candidates = postingsByTerm[0]
            .Select(static posting => posting.DocId)
            .ToHashSet();

        for (int i = 1; i < postingsByTerm.Count && candidates.Count > 0; i++)
        {
            var current = postingsByTerm[i]
                .Select(static posting => posting.DocId)
                .ToHashSet();
            candidates.IntersectWith(current);
        }

        if (candidates.Count == 0)
            return Array.Empty<FullTextSearchHit>();

        long[] ordered = candidates.ToArray();
        Array.Sort(ordered);
        var hits = new FullTextSearchHit[ordered.Length];
        for (int i = 0; i < ordered.Length; i++)
            hits[i] = new FullTextSearchHit(ordered[i], terms.Length);

        return hits;
    }

    private static IIndexStore? TryGetPostingChunksStore(SchemaCatalog catalog, IndexSchema logicalIndex)
    {
        string chunkIndexName = FullTextIndexNaming.GetPostingChunksIndexName(logicalIndex.IndexName);
        return catalog.GetIndex(chunkIndexName) == null
            ? null
            : catalog.GetIndexStore(chunkIndexName);
    }

    private static async ValueTask<List<FullTextPosting>?> TryReadChunkedPostingsAsync(
        IIndexStore postingChunksStore,
        byte[] manifestPayload,
        string term,
        CancellationToken ct)
    {
        if (!FullTextPostingChunkManifestCodec.TryGetChunks(manifestPayload, term, out var chunks))
            return null;

        int totalCount = 0;
        for (int i = 0; i < chunks.Length; i++)
            totalCount += chunks[i].PostingCount;

        var postings = new List<FullTextPosting>(totalCount);
        for (int i = 0; i < chunks.Length; i++)
        {
            FullTextPostingChunkDescriptor chunk = chunks[i];
            long chunkKey = FullTextPostingChunkKeyCodec.ComputeKey(term, chunk.FirstDocId);
            byte[]? chunkPayload = await postingChunksStore.FindAsync(chunkKey, ct);
            if (chunkPayload == null ||
                !FullTextPostingChunkPayloadCodec.TryGetPostings(chunkPayload, term, chunk.FirstDocId, out byte[] postingsPayload) ||
                !FullTextPostingsListCodec.TryDecode(postingsPayload, out var chunkPostings))
            {
                return null;
            }

            postings.AddRange(chunkPostings);
        }

        return postings;
    }
}

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
        var postingsByTerm = new List<List<FullTextPosting>>(terms.Length);
        for (int i = 0; i < terms.Length; i++)
        {
            long key = FullTextTermKeyCodec.ComputeKey(terms[i]);
            byte[]? bucketPayload = await postingsStore.FindAsync(key, ct);
            if (bucketPayload == null ||
                !FullTextPostingsPayloadCodec.TryGetMatchingPostings(bucketPayload, terms[i], out byte[] postingsPayload) ||
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
}

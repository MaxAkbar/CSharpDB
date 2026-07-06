using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextIndexMaintenance
{
    private const long MetaKey = 0;
    internal const int MaxPostingsPerChunk = 1024;

    public static bool TryResolveColumnIndices(
        IndexSchema indexSchema,
        TableSchema tableSchema,
        out int[] columnIndices)
    {
        columnIndices = Array.Empty<int>();
        if (indexSchema.Kind != IndexKind.FullText || indexSchema.Columns.Count == 0)
            return false;

        columnIndices = new int[indexSchema.Columns.Count];
        for (int i = 0; i < indexSchema.Columns.Count; i++)
        {
            int columnIndex = tableSchema.GetColumnIndex(indexSchema.Columns[i]);
            if (columnIndex < 0 || columnIndex >= tableSchema.Columns.Count)
                return false;
            if (tableSchema.Columns[columnIndex].Type != DbType.Text)
                return false;

            columnIndices[i] = columnIndex;
        }

        return true;
    }

    public static async ValueTask BackfillAsync(
        SchemaCatalog catalog,
        TableSchema tableSchema,
        IndexSchema logicalIndex,
        IRecordSerializer serializer,
        CancellationToken ct = default)
    {
        if (!TryResolveColumnIndices(logicalIndex, tableSchema, out int[] columnIndices))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Full-text index '{logicalIndex.IndexName}' references unsupported columns for table '{logicalIndex.TableName}'.");
        }

        var tableTree = catalog.GetTableTree(logicalIndex.TableName);
        var cursor = tableTree.CreateCursor();
        var tokenizer = new FullTextTokenizer(FullTextIndexOptionsCodec.Deserialize(logicalIndex.OptionsJson));

        while (await cursor.MoveNextAsync(ct))
        {
            DbValue[] row = serializer.Decode(cursor.CurrentValue.Span);
            await InsertDocumentAsync(catalog, logicalIndex, tokenizer, row, cursor.CurrentKey, columnIndices, ct);
        }
    }

    public static async ValueTask InsertAsync(
        SchemaCatalog catalog,
        TableSchema tableSchema,
        IndexSchema logicalIndex,
        DbValue[] row,
        long rowId,
        CancellationToken ct = default)
    {
        if (!TryResolveColumnIndices(logicalIndex, tableSchema, out int[] columnIndices))
            return;

        var tokenizer = new FullTextTokenizer(FullTextIndexOptionsCodec.Deserialize(logicalIndex.OptionsJson));
        await InsertDocumentAsync(catalog, logicalIndex, tokenizer, row, rowId, columnIndices, ct);
    }

    public static async ValueTask DeleteAsync(
        SchemaCatalog catalog,
        TableSchema tableSchema,
        IndexSchema logicalIndex,
        DbValue[] row,
        long rowId,
        CancellationToken ct = default)
    {
        if (!TryResolveColumnIndices(logicalIndex, tableSchema, out int[] columnIndices))
            return;

        var tokenizer = new FullTextTokenizer(FullTextIndexOptionsCodec.Deserialize(logicalIndex.OptionsJson));
        await DeleteDocumentAsync(catalog, logicalIndex, tokenizer, row, rowId, columnIndices, ct);
    }

    public static async ValueTask UpdateAsync(
        SchemaCatalog catalog,
        TableSchema tableSchema,
        IndexSchema logicalIndex,
        DbValue[] oldRow,
        DbValue[] newRow,
        long oldRowId,
        long newRowId,
        CancellationToken ct = default)
    {
        if (!TryResolveColumnIndices(logicalIndex, tableSchema, out int[] columnIndices))
            return;

        var tokenizer = new FullTextTokenizer(FullTextIndexOptionsCodec.Deserialize(logicalIndex.OptionsJson));
        await DeleteDocumentAsync(catalog, logicalIndex, tokenizer, oldRow, oldRowId, columnIndices, ct);
        await InsertDocumentAsync(catalog, logicalIndex, tokenizer, newRow, newRowId, columnIndices, ct);
    }

    private static async ValueTask InsertDocumentAsync(
        SchemaCatalog catalog,
        IndexSchema logicalIndex,
        FullTextTokenizer tokenizer,
        DbValue[] row,
        long rowId,
        int[] columnIndices,
        CancellationToken ct)
    {
        var termPositions = BuildTermPositions(row, columnIndices, tokenizer, out int docLength);
        if (termPositions.Count == 0)
            return;

        IIndexStore metaStore = catalog.GetIndexStore(FullTextIndexNaming.GetMetaIndexName(logicalIndex.IndexName));
        IIndexStore termsStore = catalog.GetIndexStore(FullTextIndexNaming.GetTermsIndexName(logicalIndex.IndexName));
        IIndexStore postingsStore = catalog.GetIndexStore(FullTextIndexNaming.GetPostingsIndexName(logicalIndex.IndexName));
        IIndexStore? postingChunksStore = TryGetPostingChunksStore(catalog, logicalIndex);
        IIndexStore docStatsStore = catalog.GetIndexStore(FullTextIndexNaming.GetDocStatsIndexName(logicalIndex.IndexName));

        await UpdateMetaAsync(metaStore, +1, docLength, ct);
        await WritePayloadAsync(docStatsStore, rowId, FullTextDocStatsPayloadCodec.Encode(docLength), ct);

        foreach ((string term, int[] positions) in termPositions)
        {
            long key = FullTextTermKeyCodec.ComputeKey(term);

            byte[]? termStatsPayload = await termsStore.FindAsync(key, ct);
            byte[] updatedTermStats = termStatsPayload == null
                ? FullTextTermStatsPayloadCodec.CreateSingle(term, 1)
                : FullTextTermStatsPayloadCodec.Adjust(termStatsPayload, term, +1, out _)!;
            await WritePayloadAsync(termsStore, key, updatedTermStats, ct);

            byte[]? postingsPayload = await postingsStore.FindAsync(key, ct);
            if (postingChunksStore != null)
            {
                await InsertPostingAsync(postingsStore, postingChunksStore, key, term, rowId, positions, postingsPayload, ct);
            }
            else
            {
                byte[] updatedPostings = postingsPayload == null
                    ? FullTextPostingsPayloadCodec.CreateSingle(term, [new FullTextPosting(rowId, positions)])
                    : FullTextPostingsPayloadCodec.Insert(postingsPayload, term, rowId, positions, out _);
                await WritePayloadAsync(postingsStore, key, updatedPostings, ct);
            }
        }
    }

    private static async ValueTask DeleteDocumentAsync(
        SchemaCatalog catalog,
        IndexSchema logicalIndex,
        FullTextTokenizer tokenizer,
        DbValue[] row,
        long rowId,
        int[] columnIndices,
        CancellationToken ct)
    {
        var termPositions = BuildTermPositions(row, columnIndices, tokenizer, out int docLength);
        if (termPositions.Count == 0)
            return;

        IIndexStore metaStore = catalog.GetIndexStore(FullTextIndexNaming.GetMetaIndexName(logicalIndex.IndexName));
        IIndexStore termsStore = catalog.GetIndexStore(FullTextIndexNaming.GetTermsIndexName(logicalIndex.IndexName));
        IIndexStore postingsStore = catalog.GetIndexStore(FullTextIndexNaming.GetPostingsIndexName(logicalIndex.IndexName));
        IIndexStore? postingChunksStore = TryGetPostingChunksStore(catalog, logicalIndex);
        IIndexStore docStatsStore = catalog.GetIndexStore(FullTextIndexNaming.GetDocStatsIndexName(logicalIndex.IndexName));

        await UpdateMetaAsync(metaStore, -1, -docLength, ct);
        await docStatsStore.DeleteAsync(rowId, ct);

        foreach ((string term, _) in termPositions)
        {
            long key = FullTextTermKeyCodec.ComputeKey(term);

            byte[]? termStatsPayload = await termsStore.FindAsync(key, ct);
            if (termStatsPayload != null)
            {
                byte[]? updatedTermStats = FullTextTermStatsPayloadCodec.Adjust(termStatsPayload, term, -1, out _);
                await WritePayloadAsync(termsStore, key, updatedTermStats, ct);
            }

            byte[]? postingsPayload = await postingsStore.FindAsync(key, ct);
            if (postingsPayload != null)
            {
                if (postingChunksStore != null)
                {
                    await DeletePostingAsync(postingsStore, postingChunksStore, key, term, rowId, postingsPayload, ct);
                }
                else
                {
                    byte[]? updatedPostings = FullTextPostingsPayloadCodec.Remove(postingsPayload, term, rowId, out bool changed);
                    if (changed)
                        await WritePayloadAsync(postingsStore, key, updatedPostings, ct);
                }
            }
        }
    }

    private static IIndexStore? TryGetPostingChunksStore(SchemaCatalog catalog, IndexSchema logicalIndex)
    {
        string chunkIndexName = FullTextIndexNaming.GetPostingChunksIndexName(logicalIndex.IndexName);
        return catalog.GetIndex(chunkIndexName) == null
            ? null
            : catalog.GetIndexStore(chunkIndexName);
    }

    private static async ValueTask InsertPostingAsync(
        IIndexStore postingsStore,
        IIndexStore postingChunksStore,
        long termKey,
        string term,
        long rowId,
        IReadOnlyList<int> positions,
        byte[]? manifestPayload,
        CancellationToken ct)
    {
        if (manifestPayload == null)
        {
            var posting = new FullTextPosting(rowId, positions.ToArray());
            var initialChunks = await WriteChunksForTermAsync(postingChunksStore, term, [posting], ct);
            await WritePayloadAsync(
                postingsStore,
                termKey,
                FullTextPostingChunkManifestCodec.CreateSingle(term, initialChunks),
                ct);
            return;
        }

        if (FullTextPostingsPayloadCodec.IsEncoded(manifestPayload))
            manifestPayload = await MigrateLegacyPostingsPayloadAsync(postingsStore, postingChunksStore, termKey, manifestPayload, ct);

        if (!FullTextPostingChunkManifestCodec.IsEncoded(manifestPayload))
            throw new InvalidOperationException("Full-text postings manifest is not in a supported format.");

        FullTextPostingChunkDescriptor[] chunks = FullTextPostingChunkManifestCodec.TryGetChunks(manifestPayload, term, out var existingChunks)
            ? existingChunks
            : [];

        if (chunks.Length == 0)
        {
            var posting = new FullTextPosting(rowId, positions.ToArray());
            var newChunks = await WriteChunksForTermAsync(postingChunksStore, term, [posting], ct);
            byte[]? updatedManifest = FullTextPostingChunkManifestCodec.Upsert(manifestPayload, term, newChunks);
            await WritePayloadAsync(postingsStore, termKey, updatedManifest, ct);
            return;
        }

        int chunkIndex = FindChunkIndex(chunks, rowId);
        FullTextPostingChunkDescriptor oldDescriptor = chunks[chunkIndex];
        List<FullTextPosting> postings = await ReadChunkPostingsAsync(postingChunksStore, term, oldDescriptor.FirstDocId, ct);

        int postingIndex = FindPostingIndex(postings, rowId);
        if (postingIndex >= 0)
            postings[postingIndex] = new FullTextPosting(rowId, positions.ToArray());
        else
            postings.Insert(~postingIndex, new FullTextPosting(rowId, positions.ToArray()));

        var updatedChunks = new List<FullTextPostingChunkDescriptor>(chunks.Length + 1);
        for (int i = 0; i < chunks.Length; i++)
        {
            if (i != chunkIndex)
            {
                updatedChunks.Add(chunks[i]);
                continue;
            }

            await RemoveChunkAsync(postingChunksStore, term, oldDescriptor.FirstDocId, ct);
            updatedChunks.AddRange(await WriteChunksForTermAsync(postingChunksStore, term, postings, ct));
        }

        byte[]? updatedPayload = FullTextPostingChunkManifestCodec.Upsert(manifestPayload, term, updatedChunks);
        await WritePayloadAsync(postingsStore, termKey, updatedPayload, ct);
    }

    private static async ValueTask DeletePostingAsync(
        IIndexStore postingsStore,
        IIndexStore postingChunksStore,
        long termKey,
        string term,
        long rowId,
        byte[] manifestPayload,
        CancellationToken ct)
    {
        if (FullTextPostingsPayloadCodec.IsEncoded(manifestPayload))
            manifestPayload = await MigrateLegacyPostingsPayloadAsync(postingsStore, postingChunksStore, termKey, manifestPayload, ct);

        if (!FullTextPostingChunkManifestCodec.IsEncoded(manifestPayload) ||
            !FullTextPostingChunkManifestCodec.TryGetChunks(manifestPayload, term, out var chunks) ||
            chunks.Length == 0)
        {
            return;
        }

        int chunkIndex = FindChunkIndex(chunks, rowId);
        FullTextPostingChunkDescriptor oldDescriptor = chunks[chunkIndex];
        if (rowId < oldDescriptor.FirstDocId || rowId > oldDescriptor.LastDocId)
            return;

        List<FullTextPosting> postings = await ReadChunkPostingsAsync(postingChunksStore, term, oldDescriptor.FirstDocId, ct);
        int postingIndex = FindPostingIndex(postings, rowId);
        if (postingIndex < 0)
            return;

        postings.RemoveAt(postingIndex);
        await RemoveChunkAsync(postingChunksStore, term, oldDescriptor.FirstDocId, ct);

        var updatedChunks = new List<FullTextPostingChunkDescriptor>(Math.Max(0, chunks.Length - 1));
        for (int i = 0; i < chunks.Length; i++)
        {
            if (i != chunkIndex)
            {
                updatedChunks.Add(chunks[i]);
                continue;
            }

            if (postings.Count > 0)
                updatedChunks.AddRange(await WriteChunksForTermAsync(postingChunksStore, term, postings, ct));
        }

        byte[]? updatedPayload = updatedChunks.Count == 0
            ? FullTextPostingChunkManifestCodec.Remove(manifestPayload, term)
            : FullTextPostingChunkManifestCodec.Upsert(manifestPayload, term, updatedChunks);
        await WritePayloadAsync(postingsStore, termKey, updatedPayload, ct);
    }

    private static async ValueTask<byte[]> MigrateLegacyPostingsPayloadAsync(
        IIndexStore postingsStore,
        IIndexStore postingChunksStore,
        long termKey,
        byte[] legacyPayload,
        CancellationToken ct)
    {
        if (!FullTextPostingsPayloadCodec.TryDecodeBuckets(legacyPayload, out var buckets))
            throw new InvalidOperationException("Legacy full-text postings payload is invalid.");

        var manifests = new List<FullTextPostingChunkManifestBucket>(buckets.Count);
        foreach (FullTextPostingsBucket bucket in buckets)
        {
            if (!FullTextPostingsListCodec.TryDecode(bucket.PostingsPayload, out var postings))
                throw new InvalidOperationException("Legacy full-text postings list is invalid.");

            FullTextPostingChunkDescriptor[] chunks = await WriteChunksForTermAsync(
                postingChunksStore,
                bucket.Term,
                postings,
                ct);
            if (chunks.Length > 0)
                manifests.Add(new FullTextPostingChunkManifestBucket(bucket.Term, chunks));
        }

        byte[]? manifestPayload = null;
        foreach (FullTextPostingChunkManifestBucket manifest in manifests)
        {
            manifestPayload = manifestPayload == null
                ? FullTextPostingChunkManifestCodec.CreateSingle(manifest.Term, manifest.Chunks)
                : FullTextPostingChunkManifestCodec.Upsert(manifestPayload, manifest.Term, manifest.Chunks);
        }

        await WritePayloadAsync(postingsStore, termKey, manifestPayload, ct);
        return manifestPayload ?? [];
    }

    private static async ValueTask<FullTextPostingChunkDescriptor[]> WriteChunksForTermAsync(
        IIndexStore postingChunksStore,
        string term,
        IReadOnlyList<FullTextPosting> postings,
        CancellationToken ct)
    {
        if (postings.Count == 0)
            return [];

        var descriptors = new List<FullTextPostingChunkDescriptor>((postings.Count / MaxPostingsPerChunk) + 1);
        for (int offset = 0; offset < postings.Count; offset += MaxPostingsPerChunk)
        {
            int count = Math.Min(MaxPostingsPerChunk, postings.Count - offset);
            FullTextPosting[] chunkPostings = new FullTextPosting[count];
            for (int i = 0; i < count; i++)
                chunkPostings[i] = postings[offset + i];

            FullTextPostingChunkDescriptor descriptor = CreateDescriptor(chunkPostings);
            await WriteChunkAsync(postingChunksStore, term, descriptor.FirstDocId, chunkPostings, ct);
            descriptors.Add(descriptor);
        }

        return descriptors.ToArray();
    }

    private static async ValueTask<List<FullTextPosting>> ReadChunkPostingsAsync(
        IIndexStore postingChunksStore,
        string term,
        long firstDocId,
        CancellationToken ct)
    {
        long chunkKey = FullTextPostingChunkKeyCodec.ComputeKey(term, firstDocId);
        byte[]? chunkPayload = await postingChunksStore.FindAsync(chunkKey, ct);
        if (chunkPayload == null ||
            !FullTextPostingChunkPayloadCodec.TryGetPostings(chunkPayload, term, firstDocId, out byte[] postingsPayload) ||
            !FullTextPostingsListCodec.TryDecode(postingsPayload, out var postings))
        {
            throw new InvalidOperationException("Full-text posting chunk is missing or corrupt.");
        }

        return postings;
    }

    private static async ValueTask WriteChunkAsync(
        IIndexStore postingChunksStore,
        string term,
        long firstDocId,
        IReadOnlyList<FullTextPosting> postings,
        CancellationToken ct)
    {
        long chunkKey = FullTextPostingChunkKeyCodec.ComputeKey(term, firstDocId);
        byte[]? existing = await postingChunksStore.FindAsync(chunkKey, ct);
        byte[] payload = existing == null
            ? FullTextPostingChunkPayloadCodec.CreateSingle(term, firstDocId, postings)
            : FullTextPostingChunkPayloadCodec.Upsert(existing, term, firstDocId, postings);
        await WritePayloadAsync(postingChunksStore, chunkKey, payload, ct);
    }

    private static async ValueTask RemoveChunkAsync(
        IIndexStore postingChunksStore,
        string term,
        long firstDocId,
        CancellationToken ct)
    {
        long chunkKey = FullTextPostingChunkKeyCodec.ComputeKey(term, firstDocId);
        byte[]? existing = await postingChunksStore.FindAsync(chunkKey, ct);
        if (existing == null)
            return;

        byte[]? updated = FullTextPostingChunkPayloadCodec.Remove(existing, term, firstDocId);
        await WritePayloadAsync(postingChunksStore, chunkKey, updated, ct);
    }

    private static FullTextPostingChunkDescriptor CreateDescriptor(IReadOnlyList<FullTextPosting> postings)
    {
        if (postings.Count == 0)
            throw new ArgumentException("Posting chunk must contain at least one posting.", nameof(postings));

        return new FullTextPostingChunkDescriptor(
            postings[0].DocId,
            postings[^1].DocId,
            postings.Count);
    }

    private static int FindChunkIndex(IReadOnlyList<FullTextPostingChunkDescriptor> chunks, long rowId)
    {
        int low = 0;
        int high = chunks.Count - 1;
        while (low <= high)
        {
            int mid = low + ((high - low) / 2);
            FullTextPostingChunkDescriptor chunk = chunks[mid];
            if (rowId < chunk.FirstDocId)
                high = mid - 1;
            else if (rowId > chunk.LastDocId)
                low = mid + 1;
            else
                return mid;
        }

        if (low >= chunks.Count)
            return chunks.Count - 1;

        return low;
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

    private static async ValueTask UpdateMetaAsync(
        IIndexStore metaStore,
        int documentDelta,
        int tokenDelta,
        CancellationToken ct)
    {
        byte[]? existingPayload = await metaStore.FindAsync(MetaKey, ct);
        long documentCount = 0;
        long totalTokenCount = 0;

        if (existingPayload != null && FullTextMetaPayloadCodec.TryDecode(existingPayload, out long storedDocumentCount, out long storedTotalTokenCount))
        {
            documentCount = storedDocumentCount;
            totalTokenCount = storedTotalTokenCount;
        }

        documentCount = checked(documentCount + documentDelta);
        totalTokenCount = checked(totalTokenCount + tokenDelta);
        if (documentCount < 0 || totalTokenCount < 0)
            throw new InvalidOperationException("Full-text corpus statistics cannot become negative.");

        await WritePayloadAsync(metaStore, MetaKey, FullTextMetaPayloadCodec.Encode(documentCount, totalTokenCount), ct);
    }

    private static Dictionary<string, int[]> BuildTermPositions(
        DbValue[] row,
        int[] columnIndices,
        FullTextTokenizer tokenizer,
        out int docLength)
    {
        docLength = 0;
        var segments = new List<string>(columnIndices.Length);
        for (int i = 0; i < columnIndices.Length; i++)
        {
            int columnIndex = columnIndices[i];
            if (columnIndex < 0 || columnIndex >= row.Length)
                continue;

            DbValue value = row[columnIndex];
            if (value.IsNull || value.Type != DbType.Text || string.IsNullOrWhiteSpace(value.AsText))
                continue;

            segments.Add(value.AsText);
        }

        if (segments.Count == 0)
            return new Dictionary<string, int[]>(StringComparer.Ordinal);

        string content = string.Join(' ', segments);
        var termPositions = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        foreach (var token in tokenizer.Tokenize(content))
        {
            docLength++;
            if (!termPositions.TryGetValue(token.Text, out var positions))
            {
                positions = [];
                termPositions[token.Text] = positions;
            }

            positions.Add(token.Position);
        }

        return termPositions.ToDictionary(static kvp => kvp.Key, static kvp => kvp.Value.ToArray(), StringComparer.Ordinal);
    }

    private static async ValueTask WritePayloadAsync(
        IIndexStore store,
        long key,
        byte[]? payload,
        CancellationToken ct)
    {
        byte[]? existing = await store.FindAsync(key, ct);
        if (payload == null || payload.Length == 0)
        {
            if (existing != null)
                await store.DeleteAsync(key, ct);
            return;
        }

        if (existing == null)
            await store.InsertAsync(key, payload, ct);
        else
            await store.ReplaceAsync(key, payload, ct);
    }
}

using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextIndexMaintenance
{
    private const long MetaKey = 0;

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
            byte[] updatedPostings = postingsPayload == null
                ? FullTextPostingsPayloadCodec.CreateSingle(term, [new FullTextPosting(rowId, positions)])
                : FullTextPostingsPayloadCodec.Insert(postingsPayload, term, rowId, positions, out _);
            await WritePayloadAsync(postingsStore, key, updatedPostings, ct);
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
                byte[]? updatedPostings = FullTextPostingsPayloadCodec.Remove(postingsPayload, term, rowId, out bool changed);
                if (changed)
                    await WritePayloadAsync(postingsStore, key, updatedPostings, ct);
            }
        }
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

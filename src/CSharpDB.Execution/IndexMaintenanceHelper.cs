using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Execution;

internal static class IndexMaintenanceHelper
{
    public static async ValueTask BackfillIndexAsync(
        SchemaCatalog catalog,
        TableSchema tableSchema,
        IndexSchema indexSchema,
        IRecordSerializer readSerializer,
        CancellationToken ct = default)
    {
        if (!TryResolveIndexColumnIndices(indexSchema, tableSchema, out var indexColumnIndices, out bool usesDirectIntegerKey))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Index '{indexSchema.IndexName}' references unsupported columns for table '{tableSchema.TableName}'.");
        }

        string?[] indexColumnCollations = CollationSupport.GetEffectiveIndexColumnCollations(
            indexSchema,
            tableSchema,
            indexColumnIndices);
        SqlIndexStorageMode storageMode = ResolveSqlIndexStorageMode(indexSchema, tableSchema);

        var tableTree = catalog.GetTableTree(indexSchema.TableName);
        var indexStore = catalog.GetIndexStore(indexSchema.IndexName);
        var scan = new TableScanOperator(tableTree, tableSchema, readSerializer);
        await scan.OpenAsync(ct);

        if (!indexSchema.IsUnique)
        {
            if (usesDirectIntegerKey)
            {
                var groupedRowIds = new SortedDictionary<long, List<long>>();

                while (await scan.MoveNextAsync(ct))
                {
                    if (!TryBuildIndexKey(
                            scan.Current,
                            indexColumnIndices,
                            indexColumnCollations,
                            usesDirectIntegerKey,
                            storageMode,
                            out long indexKey,
                            out _))
                    {
                        continue;
                    }

                    if (!groupedRowIds.TryGetValue(indexKey, out var rowIds))
                    {
                        rowIds = new List<long>();
                        groupedRowIds[indexKey] = rowIds;
                    }

                    rowIds.Add(scan.CurrentRowId);
                }

                foreach (var entry in groupedRowIds)
                    await indexStore.InsertAsync(entry.Key, RowIdPayloadCodec.CreateFromSorted(entry.Value), ct);

                return;
            }

            if (storageMode == SqlIndexStorageMode.OrderedText)
            {
                var groupedTextPayloads = new SortedDictionary<long, SortedDictionary<string, List<long>>>();

                while (await scan.MoveNextAsync(ct))
                {
                    if (!TryBuildIndexKey(
                            scan.Current,
                            indexColumnIndices,
                            indexColumnCollations,
                            usesDirectIntegerKey,
                            storageMode,
                            out long indexKey,
                            out DbValue[]? keyComponents))
                    {
                        continue;
                    }

                    string text = keyComponents![0].AsText;
                    if (!groupedTextPayloads.TryGetValue(indexKey, out var bucketEntries))
                    {
                        bucketEntries = new SortedDictionary<string, List<long>>(StringComparer.Ordinal);
                        groupedTextPayloads[indexKey] = bucketEntries;
                    }

                    if (!bucketEntries.TryGetValue(text, out var rowIds))
                    {
                        rowIds = [];
                        bucketEntries[text] = rowIds;
                    }

                    rowIds.Add(scan.CurrentRowId);
                }

                foreach (var entry in groupedTextPayloads)
                {
                    byte[] payload = OrderedTextIndexPayloadCodec.CreateFromSorted(entry.Value);
                    await indexStore.InsertAsync(entry.Key, payload, ct);
                }

                return;
            }

            var groupedPayloads = new SortedDictionary<long, byte[]>();

            while (await scan.MoveNextAsync(ct))
            {
                if (!TryBuildIndexKey(
                        scan.Current,
                        indexColumnIndices,
                        indexColumnCollations,
                        usesDirectIntegerKey,
                        storageMode,
                        out long indexKey,
                        out DbValue[]? keyComponents))
                {
                    continue;
                }

                if (!groupedPayloads.TryGetValue(indexKey, out var payload))
                {
                    groupedPayloads[indexKey] = HashedIndexPayloadCodec.CreateSingle(
                        keyComponents!,
                        scan.CurrentRowId,
                        omitTrailingInteger: storageMode == SqlIndexStorageMode.HashedTrailingInteger);
                    continue;
                }

                groupedPayloads[indexKey] = HashedIndexPayloadCodec.Insert(
                    payload,
                    keyComponents!,
                    scan.CurrentRowId,
                    out _);
            }

            foreach (var entry in groupedPayloads)
                await indexStore.InsertAsync(entry.Key, entry.Value, ct);

            return;
        }

        while (await scan.MoveNextAsync(ct))
        {
            if (!TryBuildIndexKey(
                    scan.Current,
                    indexColumnIndices,
                    indexColumnCollations,
                    usesDirectIntegerKey,
                    storageMode,
                    out long indexKey,
                    out DbValue[]? keyComponents))
            {
                continue;
            }

            if (indexSchema.IsUnique)
            {
                if (usesDirectIntegerKey)
                {
                    var existing = await indexStore.FindAsync(indexKey, ct);
                    if (existing != null)
                    {
                        throw new CSharpDbException(
                            ErrorCode.ConstraintViolation,
                            $"Duplicate key value in unique index '{indexSchema.IndexName}'.");
                    }

                    var payload = new byte[sizeof(long)];
                    BitConverter.TryWriteBytes(payload, scan.CurrentRowId);
                    await indexStore.InsertAsync(indexKey, payload, ct);
                }
                else
                {
                    await EnsureUniqueConstraintAsync(
                        pager: null,
                        indexStore,
                        indexSchema.IndexName,
                        tableTree,
                        tableSchema,
                        readSerializer,
                        indexColumnIndices,
                        indexColumnCollations,
                        keyComponents!,
                        indexKey,
                        storageMode,
                        ct);

                    await InsertRowIdAsync(
                        pager: null,
                        indexStore,
                        indexSchema.IndexName,
                        indexKey,
                        scan.CurrentRowId,
                        keyComponents,
                        storageMode,
                        ct);
                }
            }
            else
            {
                await InsertRowIdAsync(
                    pager: null,
                    indexStore,
                    indexSchema.IndexName,
                    indexKey,
                    scan.CurrentRowId,
                    keyComponents,
                    storageMode,
                    ct);
            }
        }
    }

    public static async ValueTask InsertRowIdAsync(
        Pager? pager,
        IIndexStore indexStore,
        string indexName,
        long indexKey,
        long rowId,
        DbValue[]? keyComponents = null,
        SqlIndexStorageMode storageMode = SqlIndexStorageMode.Hashed,
        CancellationToken ct = default,
        AppendOptimizedIndexMutationContext? appendContext = null,
        byte[]? reusableSingleRowIdPayload = null)
    {
        if (UsesHashedPayloadStorage(storageMode) &&
            keyComponents is { Length: > 0 } &&
            indexStore is IAppendOptimizedIndexStore appendOptimizedStore)
        {
            AppendRowIdResult appendResult = await appendOptimizedStore.TryAppendHashedRowIdAsync(
                indexKey,
                keyComponents,
                rowId,
                appendContext,
                ct);
            switch (appendResult)
            {
                case AppendRowIdResult.Appended:
                    pager?.RecordLogicalIndexWrite(indexName, indexKey);
                    return;
                case AppendRowIdResult.AlreadyExists:
                    return;
                case AppendRowIdResult.Missing:
                case AppendRowIdResult.NotApplicable:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(appendResult), appendResult, "Unknown append result.");
            }
        }

        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
        {
            byte[] initialPayload = storageMode switch
            {
                SqlIndexStorageMode.OrderedText => OrderedTextIndexPayloadCodec.CreateSingle(
                    GetOrderedTextKeyComponent(keyComponents),
                    rowId),
                _ => keyComponents is { Length: > 0 }
                    ? HashedIndexPayloadCodec.CreateSingle(
                        keyComponents,
                        rowId,
                        omitTrailingInteger: storageMode == SqlIndexStorageMode.HashedTrailingInteger)
                    : CreateSingleRowIdPayload(rowId, reusableSingleRowIdPayload),
            };
            await indexStore.InsertAsync(indexKey, initialPayload, ct);
            pager?.RecordLogicalIndexWrite(indexName, indexKey);
            return;
        }

        byte[] newPayload;
        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            if (!OrderedTextIndexPayloadCodec.IsEncoded(existing))
            {
                throw new InvalidOperationException(
                    "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
            }

            newPayload = OrderedTextIndexPayloadCodec.Insert(
                existing,
                GetOrderedTextKeyComponent(keyComponents),
                rowId,
                out bool changed);
            if (!changed)
                return;
        }
        else if (keyComponents is { Length: > 0 } && HashedIndexPayloadCodec.IsEncoded(existing))
        {
            newPayload = HashedIndexPayloadCodec.Insert(existing, keyComponents, rowId, out bool changed);
            if (!changed)
                return;
        }
        else
        {
            if (!RowIdPayloadCodec.TryInsert(existing, rowId, out newPayload))
                return;
        }

        if (await indexStore.ReplaceAsync(indexKey, newPayload, ct))
            pager?.RecordLogicalIndexWrite(indexName, indexKey);
    }

    private static byte[] CreateSingleRowIdPayload(long rowId, byte[]? reusablePayload)
    {
        if (reusablePayload is { Length: RowIdPayloadCodec.RowIdSize })
        {
            BinaryPrimitives.WriteInt64LittleEndian(reusablePayload, rowId);
            return reusablePayload;
        }

        return RowIdPayloadCodec.CreateSingle(rowId);
    }

    public static async ValueTask DeleteRowIdAsync(
        Pager? pager,
        IIndexStore indexStore,
        string indexName,
        long indexKey,
        long rowId,
        DbValue[]? keyComponents = null,
        SqlIndexStorageMode storageMode = SqlIndexStorageMode.Hashed,
        CancellationToken ct = default)
    {
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
            return;

        byte[]? newPayload;
        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            if (!OrderedTextIndexPayloadCodec.IsEncoded(existing))
            {
                throw new InvalidOperationException(
                    "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
            }

            newPayload = OrderedTextIndexPayloadCodec.Remove(
                existing,
                GetOrderedTextKeyComponent(keyComponents),
                rowId,
                out bool changed);
            if (!changed)
                return;
        }
        else if (keyComponents is { Length: > 0 } && HashedIndexPayloadCodec.IsEncoded(existing))
        {
            newPayload = HashedIndexPayloadCodec.Remove(existing, keyComponents, rowId, out bool changed);
            if (!changed)
                return;
        }
        else
        {
            if (!RowIdPayloadCodec.TryRemove(existing, rowId, out newPayload))
                return;
        }

        if (newPayload != null)
        {
            if (await indexStore.ReplaceAsync(indexKey, newPayload, ct))
                pager?.RecordLogicalIndexWrite(indexName, indexKey);
        }
        else if (await indexStore.DeleteAsync(indexKey, ct))
        {
            pager?.RecordLogicalIndexWrite(indexName, indexKey);
        }
    }

    public static bool TryResolveIndexColumnIndices(
        IndexSchema index,
        TableSchema schema,
        out int[] columnIndices,
        out bool usesDirectIntegerKey)
    {
        int count = index.Columns.Count;
        columnIndices = new int[count];
        usesDirectIntegerKey = false;
        if (count == 0)
            return false;

        for (int i = 0; i < count; i++)
        {
            int colIdx = schema.GetColumnIndex(index.Columns[i]);
            if (colIdx < 0 || colIdx >= schema.Columns.Count)
                return false;
            if (schema.Columns[colIdx].Type is not (DbType.Integer or DbType.Text))
                return false;

            columnIndices[i] = colIdx;
        }

        usesDirectIntegerKey = count == 1 && schema.Columns[columnIndices[0]].Type == DbType.Integer;
        return true;
    }

    public static bool TryBuildIndexKey(
        DbValue[] row,
        int[] indexColumnIndices,
        string?[] indexColumnCollations,
        bool usesDirectIntegerKey,
        SqlIndexStorageMode storageMode,
        out long indexKey,
        out DbValue[]? keyComponents)
        => TryBuildIndexKey(
            row,
            indexColumnIndices,
            indexColumnCollations,
            usesDirectIntegerKey,
            storageMode,
            reusableKeyComponents: null,
            out indexKey,
            out keyComponents);

    public static bool TryBuildIndexKey(
        DbValue[] row,
        int[] indexColumnIndices,
        string?[] indexColumnCollations,
        bool usesDirectIntegerKey,
        SqlIndexStorageMode storageMode,
        DbValue[]? reusableKeyComponents,
        out long indexKey,
        out DbValue[]? keyComponents)
    {
        indexKey = 0;
        keyComponents = null;

        if (indexColumnIndices.Length == 0)
            return false;

        if (usesDirectIntegerKey)
        {
            int colIdx = indexColumnIndices[0];
            if (colIdx < 0 || colIdx >= row.Length)
                return false;

            var value = row[colIdx];
            if (value.IsNull || value.Type != DbType.Integer)
                return false;

            indexKey = value.AsInteger;
            return true;
        }

        DbValue[] components = reusableKeyComponents is { Length: > 0 } &&
            reusableKeyComponents.Length == indexColumnIndices.Length
                ? reusableKeyComponents
                : new DbValue[indexColumnIndices.Length];
        for (int i = 0; i < indexColumnIndices.Length; i++)
        {
            int colIdx = indexColumnIndices[i];
            if (colIdx < 0 || colIdx >= row.Length)
                return false;

            var value = row[colIdx];
            if (value.IsNull)
                return false;
            if (value.Type is not (DbType.Integer or DbType.Text))
                return false;

            string? collation = i < indexColumnCollations.Length ? indexColumnCollations[i] : null;
            components[i] = CollationSupport.NormalizeIndexValue(value, collation);
        }

        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            indexKey = OrderedTextIndexKeyCodec.ComputeKey(GetOrderedTextKeyComponent(components));
            keyComponents = components;
            return true;
        }

        indexKey = ComputeIndexKey(components, storageMode);
        keyComponents = components;
        return true;
    }

    public static SqlIndexStorageMode ResolveSqlIndexStorageMode(IndexSchema index, TableSchema schema)
        => SqlIndexOptionsCodec.Resolve(index, schema);

    public static bool UsesOrderedTextIndexKey(IndexSchema index, TableSchema schema)
        => ResolveSqlIndexStorageMode(index, schema) == SqlIndexStorageMode.OrderedText;

    public static bool UsesHashedPayloadStorage(SqlIndexStorageMode storageMode)
        => storageMode != SqlIndexStorageMode.OrderedText;

    public static bool IndexKeyComponentsEqual(DbValue[]? left, DbValue[]? right)
    {
        if (ReferenceEquals(left, right))
            return true;
        if (left == null || right == null || left.Length != right.Length)
            return false;

        for (int i = 0; i < left.Length; i++)
        {
            if (DbValue.Compare(left[i], right[i]) != 0)
                return false;
        }

        return true;
    }

    public static long ComputeIndexKey(ReadOnlySpan<DbValue> keyComponents)
    {
        if (keyComponents.Length == 1 && keyComponents[0].Type == DbType.Integer)
            return keyComponents[0].AsInteger;

        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int i = 0; i < keyComponents.Length; i++)
            hash = HashIndexKeyComponent(hash, keyComponents[i], prime);

        return unchecked((long)hash);
    }

    public static long ComputeIndexKey(ReadOnlySpan<DbValue> keyComponents, SqlIndexStorageMode storageMode)
    {
        if (storageMode == SqlIndexStorageMode.HashedTrailingInteger &&
            keyComponents.Length > 1 &&
            keyComponents[^1].Type == DbType.Integer)
        {
            return keyComponents[^1].AsInteger;
        }

        return ComputeIndexKey(keyComponents);
    }

    public static async ValueTask EnsureUniqueConstraintAsync(
        Pager? pager,
        IIndexStore indexStore,
        string indexName,
        BTree tableTree,
        TableSchema schema,
        IRecordSerializer readSerializer,
        int[] indexColumnIndices,
        string?[] indexColumnCollations,
        DbValue[] keyComponents,
        long indexKey,
        SqlIndexStorageMode storageMode,
        CancellationToken ct)
    {
        if (storageMode == SqlIndexStorageMode.OrderedText)
        {
            await EnsureOrderedTextUniqueConstraintAsync(
                pager,
                indexStore,
                indexName,
                keyComponents,
                indexKey,
                ct);
            return;
        }

        await EnsureHashedUniqueConstraintAsync(
            pager,
            indexStore,
            indexName,
            tableTree,
            schema,
            readSerializer,
            indexColumnIndices,
            indexColumnCollations,
            keyComponents,
            indexKey,
            ct);
    }

    private static async ValueTask EnsureHashedUniqueConstraintAsync(
        Pager? pager,
        IIndexStore indexStore,
        string indexName,
        BTree tableTree,
        TableSchema schema,
        IRecordSerializer readSerializer,
        int[] indexColumnIndices,
        string?[] indexColumnCollations,
        DbValue[] keyComponents,
        long indexKey,
        CancellationToken ct)
    {
        pager?.RecordLogicalIndexRead(indexName, indexKey);
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null || existing.Length < sizeof(long))
            return;

        if (HashedIndexPayloadCodec.TryGetMatchingRowIds(existing, keyComponents, out var matchingPayload))
        {
            if (matchingPayload is { Length: > 0 })
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Duplicate key value in unique index '{indexName}'.");
            }

            return;
        }

        int entryCount = existing.Length / sizeof(long);
        int maxIndexedColumn = indexColumnIndices.Max();

        for (int i = 0; i < entryCount; i++)
        {
            long existingRowId = RowIdPayloadCodec.ReadAt(existing, i);
            var existingRowPayload = await tableTree.FindMemoryAsync(existingRowId, ct);
            if (existingRowPayload is not { } existingRowPayloadMemory)
                continue;

            var existingRow = readSerializer.DecodeUpTo(existingRowPayloadMemory.Span, maxIndexedColumn);
            if (IndexRowMatchesKeyComponents(existingRow, indexColumnIndices, indexColumnCollations, keyComponents))
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Duplicate key value in unique index '{indexName}'.");
            }
        }
    }

    private static bool IndexRowMatchesKeyComponents(
        DbValue[] row,
        int[] indexColumnIndices,
        string?[] indexColumnCollations,
        DbValue[] keyComponents)
    {
        if (indexColumnIndices.Length != keyComponents.Length)
            return false;

        for (int i = 0; i < indexColumnIndices.Length; i++)
        {
            int colIdx = indexColumnIndices[i];
            if (colIdx < 0 || colIdx >= row.Length)
                return false;

            var value = row[colIdx];
            string? collation = i < indexColumnCollations.Length ? indexColumnCollations[i] : null;
            if (value.IsNull || DbValue.Compare(CollationSupport.NormalizeIndexValue(value, collation), keyComponents[i]) != 0)
                return false;
        }

        return true;
    }

    private static async ValueTask EnsureOrderedTextUniqueConstraintAsync(
        Pager? pager,
        IIndexStore indexStore,
        string indexName,
        DbValue[] keyComponents,
        long indexKey,
        CancellationToken ct)
    {
        pager?.RecordLogicalIndexRead(indexName, indexKey);
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null || existing.Length == 0)
            return;

        if (!OrderedTextIndexPayloadCodec.IsEncoded(existing))
        {
            throw new InvalidOperationException(
                "SQL text index payload format mismatch detected. Rebuild the index before continuing.");
        }

        if (OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(
                existing,
                GetOrderedTextKeyComponent(keyComponents),
                out ReadOnlyMemory<byte> matchingRowIds) &&
            matchingRowIds.Length > 0)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Duplicate key value in unique index '{indexName}'.");
        }
    }

    private static string GetOrderedTextKeyComponent(DbValue[]? keyComponents)
    {
        if (keyComponents is not [var textComponent] || textComponent.Type != DbType.Text)
        {
            throw new InvalidOperationException(
                "Ordered text SQL indexes require a single normalized text key component.");
        }

        return textComponent.AsText;
    }

    private static ulong HashIndexKeyComponent(ulong hash, DbValue value, ulong prime)
    {
        hash ^= (byte)value.Type;
        hash *= prime;

        switch (value.Type)
        {
            case DbType.Integer:
                hash ^= unchecked((ulong)value.AsInteger);
                hash *= prime;
                return hash;
            case DbType.Text:
            {
                // Preserve the legacy SQL index hash format so persisted text indexes
                // remain readable across upgrades.
                string text = value.AsText;
                for (int i = 0; i < text.Length; i++)
                {
                    char c = text[i];
                    hash ^= (byte)c;
                    hash *= prime;
                    hash ^= (byte)(c >> 8);
                    hash *= prime;
                }

                return hash;
            }
            default:
                throw new InvalidOperationException($"Cannot hash index component of type '{value.Type}'.");
        }
    }
}

public sealed class OrderedTextIndexOverflowException : Exception
{
    public OrderedTextIndexOverflowException(string indexName, long indexKey, int cellLength, int maxCellLength)
        : base($"Ordered text index '{indexName}' produced leaf cell length {cellLength} for key {indexKey}, exceeding max leaf cell length {maxCellLength}.")
    {
        IndexName = indexName;
        IndexKey = indexKey;
        CellLength = cellLength;
        MaxCellLength = maxCellLength;
    }

    public string IndexName { get; }

    public long IndexKey { get; }

    public int CellLength { get; }

    public int MaxCellLength { get; }
}

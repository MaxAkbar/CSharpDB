using CSharpDB.Primitives;

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
                    if (!TryBuildIndexKey(scan.Current, indexColumnIndices, usesDirectIntegerKey, out long indexKey, out _))
                        continue;

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

            var groupedPayloads = new SortedDictionary<long, byte[]>();

            while (await scan.MoveNextAsync(ct))
            {
                if (!TryBuildIndexKey(scan.Current, indexColumnIndices, usesDirectIntegerKey, out long indexKey, out DbValue[]? keyComponents))
                    continue;

                if (!groupedPayloads.TryGetValue(indexKey, out var payload))
                {
                    groupedPayloads[indexKey] = HashedIndexPayloadCodec.CreateSingle(keyComponents!, scan.CurrentRowId);
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
            if (!TryBuildIndexKey(scan.Current, indexColumnIndices, usesDirectIntegerKey, out long indexKey, out DbValue[]? keyComponents))
                continue;

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
                    await EnsureHashedUniqueConstraintAsync(
                        indexStore,
                        tableTree,
                        tableSchema,
                        readSerializer,
                        indexColumnIndices,
                        keyComponents!,
                        indexKey,
                        indexSchema.IndexName,
                        ct);

                    await InsertRowIdAsync(indexStore, indexKey, scan.CurrentRowId, keyComponents, ct);
                }
            }
            else
            {
                await InsertRowIdAsync(indexStore, indexKey, scan.CurrentRowId, keyComponents, ct);
            }
        }
    }

    public static async ValueTask InsertRowIdAsync(
        IIndexStore indexStore,
        long indexKey,
        long rowId,
        DbValue[]? keyComponents = null,
        CancellationToken ct = default)
    {
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
        {
            byte[] initialPayload = keyComponents is { Length: > 0 }
                ? HashedIndexPayloadCodec.CreateSingle(keyComponents, rowId)
                : RowIdPayloadCodec.CreateSingle(rowId);
            await indexStore.InsertAsync(indexKey, initialPayload, ct);
            return;
        }

        byte[] newPayload;
        if (keyComponents is { Length: > 0 } && HashedIndexPayloadCodec.IsEncoded(existing))
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

        await indexStore.ReplaceAsync(indexKey, newPayload, ct);
    }

    public static async ValueTask DeleteRowIdAsync(
        IIndexStore indexStore,
        long indexKey,
        long rowId,
        DbValue[]? keyComponents = null,
        CancellationToken ct = default)
    {
        var existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
            return;

        byte[]? newPayload;
        if (keyComponents is { Length: > 0 } && HashedIndexPayloadCodec.IsEncoded(existing))
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
            await indexStore.ReplaceAsync(indexKey, newPayload, ct);
        else
            await indexStore.DeleteAsync(indexKey, ct);
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
        bool usesDirectIntegerKey,
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

        var components = new DbValue[indexColumnIndices.Length];
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

            components[i] = value;
        }

        indexKey = ComputeIndexKey(components);
        keyComponents = components;
        return true;
    }

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

    private static async ValueTask EnsureHashedUniqueConstraintAsync(
        IIndexStore indexStore,
        BTree tableTree,
        TableSchema schema,
        IRecordSerializer readSerializer,
        int[] indexColumnIndices,
        DbValue[] keyComponents,
        long indexKey,
        string indexName,
        CancellationToken ct)
    {
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
            if (IndexRowMatchesKeyComponents(existingRow, indexColumnIndices, keyComponents))
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
            if (value.IsNull || DbValue.Compare(value, keyComponents[i]) != 0)
                return false;
        }

        return true;
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

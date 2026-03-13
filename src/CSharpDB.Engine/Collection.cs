using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Core;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

/// <summary>
/// A typed document collection backed by a B+tree.
/// Documents are serialized as JSON and stored with string keys hashed to long B+tree keys.
/// Provides a NoSQL-style API that bypasses the SQL parser/planner entirely.
/// </summary>
[RequiresUnreferencedCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
[RequiresDynamicCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
public sealed class Collection<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    T>
{
    private const int MaxProbeDistance = 128;
    private const string CollectionIndexPrefix = "_cidx_";

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly string _catalogTableName;
    private readonly BTree _tree;
    private readonly Func<bool> _isInTransaction;
    private readonly CollectionDocumentCodec<T> _codec;
    private readonly Dictionary<string, CollectionIndexBinding<T>> _indexes = new(StringComparer.OrdinalIgnoreCase);
    private long _observedSchemaVersion;

    internal Collection(
        Pager pager,
        SchemaCatalog catalog,
        string catalogTableName,
        BTree tree,
        IRecordSerializer recordSerializer,
        Func<bool> isInTransaction)
    {
        _pager = pager;
        _catalog = catalog;
        _catalogTableName = catalogTableName;
        _tree = tree;
        _codec = new CollectionDocumentCodec<T>(recordSerializer);
        _isInTransaction = isInTransaction;
        _observedSchemaVersion = catalog.SchemaVersion;
        ReloadCollectionIndexes();
    }

    // ===== Tier 1 API =====

    /// <summary>
    /// Insert or update a document by key.
    /// </summary>
    public async ValueTask PutAsync(string key, T document, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(document);

        RefreshIndexesIfSchemaChanged();
        bool mutated = false;

        await AutoCommitAsync(async () =>
        {
            long startHash = HashDocumentKey(key);
            byte[] newPayload = _codec.Encode(key, document);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                var existing = await _tree.FindMemoryAsync(probeHash, ct);

                if (existing is not { } existingPayload)
                {
                    await _tree.InsertAsync(probeHash, newPayload, ct);
                    await InsertIntoIndexesAsync(probeHash, document, ct);
                    await _catalog.AdjustTableRowCountAsync(_catalogTableName, 1, ct);
                    mutated = true;
                    return;
                }

                if (_codec.PayloadMatchesKey(existingPayload.Span, key))
                {
                    if (_indexes.Count > 0)
                        await DeleteFromIndexesAsync(probeHash, existingPayload, ct);

                    await _tree.DeleteAsync(probeHash, ct);
                    await _tree.InsertAsync(probeHash, newPayload, ct);
                    await InsertIntoIndexesAsync(probeHash, document, ct);
                    mutated = true;
                    return;
                }
            }

            throw new CSharpDbException(
                ErrorCode.Unknown,
                $"Hash collision probe limit exceeded for key '{key}'.");
        }, ct);

        if (mutated)
            await _catalog.MarkTableColumnStatisticsStaleAsync(_catalogTableName, ct);
    }

    /// <summary>
    /// Retrieve a document by key. Returns default if not found.
    /// </summary>
    public async ValueTask<T?> GetAsync(string key, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        long startHash = HashDocumentKey(key);

        for (int probe = 0; probe < MaxProbeDistance; probe++)
        {
            long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
            var payload = await _tree.FindMemoryAsync(probeHash, ct);

            if (payload is not { } payloadMemory)
                return default;

            if (_codec.TryDecodeDocumentForKey(payloadMemory.Span, key, out var doc))
                return doc;
        }

        return default;
    }

    /// <summary>
    /// Delete a document by key. Returns true if the key existed.
    /// </summary>
    public async ValueTask<bool> DeleteAsync(string key, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        RefreshIndexesIfSchemaChanged();

        bool deleted = false;

        await AutoCommitAsync(async () =>
        {
            long startHash = HashDocumentKey(key);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                var payload = await _tree.FindMemoryAsync(probeHash, ct);

                if (payload is not { } payloadMemory)
                    return;

                if (_codec.PayloadMatchesKey(payloadMemory.Span, key))
                {
                    if (_indexes.Count > 0)
                        await DeleteFromIndexesAsync(probeHash, payloadMemory, ct);

                    await _tree.DeleteAsync(probeHash, ct);
                    await _catalog.AdjustTableRowCountAsync(_catalogTableName, -1, ct);
                    deleted = true;
                    return;
                }
            }
        }, ct);

        if (deleted)
            await _catalog.MarkTableColumnStatisticsStaleAsync(_catalogTableName, ct);

        return deleted;
    }

    /// <summary>
    /// Return the number of documents in the collection.
    /// </summary>
    public async ValueTask<long> CountAsync(CancellationToken ct = default)
        => _catalog.TryGetTableRowCount(_catalogTableName, out long rowCount)
            ? rowCount
            : await _tree.CountEntriesAsync(ct);

    /// <summary>
    /// Iterate all documents in the collection.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> ScanAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var cursor = _tree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            var (key, doc) = _codec.Decode(cursor.CurrentValue.Span);
            yield return new KeyValuePair<string, T>(key, doc);
        }
    }

    // ===== Tier 2 API =====

    /// <summary>
    /// Find all documents matching a predicate (full scan + in-memory filter).
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindAsync(
        Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        await foreach (var kvp in ScanAsync(ct))
        {
            if (predicate(kvp.Value))
                yield return kvp;
        }
    }

    /// <summary>
    /// Ensure a secondary index exists for a direct field/property selector such as x => x.Age.
    /// </summary>
    public async ValueTask EnsureIndexAsync<TField>(
        Expression<Func<T, TField>> fieldSelector,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);
        RefreshIndexesIfSchemaChanged();

        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        if (_indexes.ContainsKey(fieldPath))
            return;

        string indexName = BuildCollectionIndexName(fieldPath);
        var existing = _catalog.GetIndex(indexName);
        if (existing != null)
        {
            AttachIndexBinding(existing);
            return;
        }

        if (_isInTransaction())
        {
            throw new InvalidOperationException(
                "Collection indexes cannot be created while an explicit transaction is active.");
        }

        CollectionIndexBinding<T>.ValidateFieldPath(fieldPath);

        bool createdIndex = false;
        try
        {
            await _pager.BeginTransactionAsync(ct);

            var indexSchema = new IndexSchema
            {
                IndexName = indexName,
                TableName = _catalogTableName,
                Columns = [fieldPath],
                IsUnique = false,
            };

            await _catalog.CreateIndexAsync(indexSchema, ct);
            createdIndex = true;

            var binding = AttachIndexBinding(indexSchema);
            await BackfillIndexAsync(binding, ct);

            await _catalog.PersistRootPageChangesAsync(_catalogTableName, ct);
            await _pager.CommitAsync(ct);
            _observedSchemaVersion = _catalog.SchemaVersion;
        }
        catch
        {
            if (createdIndex)
            {
                try
                {
                    await _catalog.DropIndexAsync(indexName, ct);
                }
                catch
                {
                    // Best-effort cache cleanup before rollback.
                }
            }

            _indexes.Remove(fieldPath);

            try
            {
                await _pager.RollbackAsync(ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            RefreshIndexesIfSchemaChanged(force: true);
            throw;
        }
    }

    /// <summary>
    /// Find all documents matching a field equality predicate, using a collection index when present.
    /// Falls back to a full scan when the index does not exist.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByIndexAsync<TField>(
        Expression<Func<T, TField>> fieldSelector,
        TField value,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);
        RefreshIndexesIfSchemaChanged();

        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        var comparer = EqualityComparer<TField>.Default;
        
        if (!TryGetOrAttachIndexBinding(fieldPath, out var binding) ||
            !binding.TryBuildKeyFromValue(value, out long indexKey))
        {
            var fieldAccessor = fieldSelector.Compile();

            await foreach (var kvp in ScanAsync(ct))
            {
                if (comparer.Equals(fieldAccessor(kvp.Value), value))
                    yield return kvp;
            }

            yield break;
        }

        byte[]? payload = await binding.IndexStore.FindAsync(indexKey, ct);
        if (payload == null || payload.Length < sizeof(long))
            yield break;

        int rowIdCount = payload.Length / sizeof(long);
        for (int i = 0; i < rowIdCount; i++)
        {
            long rowId = RowIdPayloadCodec.ReadAt(payload, i);
            var documentPayload = await _tree.FindMemoryAsync(rowId, ct);
            if (documentPayload is not { } documentMemory)
                continue;

            if (binding.UsesIntegerKey)
            {
                string matchedKey = _codec.DecodeKey(documentMemory.Span);
                T matchedDocument = _codec.DecodeDocument(documentMemory.Span);
                yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                continue;
            }

            if (binding.UsesTextKey &&
                value is string textValue &&
                binding.TryDirectPayloadTextEquals(documentMemory.Span, textValue))
            {
                string matchedKey = _codec.DecodeKey(documentMemory.Span);
                T matchedDocument = _codec.DecodeDocument(documentMemory.Span);
                yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                continue;
            }

            var (key, document) = _codec.Decode(documentMemory.Span);
            if (binding.MatchesValue(document, value, comparer))
                yield return new KeyValuePair<string, T>(key, document);
        }
    }

    // ===== Internal helpers =====

    /// <summary>
    /// Hash a string document key to a long B+tree key.
    /// Uses a polynomial hash with multiplier 53 (case-sensitive, distinct from catalog hashes).
    /// </summary>
    internal static long HashDocumentKey(string key)
    {
        long hash = 0;
        foreach (char c in key)
            hash = (hash * 53) + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    private void RefreshIndexesIfSchemaChanged(bool force = false)
    {
        long currentVersion = _catalog.SchemaVersion;
        if (!force && currentVersion == _observedSchemaVersion)
            return;

        ReloadCollectionIndexes();
        _observedSchemaVersion = currentVersion;
    }

    private void ReloadCollectionIndexes()
    {
        _indexes.Clear();

        foreach (var schema in _catalog.GetIndexesForTable(_catalogTableName))
        {
            if (!IsCollectionIndexSchema(schema, out string? fieldPath))
                continue;

            _indexes[fieldPath] = CollectionIndexBinding<T>.Create(
                fieldPath,
                schema.IndexName,
                _catalog.GetIndexStore(schema.IndexName));
        }
    }

    private bool TryGetOrAttachIndexBinding(string fieldPath, out CollectionIndexBinding<T> binding)
    {
        if (_indexes.TryGetValue(fieldPath, out binding!))
            return true;

        string indexName = BuildCollectionIndexName(fieldPath);
        var schema = _catalog.GetIndex(indexName);
        if (schema == null || !IsCollectionIndexSchema(schema, out _))
        {
            binding = null!;
            return false;
        }

        binding = AttachIndexBinding(schema);
        return true;
    }

    private CollectionIndexBinding<T> AttachIndexBinding(IndexSchema schema)
    {
        if (!IsCollectionIndexSchema(schema, out string? fieldPath))
        {
            throw new InvalidOperationException(
                $"Index '{schema.IndexName}' is not a collection index for '{_catalogTableName}'.");
        }

        var binding = CollectionIndexBinding<T>.Create(
            fieldPath,
            schema.IndexName,
            _catalog.GetIndexStore(schema.IndexName));
        _indexes[fieldPath] = binding;
        _observedSchemaVersion = _catalog.SchemaVersion;
        return binding;
    }

    private string BuildCollectionIndexName(string fieldPath)
    {
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(_catalogTableName);
        byte[] fieldPathBytes = Encoding.UTF8.GetBytes(fieldPath);
        return $"{CollectionIndexPrefix}{Convert.ToHexString(tableNameBytes)}_{Convert.ToHexString(fieldPathBytes)}";
    }

    private bool IsCollectionIndexSchema(IndexSchema schema, out string fieldPath)
    {
        fieldPath = string.Empty;
        if (!string.Equals(schema.TableName, _catalogTableName, StringComparison.OrdinalIgnoreCase))
            return false;
        if (!schema.IndexName.StartsWith(CollectionIndexPrefix, StringComparison.Ordinal))
            return false;
        if (schema.Columns.Count != 1)
            return false;

        fieldPath = schema.Columns[0];
        return !string.IsNullOrWhiteSpace(fieldPath);
    }

    private async ValueTask BackfillIndexAsync(CollectionIndexBinding<T> binding, CancellationToken ct)
    {
        var cursor = _tree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            if (CollectionPayloadCodec.IsDirectPayload(cursor.CurrentValue.Span))
            {
                if (binding.TryBuildKeyFromDirectPayload(cursor.CurrentValue.Span, out long directIndexKey))
                    await InsertRowIdAsync(binding.IndexStore, directIndexKey, cursor.CurrentKey, ct);

                continue;
            }

            T document = _codec.DecodeDocument(cursor.CurrentValue.Span);
            await InsertIntoIndexAsync(binding, cursor.CurrentKey, document, ct);
        }
    }

    private async ValueTask InsertIntoIndexesAsync(long rowId, T document, CancellationToken ct)
    {
        if (_indexes.Count == 0)
            return;

        foreach (var binding in _indexes.Values)
            await InsertIntoIndexAsync(binding, rowId, document, ct);
    }

    private async ValueTask DeleteFromIndexesAsync(long rowId, T document, CancellationToken ct)
    {
        if (_indexes.Count == 0)
            return;

        foreach (var binding in _indexes.Values)
            await DeleteFromIndexAsync(binding, rowId, document, ct);
    }

    private async ValueTask DeleteFromIndexesAsync(long rowId, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        if (_indexes.Count == 0)
            return;

        if (CollectionPayloadCodec.IsDirectPayload(payload.Span))
        {
            foreach (var binding in _indexes.Values)
            {
                if (binding.TryBuildKeyFromDirectPayload(payload.Span, out long indexKey))
                    await DeleteRowIdAsync(binding.IndexStore, indexKey, rowId, ct);
            }

            return;
        }

        T document = _codec.DecodeDocument(payload.Span);
        await DeleteFromIndexesAsync(rowId, document, ct);
    }

    private static async ValueTask InsertIntoIndexAsync(
        CollectionIndexBinding<T> binding,
        long rowId,
        T document,
        CancellationToken ct)
    {
        if (!binding.TryBuildKeyFromDocument(document, out long indexKey))
            return;

        await InsertRowIdAsync(binding.IndexStore, indexKey, rowId, ct);
    }

    private static async ValueTask DeleteFromIndexAsync(
        CollectionIndexBinding<T> binding,
        long rowId,
        T document,
        CancellationToken ct)
    {
        if (!binding.TryBuildKeyFromDocument(document, out long indexKey))
            return;

        await DeleteRowIdAsync(binding.IndexStore, indexKey, rowId, ct);
    }

    private static async ValueTask InsertRowIdAsync(
        IIndexStore indexStore,
        long indexKey,
        long rowId,
        CancellationToken ct)
    {
        byte[]? existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
        {
            await indexStore.InsertAsync(indexKey, RowIdPayloadCodec.CreateSingle(rowId), ct);
            return;
        }

        if (!RowIdPayloadCodec.TryInsertSorted(existing, rowId, out byte[] newPayload))
            return;

        await indexStore.DeleteAsync(indexKey, ct);
        await indexStore.InsertAsync(indexKey, newPayload, ct);
    }

    private static async ValueTask DeleteRowIdAsync(
        IIndexStore indexStore,
        long indexKey,
        long rowId,
        CancellationToken ct)
    {
        byte[]? existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
            return;

        if (!RowIdPayloadCodec.TryRemoveSorted(existing, rowId, out byte[]? newPayload))
            return;

        await indexStore.DeleteAsync(indexKey, ct);
        if (newPayload == null)
            return;

        await indexStore.InsertAsync(indexKey, newPayload, ct);
    }

    /// <summary>
    /// Auto-commit wrapper: begins a transaction, executes the action, and commits.
    /// If an explicit transaction is active, just executes directly.
    /// </summary>
    private async ValueTask AutoCommitAsync(Func<ValueTask> action, CancellationToken ct)
    {
        if (_isInTransaction())
        {
            await action();
            return;
        }

        await _pager.BeginTransactionAsync(ct);
        try
        {
            uint rootBefore = _tree.RootPageId;
            await action();
            uint rootAfter = _tree.RootPageId;
            if (rootBefore != rootAfter || _indexes.Count > 0)
                await _catalog.PersistRootPageChangesAsync(_catalogTableName, ct);
            await _pager.CommitAsync(ct);
        }
        catch
        {
            await _pager.RollbackAsync(ct);
            await _catalog.ReloadAsync(ct);
            throw;
        }
    }
}

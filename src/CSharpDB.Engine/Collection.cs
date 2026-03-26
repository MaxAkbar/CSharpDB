using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Storage.Indexing;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

/// <summary>
/// Internal interface for refreshing collection tree references after rollback.
/// </summary>
internal interface ICollectionTreeRefresh
{
    void RefreshTreeFromCatalog();
}

/// <summary>
/// A typed document collection backed by a B+tree.
/// Documents are serialized as JSON and stored with string keys hashed to long B+tree keys.
/// Provides a NoSQL-style API that bypasses the SQL parser/planner entirely.
/// </summary>
[RequiresUnreferencedCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
[RequiresDynamicCode("Collection<T> uses reflection-based JSON serialization and member binding. Use SQL API for NativeAOT scenarios.")]
public sealed class Collection<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)]
    T> : ICollectionTreeRefresh
{
    private const int MaxProbeDistance = 128;
    private const string CollectionIndexPrefix = "_cidx_";

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly string _catalogTableName;
    private BTree _tree;
    private readonly Func<bool> _isInTransaction;
    private readonly Func<CancellationToken, ValueTask<IDisposable>> _enterWriteScopeAsync;
    private readonly Func<string, CancellationToken, ValueTask<PagerCommitResult>> _beginImplicitCommitAsync;
    private readonly Func<CancellationToken, ValueTask> _afterImplicitCommitAsync;
    private readonly CollectionDocumentCodec<T> _codec;
    private readonly Dictionary<string, CollectionIndexBinding<T>> _indexes = new(StringComparer.OrdinalIgnoreCase);
    private long _observedSchemaVersion;

    internal Collection(
        Pager pager,
        SchemaCatalog catalog,
        string catalogTableName,
        BTree tree,
        IRecordSerializer recordSerializer,
        Func<bool> isInTransaction,
        Func<CancellationToken, ValueTask<IDisposable>> enterWriteScopeAsync,
        Func<string, CancellationToken, ValueTask<PagerCommitResult>> beginImplicitCommitAsync,
        Func<CancellationToken, ValueTask> afterImplicitCommitAsync)
    {
        _pager = pager;
        _catalog = catalog;
        _catalogTableName = catalogTableName;
        _tree = tree;
        _codec = new CollectionDocumentCodec<T>(recordSerializer);
        _isInTransaction = isInTransaction;
        _enterWriteScopeAsync = enterWriteScopeAsync ?? throw new ArgumentNullException(nameof(enterWriteScopeAsync));
        _beginImplicitCommitAsync = beginImplicitCommitAsync ?? throw new ArgumentNullException(nameof(beginImplicitCommitAsync));
        _afterImplicitCommitAsync = afterImplicitCommitAsync ?? throw new ArgumentNullException(nameof(afterImplicitCommitAsync));
        _observedSchemaVersion = catalog.SchemaVersion;
        ReloadCollectionIndexes();
    }

    /// <summary>
    /// Refresh the underlying BTree reference from the catalog after rollback.
    /// </summary>
    void ICollectionTreeRefresh.RefreshTreeFromCatalog()
    {
        _tree = _catalog.GetTableTree(_catalogTableName);
        _observedSchemaVersion = _catalog.SchemaVersion;
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
                    if (HasZeroCachedRowCount())
                        await SyncRowCountAsync(ct);
                    else
                        await _catalog.AdjustTableRowCountAsync(_catalogTableName, 1, ct);
                    mutated = true;
                    return;
                }

                if (_codec.PayloadMatchesKey(existingPayload.Span, key))
                {
                    if (_indexes.Count > 0)
                        await DeleteFromIndexesAsync(probeHash, existingPayload, ct);

                    await _tree.ReplaceAsync(probeHash, newPayload, ct);
                    await InsertIntoIndexesAsync(probeHash, document, ct);
                    if (ShouldReconcileRowCount(existingPayload.Span))
                        await SyncRowCountAsync(ct);
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
                    if (ShouldReconcileRowCount(payloadMemory.Span))
                        await SyncRowCountAsync(ct);
                    else
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
        => _catalog.TryGetTableRowCount(_catalogTableName, out long rowCount) && rowCount > 0
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
    /// Find all documents matching a field/property selector path, using a collection index when present
    /// and falling back to direct payload path matching when absent.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByPathAsync<TField>(
        Expression<Func<T, TField>> fieldSelector,
        TField value,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);
        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        await foreach (var match in FindByFieldPathCoreAsync(fieldPath, value, ct))
            yield return match;
    }

    /// <summary>
    /// Find all documents matching a path equality predicate, or array-contains predicate for terminal
    /// array-element paths such as <c>Tags[]</c> or <c>$.tags[]</c>. Uses a collection index when present
    /// and falls back to direct payload path matching when absent.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByPathAsync<TField>(
        string fieldPath,
        TField value,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        string canonicalFieldPath = CollectionIndexBinding<T>.NormalizeFieldPath(fieldPath);
        await foreach (var match in FindByFieldPathCoreAsync(canonicalFieldPath, value, ct))
            yield return match;
    }

    /// <summary>
    /// Find all documents whose scalar path value falls within the supplied bounded range.
    /// Array-element paths are not supported for range queries.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByPathRangeAsync<TField>(
        Expression<Func<T, TField>> fieldSelector,
        TField lowerBound,
        TField upperBound,
        bool lowerInclusive = true,
        bool upperInclusive = true,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);
        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        await foreach (var match in FindByFieldPathRangeCoreAsync(
            fieldPath,
            lowerBound,
            upperBound,
            lowerInclusive,
            upperInclusive,
            ct))
        {
            yield return match;
        }
    }

    /// <summary>
    /// Find all documents whose scalar path value falls within the supplied bounded range.
    /// Supports paths such as <c>Age</c> or <c>$.address.zipCode</c>.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByPathRangeAsync<TField>(
        string fieldPath,
        TField lowerBound,
        TField upperBound,
        bool lowerInclusive = true,
        bool upperInclusive = true,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        string canonicalFieldPath = CollectionIndexBinding<T>.NormalizeFieldPath(fieldPath);
        await foreach (var match in FindByFieldPathRangeCoreAsync(
            canonicalFieldPath,
            lowerBound,
            upperBound,
            lowerInclusive,
            upperInclusive,
            ct))
        {
            yield return match;
        }
    }

    /// <summary>
    /// Ensure a secondary index exists for a field/property selector path such as x => x.Age or x => x.Address.City.
    /// </summary>
    public async ValueTask EnsureIndexAsync<TField>(
        Expression<Func<T, TField>> fieldSelector,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(fieldSelector);
        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        await EnsureIndexCoreAsync(fieldPath, ct);
    }

    /// <summary>
    /// Ensure a secondary index exists for a path such as <c>Address.City</c> or <c>$.address.city</c>.
    /// </summary>
    public ValueTask EnsureIndexAsync(
        string fieldPath,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);
        return EnsureIndexCoreAsync(CollectionIndexBinding<T>.NormalizeFieldPath(fieldPath), ct);
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
        string fieldPath = CollectionIndexBinding<T>.GetFieldPath(fieldSelector);
        await foreach (var match in FindByFieldPathCoreAsync(fieldPath, value, ct))
            yield return match;
    }

    /// <summary>
    /// Find all documents matching a path equality predicate, using a collection index when present.
    /// Supports paths such as <c>Address.City</c> or <c>$.address.city</c>.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindByIndexAsync<TField>(
        string fieldPath,
        TField value,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        string canonicalFieldPath = CollectionIndexBinding<T>.NormalizeFieldPath(fieldPath);
        await foreach (var match in FindByFieldPathCoreAsync(
            canonicalFieldPath,
            value,
            ct))
        {
            yield return match;
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
        var groupedRowIds = new SortedDictionary<long, List<long>>();
        var groupedTextRowIds = new SortedDictionary<long, SortedDictionary<string, List<long>>>();
        var indexKeys = new HashSet<long>();
        var textValues = new HashSet<string>(StringComparer.Ordinal);

        while (await cursor.MoveNextAsync(ct))
        {
            long rowId = cursor.CurrentKey;

            if (binding.UsesTextKey)
            {
                textValues.Clear();
                if (CollectionPayloadCodec.IsDirectPayload(cursor.CurrentValue.Span))
                {
                    if (!binding.TryCollectTextValuesFromDirectPayload(cursor.CurrentValue.Span, textValues))
                        continue;
                }
                else
                {
                    T textDocument = _codec.DecodeDocument(cursor.CurrentValue.Span);
                    if (!binding.TryCollectTextValuesFromDocument(textDocument, textValues))
                        continue;
                }

                foreach (string textValue in textValues)
                    AddGroupedTextRowId(groupedTextRowIds, textValue, rowId);

                continue;
            }

            indexKeys.Clear();
            if (CollectionPayloadCodec.IsDirectPayload(cursor.CurrentValue.Span))
            {
                if (binding.TryCollectKeysFromDirectPayload(cursor.CurrentValue.Span, indexKeys))
                {
                    foreach (long directIndexKey in indexKeys)
                        AddGroupedRowId(groupedRowIds, directIndexKey, rowId);
                }

                continue;
            }

            T document = _codec.DecodeDocument(cursor.CurrentValue.Span);
            if (!binding.TryCollectKeysFromDocument(document, indexKeys))
                continue;

            foreach (long indexKey in indexKeys)
                AddGroupedRowId(groupedRowIds, indexKey, rowId);
        }

        foreach (var entry in groupedRowIds)
            await binding.IndexStore.InsertAsync(entry.Key, RowIdPayloadCodec.CreateFromSorted(entry.Value), ct);

        foreach (var entry in groupedTextRowIds)
            await binding.IndexStore.InsertAsync(entry.Key, OrderedTextIndexPayloadCodec.CreateFromSorted(entry.Value), ct);
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
                if (binding.UsesTextKey)
                {
                    var textValues = new HashSet<string>(StringComparer.Ordinal);
                    if (!binding.TryCollectTextValuesFromDirectPayload(payload.Span, textValues))
                        continue;

                    foreach (string textValue in textValues)
                        await DeleteTextRowIdAsync(binding.IndexStore, textValue, rowId, ct);

                    continue;
                }

                var indexKeys = new HashSet<long>();
                if (!binding.TryCollectKeysFromDirectPayload(payload.Span, indexKeys))
                    continue;

                foreach (long indexKey in indexKeys)
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
        if (binding.UsesTextKey)
        {
            var textValues = new HashSet<string>(StringComparer.Ordinal);
            if (!binding.TryCollectTextValuesFromDocument(document, textValues))
                return;

            foreach (string textValue in textValues)
                await InsertTextRowIdAsync(binding.IndexStore, textValue, rowId, ct);

            return;
        }

        var indexKeys = new HashSet<long>();
        if (!binding.TryCollectKeysFromDocument(document, indexKeys))
            return;

        foreach (long indexKey in indexKeys)
            await InsertRowIdAsync(binding.IndexStore, indexKey, rowId, ct);
    }

    private static async ValueTask DeleteFromIndexAsync(
        CollectionIndexBinding<T> binding,
        long rowId,
        T document,
        CancellationToken ct)
    {
        if (binding.UsesTextKey)
        {
            var textValues = new HashSet<string>(StringComparer.Ordinal);
            if (!binding.TryCollectTextValuesFromDocument(document, textValues))
                return;

            foreach (string textValue in textValues)
                await DeleteTextRowIdAsync(binding.IndexStore, textValue, rowId, ct);

            return;
        }

        var indexKeys = new HashSet<long>();
        if (!binding.TryCollectKeysFromDocument(document, indexKeys))
            return;

        foreach (long indexKey in indexKeys)
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

        if (!RowIdPayloadCodec.TryInsert(existing, rowId, out byte[] newPayload))
            return;

        await indexStore.DeleteAsync(indexKey, ct);
        await indexStore.InsertAsync(indexKey, newPayload, ct);
    }

    private static async ValueTask InsertTextRowIdAsync(
        IIndexStore indexStore,
        string text,
        long rowId,
        CancellationToken ct)
    {
        long indexKey = OrderedTextIndexKeyCodec.ComputeKey(text);
        byte[]? existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
        {
            await indexStore.InsertAsync(indexKey, OrderedTextIndexPayloadCodec.CreateSingle(text, rowId), ct);
            return;
        }

        if (!OrderedTextIndexPayloadCodec.IsEncoded(existing))
        {
            throw new InvalidOperationException(
                "Collection text index payload format mismatch detected. Drop and recreate the text collection index.");
        }

        byte[] newPayload = OrderedTextIndexPayloadCodec.Insert(existing, text, rowId, out bool changed);
        if (!changed)
            return;

        await indexStore.ReplaceAsync(indexKey, newPayload, ct);
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

        if (!RowIdPayloadCodec.TryRemove(existing, rowId, out byte[]? newPayload))
            return;

        if (newPayload == null)
        {
            await indexStore.DeleteAsync(indexKey, ct);
            return;
        }

        await indexStore.ReplaceAsync(indexKey, newPayload, ct);
    }

    private bool HasZeroCachedRowCount()
        => _catalog.TryGetTableRowCount(_catalogTableName, out long rowCount) && rowCount == 0;

    private bool ShouldReconcileRowCount(ReadOnlySpan<byte> payload)
        => HasZeroCachedRowCount() ||
           (_codec.UsesDirectPayloadFormat && !CollectionPayloadCodec.IsDirectPayload(payload));

    private async ValueTask SyncRowCountAsync(CancellationToken ct)
    {
        long rowCount = await _tree.CountEntriesAsync(ct);
        await _catalog.SetTableRowCountAsync(_catalogTableName, rowCount, ct);
    }

    private static async ValueTask DeleteTextRowIdAsync(
        IIndexStore indexStore,
        string text,
        long rowId,
        CancellationToken ct)
    {
        long indexKey = OrderedTextIndexKeyCodec.ComputeKey(text);
        byte[]? existing = await indexStore.FindAsync(indexKey, ct);
        if (existing == null)
            return;

        if (!OrderedTextIndexPayloadCodec.IsEncoded(existing))
        {
            throw new InvalidOperationException(
                "Collection text index payload format mismatch detected. Drop and recreate the text collection index.");
        }

        byte[]? newPayload = OrderedTextIndexPayloadCodec.Remove(existing, text, rowId, out bool changed);
        if (!changed)
            return;

        await indexStore.DeleteAsync(indexKey, ct);
        if (newPayload == null)
            return;

        await indexStore.InsertAsync(indexKey, newPayload, ct);
    }

    private async ValueTask EnsureIndexCoreAsync(string fieldPath, CancellationToken ct)
    {
        RefreshIndexesIfSchemaChanged();

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

        using var writeScope = await _enterWriteScopeAsync(ct);
        RefreshIndexesIfSchemaChanged();

        if (_indexes.ContainsKey(fieldPath))
            return;

        existing = _catalog.GetIndex(indexName);
        if (existing != null)
        {
            AttachIndexBinding(existing);
            return;
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
                Kind = IndexKind.Collection,
            };

            await _catalog.CreateIndexAsync(indexSchema, ct);
            createdIndex = true;

            var binding = AttachIndexBinding(indexSchema);
            await BackfillIndexAsync(binding, ct);

            PagerCommitResult commit = await _beginImplicitCommitAsync(_catalogTableName, ct);
            await commit.WaitAsync(ct);
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

    private async IAsyncEnumerable<KeyValuePair<string, T>> FindByFieldPathCoreAsync<TField>(
        string fieldPath,
        TField value,
        [EnumeratorCancellation] CancellationToken ct)
    {
        RefreshIndexesIfSchemaChanged();

        CollectionIndexBinding<T> binding = TryGetOrAttachIndexBinding(fieldPath, out var attachedBinding)
            ? attachedBinding
            : CollectionIndexBinding<T>.CreateTransient(fieldPath);
        var comparer = EqualityComparer<TField>.Default;

        if (!TryGetOrAttachIndexBinding(fieldPath, out attachedBinding) ||
            !attachedBinding.TryBuildKeyFromValue(value, out long indexKey))
        {
            bool canCompareDirectPayload = CollectionIndexBinding<T>.TryConvertComparableValue(value, out var expectedValue);
            var cursor = _tree.CreateCursor();
            while (await cursor.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> fallbackPayload = cursor.CurrentValue.Span;
                if (canCompareDirectPayload && CollectionPayloadCodec.IsDirectPayload(fallbackPayload))
                {
                    if (!binding.TryDirectPayloadValueEquals(fallbackPayload, expectedValue))
                        continue;

                    var (matchedKey, matchedDocument) = _codec.Decode(fallbackPayload);
                    yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                    continue;
                }

                var (fallbackKey, fallbackDocument) = _codec.Decode(fallbackPayload);
                if (binding.MatchesValue(fallbackDocument, value, comparer))
                    yield return new KeyValuePair<string, T>(fallbackKey, fallbackDocument);
            }

            yield break;
        }

        byte[]? payload = await binding.IndexStore.FindAsync(indexKey, ct);
        if (payload == null)
            yield break;

        ReadOnlyMemory<byte> rowIdPayload = payload;
        if (attachedBinding.UsesTextKey)
        {
            if (!CollectionIndexBinding<T>.TryConvertComparableValue(value, out var expectedValue) ||
                expectedValue.Type != DbType.Text)
            {
                yield break;
            }

            if (!OrderedTextIndexPayloadCodec.IsEncoded(payload) ||
                !OrderedTextIndexPayloadCodec.TryGetMatchingRowIdPayloadSlice(payload, expectedValue.AsText, out rowIdPayload))
            {
                yield break;
            }
        }

        if (rowIdPayload.Length < sizeof(long))
            yield break;

        int rowIdCount = rowIdPayload.Length / sizeof(long);
        for (int i = 0; i < rowIdCount; i++)
        {
            long rowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, i);
            var documentPayload = await _tree.FindMemoryAsync(rowId, ct);
            if (documentPayload is not { } documentMemory)
                continue;

            if (attachedBinding.UsesIntegerKey)
            {
                string matchedKey = _codec.DecodeKey(documentMemory.Span);
                T matchedDocument = _codec.DecodeDocument(documentMemory.Span);
                yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                continue;
            }

            if (CollectionIndexBinding<T>.TryConvertComparableValue(value, out var expectedValue) &&
                CollectionPayloadCodec.IsDirectPayload(documentMemory.Span) &&
                binding.TryDirectPayloadValueEquals(documentMemory.Span, expectedValue))
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

    private async IAsyncEnumerable<KeyValuePair<string, T>> FindByFieldPathRangeCoreAsync<TField>(
        string fieldPath,
        TField lowerBound,
        TField upperBound,
        bool lowerInclusive,
        bool upperInclusive,
        [EnumeratorCancellation] CancellationToken ct)
    {
        RefreshIndexesIfSchemaChanged();

        bool hasIndex = TryGetOrAttachIndexBinding(fieldPath, out var attachedBinding);
        CollectionIndexBinding<T> binding = hasIndex
            ? attachedBinding
            : CollectionIndexBinding<T>.CreateTransient(fieldPath);

        if (binding.IsMultiValueArray)
        {
            throw new NotSupportedException(
                "Collection path range queries currently support only scalar field paths.");
        }

        if (!CollectionIndexBinding<T>.TryConvertComparableValue(lowerBound, out var lowerValue) ||
            !CollectionIndexBinding<T>.TryConvertComparableValue(upperBound, out var upperValue))
        {
            throw new NotSupportedException(
                "Collection path range queries currently support only string and integer bounds.");
        }

        if (lowerValue.Type != upperValue.Type)
        {
            throw new ArgumentException(
                "Collection path range query bounds must use the same comparable type.",
                nameof(upperBound));
        }

        int boundComparison = DbValue.Compare(lowerValue, upperValue);
        if (boundComparison > 0 ||
            (boundComparison == 0 && (!lowerInclusive || !upperInclusive)))
        {
            yield break;
        }

        long lowerKey = 0;
        long upperKey = 0;
        bool canUseOrderedIndex =
            hasIndex &&
            attachedBinding.SupportsOrderedRange &&
            lowerValue.Type == upperValue.Type &&
            ((attachedBinding.UsesIntegerKey && lowerValue.Type == DbType.Integer) ||
             (attachedBinding.UsesTextKey && lowerValue.Type == DbType.Text));

        if (canUseOrderedIndex)
        {
            canUseOrderedIndex =
                attachedBinding.TryBuildKeyFromValue(lowerBound, out lowerKey) &&
                attachedBinding.TryBuildKeyFromValue(upperBound, out upperKey);
        }

        if (!canUseOrderedIndex)
        {
            var cursor = _tree.CreateCursor();
            while (await cursor.MoveNextAsync(ct))
            {
                ReadOnlySpan<byte> payload = cursor.CurrentValue.Span;
                if (CollectionPayloadCodec.IsDirectPayload(payload))
                {
                    if (!binding.TryDirectPayloadValueInRange(payload, lowerValue, lowerInclusive, upperValue, upperInclusive))
                        continue;

                    var (matchedKey, matchedDocument) = _codec.Decode(payload);
                    yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                    continue;
                }

                var (fallbackKey, fallbackDocument) = _codec.Decode(payload);
                if (binding.MatchesRangeValue(fallbackDocument, lowerValue, lowerInclusive, upperValue, upperInclusive))
                    yield return new KeyValuePair<string, T>(fallbackKey, fallbackDocument);
            }

            yield break;
        }

        var range = new IndexScanRange(lowerKey, lowerInclusive, upperKey, upperInclusive);
        var indexCursor = attachedBinding.IndexStore.CreateCursor(range);
        var textRowIds = attachedBinding.UsesTextKey
            ? new List<long>()
            : null;
        while (await indexCursor.MoveNextAsync(ct))
        {
            ReadOnlyMemory<byte> rowIdPayload = indexCursor.CurrentValue;
            if (attachedBinding.UsesTextKey)
            {
                if (!OrderedTextIndexPayloadCodec.IsEncoded(rowIdPayload.Span))
                    continue;

                textRowIds!.Clear();
                if (!OrderedTextIndexPayloadCodec.TryCollectMatchingRowIdsInRange(
                        rowIdPayload.Span,
                        lowerValue.AsText,
                        lowerInclusive,
                        upperValue.AsText,
                        upperInclusive,
                        textRowIds))
                {
                    continue;
                }

                for (int i = 0; i < textRowIds.Count; i++)
                {
                    long rowId = textRowIds[i];
                    var documentPayload = await _tree.FindMemoryAsync(rowId, ct);
                    if (documentPayload is not { } documentMemory)
                        continue;

                    if (CollectionPayloadCodec.IsDirectPayload(documentMemory.Span))
                    {
                        if (!binding.TryDirectPayloadValueInRange(documentMemory.Span, lowerValue, lowerInclusive, upperValue, upperInclusive))
                            continue;

                        string matchedKey = _codec.DecodeKey(documentMemory.Span);
                        T matchedDocument = _codec.DecodeDocument(documentMemory.Span);
                        yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                        continue;
                    }

                    var (key, document) = _codec.Decode(documentMemory.Span);
                    if (binding.MatchesRangeValue(document, lowerValue, lowerInclusive, upperValue, upperInclusive))
                        yield return new KeyValuePair<string, T>(key, document);
                }

                continue;
            }

            if (rowIdPayload.Length < sizeof(long))
                continue;

            int rowIdCount = rowIdPayload.Length / sizeof(long);
            for (int i = 0; i < rowIdCount; i++)
            {
                long rowId = RowIdPayloadCodec.ReadAt(rowIdPayload.Span, i);
                var documentPayload = await _tree.FindMemoryAsync(rowId, ct);
                if (documentPayload is not { } documentMemory)
                    continue;

                if (CollectionPayloadCodec.IsDirectPayload(documentMemory.Span))
                {
                    if (!binding.TryDirectPayloadValueInRange(documentMemory.Span, lowerValue, lowerInclusive, upperValue, upperInclusive))
                        continue;

                    string matchedKey = _codec.DecodeKey(documentMemory.Span);
                    T matchedDocument = _codec.DecodeDocument(documentMemory.Span);
                    yield return new KeyValuePair<string, T>(matchedKey, matchedDocument);
                    continue;
                }

                var (key, document) = _codec.Decode(documentMemory.Span);
                if (binding.MatchesRangeValue(document, lowerValue, lowerInclusive, upperValue, upperInclusive))
                    yield return new KeyValuePair<string, T>(key, document);
            }
        }
    }

    private static void AddGroupedRowId(
        SortedDictionary<long, List<long>> groupedRowIds,
        long indexKey,
        long rowId)
    {
        if (!groupedRowIds.TryGetValue(indexKey, out var rowIds))
        {
            rowIds = new List<long>();
            groupedRowIds[indexKey] = rowIds;
        }

        rowIds.Add(rowId);
    }

    private static void AddGroupedTextRowId(
        SortedDictionary<long, SortedDictionary<string, List<long>>> groupedTextRowIds,
        string text,
        long rowId)
    {
        long indexKey = OrderedTextIndexKeyCodec.ComputeKey(text);
        if (!groupedTextRowIds.TryGetValue(indexKey, out var textBuckets))
        {
            textBuckets = new SortedDictionary<string, List<long>>(StringComparer.Ordinal);
            groupedTextRowIds[indexKey] = textBuckets;
        }

        if (!textBuckets.TryGetValue(text, out var rowIds))
        {
            rowIds = new List<long>();
            textBuckets[text] = rowIds;
        }

        rowIds.Add(rowId);
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

        PagerCommitResult commit = PagerCommitResult.Completed;
        IDisposable? writeScope = null;
        try
        {
            writeScope = await _enterWriteScopeAsync(ct);
            await _pager.BeginTransactionAsync(ct);
            await action();
            commit = await _beginImplicitCommitAsync(_catalogTableName, ct);
        }
        catch
        {
            await _pager.RollbackAsync(ct);
            await _catalog.ReloadAsync(ct);
            throw;
        }
        finally
        {
            writeScope?.Dispose();
        }

        await commit.WaitAsync(ct);
        await _afterImplicitCommitAsync(ct);
    }
}

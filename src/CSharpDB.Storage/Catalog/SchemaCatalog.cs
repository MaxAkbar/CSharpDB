using CSharpDB.Core;

namespace CSharpDB.Storage.Catalog;

/// <summary>
/// The schema catalog stores table metadata in a B+tree.
/// Key = hash of table name, Value = serialized TableSchema.
/// Also maintains an in-memory cache of all schemas.
///
/// Index metadata is stored in a separate sub-tree whose root page is
/// stored as a sentinel entry (key = long.MaxValue) in the main catalog tree.
///
/// View definitions are stored as sentinel entry (key = long.MaxValue - 1)
/// pointing to a sub-tree where each entry maps view name hash to SQL text.
/// </summary>
public sealed class SchemaCatalog
{
    private const long IndexCatalogSentinel = long.MaxValue;
    private const long ViewCatalogSentinel = long.MaxValue - 1;
    private const long TriggerCatalogSentinel = long.MaxValue - 2;

    private readonly Pager _pager;
    private readonly ISchemaSerializer _schemaSerializer;
    private readonly IIndexProvider _indexProvider;
    private readonly ICatalogStore _catalogStore;
    private readonly CatalogCache _cacheState = new();
    private BTree? _catalogTree;
    private Dictionary<string, TableSchema> _cache => _cacheState.Tables;
    private Dictionary<string, uint> _tableRootPages => _cacheState.TableRootPages;
    private Dictionary<string, BTree> _tableTrees => _cacheState.TableTrees;

    // Index catalog
    private BTree? _indexCatalogTree;
    private Dictionary<string, IndexSchema> _indexCache => _cacheState.Indexes;
    private Dictionary<string, uint> _indexRootPages => _cacheState.IndexRootPages;
    private Dictionary<string, IIndexStore> _indexStores => _cacheState.IndexStores;
    private Dictionary<string, IndexSchema[]> _indexesByTable => _cacheState.IndexesByTable;

    // View catalog
    private BTree? _viewCatalogTree;
    private Dictionary<string, string> _viewCache => _cacheState.Views; // viewName -> SQL

    // Trigger catalog
    private BTree? _triggerCatalogTree;
    private Dictionary<string, TriggerSchema> _triggerCache => _cacheState.Triggers;

    private SchemaCatalog(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore)
    {
        _pager = pager;
        _schemaSerializer = schemaSerializer;
        _indexProvider = indexProvider;
        _catalogStore = catalogStore;
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            new DefaultSchemaSerializer(),
            new BTreeIndexProvider(),
            new CatalogStore(),
            ct);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            schemaSerializer,
            indexProvider,
            new CatalogStore(),
            ct);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        CancellationToken ct = default)
    {
        var catalog = new SchemaCatalog(pager, schemaSerializer, indexProvider, catalogStore);

        if (pager.SchemaRootPage != PageConstants.NullPageId)
        {
            catalog._catalogTree = new BTree(pager, pager.SchemaRootPage);
            await catalog.LoadAllAsync(ct);
        }

        return catalog;
    }

    private async ValueTask EnsureCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_catalogTree != null) return;

        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _pager.SchemaRootPage = rootPage;
        _catalogTree = new BTree(_pager, rootPage);
    }

    private async ValueTask EnsureIndexCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_indexCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _indexCatalogTree = new BTree(_pager, rootPage);

        // Store the root page as sentinel in the main catalog
        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        // Delete existing sentinel if any, then insert
        try { await _catalogTree!.DeleteAsync(IndexCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(IndexCatalogSentinel, payload, ct);
    }

    private async ValueTask EnsureViewCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_viewCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _viewCatalogTree = new BTree(_pager, rootPage);

        // Store the root page as sentinel in the main catalog
        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(ViewCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(ViewCatalogSentinel, payload, ct);
    }

    private async ValueTask EnsureTriggerCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_triggerCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _triggerCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(TriggerCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(TriggerCatalogSentinel, payload, ct);
    }

    private async ValueTask LoadAllAsync(CancellationToken ct = default)
    {
        var cursor = _catalogTree!.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            if (cursor.CurrentKey == IndexCatalogSentinel)
            {
                uint indexRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _indexCatalogTree = new BTree(_pager, indexRootPage);
                continue;
            }

            if (cursor.CurrentKey == ViewCatalogSentinel)
            {
                uint viewRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _viewCatalogTree = new BTree(_pager, viewRootPage);
                continue;
            }

            if (cursor.CurrentKey == TriggerCatalogSentinel)
            {
                uint triggerRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _triggerCatalogTree = new BTree(_pager, triggerRootPage);
                continue;
            }

            var data = cursor.CurrentValue;
            // Data format: [4 bytes root page ID] [schema bytes]
            uint rootPage = _catalogStore.ReadRootPage(data.Span);
            var schema = _schemaSerializer.Deserialize(data.Span[4..]);
            _cache[schema.TableName] = schema;
            _tableRootPages[schema.TableName] = rootPage;
        }

        // Load index entries
        if (_indexCatalogTree != null)
        {
            var idxCursor = _indexCatalogTree.CreateCursor();
            while (await idxCursor.MoveNextAsync(ct))
            {
                var data = idxCursor.CurrentValue;
                uint rootPage = _catalogStore.ReadRootPage(data.Span);
                var indexSchema = _schemaSerializer.DeserializeIndex(data.Span[4..]);
                _indexCache[indexSchema.IndexName] = indexSchema;
                _indexRootPages[indexSchema.IndexName] = rootPage;
                _indexStores[indexSchema.IndexName] = _indexProvider.CreateIndexStore(_pager, rootPage);
                AddIndexToTableCache(indexSchema);
            }
        }

        // Load view entries
        if (_viewCatalogTree != null)
        {
            var viewCursor = _viewCatalogTree.CreateCursor();
            while (await viewCursor.MoveNextAsync(ct))
            {
                var data = viewCursor.CurrentValue;
                string viewName = _catalogStore.ReadLengthPrefixedString(data.Span, 0, out int offset);
                string sql = _catalogStore.ReadLengthPrefixedString(data.Span, offset, out _);
                _viewCache[viewName] = sql;
            }
        }

        // Load trigger entries
        if (_triggerCatalogTree != null)
        {
            var trigCursor = _triggerCatalogTree.CreateCursor();
            while (await trigCursor.MoveNextAsync(ct))
            {
                var triggerSchema = _schemaSerializer.DeserializeTrigger(trigCursor.CurrentValue.Span);
                _triggerCache[triggerSchema.TriggerName] = triggerSchema;
            }
        }
    }

    // ============ TABLE operations ============

    public TableSchema? GetTable(string tableName)
    {
        _cache.TryGetValue(tableName, out var schema);
        return schema;
    }

    public uint GetTableRootPage(string tableName)
    {
        if (_tableRootPages.TryGetValue(tableName, out var rootPage))
            return rootPage;
        throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");
    }

    public IReadOnlyCollection<string> GetTableNames() => _cache.Keys;

    /// <summary>
    /// Persist table/index root page changes caused by B+tree root splits.
    /// Uses cached tree instances and only rewrites catalog entries when a root ID changed.
    /// </summary>
    public async ValueTask PersistRootPageChangesAsync(string tableName, CancellationToken ct = default)
    {
        await PersistTableRootPageChangeAsync(tableName, ct);

        foreach (var idx in GetIndexesForTable(tableName))
            await PersistIndexRootPageChangeAsync(idx.IndexName, ct);
    }

    public async ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct = default)
    {
        if (_cache.ContainsKey(schema.TableName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Table '{schema.TableName}' already exists.");

        await EnsureCatalogTreeAsync(ct);

        // Create a new B+tree for the table's data
        uint tableRootPage = await BTree.CreateNewAsync(_pager, ct);

        // Serialize: [rootPage:4 bytes] [schema bytes]
        var schemaBytes = _schemaSerializer.Serialize(schema);
        var payload = _catalogStore.WriteRootPayload(tableRootPage, schemaBytes);

        long key = _schemaSerializer.TableNameToKey(schema.TableName);
        await _catalogTree!.InsertAsync(key, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[schema.TableName] = schema;
        _tableRootPages[schema.TableName] = tableRootPage;
        _tableTrees[schema.TableName] = new BTree(_pager, tableRootPage);
    }

    public async ValueTask DropTableAsync(string tableName, CancellationToken ct = default)
    {
        if (!_cache.ContainsKey(tableName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        // Also drop all indexes on this table
        var indexesToDrop = GetIndexesForTable(tableName);
        foreach (var idx in indexesToDrop)
            await DropIndexAsync(idx.IndexName, ct);

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache.Remove(tableName);
        _tableRootPages.Remove(tableName);
        _tableTrees.Remove(tableName);
    }

    /// <summary>
    /// Updates the schema for an existing table while keeping the same data root page.
    /// Used by ALTER TABLE operations.
    /// </summary>
    public async ValueTask UpdateTableSchemaAsync(string oldTableName, TableSchema newSchema, CancellationToken ct = default)
    {
        if (!_tableRootPages.TryGetValue(oldTableName, out uint rootPage))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{oldTableName}' not found.");

        // Delete old catalog entry
        long oldKey = _schemaSerializer.TableNameToKey(oldTableName);
        await _catalogTree!.DeleteAsync(oldKey, ct);
        _cache.Remove(oldTableName);
        _tableRootPages.Remove(oldTableName);

        // Insert new catalog entry with same root page
        var schemaBytes = _schemaSerializer.Serialize(newSchema);
        var payload = _catalogStore.WriteRootPayload(rootPage, schemaBytes);

        long newKey = _schemaSerializer.TableNameToKey(newSchema.TableName);
        await _catalogTree!.InsertAsync(newKey, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[newSchema.TableName] = newSchema;
        _tableRootPages[newSchema.TableName] = rootPage;

        if (_tableTrees.Remove(oldTableName, out var existingTree))
            _tableTrees[newSchema.TableName] = existingTree;
    }

    /// <summary>
    /// Get the B+tree for a table's data.
    /// </summary>
    public BTree GetTableTree(string tableName)
    {
        if (_tableTrees.TryGetValue(tableName, out var tree))
            return tree;

        uint rootPage = GetTableRootPage(tableName);
        tree = new BTree(_pager, rootPage);
        _tableTrees[tableName] = tree;
        return tree;
    }

    /// <summary>
    /// Get the B+tree for a table's data, using a specified pager.
    /// Used by snapshot readers to route reads through a snapshot pager.
    /// </summary>
    public BTree GetTableTree(string tableName, Pager pager)
    {
        if (ReferenceEquals(pager, _pager))
            return GetTableTree(tableName);

        uint rootPage = GetTableRootPage(tableName);
        return new BTree(pager, rootPage);
    }

    // ============ INDEX operations ============

    public IndexSchema? GetIndex(string indexName)
    {
        _indexCache.TryGetValue(indexName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<IndexSchema> GetIndexes() => _indexCache.Values.ToArray();

    public IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName)
    {
        if (_indexesByTable.TryGetValue(tableName, out var indexes))
            return indexes;

        return Array.Empty<IndexSchema>();
    }

    public BTree GetIndexTree(string indexName)
    {
        return GetIndexStore(indexName).Tree;
    }

    /// <summary>
    /// Get the B+tree for an index, using a specified pager.
    /// Used by snapshot readers to route reads through a snapshot pager.
    /// </summary>
    public BTree GetIndexTree(string indexName, Pager pager)
    {
        return GetIndexStore(indexName, pager).Tree;
    }

    /// <summary>
    /// Get an index store using the catalog pager.
    /// </summary>
    public IIndexStore GetIndexStore(string indexName)
    {
        if (_indexStores.TryGetValue(indexName, out var store))
            return store;

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage))
        {
            store = _indexProvider.CreateIndexStore(_pager, rootPage);
            _indexStores[indexName] = store;
            return store;
        }

        throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
    }

    /// <summary>
    /// Get an index store routed to a specific pager (for snapshot readers).
    /// </summary>
    public IIndexStore GetIndexStore(string indexName, Pager pager)
    {
        if (ReferenceEquals(pager, _pager))
            return GetIndexStore(indexName);

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage))
            return _indexProvider.CreateIndexStore(pager, rootPage);
        throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
    }

    public async ValueTask CreateIndexAsync(IndexSchema schema, CancellationToken ct = default)
    {
        if (_indexCache.ContainsKey(schema.IndexName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Index '{schema.IndexName}' already exists.");

        await EnsureIndexCatalogTreeAsync(ct);

        // Create a new B+tree for the index data
        uint indexRootPage = await BTree.CreateNewAsync(_pager, ct);

        // Serialize: [rootPage:4 bytes] [index schema bytes]
        var indexBytes = _schemaSerializer.SerializeIndex(schema);
        var payload = _catalogStore.WriteRootPayload(indexRootPage, indexBytes);

        long key = _schemaSerializer.IndexNameToKey(schema.IndexName);
        await _indexCatalogTree!.InsertAsync(key, payload, ct);

        _indexCache[schema.IndexName] = schema;
        _indexRootPages[schema.IndexName] = indexRootPage;
        _indexStores[schema.IndexName] = _indexProvider.CreateIndexStore(_pager, indexRootPage);
        AddIndexToTableCache(schema);
    }

    public async ValueTask DropIndexAsync(string indexName, CancellationToken ct = default)
    {
        if (!_indexCache.TryGetValue(indexName, out var schema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");

        long key = _schemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);

        _indexCache.Remove(indexName);
        _indexRootPages.Remove(indexName);
        _indexStores.Remove(indexName);
        RemoveIndexFromTableCache(schema);
    }

    // ============ VIEW operations ============

    public string? GetViewSql(string viewName)
    {
        _viewCache.TryGetValue(viewName, out var sql);
        return sql;
    }

    public IReadOnlyCollection<string> GetViewNames() => _viewCache.Keys.ToArray();

    public bool IsView(string name) => _viewCache.ContainsKey(name);

    public async ValueTask CreateViewAsync(string viewName, string sql, CancellationToken ct = default)
    {
        if (_viewCache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"View '{viewName}' already exists.");

        // Views must not conflict with table names
        if (_cache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"A table named '{viewName}' already exists.");

        await EnsureViewCatalogTreeAsync(ct);

        // Serialize: [nameLen:4][nameUtf8][sqlLen:4][sqlUtf8]
        var payload = _catalogStore.WriteLengthPrefixedStrings(viewName, sql);
        long key = _schemaSerializer.ViewNameToKey(viewName);
        await _viewCatalogTree!.InsertAsync(key, payload, ct);

        _viewCache[viewName] = sql;
    }

    public async ValueTask DropViewAsync(string viewName, CancellationToken ct = default)
    {
        if (!_viewCache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"View '{viewName}' not found.");

        long key = _schemaSerializer.ViewNameToKey(viewName);
        await _viewCatalogTree!.DeleteAsync(key, ct);

        _viewCache.Remove(viewName);
    }

    // ============ TRIGGER operations ============

    public TriggerSchema? GetTrigger(string triggerName)
    {
        _triggerCache.TryGetValue(triggerName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _triggerCache.Values.ToArray();

    public IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName)
    {
        var result = new List<TriggerSchema>();
        foreach (var trig in _triggerCache.Values)
        {
            if (string.Equals(trig.TableName, tableName, StringComparison.OrdinalIgnoreCase))
                result.Add(trig);
        }
        return result;
    }

    public async ValueTask CreateTriggerAsync(TriggerSchema schema, CancellationToken ct = default)
    {
        if (_triggerCache.ContainsKey(schema.TriggerName))
            throw new CSharpDbException(ErrorCode.TriggerAlreadyExists, $"Trigger '{schema.TriggerName}' already exists.");

        await EnsureTriggerCatalogTreeAsync(ct);

        var payload = _schemaSerializer.SerializeTrigger(schema);
        long key = _schemaSerializer.TriggerNameToKey(schema.TriggerName);
        await _triggerCatalogTree!.InsertAsync(key, payload, ct);

        _triggerCache[schema.TriggerName] = schema;
    }

    public async ValueTask DropTriggerAsync(string triggerName, CancellationToken ct = default)
    {
        if (!_triggerCache.ContainsKey(triggerName))
            throw new CSharpDbException(ErrorCode.TriggerNotFound, $"Trigger '{triggerName}' not found.");

        long key = _schemaSerializer.TriggerNameToKey(triggerName);
        await _triggerCatalogTree!.DeleteAsync(key, ct);

        _triggerCache.Remove(triggerName);
    }

    // ============ Helpers ============

    private void AddIndexToTableCache(IndexSchema schema)
    {
        _cacheState.AddIndexToTable(schema);
    }

    private void RemoveIndexFromTableCache(IndexSchema schema)
    {
        _cacheState.RemoveIndexFromTable(schema);
    }

    private async ValueTask PersistTableRootPageChangeAsync(string tableName, CancellationToken ct)
    {
        if (!_tableTrees.TryGetValue(tableName, out var tree))
            return;

        if (!_tableRootPages.TryGetValue(tableName, out uint persistedRootPage))
            return;

        uint currentRootPage = tree.RootPageId;
        if (currentRootPage == persistedRootPage)
            return;

        var schema = _cache[tableName];
        var schemaBytes = _schemaSerializer.Serialize(schema);
        var payload = _catalogStore.WriteRootPayload(currentRootPage, schemaBytes);

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await _catalogTree.InsertAsync(key, payload, ct);

        _tableRootPages[tableName] = currentRootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask PersistIndexRootPageChangeAsync(string indexName, CancellationToken ct)
    {
        if (!_indexStores.TryGetValue(indexName, out var store))
            return;

        if (!_indexRootPages.TryGetValue(indexName, out uint persistedRootPage))
            return;

        uint currentRootPage = store.RootPageId;
        if (currentRootPage == persistedRootPage)
            return;

        var schema = _indexCache[indexName];
        var schemaBytes = _schemaSerializer.SerializeIndex(schema);
        var payload = _catalogStore.WriteRootPayload(currentRootPage, schemaBytes);

        long key = _schemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);
        await _indexCatalogTree.InsertAsync(key, payload, ct);

        _indexRootPages[indexName] = currentRootPage;
    }
}


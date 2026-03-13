using System.Buffers.Binary;
using System.Text;
using CSharpDB.Core;

namespace CSharpDB.Storage.Catalog;

/// <summary>
/// Domain service for schema catalog operations.
/// Maintains B+tree-backed catalog metadata with in-memory caches.
/// </summary>
internal sealed class CatalogService
{
    private const long IndexCatalogSentinel = long.MaxValue;
    private const long ViewCatalogSentinel = long.MaxValue - 1;
    private const long TriggerCatalogSentinel = long.MaxValue - 2;
    private const long TableStatsCatalogSentinel = long.MaxValue - 3;

    private readonly Pager _pager;
    private readonly ISchemaSerializer _schemaSerializer;
    private readonly IIndexProvider _indexProvider;
    private readonly ICatalogStore _catalogStore;
    private readonly CatalogCache _cacheState = new();
    private BTree? _catalogTree;
    private long _schemaVersion;
    private IndexSchema[] _indexesSnapshot = Array.Empty<IndexSchema>();
    private string[] _viewNamesSnapshot = Array.Empty<string>();
    private TriggerSchema[] _triggersSnapshot = Array.Empty<TriggerSchema>();
    private TableStatistics[] _tableStatisticsSnapshot = Array.Empty<TableStatistics>();
    private bool _indexesSnapshotDirty = true;
    private bool _viewNamesSnapshotDirty = true;
    private bool _triggersSnapshotDirty = true;
    private bool _tableStatisticsSnapshotDirty = true;
    private Dictionary<string, TableSchema> _cache => _cacheState.Tables;
    private Dictionary<string, uint> _tableRootPages => _cacheState.TableRootPages;
    private Dictionary<string, BTree> _tableTrees => _cacheState.TableTrees;
    private readonly Dictionary<string, long> _persistedTableNextRowIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TableStatistics> _tableStatsCache = new(StringComparer.OrdinalIgnoreCase);
    private uint _persistedIndexCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedViewCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedTriggerCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedTableStatsCatalogRootPage = PageConstants.NullPageId;

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
    private Dictionary<string, TriggerSchema[]> _triggersByTable => _cacheState.TriggersByTable;

    // Table statistics catalog
    private BTree? _tableStatsCatalogTree;

    private CatalogService(
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

    public static async ValueTask<CatalogService> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            new DefaultSchemaSerializer(),
            new BTreeIndexProvider(),
            new CatalogStore(),
            ct);
    }

    public static async ValueTask<CatalogService> CreateAsync(
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

    public static async ValueTask<CatalogService> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        CancellationToken ct = default)
    {
        var catalog = new CatalogService(pager, schemaSerializer, indexProvider, catalogStore);

        if (pager.SchemaRootPage != PageConstants.NullPageId)
        {
            catalog._catalogTree = new BTree(pager, pager.SchemaRootPage);
            await catalog.LoadAllAsync(ct);
        }

        return catalog;
    }

    public long SchemaVersion => Volatile.Read(ref _schemaVersion);

    public async ValueTask ReloadAsync(CancellationToken ct = default)
    {
        _catalogTree = null;
        _indexCatalogTree = null;
        _viewCatalogTree = null;
        _triggerCatalogTree = null;
        _tableStatsCatalogTree = null;
        _persistedIndexCatalogRootPage = PageConstants.NullPageId;
        _persistedViewCatalogRootPage = PageConstants.NullPageId;
        _persistedTriggerCatalogRootPage = PageConstants.NullPageId;
        _persistedTableStatsCatalogRootPage = PageConstants.NullPageId;

        _cache.Clear();
        _tableRootPages.Clear();
        _tableTrees.Clear();
        _persistedTableNextRowIds.Clear();
        _indexCache.Clear();
        _indexRootPages.Clear();
        _indexStores.Clear();
        _indexesByTable.Clear();
        _viewCache.Clear();
        _triggerCache.Clear();
        _triggersByTable.Clear();
        _tableStatsCache.Clear();

        _indexesSnapshot = Array.Empty<IndexSchema>();
        _viewNamesSnapshot = Array.Empty<string>();
        _triggersSnapshot = Array.Empty<TriggerSchema>();
        _tableStatisticsSnapshot = Array.Empty<TableStatistics>();
        _indexesSnapshotDirty = true;
        _viewNamesSnapshotDirty = true;
        _triggersSnapshotDirty = true;
        _tableStatisticsSnapshotDirty = true;

        if (_pager.SchemaRootPage != PageConstants.NullPageId)
        {
            _catalogTree = new BTree(_pager, _pager.SchemaRootPage);
            await LoadAllAsync(ct);
        }

        IncrementSchemaVersion();
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
        _persistedIndexCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
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
        _persistedViewCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
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
        _persistedTriggerCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask EnsureTableStatsCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_tableStatsCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _tableStatsCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(TableStatsCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(TableStatsCatalogSentinel, payload, ct);
        _persistedTableStatsCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
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
                _persistedIndexCatalogRootPage = indexRootPage;
                continue;
            }

            if (cursor.CurrentKey == ViewCatalogSentinel)
            {
                uint viewRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _viewCatalogTree = new BTree(_pager, viewRootPage);
                _persistedViewCatalogRootPage = viewRootPage;
                continue;
            }

            if (cursor.CurrentKey == TriggerCatalogSentinel)
            {
                uint triggerRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _triggerCatalogTree = new BTree(_pager, triggerRootPage);
                _persistedTriggerCatalogRootPage = triggerRootPage;
                continue;
            }

            if (cursor.CurrentKey == TableStatsCatalogSentinel)
            {
                uint tableStatsRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _tableStatsCatalogTree = new BTree(_pager, tableStatsRootPage);
                _persistedTableStatsCatalogRootPage = tableStatsRootPage;
                continue;
            }

            var data = cursor.CurrentValue;
            // Data format: [4 bytes root page ID] [schema bytes]
            uint rootPage = _catalogStore.ReadRootPage(data.Span);
            var schema = _schemaSerializer.Deserialize(data.Span[4..]);
            _cache[schema.TableName] = schema;
            _tableRootPages[schema.TableName] = rootPage;
            _persistedTableNextRowIds[schema.TableName] = schema.NextRowId;
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
                AddTriggerToTableCache(triggerSchema);
            }
        }

        if (_tableStatsCatalogTree != null)
        {
            var statsCursor = _tableStatsCatalogTree.CreateCursor();
            while (await statsCursor.MoveNextAsync(ct))
            {
                var stats = DeserializeTableStatistics(statsCursor.CurrentValue.Span);
                _tableStatsCache[stats.TableName] = stats;
            }
        }
    }

    // ============ TABLE operations ============

    public TableSchema? GetTable(string tableName)
    {
        _cache.TryGetValue(tableName, out var schema);
        return schema;
    }

    public TableStatistics? GetTableStatistics(string tableName)
    {
        _tableStatsCache.TryGetValue(tableName, out var stats);
        return stats;
    }

    public IReadOnlyCollection<TableStatistics> GetTableStatistics()
    {
        if (_tableStatisticsSnapshotDirty)
        {
            _tableStatisticsSnapshot = _tableStatsCache.Values.ToArray();
            _tableStatisticsSnapshotDirty = false;
        }

        return _tableStatisticsSnapshot;
    }

    public bool TryGetTableRowCount(string tableName, out long rowCount)
    {
        if (_tableStatsCache.TryGetValue(tableName, out var stats))
        {
            rowCount = stats.RowCount;
            return true;
        }

        rowCount = 0;
        return false;
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

        await PersistAuxiliaryCatalogRootPageChangesAsync(ct);
    }

    /// <summary>
    /// Persist root-page changes for all currently tracked table and index trees.
    /// </summary>
    public async ValueTask PersistAllRootPageChangesAsync(CancellationToken ct = default)
    {
        foreach (var tableName in _tableTrees.Keys)
            await PersistTableRootPageChangeAsync(tableName, ct);

        foreach (var indexName in _indexStores.Keys)
            await PersistIndexRootPageChangeAsync(indexName, ct);

        await PersistAuxiliaryCatalogRootPageChangesAsync(ct);
    }

    public async ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct = default)
        => await CreateTableCoreAsync(schema, normalizeNewSchema: true, ct);

    public async ValueTask CreateTableExactAsync(TableSchema schema, CancellationToken ct = default)
        => await CreateTableCoreAsync(schema, normalizeNewSchema: false, ct);

    private async ValueTask CreateTableCoreAsync(
        TableSchema schema,
        bool normalizeNewSchema,
        CancellationToken ct)
    {
        if (_cache.ContainsKey(schema.TableName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Table '{schema.TableName}' already exists.");

        await EnsureCatalogTreeAsync(ct);

        var storedSchema = normalizeNewSchema ? NormalizeNewTableSchema(schema) : schema;

        // Create a new B+tree for the table's data
        uint tableRootPage = await BTree.CreateNewAsync(_pager, ct);

        // Serialize: [rootPage:4 bytes] [schema bytes]
        var schemaBytes = _schemaSerializer.Serialize(storedSchema);
        var payload = _catalogStore.WriteRootPayload(tableRootPage, schemaBytes);

        long key = _schemaSerializer.TableNameToKey(storedSchema.TableName);
        await _catalogTree!.InsertAsync(key, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[storedSchema.TableName] = storedSchema;
        _tableRootPages[storedSchema.TableName] = tableRootPage;
        _tableTrees[storedSchema.TableName] = new BTree(_pager, tableRootPage);
        _tableTrees[storedSchema.TableName].SetCachedEntryCount(0);
        _persistedTableNextRowIds[storedSchema.TableName] = storedSchema.NextRowId;
        await UpsertTableStatisticsAsync(
            new TableStatistics
            {
                TableName = storedSchema.TableName,
                RowCount = 0,
                HasStaleColumns = false,
            },
            ct);
        IncrementSchemaVersion();
    }

    public async ValueTask DropTableAsync(string tableName, CancellationToken ct = default)
    {
        if (!_cache.ContainsKey(tableName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        uint tableRootPage = _tableTrees.TryGetValue(tableName, out var existingTree)
            ? existingTree.RootPageId
            : _tableRootPages[tableName];

        // Also drop all indexes on this table
        var indexesToDrop = GetIndexesForTable(tableName);
        foreach (var idx in indexesToDrop)
            await DropIndexAsync(idx.IndexName, ct);

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await new BTree(_pager, tableRootPage).ReclaimAsync(ct);
        await DeleteTableStatisticsAsync(tableName, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache.Remove(tableName);
        _tableRootPages.Remove(tableName);
        _tableTrees.Remove(tableName);
        _persistedTableNextRowIds.Remove(tableName);
        IncrementSchemaVersion();
    }

    /// <summary>
    /// Updates the schema for an existing table while keeping the same data root page.
    /// Used by ALTER TABLE operations.
    /// </summary>
    public async ValueTask UpdateTableSchemaAsync(string oldTableName, TableSchema newSchema, CancellationToken ct = default)
    {
        if (!_tableRootPages.TryGetValue(oldTableName, out uint rootPage))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{oldTableName}' not found.");

        if (!_cache.TryGetValue(oldTableName, out var oldSchema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{oldTableName}' not found.");

        var storedSchema = NormalizeUpdatedTableSchema(newSchema, oldSchema.NextRowId);

        // Delete old catalog entry
        long oldKey = _schemaSerializer.TableNameToKey(oldTableName);
        await _catalogTree!.DeleteAsync(oldKey, ct);
        _cache.Remove(oldTableName);
        _tableRootPages.Remove(oldTableName);
        _persistedTableNextRowIds.Remove(oldTableName);

        // Insert new catalog entry with same root page
        var schemaBytes = _schemaSerializer.Serialize(storedSchema);
        var payload = _catalogStore.WriteRootPayload(rootPage, schemaBytes);

        long newKey = _schemaSerializer.TableNameToKey(storedSchema.TableName);
        await _catalogTree!.InsertAsync(newKey, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[storedSchema.TableName] = storedSchema;
        _tableRootPages[storedSchema.TableName] = rootPage;
        _persistedTableNextRowIds[storedSchema.TableName] = storedSchema.NextRowId;

        if (_tableTrees.Remove(oldTableName, out var existingTree))
            _tableTrees[storedSchema.TableName] = existingTree;

        if (!string.Equals(oldTableName, storedSchema.TableName, StringComparison.OrdinalIgnoreCase))
            await RenameTableStatisticsAsync(oldTableName, storedSchema.TableName, ct);

        IncrementSchemaVersion();
    }

    public async ValueTask SetTableRowCountAsync(string tableName, long rowCount, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount);
        await UpsertTableStatisticsAsync(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = rowCount,
                HasStaleColumns = false,
            },
            ct);
    }

    public async ValueTask AdjustTableRowCountAsync(string tableName, long delta, CancellationToken ct = default)
    {
        long rowCount;
        if (_tableStatsCache.TryGetValue(tableName, out var existing))
        {
            rowCount = checked(existing.RowCount + delta);
            if (rowCount < 0)
                throw new InvalidOperationException($"Table '{tableName}' row count would become negative.");
        }
        else
        {
            rowCount = await GetTableTree(tableName).CountEntriesAsync(ct);
        }

        await SetTableRowCountAsync(tableName, rowCount, ct);
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
        if (_tableStatsCache.TryGetValue(tableName, out var stats))
            tree.SetCachedEntryCount(stats.RowCount);
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
        var tree = new BTree(pager, rootPage);
        if (_tableStatsCache.TryGetValue(tableName, out var stats))
            tree.SetCachedEntryCount(stats.RowCount);
        return tree;
    }

    // ============ INDEX operations ============

    public IndexSchema? GetIndex(string indexName)
    {
        _indexCache.TryGetValue(indexName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<IndexSchema> GetIndexes()
    {
        if (_indexesSnapshotDirty)
        {
            _indexesSnapshot = _indexCache.Values.ToArray();
            _indexesSnapshotDirty = false;
        }

        return _indexesSnapshot;
    }

    public IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName)
    {
        if (_indexesByTable.TryGetValue(tableName, out var indexes))
            return indexes;

        return Array.Empty<IndexSchema>();
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
        _indexesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    public async ValueTask DropIndexAsync(string indexName, CancellationToken ct = default)
    {
        if (!_indexCache.TryGetValue(indexName, out var schema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");

        if (!_indexStores.TryGetValue(indexName, out var store))
            store = _indexProvider.CreateIndexStore(_pager, _indexRootPages[indexName]);

        long key = _schemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);
        if (store is IReclaimableIndexStore reclaimable)
            await reclaimable.ReclaimAsync(ct);

        _indexCache.Remove(indexName);
        _indexRootPages.Remove(indexName);
        _indexStores.Remove(indexName);
        RemoveIndexFromTableCache(schema);
        _indexesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    // ============ VIEW operations ============

    public string? GetViewSql(string viewName)
    {
        _viewCache.TryGetValue(viewName, out var sql);
        return sql;
    }

    public IReadOnlyCollection<string> GetViewNames()
    {
        if (_viewNamesSnapshotDirty)
        {
            _viewNamesSnapshot = _viewCache.Keys.ToArray();
            _viewNamesSnapshotDirty = false;
        }

        return _viewNamesSnapshot;
    }

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
        _viewNamesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    public async ValueTask DropViewAsync(string viewName, CancellationToken ct = default)
    {
        if (!_viewCache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"View '{viewName}' not found.");

        long key = _schemaSerializer.ViewNameToKey(viewName);
        await _viewCatalogTree!.DeleteAsync(key, ct);

        _viewCache.Remove(viewName);
        _viewNamesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    // ============ TRIGGER operations ============

    public TriggerSchema? GetTrigger(string triggerName)
    {
        _triggerCache.TryGetValue(triggerName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<TriggerSchema> GetTriggers()
    {
        if (_triggersSnapshotDirty)
        {
            _triggersSnapshot = _triggerCache.Values.ToArray();
            _triggersSnapshotDirty = false;
        }

        return _triggersSnapshot;
    }

    public IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName)
    {
        if (_triggersByTable.TryGetValue(tableName, out var triggers))
            return triggers;

        return Array.Empty<TriggerSchema>();
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
        AddTriggerToTableCache(schema);
        _triggersSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    public async ValueTask DropTriggerAsync(string triggerName, CancellationToken ct = default)
    {
        if (!_triggerCache.TryGetValue(triggerName, out var schema))
            throw new CSharpDbException(ErrorCode.TriggerNotFound, $"Trigger '{triggerName}' not found.");

        long key = _schemaSerializer.TriggerNameToKey(triggerName);
        await _triggerCatalogTree!.DeleteAsync(key, ct);

        _triggerCache.Remove(triggerName);
        RemoveTriggerFromTableCache(schema);
        _triggersSnapshotDirty = true;
        IncrementSchemaVersion();
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

    private void AddTriggerToTableCache(TriggerSchema schema)
    {
        _cacheState.AddTriggerToTable(schema);
    }

    private void RemoveTriggerFromTableCache(TriggerSchema schema)
    {
        _cacheState.RemoveTriggerFromTable(schema);
    }

    private static TableSchema NormalizeNewTableSchema(TableSchema schema)
    {
        long normalizedNextRowId = schema.NextRowId > 0 ? schema.NextRowId : 1;

        if (schema.NextRowId == normalizedNextRowId)
            return schema;

        return CloneWithNextRowId(schema, normalizedNextRowId);
    }

    private static TableSchema NormalizeUpdatedTableSchema(TableSchema schema, long persistedNextRowId)
    {
        // Legacy catalog entries use 0 to mean "unknown"; preserve that sentinel on ALTER/RENAME
        // so the allocator recomputes max(rowid)+1 instead of resetting to 1 after a schema rewrite.
        long normalizedNextRowId = schema.NextRowId > 0 ? schema.NextRowId : persistedNextRowId;
        if (normalizedNextRowId < 0)
            normalizedNextRowId = 0;

        if (schema.NextRowId == normalizedNextRowId)
            return schema;

        return CloneWithNextRowId(schema, normalizedNextRowId);
    }

    private static TableSchema CloneWithNextRowId(TableSchema schema, long normalizedNextRowId)
    {
        return new TableSchema
        {
            TableName = schema.TableName,
            Columns = schema.Columns,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = normalizedNextRowId,
        };
    }

    private async ValueTask UpsertTableStatisticsAsync(TableStatistics stats, CancellationToken ct)
    {
        await EnsureTableStatsCatalogTreeAsync(ct);

        byte[] payload = SerializeTableStatistics(stats);
        long key = _schemaSerializer.TableNameToKey(stats.TableName);

        try { await _tableStatsCatalogTree!.DeleteAsync(key, ct); } catch { }
        await _tableStatsCatalogTree!.InsertAsync(key, payload, ct);

        _tableStatsCache[stats.TableName] = stats;
        _tableStatisticsSnapshotDirty = true;
        if (_tableTrees.TryGetValue(stats.TableName, out var tree))
            tree.SetCachedEntryCount(stats.RowCount);
    }

    private async ValueTask DeleteTableStatisticsAsync(string tableName, CancellationToken ct)
    {
        if (_tableStatsCatalogTree == null)
        {
            _tableStatsCache.Remove(tableName);
            _tableStatisticsSnapshotDirty = true;
            return;
        }

        long key = _schemaSerializer.TableNameToKey(tableName);
        try { await _tableStatsCatalogTree.DeleteAsync(key, ct); } catch { }
        _tableStatsCache.Remove(tableName);
        _tableStatisticsSnapshotDirty = true;
    }

    private async ValueTask RenameTableStatisticsAsync(string oldTableName, string newTableName, CancellationToken ct)
    {
        if (!_tableStatsCache.TryGetValue(oldTableName, out var stats))
            return;

        await DeleteTableStatisticsAsync(oldTableName, ct);
        await UpsertTableStatisticsAsync(
            new TableStatistics
            {
                TableName = newTableName,
                RowCount = stats.RowCount,
                HasStaleColumns = stats.HasStaleColumns,
            },
            ct);
    }

    private static byte[] SerializeTableStatistics(TableStatistics stats)
    {
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(stats.TableName);
        byte[] payload = new byte[4 + tableNameBytes.Length + 8 + 1];
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(0, 4), tableNameBytes.Length);
        tableNameBytes.CopyTo(payload.AsSpan(4));
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(4 + tableNameBytes.Length, 8), stats.RowCount);
        payload[^1] = stats.HasStaleColumns ? (byte)1 : (byte)0;
        return payload;
    }

    private static TableStatistics DeserializeTableStatistics(ReadOnlySpan<byte> payload)
    {
        int tableNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
        string tableName = Encoding.UTF8.GetString(payload.Slice(4, tableNameLength));
        long rowCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(4 + tableNameLength, 8));
        bool hasStaleColumns = payload[4 + tableNameLength + 8] != 0;
        return new TableStatistics
        {
            TableName = tableName,
            RowCount = rowCount,
            HasStaleColumns = hasStaleColumns,
        };
    }

    private void IncrementSchemaVersion()
    {
        Interlocked.Increment(ref _schemaVersion);
    }

    private async ValueTask PersistAuxiliaryCatalogRootPageChangesAsync(CancellationToken ct)
    {
        await PersistIndexCatalogRootPageChangeAsync(ct);
        await PersistViewCatalogRootPageChangeAsync(ct);
        await PersistTriggerCatalogRootPageChangeAsync(ct);
        await PersistTableStatsCatalogRootPageChangeAsync(ct);
    }

    private ValueTask PersistIndexCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_indexCatalogTree, IndexCatalogSentinel, _persistedIndexCatalogRootPage, ct, rootPage => _persistedIndexCatalogRootPage = rootPage);

    private ValueTask PersistViewCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_viewCatalogTree, ViewCatalogSentinel, _persistedViewCatalogRootPage, ct, rootPage => _persistedViewCatalogRootPage = rootPage);

    private ValueTask PersistTriggerCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_triggerCatalogTree, TriggerCatalogSentinel, _persistedTriggerCatalogRootPage, ct, rootPage => _persistedTriggerCatalogRootPage = rootPage);

    private ValueTask PersistTableStatsCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_tableStatsCatalogTree, TableStatsCatalogSentinel, _persistedTableStatsCatalogRootPage, ct, rootPage => _persistedTableStatsCatalogRootPage = rootPage);

    private async ValueTask PersistAuxiliaryCatalogRootPageChangeAsync(
        BTree? tree,
        long sentinelKey,
        uint persistedRootPage,
        CancellationToken ct,
        Action<uint> setPersistedRootPage)
    {
        if (tree == null)
            return;

        uint currentRootPage = tree.RootPageId;
        if (persistedRootPage == currentRootPage)
            return;

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, currentRootPage);
        try { await _catalogTree!.DeleteAsync(sentinelKey, ct); } catch { }
        await _catalogTree!.InsertAsync(sentinelKey, payload, ct);

        setPersistedRootPage(currentRootPage);
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask PersistTableRootPageChangeAsync(string tableName, CancellationToken ct)
    {
        if (!_tableTrees.TryGetValue(tableName, out var tree))
            return;

        if (!_tableRootPages.TryGetValue(tableName, out uint persistedRootPage))
            return;

        if (!_cache.TryGetValue(tableName, out var schema))
            return;

        uint currentRootPage = tree.RootPageId;
        _persistedTableNextRowIds.TryGetValue(tableName, out long persistedNextRowId);
        bool metadataChanged = persistedNextRowId != schema.NextRowId;
        if (currentRootPage == persistedRootPage && !metadataChanged)
            return;

        var schemaBytes = _schemaSerializer.Serialize(schema);
        var payload = _catalogStore.WriteRootPayload(currentRootPage, schemaBytes);

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await _catalogTree.InsertAsync(key, payload, ct);

        _tableRootPages[tableName] = currentRootPage;
        _persistedTableNextRowIds[tableName] = schema.NextRowId;
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


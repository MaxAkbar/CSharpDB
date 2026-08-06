using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

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
    private const long ColumnStatsCatalogSentinel = long.MaxValue - 4;
    private const long ColumnDistributionStatsCatalogSentinel = long.MaxValue - 5;
    private const long IndexPrefixStatsCatalogSentinel = long.MaxValue - 6;
    private const long RowVersionHighWaterCatalogSentinel = long.MaxValue - 7;

    private readonly Pager _pager;
    private readonly ISchemaSerializer _schemaSerializer;
    private readonly IIndexProvider _indexProvider;
    private readonly ICatalogStore _catalogStore;
    private readonly AdvisoryStatisticsPersistenceMode _advisoryStatisticsPersistenceMode;
    private readonly CatalogCache _cacheState = new();
    private BTree? _catalogTree;
    private long _schemaVersion;
    private ulong _rowVersionHighWater;
    private IndexSchema[] _indexesSnapshot = Array.Empty<IndexSchema>();
    private string[] _viewNamesSnapshot = Array.Empty<string>();
    private TriggerSchema[] _triggersSnapshot = Array.Empty<TriggerSchema>();
    private TableStatistics[] _tableStatisticsSnapshot = Array.Empty<TableStatistics>();
    private ColumnStatistics[] _columnStatisticsSnapshot = Array.Empty<ColumnStatistics>();
    private bool _indexesSnapshotDirty = true;
    private bool _viewNamesSnapshotDirty = true;
    private bool _triggersSnapshotDirty = true;
    private bool _tableStatisticsSnapshotDirty = true;
    private bool _columnStatisticsSnapshotDirty = true;
    private Dictionary<string, TableSchema> _cache => _cacheState.Tables;
    private Dictionary<string, uint> _tableRootPages => _cacheState.TableRootPages;
    private Dictionary<string, BTree> _tableTrees => _cacheState.TableTrees;
    private Dictionary<string, ForeignKeyDefinition[]> _foreignKeysByTable => _cacheState.ForeignKeysByTable;
    private Dictionary<string, TableForeignKeyReference[]> _referencingForeignKeysByParentTable => _cacheState.ReferencingForeignKeysByParentTable;
    private Dictionary<Guid, TableForeignKeyReference[]> _referencingForeignKeysByParentTableId => _cacheState.ReferencingForeignKeysByParentTableId;
    private readonly Dictionary<string, long> _persistedTableNextRowIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TableStatistics> _tableStatsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _dirtyTableStatistics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _pendingTableRowCountDeltas = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _exactTableRowCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ColumnStatistics> _columnStatsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _dirtyColumnStatistics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ColumnStatistics[]> _columnStatsByTableSnapshot = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ColumnDistributionStatistics> _columnDistributionStatsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ColumnDistributionStatistics[]> _columnDistributionStatsByTable = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IndexPrefixStatistics> _indexPrefixStatsCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IndexPrefixStatistics[]> _indexPrefixStatsByTable = new(StringComparer.OrdinalIgnoreCase);
    private bool _advisoryCatalogContentChanged;
    private uint _persistedIndexCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedViewCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedTriggerCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedTableStatsCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedColumnStatsCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedColumnDistributionStatsCatalogRootPage = PageConstants.NullPageId;
    private uint _persistedIndexPrefixStatsCatalogRootPage = PageConstants.NullPageId;

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

    // Column statistics catalog
    private BTree? _columnStatsCatalogTree;

    // Internal planner distribution catalogs
    private BTree? _columnDistributionStatsCatalogTree;
    private BTree? _indexPrefixStatsCatalogTree;

    private CatalogService(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode)
    {
        _pager = pager;
        _schemaSerializer = schemaSerializer;
        _indexProvider = indexProvider;
        _catalogStore = catalogStore;
        _advisoryStatisticsPersistenceMode = advisoryStatisticsPersistenceMode;
    }

    public static async ValueTask<CatalogService> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            AdvisoryStatisticsPersistenceMode.Immediate,
            ct);
    }

    public static async ValueTask<CatalogService> CreateAsync(
        Pager pager,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode,
        CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            new DefaultSchemaSerializer(),
            new BTreeIndexProvider(),
            new CatalogStore(),
            advisoryStatisticsPersistenceMode,
            ct);
    }

    public static async ValueTask<CatalogService> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Immediate,
        CancellationToken ct = default)
    {
        return await CreateAsync(
            pager,
            schemaSerializer,
            indexProvider,
            new CatalogStore(),
            advisoryStatisticsPersistenceMode,
            ct);
    }

    public static async ValueTask<CatalogService> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Immediate,
        CancellationToken ct = default)
    {
        var catalog = new CatalogService(
            pager,
            schemaSerializer,
            indexProvider,
            catalogStore,
            advisoryStatisticsPersistenceMode);

        if (pager.SchemaRootPage != PageConstants.NullPageId)
        {
            catalog._catalogTree = new BTree(pager, pager.SchemaRootPage);
            await catalog.LoadAllAsync(ct);
        }

        return catalog;
    }

    public long SchemaVersion => Volatile.Read(ref _schemaVersion);
    public ulong RowVersionHighWater => _rowVersionHighWater;
    public bool HasAdvisoryCatalogContentChanges => _advisoryCatalogContentChanged;

    public async ValueTask ReloadAsync(CancellationToken ct = default)
    {
        _catalogTree = null;
        _indexCatalogTree = null;
        _viewCatalogTree = null;
        _triggerCatalogTree = null;
        _tableStatsCatalogTree = null;
        _columnStatsCatalogTree = null;
        _columnDistributionStatsCatalogTree = null;
        _indexPrefixStatsCatalogTree = null;
        _persistedIndexCatalogRootPage = PageConstants.NullPageId;
        _persistedViewCatalogRootPage = PageConstants.NullPageId;
        _persistedTriggerCatalogRootPage = PageConstants.NullPageId;
        _persistedTableStatsCatalogRootPage = PageConstants.NullPageId;
        _persistedColumnStatsCatalogRootPage = PageConstants.NullPageId;
        _persistedColumnDistributionStatsCatalogRootPage = PageConstants.NullPageId;
        _persistedIndexPrefixStatsCatalogRootPage = PageConstants.NullPageId;

        _cache.Clear();
        _tableRootPages.Clear();
        _tableTrees.Clear();
        _persistedTableNextRowIds.Clear();
        _rowVersionHighWater = 0;
        _foreignKeysByTable.Clear();
        _referencingForeignKeysByParentTable.Clear();
        _referencingForeignKeysByParentTableId.Clear();
        _indexCache.Clear();
        _indexRootPages.Clear();
        _indexStores.Clear();
        _indexesByTable.Clear();
        _viewCache.Clear();
        _triggerCache.Clear();
        _triggersByTable.Clear();
        _tableStatsCache.Clear();
        _dirtyTableStatistics.Clear();
        _pendingTableRowCountDeltas.Clear();
        _exactTableRowCounts.Clear();
        _columnStatsCache.Clear();
        _dirtyColumnStatistics.Clear();
        _columnStatsByTableSnapshot.Clear();
        _columnDistributionStatsCache.Clear();
        _columnDistributionStatsByTable.Clear();
        _indexPrefixStatsCache.Clear();
        _indexPrefixStatsByTable.Clear();
        _advisoryCatalogContentChanged = false;

        _indexesSnapshot = Array.Empty<IndexSchema>();
        _viewNamesSnapshot = Array.Empty<string>();
        _triggersSnapshot = Array.Empty<TriggerSchema>();
        _tableStatisticsSnapshot = Array.Empty<TableStatistics>();
        _columnStatisticsSnapshot = Array.Empty<ColumnStatistics>();
        _indexesSnapshotDirty = true;
        _viewNamesSnapshotDirty = true;
        _triggersSnapshotDirty = true;
        _tableStatisticsSnapshotDirty = true;
        _columnStatisticsSnapshotDirty = true;

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

    private async ValueTask EnsureColumnStatsCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_columnStatsCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _columnStatsCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(ColumnStatsCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(ColumnStatsCatalogSentinel, payload, ct);
        _persistedColumnStatsCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask EnsureColumnDistributionStatsCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_columnDistributionStatsCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _columnDistributionStatsCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(ColumnDistributionStatsCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(ColumnDistributionStatsCatalogSentinel, payload, ct);
        _persistedColumnDistributionStatsCatalogRootPage = rootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask EnsureIndexPrefixStatsCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_indexPrefixStatsCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _indexPrefixStatsCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(IndexPrefixStatsCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(IndexPrefixStatsCatalogSentinel, payload, ct);
        _persistedIndexPrefixStatsCatalogRootPage = rootPage;
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

            if (cursor.CurrentKey == ColumnStatsCatalogSentinel)
            {
                uint columnStatsRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _columnStatsCatalogTree = new BTree(_pager, columnStatsRootPage);
                _persistedColumnStatsCatalogRootPage = columnStatsRootPage;
                continue;
            }

            if (cursor.CurrentKey == ColumnDistributionStatsCatalogSentinel)
            {
                uint columnDistributionStatsRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _columnDistributionStatsCatalogTree = new BTree(_pager, columnDistributionStatsRootPage);
                _persistedColumnDistributionStatsCatalogRootPage = columnDistributionStatsRootPage;
                continue;
            }

            if (cursor.CurrentKey == IndexPrefixStatsCatalogSentinel)
            {
                uint indexPrefixStatsRootPage = _catalogStore.ReadRootPage(cursor.CurrentValue.Span);
                _indexPrefixStatsCatalogTree = new BTree(_pager, indexPrefixStatsRootPage);
                _persistedIndexPrefixStatsCatalogRootPage = indexPrefixStatsRootPage;
                continue;
            }

            if (cursor.CurrentKey == RowVersionHighWaterCatalogSentinel)
            {
                if (cursor.CurrentValue.Length != sizeof(ulong))
                {
                    throw new CSharpDbException(
                        ErrorCode.CorruptDatabase,
                        "The database-wide ROWVERSION allocator metadata is invalid.");
                }

                _rowVersionHighWater = BinaryPrimitives.ReadUInt64LittleEndian(cursor.CurrentValue.Span);
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

        ValidateLoadedStableIdentityUniqueness();
        HydrateLoadedForeignKeyBindings();
        RebuildForeignKeyCaches();

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
                _indexStores[indexSchema.IndexName] = CreateIndexStore(_pager, indexSchema, rootPage);
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

        if (_columnStatsCatalogTree != null)
        {
            var columnStatsCursor = _columnStatsCatalogTree.CreateCursor();
            while (await columnStatsCursor.MoveNextAsync(ct))
            {
                CacheColumnStatistics(DeserializeColumnStatistics(columnStatsCursor.CurrentValue.Span));
            }
        }

        if (_columnDistributionStatsCatalogTree != null)
        {
            var columnDistributionStatsCursor = _columnDistributionStatsCatalogTree.CreateCursor();
            while (await columnDistributionStatsCursor.MoveNextAsync(ct))
            {
                CacheColumnDistributionStatistics(DeserializeColumnDistributionStatistics(columnDistributionStatsCursor.CurrentValue.Span));
            }
        }

        if (_indexPrefixStatsCatalogTree != null)
        {
            var indexPrefixStatsCursor = _indexPrefixStatsCatalogTree.CreateCursor();
            while (await indexPrefixStatsCursor.MoveNextAsync(ct))
            {
                CacheIndexPrefixStatistics(DeserializeIndexPrefixStatistics(indexPrefixStatsCursor.CurrentValue.Span));
            }
        }

        ReconcileLoadedStatisticsFreshness();
        if (_advisoryStatisticsPersistenceMode == AdvisoryStatisticsPersistenceMode.Immediate)
            await PopulateImmediateTableStatisticsAsync(ct);
    }

    public async ValueTask PersistRowVersionHighWaterAsync(
        ulong rowVersionHighWater,
        CancellationToken ct = default)
    {
        if (rowVersionHighWater <= _rowVersionHighWater)
            return;

        await EnsureCatalogTreeAsync(ct);
        var payload = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(payload, rowVersionHighWater);
        // Replace defensively instead of trusting the catalog cache. An
        // isolated write transaction can commit allocator metadata before a
        // shared catalog instance reloads, and the durable sentinel must still
        // be updated rather than treated as a duplicate key.
        await _catalogTree!.DeleteAsync(RowVersionHighWaterCatalogSentinel, ct);
        await _catalogTree!.InsertAsync(RowVersionHighWaterCatalogSentinel, payload, ct);
        _rowVersionHighWater = rowVersionHighWater;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    // ============ TABLE operations ============

    public TableSchema? GetTable(string tableName)
    {
        _cache.TryGetValue(tableName, out var schema);
        return schema;
    }

    public IReadOnlyList<ForeignKeyDefinition> GetForeignKeysForTable(string tableName)
    {
        if (_foreignKeysByTable.TryGetValue(tableName, out var foreignKeys))
            return foreignKeys;

        return Array.Empty<ForeignKeyDefinition>();
    }

    public IReadOnlyList<TableForeignKeyReference> GetReferencingForeignKeys(string parentTableName)
    {
        TableForeignKeyReference[] identityReferences = Array.Empty<TableForeignKeyReference>();
        if (_cache.TryGetValue(parentTableName, out TableSchema? parentSchema) &&
            parentSchema.SchemaId != Guid.Empty &&
            _referencingForeignKeysByParentTableId.TryGetValue(parentSchema.SchemaId, out var cachedIdentityReferences))
        {
            identityReferences = cachedIdentityReferences;
        }

        _referencingForeignKeysByParentTable.TryGetValue(
            parentTableName,
            out TableForeignKeyReference[]? nameReferences);
        nameReferences ??= Array.Empty<TableForeignKeyReference>();

        if (identityReferences.Length == 0)
            return nameReferences;
        if (nameReferences.Length == 0)
            return identityReferences;

        var combined = new List<TableForeignKeyReference>(
            identityReferences.Length + nameReferences.Length);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddDistinctReferences(identityReferences, combined, seen);
        AddDistinctReferences(nameReferences, combined, seen);
        return combined;
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

    public IReadOnlyCollection<TableStatistics> GetDirtyTableStatistics()
    {
        if (_dirtyTableStatistics.Count == 0)
            return Array.Empty<TableStatistics>();

        var dirty = new List<TableStatistics>(_dirtyTableStatistics.Count);
        foreach (string tableName in _dirtyTableStatistics)
        {
            if (_tableStatsCache.TryGetValue(tableName, out var stats))
                dirty.Add(stats);
        }

        return dirty;
    }

    public IReadOnlyCollection<KeyValuePair<string, long>> GetPendingTableRowCountDeltas()
    {
        if (_pendingTableRowCountDeltas.Count == 0)
            return Array.Empty<KeyValuePair<string, long>>();

        return _pendingTableRowCountDeltas
            .Where(static entry => entry.Value != 0)
            .ToArray();
    }

    public ColumnStatistics? GetColumnStatistics(string tableName, string columnName)
    {
        _columnStatsCache.TryGetValue(GetColumnStatisticsCacheKey(tableName, columnName), out var stats);
        return stats;
    }

    public IReadOnlyCollection<ColumnStatistics> GetColumnStatistics(string tableName)
    {
        if (_columnStatsByTableSnapshot.TryGetValue(tableName, out var stats))
            return stats;

        return Array.Empty<ColumnStatistics>();
    }

    public IReadOnlyCollection<ColumnStatistics> GetColumnStatistics()
    {
        if (_columnStatisticsSnapshotDirty)
        {
            _columnStatisticsSnapshot = _columnStatsCache.Values.ToArray();
            _columnStatisticsSnapshotDirty = false;
        }

        return _columnStatisticsSnapshot;
    }

    public IReadOnlyCollection<ColumnStatistics> GetDirtyColumnStatistics()
    {
        if (_dirtyColumnStatistics.Count == 0)
            return Array.Empty<ColumnStatistics>();

        var dirty = new List<ColumnStatistics>(_dirtyColumnStatistics.Count);
        foreach (string cacheKey in _dirtyColumnStatistics)
        {
            if (_columnStatsCache.TryGetValue(cacheKey, out var stats))
                dirty.Add(stats);
        }

        return dirty;
    }

    internal IReadOnlyCollection<ColumnDistributionStatistics> GetColumnDistributionStatistics()
    {
        if (_columnDistributionStatsCache.Count == 0)
            return Array.Empty<ColumnDistributionStatistics>();

        return _columnDistributionStatsCache.Values.ToArray();
    }

    internal IReadOnlyCollection<IndexPrefixStatistics> GetIndexPrefixStatistics()
    {
        if (_indexPrefixStatsCache.Count == 0)
            return Array.Empty<IndexPrefixStatistics>();

        return _indexPrefixStatsCache.Values.ToArray();
    }

    public void ApplyCommittedAdvisoryStatisticsSnapshot(
        IReadOnlyCollection<TableStatistics> tableStatistics,
        IReadOnlyCollection<ColumnStatistics> columnStatistics,
        bool markDirty = false)
    {
        ArgumentNullException.ThrowIfNull(tableStatistics);
        ArgumentNullException.ThrowIfNull(columnStatistics);

        foreach (TableStatistics stats in tableStatistics)
        {
            if (!markDirty)
            {
                _dirtyTableStatistics.Remove(stats.TableName);
                _pendingTableRowCountDeltas.Remove(stats.TableName);
            }

            CacheTableStatistics(stats, stats.RowCountIsExact, markDirty);
        }

        foreach (ColumnStatistics stats in columnStatistics)
        {
            string cacheKey = GetColumnStatisticsCacheKey(stats.TableName, stats.ColumnName);
            if (!markDirty)
                _dirtyColumnStatistics.Remove(cacheKey);

            CacheColumnStatistics(
                new ColumnStatistics
                {
                    TableName = stats.TableName,
                    ColumnName = stats.ColumnName,
                    DistinctCount = stats.DistinctCount,
                    NonNullCount = stats.NonNullCount,
                    MinValue = stats.MinValue,
                    MaxValue = stats.MaxValue,
                    IsStale = stats.IsStale,
                },
                markDirty);
        }
    }

    public void ApplyCommittedTableRowCountDeltas(IReadOnlyCollection<KeyValuePair<string, long>> rowCountDeltas)
    {
        ArgumentNullException.ThrowIfNull(rowCountDeltas);

        foreach ((string tableName, long delta) in rowCountDeltas)
        {
            if (delta == 0)
                continue;

            if (_pendingTableRowCountDeltas.TryGetValue(tableName, out long existing))
                _pendingTableRowCountDeltas[tableName] = checked(existing + delta);
            else
                _pendingTableRowCountDeltas[tableName] = delta;
        }
    }

    public void ApplyCommittedTableMetadataSnapshot(IReadOnlyCollection<KeyValuePair<string, long>> nextRowIds)
    {
        ArgumentNullException.ThrowIfNull(nextRowIds);

        foreach ((string tableName, long nextRowId) in nextRowIds)
        {
            if (nextRowId <= 0)
                continue;

            if (_cache.TryGetValue(tableName, out var schema) && schema.NextRowId < nextRowId)
                schema.NextRowId = nextRowId;

            _persistedTableNextRowIds[tableName] = nextRowId;
        }
    }

    public bool TryGetFreshColumnStatistics(string tableName, string columnName, out ColumnStatistics stats)
    {
        if (_columnStatsCache.TryGetValue(GetColumnStatisticsCacheKey(tableName, columnName), out stats!) &&
            !stats.IsStale)
        {
            return true;
        }

        stats = null!;
        return false;
    }

    public bool TryGetFreshColumnDistributionStatistics(string tableName, string columnName, out ColumnDistributionStatistics stats)
    {
        if (_columnDistributionStatsCache.TryGetValue(GetColumnDistributionStatisticsCacheKey(tableName, columnName), out stats!) &&
            TryGetFreshColumnStatistics(tableName, columnName, out _))
        {
            return true;
        }

        stats = null!;
        return false;
    }

    public bool TryGetFreshIndexPrefixStatistics(string indexName, out IndexPrefixStatistics stats)
    {
        if (_indexPrefixStatsCache.TryGetValue(indexName, out stats!) &&
            _tableStatsCache.TryGetValue(stats.TableName, out var tableStats) &&
            !tableStats.HasStaleColumns)
        {
            return true;
        }

        stats = null!;
        return false;
    }

    public bool TryGetEstimatedTableRowCount(string tableName, out long rowCount)
    {
        if (_tableStatsCache.TryGetValue(tableName, out var stats))
        {
            rowCount = stats.RowCount;
            return true;
        }

        rowCount = 0;
        return false;
    }

    public bool TryGetExactTableRowCount(string tableName, out long rowCount)
    {
        if (_exactTableRowCounts.Contains(tableName) &&
            _tableStatsCache.TryGetValue(tableName, out var stats))
        {
            rowCount = stats.RowCount;
            return true;
        }

        rowCount = 0;
        return false;
    }

    public async ValueTask<long> GetExactTableRowCountAsync(string tableName, CancellationToken ct = default)
    {
        if (TryGetExactTableRowCount(tableName, out long rowCount))
            return rowCount;

        long exactRowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
        bool hasStaleColumns = _tableStatsCache.TryGetValue(tableName, out var existing) && existing.HasStaleColumns;
        uint lastPersistedChangeCounter = existing?.LastPersistedChangeCounter ?? 0;
        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = exactRowCount,
                HasStaleColumns = hasStaleColumns,
                LastPersistedChangeCounter = lastPersistedChangeCounter,
            },
            isExact: true,
            markDirty: false);
        return exactRowCount;
    }

    public bool HasDirtyAdvisoryStatistics => _dirtyTableStatistics.Count > 0 || _dirtyColumnStatistics.Count > 0;

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

    public async ValueTask<bool> PersistAllRootPageChangesAndDetectChangesAsync(CancellationToken ct = default)
    {
        bool changed = false;

        foreach (var tableName in _tableTrees.Keys)
            changed |= await PersistTableRootPageChangeAsync(tableName, ct);

        foreach (var indexName in _indexStores.Keys)
            changed |= await PersistIndexRootPageChangeAsync(indexName, ct);

        changed |= await PersistAuxiliaryCatalogRootPageChangesAsync(ct);
        return changed;
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

        var storedSchema = normalizeNewSchema
            ? NormalizeNewTableSchema(schema)
            : NormalizeSchemaIdentities(schema, previous: null, schema.NextRowId);
        ValidateStableIdentityUniqueness(storedSchema, excludedTableName: null);

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
        _tableTrees[storedSchema.TableName] = new BTree(_pager, tableRootPage, storedSchema.TableName);
        _tableTrees[storedSchema.TableName].SetCachedEntryCount(0);
        _persistedTableNextRowIds[storedSchema.TableName] = storedSchema.NextRowId;
        await RefreshForeignKeyBindingsAsync(persistChanges: true, ct);
        RebuildForeignKeyCaches();
        await UpsertTableStatisticsAsync(
            new TableStatistics
            {
                TableName = storedSchema.TableName,
                RowCount = 0,
                HasStaleColumns = false,
                LastPersistedChangeCounter = 0,
            },
            isExact: true,
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
        {
            if (!_indexCache.ContainsKey(idx.IndexName))
                continue;

            if (idx.Kind == IndexKind.ForeignKeyInternal)
                await DropForeignKeyOwnedIndexAsync(idx.IndexName, ct);
            else if (idx.Kind == IndexKind.ConstraintInternal)
                await DropConstraintOwnedIndexAsync(idx.IndexName, ct);
            else
                await DropIndexAsync(idx.IndexName, ct);
        }

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await new BTree(_pager, tableRootPage).ReclaimAsync(ct);
        await DeleteTableStatisticsAsync(tableName, ct);
        await DeleteColumnStatisticsAsync(tableName, ct);
        await DeleteColumnDistributionStatisticsAsync(tableName, ct);
        await DeleteIndexPrefixStatisticsForTableAsync(tableName, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache.Remove(tableName);
        _tableRootPages.Remove(tableName);
        _tableTrees.Remove(tableName);
        _persistedTableNextRowIds.Remove(tableName);
        RebuildForeignKeyCaches();
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

        ValidateRequestedIdentityImmutability(oldSchema, newSchema);
        var storedSchema = NormalizeUpdatedTableSchema(newSchema, oldSchema);
        ValidateStableIdentityUniqueness(storedSchema, oldTableName);

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
        await RefreshForeignKeyBindingsAsync(persistChanges: true, ct);
        RebuildForeignKeyCaches();

        if (_tableTrees.Remove(oldTableName, out var existingTree))
        {
            var renamedTree = new BTree(_pager, rootPage, storedSchema.TableName);
            if (existingTree.TryGetCachedEntryCount(out long cachedEntryCount))
                renamedTree.SetCachedEntryCount(cachedEntryCount);

            _tableTrees[storedSchema.TableName] = renamedTree;
        }

        bool isPureTableRename =
            !string.Equals(oldTableName, storedSchema.TableName, StringComparison.OrdinalIgnoreCase) &&
            HaveMatchingColumnNames(oldSchema, storedSchema);

        if (!string.Equals(oldTableName, storedSchema.TableName, StringComparison.OrdinalIgnoreCase))
            await RenameTableStatisticsAsync(oldTableName, storedSchema.TableName, ct);

        if (isPureTableRename)
            await RenameColumnStatisticsAsync(oldTableName, storedSchema.TableName, ct);
        else
            await DeleteColumnStatisticsAsync(storedSchema.TableName, ct);

        await DeleteColumnDistributionStatisticsAsync(oldTableName, ct);
        if (!string.Equals(oldTableName, storedSchema.TableName, StringComparison.OrdinalIgnoreCase))
            await DeleteColumnDistributionStatisticsAsync(storedSchema.TableName, ct);

        await DeleteIndexPrefixStatisticsForTableAsync(oldTableName, ct);
        if (!string.Equals(oldTableName, storedSchema.TableName, StringComparison.OrdinalIgnoreCase))
            await DeleteIndexPrefixStatisticsForTableAsync(storedSchema.TableName, ct);

        IncrementSchemaVersion();
    }

    /// <summary>
    /// Applies stable identities from a structurally equivalent schema without
    /// changing the live table shape or storage. This is a trusted recovery
    /// path; ordinary schema updates cannot replace existing identities.
    /// </summary>
    public async ValueTask ApplyTableSchemaIdentitiesAsync(
        string tableName,
        TableSchema identitySource,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(identitySource);
        if (!_pager.IsWriteTransactionActive)
        {
            throw new InvalidOperationException(
                "Stable schema identities can only be adopted inside an explicit storage transaction.");
        }

        if (!_cache.TryGetValue(tableName, out TableSchema? liveSchema) ||
            !_tableRootPages.ContainsKey(tableName))
        {
            throw new CSharpDbException(
                ErrorCode.TableNotFound,
                $"Table '{tableName}' not found.");
        }

        TableSchema adoptedSchema = AdoptStructurallyEquivalentIdentities(
            liveSchema,
            identitySource);
        ValidateStableIdentityUniqueness(adoptedSchema, tableName);

        await PersistTableSchemaPayloadAsync(adoptedSchema, ct);
        _cache[tableName] = adoptedSchema;
        await RefreshForeignKeyBindingsAsync(persistChanges: true, ct);
        RebuildForeignKeyCaches();
        IncrementSchemaVersion();
    }

    /// <summary>
    /// Atomically replaces a table's catalog payload with a new schema and data
    /// root. The caller owns reclaiming the previous root after this method
    /// succeeds; keeping reclamation separate lets a shadow rewrite retain the
    /// original tree until the catalog swap is durable in the active transaction.
    /// </summary>
    public async ValueTask<uint> ReplaceTableStorageAsync(
        string tableName,
        TableSchema newSchema,
        uint replacementRootPage,
        long exactRowCount,
        CancellationToken ct = default)
    {
        TableAndIndexStorageReplacement replacement =
            await ReplaceTableAndIndexStorageAsync(
                tableName,
                newSchema,
                replacementRootPage,
                exactRowCount,
                new Dictionary<string, IIndexStore>(StringComparer.OrdinalIgnoreCase),
                ct);
        return replacement.PreviousTableRootPage;
    }

    /// <summary>
    /// Creates an index store that is not yet present in the catalog. The
    /// caller owns the returned store until it is adopted by
    /// <see cref="ReplaceTableAndIndexStorageAsync"/>.
    /// </summary>
    public async ValueTask<IIndexStore> CreateDetachedIndexStoreAsync(
        IndexSchema schema,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(schema);

        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        try
        {
            return CreateIndexStore(_pager, schema, rootPage);
        }
        catch
        {
            try
            {
                await new BTree(_pager, rootPage).ReclaimAsync(CancellationToken.None);
            }
            catch
            {
                // The active transaction remains the final cleanup boundary.
            }

            throw;
        }
    }

    /// <summary>
    /// Replaces a table root and selected index roots in the active pager
    /// transaction. All inputs are validated and serialized before any root
    /// becomes authoritative; in-memory caches are updated only after every
    /// catalog replacement succeeds.
    /// </summary>
    public async ValueTask<TableAndIndexStorageReplacement> ReplaceTableAndIndexStorageAsync(
        string tableName,
        TableSchema newSchema,
        uint replacementTableRootPage,
        long exactRowCount,
        IReadOnlyDictionary<string, IIndexStore> replacementIndexes,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(exactRowCount);
        ArgumentNullException.ThrowIfNull(newSchema);
        ArgumentNullException.ThrowIfNull(replacementIndexes);

        if (!_tableRootPages.TryGetValue(tableName, out uint persistedRootPage) ||
            !_cache.TryGetValue(tableName, out TableSchema? oldSchema))
        {
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");
        }

        if (!string.Equals(tableName, newSchema.TableName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Replacing table storage cannot rename the table.");

        if (replacementTableRootPage == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(replacementTableRootPage));

        uint previousRootPage = _tableTrees.TryGetValue(tableName, out BTree? existingTree)
            ? existingTree.RootPageId
            : persistedRootPage;
        ValidateRequestedIdentityImmutability(oldSchema, newSchema);
        var storedSchema = NormalizeUpdatedTableSchema(newSchema, oldSchema);
        ValidateStableIdentityUniqueness(storedSchema, tableName);
        byte[] schemaBytes = _schemaSerializer.Serialize(storedSchema);
        byte[] tablePayload = _catalogStore.WriteRootPayload(replacementTableRootPage, schemaBytes);
        long tableKey = _schemaSerializer.TableNameToKey(tableName);

        var liveRoots = new HashSet<uint>();
        foreach ((string liveTableName, uint persistedTableRoot) in _tableRootPages)
        {
            uint liveRoot = _tableTrees.TryGetValue(liveTableName, out BTree? liveTree)
                ? liveTree.RootPageId
                : persistedTableRoot;
            liveRoots.Add(liveRoot);
        }

        foreach ((string liveIndexName, uint persistedIndexRoot) in _indexRootPages)
        {
            uint liveRoot = _indexStores.TryGetValue(liveIndexName, out IIndexStore? liveStore)
                ? liveStore.RootPageId
                : persistedIndexRoot;
            liveRoots.Add(liveRoot);
        }

        var replacementRoots = new HashSet<uint> { replacementTableRootPage };
        if (liveRoots.Contains(replacementTableRootPage))
        {
            throw new InvalidOperationException(
                "Replacement table storage must use a detached root.");
        }

        var preparedIndexes = new List<(
            string IndexName,
            IndexSchema Schema,
            IIndexStore ReplacementStore,
            IIndexStore PreviousStore,
            long CatalogKey,
            byte[] Payload)>(replacementIndexes.Count);
        var seenIndexNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach ((string replacementName, IIndexStore replacementStore) in replacementIndexes)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(replacementName);
            ArgumentNullException.ThrowIfNull(replacementStore);

            if (!seenIndexNames.Add(replacementName))
            {
                throw new InvalidOperationException(
                    $"Replacement index '{replacementName}' was specified more than once.");
            }

            if (!_indexCache.TryGetValue(replacementName, out IndexSchema? indexSchema) ||
                !_indexRootPages.TryGetValue(replacementName, out uint persistedIndexRoot))
            {
                throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Index '{replacementName}' not found.");
            }

            if (!string.Equals(indexSchema.TableName, tableName, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Replacement index '{replacementName}' does not belong to table '{tableName}'.");
            }

            if (indexSchema.State != IndexState.Ready)
            {
                throw new InvalidOperationException(
                    $"Replacement index '{replacementName}' is not ready.");
            }

            uint replacementRoot = replacementStore.RootPageId;
            if (replacementRoot == PageConstants.NullPageId ||
                !replacementRoots.Add(replacementRoot) ||
                liveRoots.Contains(replacementRoot))
            {
                throw new InvalidOperationException(
                    $"Replacement index '{replacementName}' must use a distinct detached root.");
            }

            IIndexStore previousStore =
                _indexStores.TryGetValue(replacementName, out IIndexStore? cachedStore)
                    ? cachedStore
                    : CreateIndexStore(_pager, indexSchema, persistedIndexRoot);
            byte[] indexBytes = _schemaSerializer.SerializeIndex(indexSchema);
            byte[] indexPayload = _catalogStore.WriteRootPayload(replacementRoot, indexBytes);
            long indexKey = _schemaSerializer.IndexNameToKey(replacementName);
            preparedIndexes.Add((
                replacementName,
                indexSchema,
                replacementStore,
                previousStore,
                indexKey,
                indexPayload));
        }

        // Schema-specific statistics cannot survive a physical rewrite. Clear
        // them before the root swap so no fallible catalog work follows the
        // point at which the replacement becomes authoritative.
        await DeleteColumnStatisticsAsync(tableName, ct);
        await DeleteColumnDistributionStatisticsAsync(tableName, ct);
        await DeleteIndexPrefixStatisticsForTableAsync(tableName, ct);

        if (preparedIndexes.Count > 0)
            await EnsureIndexCatalogTreeAsync(ct);

        for (int i = 0; i < preparedIndexes.Count; i++)
        {
            var prepared = preparedIndexes[i];
            if (!await _indexCatalogTree!.ReplaceAsync(
                    prepared.CatalogKey,
                    prepared.Payload,
                    ct))
            {
                throw new CSharpDbException(
                    ErrorCode.TableNotFound,
                    $"Index '{prepared.IndexName}' not found.");
            }
        }

        // Replace the table payload last. A failure before this point leaves
        // the write transaction rollback-only and no cache points at a shadow
        // root.
        if (!await _catalogTree!.ReplaceAsync(tableKey, tablePayload, ct))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        _pager.SchemaRootPage = _catalogTree.RootPageId;
        _cache[tableName] = storedSchema;
        _tableRootPages[tableName] = replacementTableRootPage;
        _persistedTableNextRowIds[tableName] = storedSchema.NextRowId;

        var replacementTree = new BTree(_pager, replacementTableRootPage, tableName);
        replacementTree.SetCachedEntryCount(exactRowCount);
        _tableTrees[tableName] = replacementTree;

        var previousIndexStores =
            new Dictionary<string, IIndexStore>(
                preparedIndexes.Count,
                StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < preparedIndexes.Count; i++)
        {
            var prepared = preparedIndexes[i];
            _indexRootPages[prepared.IndexName] = prepared.ReplacementStore.RootPageId;
            _indexStores[prepared.IndexName] = prepared.ReplacementStore;
            previousIndexStores[prepared.IndexName] = prepared.PreviousStore;
        }

        uint lastPersistedChangeCounter = _tableStatsCache.TryGetValue(tableName, out TableStatistics? stats)
            ? stats.LastPersistedChangeCounter
            : 0;
        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = exactRowCount,
                HasStaleColumns = false,
                LastPersistedChangeCounter = lastPersistedChangeCounter,
            },
            isExact: true,
            markDirty: true);

        await RefreshForeignKeyBindingsAsync(persistChanges: true, ct);
        RebuildForeignKeyCaches();
        IncrementSchemaVersion();
        return new TableAndIndexStorageReplacement(previousRootPage, previousIndexStores);
    }

    public async ValueTask SetTableRowCountAsync(string tableName, long rowCount, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rowCount);
        _pendingTableRowCountDeltas.Remove(tableName);
        uint lastPersistedChangeCounter = _tableStatsCache.TryGetValue(tableName, out var existing)
            ? existing.LastPersistedChangeCounter
            : 0;
        bool hasStaleColumns = existing is not null && existing.HasStaleColumns;

        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = rowCount,
                HasStaleColumns = hasStaleColumns,
                LastPersistedChangeCounter = lastPersistedChangeCounter,
            },
            isExact: true,
            markDirty: true);
    }

    public async ValueTask AdjustTableRowCountAsync(string tableName, long delta, CancellationToken ct = default)
    {
        AccumulateTableRowCountDelta(tableName, delta);
        long rowCount;
        bool hasStaleColumns;
        uint lastPersistedChangeCounter;
        if (_tableStatsCache.TryGetValue(tableName, out var existing))
        {
            lastPersistedChangeCounter = existing.LastPersistedChangeCounter;
            if (_exactTableRowCounts.Contains(tableName))
            {
                rowCount = checked(existing.RowCount + delta);
            }
            else
            {
                long actualRowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
                rowCount = checked(actualRowCount + delta);
            }

            if (rowCount < 0)
            {
                long actualRowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
                rowCount = checked(actualRowCount + delta);
                if (rowCount < 0)
                    throw new InvalidOperationException($"Table '{tableName}' row count would become negative.");
            }
            hasStaleColumns = existing.HasStaleColumns;
        }
        else
        {
            rowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
            hasStaleColumns = false;
            lastPersistedChangeCounter = 0;
        }

        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = rowCount,
                HasStaleColumns = hasStaleColumns,
                LastPersistedChangeCounter = lastPersistedChangeCounter,
            },
            isExact: true,
            markDirty: true);
    }

    public async ValueTask AdjustTableRowCountKnownExactAsync(string tableName, long delta, CancellationToken ct = default)
    {
        AccumulateTableRowCountDelta(tableName, delta);
        long rowCount;
        bool hasStaleColumns;
        uint lastPersistedChangeCounter;
        if (_tableStatsCache.TryGetValue(tableName, out var existing))
        {
            rowCount = checked(existing.RowCount + delta);
            hasStaleColumns = existing.HasStaleColumns;
            lastPersistedChangeCounter = existing.LastPersistedChangeCounter;
        }
        else
        {
            var tree = GetTableTree(tableName);
            long cachedOrExactCount = await tree.CountEntriesAsync(ct);
            rowCount = checked(cachedOrExactCount + delta);
            hasStaleColumns = false;
            lastPersistedChangeCounter = 0;
        }

        if (rowCount < 0)
            throw new InvalidOperationException($"Table '{tableName}' row count would become negative.");

        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = rowCount,
                HasStaleColumns = hasStaleColumns,
                LastPersistedChangeCounter = lastPersistedChangeCounter,
            },
            isExact: true,
            markDirty: true);
    }

    public async ValueTask PersistDirtyTableStatisticsAsync(CancellationToken ct = default)
    {
        if (_dirtyTableStatistics.Count == 0)
            return;

        string[] tableNames = _dirtyTableStatistics.ToArray();
        foreach (string tableName in tableNames)
        {
            if (!_tableStatsCache.TryGetValue(tableName, out var stats))
            {
                _dirtyTableStatistics.Remove(tableName);
                _pendingTableRowCountDeltas.Remove(tableName);
                continue;
            }

            await UpsertTableStatisticsAsync(stats, _exactTableRowCounts.Contains(tableName), ct);
            _dirtyTableStatistics.Remove(tableName);
            _pendingTableRowCountDeltas.Remove(tableName);
        }
    }

    public async ValueTask PersistDirtyAdvisoryStatisticsAsync(CancellationToken ct = default)
    {
        await PersistDirtyColumnStatisticsAsync(ct);
        await PersistDirtyTableStatisticsAsync(ct);
    }

    public async ValueTask ReplaceColumnStatisticsAsync(
        string tableName,
        IReadOnlyList<ColumnStatistics> columnStatistics,
        CancellationToken ct = default)
    {
        _advisoryCatalogContentChanged = true;
        await DeleteColumnStatisticsAsync(tableName, ct);

        bool hasStaleColumns = false;
        for (int i = 0; i < columnStatistics.Count; i++)
        {
            var stats = columnStatistics[i];
            var normalized = new ColumnStatistics
            {
                TableName = tableName,
                ColumnName = stats.ColumnName,
                DistinctCount = stats.DistinctCount,
                NonNullCount = stats.NonNullCount,
                MinValue = stats.MinValue,
                MaxValue = stats.MaxValue,
                IsStale = stats.IsStale,
            };

            await UpsertColumnStatisticsAsync(normalized, ct);
            hasStaleColumns |= normalized.IsStale;
        }

        await SetTableHasStaleColumnsAsync(tableName, hasStaleColumns, ct);
    }

    public async ValueTask ReplaceColumnDistributionStatisticsAsync(
        string tableName,
        IReadOnlyList<ColumnDistributionStatistics> columnStatistics,
        CancellationToken ct = default)
    {
        _advisoryCatalogContentChanged = true;
        await DeleteColumnDistributionStatisticsAsync(tableName, ct);

        for (int i = 0; i < columnStatistics.Count; i++)
        {
            var stats = columnStatistics[i];
            var normalized = new ColumnDistributionStatistics
            {
                TableName = tableName,
                ColumnName = stats.ColumnName,
                HistogramBuckets = stats.HistogramBuckets
                    .Select(static bucket => new HistogramBucketStatistics
                    {
                        LowerBound = bucket.LowerBound,
                        UpperBound = bucket.UpperBound,
                        RowCount = bucket.RowCount,
                    })
                    .ToArray(),
                FrequentValues = stats.FrequentValues
                    .Select(static value => new FrequentValueStatistics
                    {
                        Value = value.Value,
                        RowCount = value.RowCount,
                    })
                    .ToArray(),
            };

            await UpsertColumnDistributionStatisticsAsync(normalized, ct);
        }
    }

    public async ValueTask ReplaceIndexPrefixStatisticsForTableAsync(
        string tableName,
        IReadOnlyList<IndexPrefixStatistics> indexStatistics,
        CancellationToken ct = default)
    {
        _advisoryCatalogContentChanged = true;
        await DeleteIndexPrefixStatisticsForTableAsync(tableName, ct);

        for (int i = 0; i < indexStatistics.Count; i++)
        {
            var stats = indexStatistics[i];
            var normalized = new IndexPrefixStatistics
            {
                IndexName = stats.IndexName,
                TableName = tableName,
                PrefixColumns = stats.PrefixColumns.ToArray(),
                PrefixDistinctCounts = stats.PrefixDistinctCounts.ToArray(),
            };

            await UpsertIndexPrefixStatisticsAsync(normalized, ct);
        }
    }

    public async ValueTask MarkTableColumnStatisticsStaleAsync(string tableName, CancellationToken ct = default)
    {
        if (!_columnStatsByTableSnapshot.TryGetValue(tableName, out var stats) || stats.Length == 0)
            return;

        if (_advisoryStatisticsPersistenceMode == AdvisoryStatisticsPersistenceMode.Deferred)
        {
            bool deferredChanged = false;
            for (int i = 0; i < stats.Length; i++)
            {
                if (stats[i].IsStale)
                    continue;

                CacheColumnStatistics(
                    new ColumnStatistics
                    {
                        TableName = stats[i].TableName,
                        ColumnName = stats[i].ColumnName,
                        DistinctCount = stats[i].DistinctCount,
                        NonNullCount = stats[i].NonNullCount,
                        MinValue = stats[i].MinValue,
                        MaxValue = stats[i].MaxValue,
                        IsStale = true,
                    },
                    markDirty: true);
                deferredChanged = true;
            }

            if (deferredChanged)
                await SetTableHasStaleColumnsAsync(tableName, hasStaleColumns: true, ct);

            return;
        }

        bool changed = false;
        for (int i = 0; i < stats.Length; i++)
        {
            if (stats[i].IsStale)
                continue;

            await UpsertColumnStatisticsAsync(
                new ColumnStatistics
                {
                    TableName = stats[i].TableName,
                    ColumnName = stats[i].ColumnName,
                    DistinctCount = stats[i].DistinctCount,
                    NonNullCount = stats[i].NonNullCount,
                    MinValue = stats[i].MinValue,
                    MaxValue = stats[i].MaxValue,
                    IsStale = true,
                },
                ct);
            changed = true;
        }

        if (changed)
            await SetTableHasStaleColumnsAsync(tableName, hasStaleColumns: true, ct);
    }

    /// <summary>
    /// Get the B+tree for a table's data.
    /// </summary>
    public BTree GetTableTree(string tableName)
    {
        if (_tableTrees.TryGetValue(tableName, out var tree))
            return tree;

        uint rootPage = GetTableRootPage(tableName);
        tree = new BTree(_pager, rootPage, tableName);
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
        var tree = new BTree(pager, rootPage, tableName);
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

    public IReadOnlyList<IndexSchema> GetSqlIndexesForTable(string tableName)
    {
        if (!_indexesByTable.TryGetValue(tableName, out var indexes) || indexes.Length == 0)
            return Array.Empty<IndexSchema>();

        var sqlIndexes = new List<IndexSchema>(indexes.Length);
        for (int i = 0; i < indexes.Length; i++)
        {
            if (indexes[i].Kind is IndexKind.Sql or IndexKind.ConstraintInternal)
                sqlIndexes.Add(indexes[i]);
        }

        return sqlIndexes.Count == 0 ? Array.Empty<IndexSchema>() : sqlIndexes.ToArray();
    }

    /// <summary>
    /// Get an index store using the catalog pager.
    /// </summary>
    public IIndexStore GetIndexStore(string indexName)
    {
        if (_indexStores.TryGetValue(indexName, out var store))
            return store;

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage) &&
            _indexCache.TryGetValue(indexName, out var schema))
        {
            store = CreateIndexStore(_pager, schema, rootPage);
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

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage) &&
            _indexCache.TryGetValue(indexName, out var schema))
        {
            return CreateIndexStore(pager, schema, rootPage);
        }

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
        _indexStores[schema.IndexName] = CreateIndexStore(_pager, schema, indexRootPage);
        AddIndexToTableCache(schema);
        _indexesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    public async ValueTask UpdateIndexSchemaAsync(string oldIndexName, IndexSchema newSchema, CancellationToken ct = default)
    {
        if (!_indexRootPages.TryGetValue(oldIndexName, out uint rootPage))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{oldIndexName}' not found.");

        if (!_indexCache.TryGetValue(oldIndexName, out var oldSchema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{oldIndexName}' not found.");

        if (!string.Equals(oldIndexName, newSchema.IndexName, StringComparison.OrdinalIgnoreCase) &&
            _indexCache.ContainsKey(newSchema.IndexName))
        {
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Index '{newSchema.IndexName}' already exists.");
        }

        long oldKey = _schemaSerializer.IndexNameToKey(oldIndexName);
        await _indexCatalogTree!.DeleteAsync(oldKey, ct);
        _indexCache.Remove(oldIndexName);
        _indexRootPages.Remove(oldIndexName);
        _indexStores.Remove(oldIndexName);
        RemoveIndexFromTableCache(oldSchema);
        await DeleteIndexPrefixStatisticsAsync(oldIndexName, ct);

        byte[] indexBytes = _schemaSerializer.SerializeIndex(newSchema);
        var payload = _catalogStore.WriteRootPayload(rootPage, indexBytes);
        long newKey = _schemaSerializer.IndexNameToKey(newSchema.IndexName);
        await _indexCatalogTree.InsertAsync(newKey, payload, ct);

        _indexCache[newSchema.IndexName] = newSchema;
        _indexRootPages[newSchema.IndexName] = rootPage;
        _indexStores[newSchema.IndexName] = CreateIndexStore(_pager, newSchema, rootPage);
        AddIndexToTableCache(newSchema);
        _indexesSnapshotDirty = true;
        IncrementSchemaVersion();
    }

    public async ValueTask DropIndexAsync(string indexName, CancellationToken ct = default)
        => _ = await DropIndexAsyncCoreAsync(indexName, allowOwnedFullTextDrop: false, ignoreCorruptReclaim: false, ct);

    public ValueTask<bool> DropIndexAllowCorruptReclaimAsync(string indexName, CancellationToken ct = default)
        => DropIndexAsyncCoreAsync(
            indexName,
            allowOwnedFullTextDrop: false,
            ignoreCorruptReclaim: true,
            ct,
            allowOwnedForeignKeyDrop: true,
            allowOwnedConstraintDrop: true);

    public async ValueTask DropForeignKeyOwnedIndexAsync(string indexName, CancellationToken ct = default)
        => _ = await DropIndexAsyncCoreAsync(indexName, allowOwnedFullTextDrop: false, ignoreCorruptReclaim: false, ct, allowOwnedForeignKeyDrop: true);

    public async ValueTask DropConstraintOwnedIndexAsync(string indexName, CancellationToken ct = default)
        => _ = await DropIndexAsyncCoreAsync(
            indexName,
            allowOwnedFullTextDrop: false,
            ignoreCorruptReclaim: false,
            ct,
            allowOwnedConstraintDrop: true);

    private async ValueTask<bool> DropIndexAsyncCoreAsync(
        string indexName,
        bool allowOwnedFullTextDrop,
        bool ignoreCorruptReclaim,
        CancellationToken ct,
        bool allowOwnedForeignKeyDrop = false,
        bool allowOwnedConstraintDrop = false)
    {
        if (!_indexCache.TryGetValue(indexName, out var schema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");

        if (schema.Kind == IndexKind.FullTextInternal && !allowOwnedFullTextDrop)
        {
            string ownerIndexName = string.IsNullOrWhiteSpace(schema.OwnerIndexName)
                ? "its owning full-text index"
                : $"'{schema.OwnerIndexName}'";

            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Full-text owned index '{indexName}' cannot be dropped directly; drop {ownerIndexName} instead.");
        }

        if (schema.Kind == IndexKind.ForeignKeyInternal && !allowOwnedForeignKeyDrop)
        {
            string ownerConstraintName = string.IsNullOrWhiteSpace(schema.OwnerIndexName)
                ? "its owning foreign key constraint"
                : $"foreign key '{schema.OwnerIndexName}'";

            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Foreign key support index '{indexName}' cannot be dropped directly; drop {ownerConstraintName} instead.");
        }

        if (schema.Kind == IndexKind.ConstraintInternal && !allowOwnedConstraintDrop)
        {
            string ownerConstraintName = string.IsNullOrWhiteSpace(schema.OwnerIndexName)
                ? "its owning key constraint"
                : $"key constraint '{schema.OwnerIndexName}'";

            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Key constraint backing index '{indexName}' cannot be dropped directly; drop {ownerConstraintName} instead.");
        }

        if (schema.Kind == IndexKind.FullText)
        {
            string[] ownedIndexes = _indexCache.Values
                .Where(static idx => idx.Kind == IndexKind.FullTextInternal)
                .Where(idx => string.Equals(idx.OwnerIndexName, indexName, StringComparison.OrdinalIgnoreCase))
                .Select(static idx => idx.IndexName)
                .ToArray();

            bool skippedOwnedReclaim = false;
            for (int i = 0; i < ownedIndexes.Length; i++)
            {
                if (_indexCache.ContainsKey(ownedIndexes[i]))
                    skippedOwnedReclaim |= await DropIndexAsyncCoreAsync(
                        ownedIndexes[i],
                        allowOwnedFullTextDrop: true,
                        ignoreCorruptReclaim,
                        ct);
            }

            return await DropIndexCoreAsync(indexName, schema, ignoreCorruptReclaim, skippedOwnedReclaim, ct);
        }

        return await DropIndexCoreAsync(indexName, schema, ignoreCorruptReclaim, skippedOwnedReclaim: false, ct);
    }

    private async ValueTask<bool> DropIndexCoreAsync(
        string indexName,
        IndexSchema schema,
        bool ignoreCorruptReclaim,
        bool skippedOwnedReclaim,
        CancellationToken ct)
    {
        if (!_indexStores.TryGetValue(indexName, out var store))
            store = CreateIndexStore(_pager, _indexCache[indexName], _indexRootPages[indexName]);

        long key = _schemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);
        bool skippedCorruptReclaim = skippedOwnedReclaim;
        if (store is IReclaimableIndexStore reclaimable)
        {
            try
            {
                await reclaimable.ReclaimAsync(ct);
            }
            catch (CSharpDbException ex) when (ignoreCorruptReclaim && ex.Code == ErrorCode.CorruptDatabase)
            {
                skippedCorruptReclaim = true;
            }
        }

        _indexCache.Remove(indexName);
        _indexRootPages.Remove(indexName);
        _indexStores.Remove(indexName);
        RemoveIndexFromTableCache(schema);
        await DeleteIndexPrefixStatisticsAsync(indexName, ct);
        _indexesSnapshotDirty = true;
        IncrementSchemaVersion();
        return skippedCorruptReclaim;
    }

    private IIndexStore CreateIndexStore(Pager pager, IndexSchema schema, uint rootPageId)
    {
        IIndexStore store = _indexProvider.CreateIndexStore(pager, rootPageId, schema.IndexName);
        return ShouldUseOverflowingIndexStore(schema)
            ? new OverflowingIndexStore(store, pager)
            : store;
    }

    private static bool ShouldUseOverflowingIndexStore(IndexSchema schema)
    {
        // FullTextInternal stores keep one postings blob per term, and a hot
        // term (one that appears in thousands of rows) grows that blob past a
        // single leaf cell. A cell larger than a page can never be split
        // ("Unable to split leaf page N: no byte-balanced redistribution fits
        // within page capacity"), so these stores must spill to overflow
        // pages like duplicate-heavy Collection/Sql buckets do.
        return schema.Kind is IndexKind.Collection or IndexKind.Sql or IndexKind.FullTextInternal;
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

    private static void AddDistinctReferences(
        IReadOnlyList<TableForeignKeyReference> source,
        ICollection<TableForeignKeyReference> destination,
        ISet<string> seen)
    {
        for (int i = 0; i < source.Count; i++)
        {
            TableForeignKeyReference reference = source[i];
            string identity = reference.ForeignKey.SchemaId != Guid.Empty
                ? reference.ForeignKey.SchemaId.ToString("N")
                : reference.ForeignKey.ConstraintName;
            string key = $"{reference.TableName}\u001f{identity}";
            if (seen.Add(key))
                destination.Add(reference);
        }
    }

    private void ValidateLoadedStableIdentityUniqueness()
    {
        var identities = new Dictionary<Guid, string>();
        foreach (TableSchema schema in _cache.Values)
        {
            foreach ((Guid id, string description) in EnumerateOwnedSchemaIdentities(schema))
            {
                if (id == Guid.Empty)
                {
                    throw new CSharpDbException(
                        ErrorCode.CorruptDatabase,
                        $"Catalog object {description} has an empty stable identity.");
                }

                if (identities.TryGetValue(id, out string? existing))
                {
                    throw new CSharpDbException(
                        ErrorCode.CorruptDatabase,
                        $"Catalog objects {existing} and {description} share stable identity '{id}'.");
                }

                identities.Add(id, description);
            }
        }
    }

    private void ValidateStableIdentityUniqueness(
        TableSchema candidate,
        string? excludedTableName)
    {
        var identities = new Dictionary<Guid, string>();
        foreach ((Guid id, string description) in EnumerateOwnedSchemaIdentities(candidate))
        {
            if (id == Guid.Empty)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"Catalog object {description} must have a stable identity.");
            }

            if (identities.TryGetValue(id, out string? existing))
                ThrowDuplicateStableIdentity(id, existing, description);

            identities.Add(id, description);
        }

        foreach (TableSchema existingSchema in _cache.Values)
        {
            if (excludedTableName is not null &&
                string.Equals(
                    existingSchema.TableName,
                    excludedTableName,
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach ((Guid id, string description) in EnumerateOwnedSchemaIdentities(existingSchema))
            {
                if (identities.TryGetValue(id, out string? candidateDescription))
                    ThrowDuplicateStableIdentity(id, candidateDescription, description);
            }
        }
    }

    private static void ThrowDuplicateStableIdentity(
        Guid id,
        string firstDescription,
        string secondDescription)
    {
        throw new CSharpDbException(
            ErrorCode.ConstraintViolation,
            $"Catalog objects {firstDescription} and {secondDescription} cannot share stable identity '{id}'.");
    }

    private static IEnumerable<(Guid Id, string Description)> EnumerateOwnedSchemaIdentities(
        TableSchema schema)
    {
        yield return (schema.SchemaId, $"table '{schema.TableName}'");

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            ColumnDefinition column = schema.Columns[i];
            yield return (
                column.SchemaId,
                $"column '{schema.TableName}.{column.Name}'");
        }

        for (int i = 0; i < schema.ForeignKeys.Count; i++)
        {
            ForeignKeyDefinition foreignKey = schema.ForeignKeys[i];
            yield return (
                foreignKey.SchemaId,
                $"foreign key '{schema.TableName}.{foreignKey.ConstraintName}'");
        }

        for (int i = 0; i < schema.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition check = schema.CheckConstraints[i];
            string name = check.ConstraintName ?? $"<unnamed:{i}>";
            yield return (
                check.SchemaId,
                $"check constraint '{schema.TableName}.{name}'");
        }

        for (int i = 0; i < schema.KeyConstraints.Count; i++)
        {
            KeyConstraintDefinition key = schema.KeyConstraints[i];
            string name = key.ConstraintName ?? $"<unnamed:{i}>";
            yield return (
                key.SchemaId,
                $"key constraint '{schema.TableName}.{name}'");
        }
    }

    private static void ValidateRequestedIdentityImmutability(
        TableSchema current,
        TableSchema requested)
    {
        Dictionary<Guid, (string Kind, string Description)>
            currentIdentityOwners = BuildCurrentIdentityOwners(current);
        if (requested.SchemaId != Guid.Empty &&
            requested.SchemaId != current.SchemaId)
        {
            ThrowStableIdentityReplacement(
                $"table '{current.TableName}'",
                current.SchemaId,
                requested.SchemaId);
        }

        var retainedColumnIds = new HashSet<Guid>();
        int addedColumnCount = 0;
        for (int i = 0; i < requested.Columns.Count; i++)
        {
            ColumnDefinition candidate = requested.Columns[i];
            ValidateRequestedIdentityObjectKind(
                currentIdentityOwners,
                candidate.SchemaId,
                "column",
                $"column '{current.TableName}.{candidate.Name}'");
            ColumnDefinition? existingByIdentity = candidate.SchemaId != Guid.Empty
                ? current.Columns.FirstOrDefault(
                    column => column.SchemaId == candidate.SchemaId)
                : null;
            ColumnDefinition? existingByName = current.Columns.FirstOrDefault(
                column => string.Equals(
                    column.Name,
                    candidate.Name,
                    StringComparison.OrdinalIgnoreCase));
            ValidateIdentityDoesNotBelongToDifferentObject(
                candidate.SchemaId,
                existingByIdentity?.SchemaId,
                existingByName?.SchemaId,
                $"column '{current.TableName}.{candidate.Name}'");
            ColumnDefinition? existing = existingByIdentity ?? existingByName;
            if (existing is null)
            {
                addedColumnCount++;
                continue;
            }

            retainedColumnIds.Add(existing.SchemaId);
            if (candidate.SchemaId != Guid.Empty &&
                existing.SchemaId != candidate.SchemaId)
            {
                ThrowStableIdentityReplacement(
                    $"column '{current.TableName}.{existing.Name}'",
                    existing.SchemaId,
                    candidate.SchemaId);
            }
        }
        ValidateNoMixedIdentityRemovalAndAddition(
            current.TableName,
            "columns",
            current.Columns.Count - retainedColumnIds.Count,
            addedColumnCount);

        var retainedForeignKeyIds = new HashSet<Guid>();
        int addedForeignKeyCount = 0;
        for (int i = 0; i < requested.ForeignKeys.Count; i++)
        {
            ForeignKeyDefinition candidate = requested.ForeignKeys[i];
            ValidateRequestedIdentityObjectKind(
                currentIdentityOwners,
                candidate.SchemaId,
                "foreign key",
                $"foreign key '{current.TableName}.{candidate.ConstraintName}'");
            ForeignKeyDefinition? existingByIdentity =
                candidate.SchemaId != Guid.Empty
                ? current.ForeignKeys.FirstOrDefault(
                    foreignKey => foreignKey.SchemaId == candidate.SchemaId)
                : null;
            ForeignKeyDefinition? existingByName =
                current.ForeignKeys.FirstOrDefault(
                foreignKey => string.Equals(
                    foreignKey.ConstraintName,
                    candidate.ConstraintName,
                    StringComparison.OrdinalIgnoreCase));
            ValidateIdentityDoesNotBelongToDifferentObject(
                candidate.SchemaId,
                existingByIdentity?.SchemaId,
                existingByName?.SchemaId,
                $"foreign key '{current.TableName}.{candidate.ConstraintName}'");
            ForeignKeyDefinition? existing =
                existingByIdentity ?? existingByName;
            if (existing is null)
            {
                addedForeignKeyCount++;
                continue;
            }

            retainedForeignKeyIds.Add(existing.SchemaId);
            if (candidate.SchemaId != Guid.Empty &&
                existing.SchemaId != candidate.SchemaId)
            {
                ThrowStableIdentityReplacement(
                    $"foreign key '{current.TableName}.{existing.ConstraintName}'",
                    existing.SchemaId,
                    candidate.SchemaId);
            }
        }
        ValidateNoMixedIdentityRemovalAndAddition(
            current.TableName,
            "foreign keys",
            current.ForeignKeys.Count - retainedForeignKeyIds.Count,
            addedForeignKeyCount);

        var retainedCheckIds = new HashSet<Guid>();
        int addedCheckCount = 0;
        for (int i = 0; i < requested.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition candidate = requested.CheckConstraints[i];
            ValidateRequestedIdentityObjectKind(
                currentIdentityOwners,
                candidate.SchemaId,
                "check constraint",
                $"check constraint on table '{current.TableName}'");
            CheckConstraintDefinition? existingByIdentity =
                candidate.SchemaId != Guid.Empty
                ? current.CheckConstraints.FirstOrDefault(
                    check => check.SchemaId == candidate.SchemaId)
                : null;
            CheckConstraintDefinition? existingByStructure =
                current.CheckConstraints.FirstOrDefault(
                check => CheckConstraintsRepresentSameObject(check, candidate));
            ValidateIdentityDoesNotBelongToDifferentObject(
                candidate.SchemaId,
                existingByIdentity?.SchemaId,
                existingByStructure?.SchemaId,
                $"check constraint on table '{current.TableName}'");
            CheckConstraintDefinition? existing =
                existingByIdentity ?? existingByStructure;
            if (existing is null)
            {
                addedCheckCount++;
                continue;
            }

            retainedCheckIds.Add(existing.SchemaId);
            if (candidate.SchemaId != Guid.Empty &&
                existing.SchemaId != candidate.SchemaId)
            {
                ThrowStableIdentityReplacement(
                    $"check constraint on table '{current.TableName}'",
                    existing.SchemaId,
                    candidate.SchemaId);
            }
        }
        ValidateNoMixedIdentityRemovalAndAddition(
            current.TableName,
            "check constraints",
            current.CheckConstraints.Count - retainedCheckIds.Count,
            addedCheckCount);

        var retainedKeyIds = new HashSet<Guid>();
        int addedKeyCount = 0;
        for (int i = 0; i < requested.KeyConstraints.Count; i++)
        {
            KeyConstraintDefinition candidate = requested.KeyConstraints[i];
            ValidateRequestedIdentityObjectKind(
                currentIdentityOwners,
                candidate.SchemaId,
                "key constraint",
                $"key constraint on table '{current.TableName}'");
            KeyConstraintDefinition? existingByIdentity =
                candidate.SchemaId != Guid.Empty
                ? current.KeyConstraints.FirstOrDefault(
                    key => key.SchemaId == candidate.SchemaId)
                : null;
            KeyConstraintDefinition? existingByStructure =
                current.KeyConstraints.FirstOrDefault(
                key => KeyConstraintsRepresentSameObject(key, candidate));
            ValidateIdentityDoesNotBelongToDifferentObject(
                candidate.SchemaId,
                existingByIdentity?.SchemaId,
                existingByStructure?.SchemaId,
                $"key constraint on table '{current.TableName}'");
            KeyConstraintDefinition? existing =
                existingByIdentity ?? existingByStructure;
            if (existing is null)
            {
                addedKeyCount++;
                continue;
            }

            retainedKeyIds.Add(existing.SchemaId);
            if (candidate.SchemaId != Guid.Empty &&
                existing.SchemaId != candidate.SchemaId)
            {
                ThrowStableIdentityReplacement(
                    $"key constraint on table '{current.TableName}'",
                    existing.SchemaId,
                    candidate.SchemaId);
            }
        }
        ValidateNoMixedIdentityRemovalAndAddition(
            current.TableName,
            "key constraints",
            current.KeyConstraints.Count - retainedKeyIds.Count,
            addedKeyCount);
    }

    private static Dictionary<Guid, (string Kind, string Description)>
        BuildCurrentIdentityOwners(TableSchema schema)
    {
        var owners =
            new Dictionary<Guid, (string Kind, string Description)>
            {
                [schema.SchemaId] =
                    ("table", $"table '{schema.TableName}'"),
            };
        foreach (ColumnDefinition column in schema.Columns)
        {
            owners[column.SchemaId] =
                ("column", $"column '{schema.TableName}.{column.Name}'");
        }
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            owners[foreignKey.SchemaId] =
                (
                    "foreign key",
                    $"foreign key '{schema.TableName}.{foreignKey.ConstraintName}'");
        }
        for (int i = 0; i < schema.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition check = schema.CheckConstraints[i];
            string name = check.ConstraintName ?? $"<unnamed:{i}>";
            owners[check.SchemaId] =
                (
                    "check constraint",
                    $"check constraint '{schema.TableName}.{name}'");
        }
        for (int i = 0; i < schema.KeyConstraints.Count; i++)
        {
            KeyConstraintDefinition key = schema.KeyConstraints[i];
            string name = key.ConstraintName ?? $"<unnamed:{i}>";
            owners[key.SchemaId] =
                (
                    "key constraint",
                    $"key constraint '{schema.TableName}.{name}'");
        }

        return owners;
    }

    private static void ValidateRequestedIdentityObjectKind(
        IReadOnlyDictionary<Guid, (string Kind, string Description)> owners,
        Guid requestedIdentity,
        string requestedKind,
        string requestedDescription)
    {
        if (requestedIdentity == Guid.Empty ||
            !owners.TryGetValue(requestedIdentity, out var owner) ||
            string.Equals(
                owner.Kind,
                requestedKind,
                StringComparison.Ordinal))
        {
            return;
        }

        throw new CSharpDbException(
            ErrorCode.ConstraintViolation,
            $"Stable identity '{requestedIdentity}' owned by {owner.Description} cannot be reassigned to {requestedDescription} during an ordinary schema update.");
    }

    private static void ValidateIdentityDoesNotBelongToDifferentObject(
        Guid requestedIdentity,
        Guid? identityMatch,
        Guid? structuralMatch,
        string description)
    {
        if (requestedIdentity == Guid.Empty ||
            identityMatch is null ||
            structuralMatch is null ||
            identityMatch == structuralMatch)
        {
            return;
        }

        ThrowStableIdentityReplacement(
            description,
            structuralMatch.Value,
            requestedIdentity);
    }

    private static void ValidateNoMixedIdentityRemovalAndAddition(
        string tableName,
        string objectKind,
        int removedCount,
        int addedCount)
    {
        if (removedCount == 0 || addedCount == 0)
            return;

        throw new CSharpDbException(
            ErrorCode.ConstraintViolation,
            $"An ordinary schema update for table '{tableName}' cannot replace {objectKind} by removing and adding them in the same catalog operation. Preserve their stable identities or perform distinct drop and add operations.");
    }

    private static void ThrowStableIdentityReplacement(
        string description,
        Guid existing,
        Guid replacement)
    {
        throw new CSharpDbException(
            ErrorCode.ConstraintViolation,
            $"Stable identity '{existing}' for {description} cannot be replaced with '{replacement}' during an ordinary schema update.");
    }

    private void HydrateLoadedForeignKeyBindings()
    {
        foreach (TableSchema schema in _cache.Values.ToArray())
        {
            TableSchema hydrated = ResolveForeignKeyBindings(
                schema,
                loading: true,
                out bool changed);
            if (changed)
                _cache[schema.TableName] = hydrated;
        }
    }

    private async ValueTask RefreshForeignKeyBindingsAsync(
        bool persistChanges,
        CancellationToken ct)
    {
        foreach (TableSchema schema in _cache.Values.ToArray())
        {
            TableSchema hydrated = ResolveForeignKeyBindings(
                schema,
                loading: false,
                out bool changed);
            if (!changed)
                continue;

            if (persistChanges)
                await PersistTableSchemaPayloadAsync(hydrated, ct);

            _cache[schema.TableName] = hydrated;
        }
    }

    private TableSchema ResolveForeignKeyBindings(
        TableSchema schema,
        bool loading,
        out bool changed)
    {
        changed = false;
        if (schema.ForeignKeys.Count == 0)
            return schema;

        var resolved = new ForeignKeyDefinition[schema.ForeignKeys.Count];
        for (int i = 0; i < schema.ForeignKeys.Count; i++)
        {
            ForeignKeyDefinition foreignKey = schema.ForeignKeys[i];
            ForeignKeyDefinition hydrated = ResolveForeignKeyBinding(
                schema,
                foreignKey,
                loading);
            resolved[i] = hydrated;
            changed |= !ForeignKeyBindingsEqual(foreignKey, hydrated);
        }

        if (!changed)
            return schema;

        return new TableSchema
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = schema.Columns,
            ForeignKeys = resolved,
            CheckConstraints = schema.CheckConstraints,
            KeyConstraints = schema.KeyConstraints,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = schema.NextRowId,
        };
    }

    private ForeignKeyDefinition ResolveForeignKeyBinding(
        TableSchema childSchema,
        ForeignKeyDefinition foreignKey,
        bool loading)
    {
        IReadOnlyList<string> requestedChildColumnNames =
            GetForeignKeyColumnNames(foreignKey);
        IReadOnlyList<string> requestedReferencedColumnNames =
            GetForeignKeyReferencedColumnNames(foreignKey);
        bool hasAnyStableBinding =
            foreignKey.ReferencedTableSchemaId != Guid.Empty ||
            foreignKey.ReferencedKeySchemaId != Guid.Empty ||
            foreignKey.ColumnSchemaIds.Count != 0 ||
            foreignKey.ReferencedColumnSchemaIds.Count != 0;
        bool hasCompleteStableBinding =
            foreignKey.ReferencedTableSchemaId != Guid.Empty &&
            foreignKey.ColumnSchemaIds.Count ==
            requestedChildColumnNames.Count &&
            foreignKey.ReferencedColumnSchemaIds.Count ==
            requestedReferencedColumnNames.Count;
        if (hasAnyStableBinding && !hasCompleteStableBinding)
        {
            ThrowForeignKeyBindingError(
                childSchema,
                foreignKey,
                loading,
                "has partial stable identity bindings");
        }

        (string[] childColumnNames, Guid[] childColumnIds) =
            ResolveForeignKeyColumns(
                childSchema,
                foreignKey,
                childSchema.Columns,
                requestedChildColumnNames,
                foreignKey.ColumnSchemaIds,
                "child",
                loading);

        TableSchema? referencedSchema = null;
        _cache.TryGetValue(
            foreignKey.ReferencedTableName,
            out TableSchema? referencedSchemaByName);
        if (foreignKey.ReferencedTableSchemaId != Guid.Empty)
        {
            referencedSchema = _cache.Values.FirstOrDefault(
                candidate =>
                    candidate.SchemaId == foreignKey.ReferencedTableSchemaId);

            if (referencedSchema is null)
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    referencedSchemaByName is null
                        ? $"is bound to missing table identity '{foreignKey.ReferencedTableSchemaId}'"
                        : $"is bound to missing table identity '{foreignKey.ReferencedTableSchemaId}', while its name resolves to different table identity '{referencedSchemaByName.SchemaId}'");
            }

            if (referencedSchemaByName is not null &&
                referencedSchema.SchemaId != referencedSchemaByName.SchemaId)
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    "has conflicting referenced-table name and identity bindings");
            }
            if (loading && referencedSchemaByName is null)
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    "has a referenced-table name that does not match its stable table identity");
            }
        }
        else
        {
            referencedSchema = referencedSchemaByName;
            if (referencedSchema is null)
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    $"references missing table '{foreignKey.ReferencedTableName}'");
            }
        }

        (string[] referencedColumnNames, Guid[] referencedColumnIds) =
            ResolveForeignKeyColumns(
                childSchema,
                foreignKey,
                referencedSchema.Columns,
                requestedReferencedColumnNames,
                foreignKey.ReferencedColumnSchemaIds,
                "referenced",
                loading);

        Guid referencedKeyId = foreignKey.ReferencedKeySchemaId;
        if (referencedKeyId != Guid.Empty)
        {
            KeyConstraintDefinition? referencedKey =
                referencedSchema.KeyConstraints.FirstOrDefault(
                    key => key.SchemaId == referencedKeyId);
            if (referencedKey is null ||
                !OrderedNamesEqual(
                    referencedKey.Columns,
                    referencedColumnNames))
            {
                if (loading)
                {
                    ThrowForeignKeyBindingError(
                        childSchema,
                        foreignKey,
                        loading,
                        $"has invalid referenced-key identity '{referencedKeyId}'");
                }

                referencedKeyId = referencedSchema.KeyConstraints
                    .FirstOrDefault(key => OrderedNamesEqual(
                        key.Columns,
                        referencedColumnNames))?.SchemaId ?? Guid.Empty;
            }
        }
        else
        {
            referencedKeyId = referencedSchema.KeyConstraints
                .FirstOrDefault(key => OrderedNamesEqual(
                    key.Columns,
                    referencedColumnNames))?.SchemaId ?? Guid.Empty;
        }

        return new ForeignKeyDefinition
        {
            SchemaId = foreignKey.SchemaId,
            ColumnSchemaIds = childColumnIds,
            ReferencedTableSchemaId = referencedSchema.SchemaId,
            ReferencedColumnSchemaIds = referencedColumnIds,
            ReferencedKeySchemaId = referencedKeyId,
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = childColumnNames[0],
            ReferencedTableName = referencedSchema.TableName,
            ReferencedColumnName = referencedColumnNames[0],
            ColumnNames = childColumnNames,
            ReferencedColumnNames = referencedColumnNames,
            OnDelete = foreignKey.OnDelete,
            OnUpdate = foreignKey.OnUpdate,
            SupportingIndexName = foreignKey.SupportingIndexName,
        };
    }

    private static (string[] Names, Guid[] Identities)
        ResolveForeignKeyColumns(
            TableSchema childSchema,
            ForeignKeyDefinition foreignKey,
            IReadOnlyList<ColumnDefinition> availableColumns,
            IReadOnlyList<string> requestedNames,
        IReadOnlyList<Guid> boundIdentities,
        string bindingKind,
        bool loading)
    {
        if (requestedNames.Count == 0 ||
            requestedNames.Distinct(
                StringComparer.OrdinalIgnoreCase).Count() !=
            requestedNames.Count)
        {
            ThrowForeignKeyBindingError(
                childSchema,
                foreignKey,
                loading,
                $"has invalid or repeated {bindingKind} columns");
        }

        Guid[] identitiesByName = ResolveColumnIds(
            availableColumns,
            requestedNames);
        if (boundIdentities.Count == 0)
        {
            if (identitiesByName.Any(static identity =>
                    identity == Guid.Empty))
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    $"references a missing {bindingKind} column");
            }

            return (requestedNames.ToArray(), identitiesByName);
        }

        if (boundIdentities.Count != requestedNames.Count ||
            boundIdentities.Any(static identity =>
                identity == Guid.Empty) ||
            boundIdentities.Distinct().Count() !=
            boundIdentities.Count)
        {
            ThrowForeignKeyBindingError(
                childSchema,
                foreignKey,
                loading,
                $"has invalid {bindingKind}-column identity bindings");
        }

        var columnsByIdentity = new ColumnDefinition[boundIdentities.Count];
        for (int i = 0; i < boundIdentities.Count; i++)
        {
            ColumnDefinition? boundColumn =
                availableColumns.FirstOrDefault(column =>
                    column.SchemaId == boundIdentities[i]);
            if (boundColumn is null)
            {
                ThrowForeignKeyBindingError(
                    childSchema,
                    foreignKey,
                    loading,
                    $"is bound to missing {bindingKind} column identity '{boundIdentities[i]}'");
            }

            columnsByIdentity[i] = boundColumn;
        }

        if (identitiesByName.SequenceEqual(boundIdentities))
            return (requestedNames.ToArray(), boundIdentities.ToArray());

        if (loading)
        {
            ThrowForeignKeyBindingError(
                childSchema,
                foreignKey,
                loading,
                $"has conflicting {bindingKind}-column name and identity bindings");
        }

        return (
            columnsByIdentity.Select(static column =>
                column.Name).ToArray(),
            boundIdentities.ToArray());
    }

    [DoesNotReturn]
    private static void ThrowForeignKeyBindingError(
        TableSchema childSchema,
        ForeignKeyDefinition foreignKey,
        bool loading,
        string detail)
    {
        throw new CSharpDbException(
            loading
                ? ErrorCode.CorruptDatabase
                : ErrorCode.ConstraintViolation,
            $"Foreign key '{childSchema.TableName}.{foreignKey.ConstraintName}' {detail}.");
    }

    private static bool ForeignKeyBindingsEqual(
        ForeignKeyDefinition left,
        ForeignKeyDefinition right) =>
        left.ReferencedTableSchemaId == right.ReferencedTableSchemaId &&
        left.ReferencedKeySchemaId == right.ReferencedKeySchemaId &&
        left.ColumnSchemaIds.SequenceEqual(right.ColumnSchemaIds) &&
        left.ReferencedColumnSchemaIds.SequenceEqual(
            right.ReferencedColumnSchemaIds) &&
        string.Equals(
            left.ColumnName,
            right.ColumnName,
            StringComparison.OrdinalIgnoreCase) &&
        string.Equals(
            left.ReferencedTableName,
            right.ReferencedTableName,
            StringComparison.OrdinalIgnoreCase) &&
        string.Equals(
            left.ReferencedColumnName,
            right.ReferencedColumnName,
            StringComparison.OrdinalIgnoreCase) &&
        left.ColumnNames.SequenceEqual(
            right.ColumnNames,
            StringComparer.OrdinalIgnoreCase) &&
        left.ReferencedColumnNames.SequenceEqual(
            right.ReferencedColumnNames,
            StringComparer.OrdinalIgnoreCase);

    private static IReadOnlyList<string> GetForeignKeyColumnNames(
        ForeignKeyDefinition foreignKey) =>
        foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName];

    private static IReadOnlyList<string> GetForeignKeyReferencedColumnNames(
        ForeignKeyDefinition foreignKey) =>
        foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName];

    private async ValueTask PersistTableSchemaPayloadAsync(
        TableSchema schema,
        CancellationToken ct)
    {
        if (!_tableRootPages.TryGetValue(schema.TableName, out uint rootPage))
        {
            throw new CSharpDbException(
                ErrorCode.TableNotFound,
                $"Table '{schema.TableName}' not found.");
        }

        byte[] schemaBytes = _schemaSerializer.Serialize(schema);
        byte[] payload = _catalogStore.WriteRootPayload(rootPage, schemaBytes);
        long key = _schemaSerializer.TableNameToKey(schema.TableName);
        if (!await _catalogTree!.ReplaceAsync(key, payload, ct))
        {
            throw new CSharpDbException(
                ErrorCode.TableNotFound,
                $"Table '{schema.TableName}' not found.");
        }

        _pager.SchemaRootPage = _catalogTree.RootPageId;
        _persistedTableNextRowIds[schema.TableName] = schema.NextRowId;
    }

    private void RebuildForeignKeyCaches()
    {
        _foreignKeysByTable.Clear();
        _referencingForeignKeysByParentTable.Clear();
        _referencingForeignKeysByParentTableId.Clear();

        foreach (TableSchema schema in _cache.Values)
        {
            ForeignKeyDefinition[] foreignKeys = schema.ForeignKeys.ToArray();
            if (foreignKeys.Length > 0)
                _foreignKeysByTable[schema.TableName] = foreignKeys;

            for (int i = 0; i < foreignKeys.Length; i++)
                AddReferencingForeignKey(schema.TableName, foreignKeys[i]);
        }
    }

    private void AddReferencingForeignKey(string tableName, ForeignKeyDefinition foreignKey)
    {
        var reference = new TableForeignKeyReference
        {
            TableName = tableName,
            ForeignKey = foreignKey,
        };

        Guid referencedTableSchemaId = foreignKey.ReferencedTableSchemaId;
        if (referencedTableSchemaId == Guid.Empty &&
            _cache.TryGetValue(foreignKey.ReferencedTableName, out TableSchema? referencedSchema))
        {
            referencedTableSchemaId = referencedSchema.SchemaId;
        }

        if (referencedTableSchemaId != Guid.Empty)
            AddReferencingForeignKeyByIdentity(referencedTableSchemaId, reference);

        if (_referencingForeignKeysByParentTable.TryGetValue(foreignKey.ReferencedTableName, out var existing))
        {
            var updated = new TableForeignKeyReference[existing.Length + 1];
            Array.Copy(existing, updated, existing.Length);
            updated[^1] = reference;
            _referencingForeignKeysByParentTable[foreignKey.ReferencedTableName] = updated;
            return;
        }

        _referencingForeignKeysByParentTable[foreignKey.ReferencedTableName] = new[] { reference };
    }

    private void AddReferencingForeignKeyByIdentity(
        Guid referencedTableSchemaId,
        TableForeignKeyReference reference)
    {
        if (_referencingForeignKeysByParentTableId.TryGetValue(referencedTableSchemaId, out var existing))
        {
            var updated = new TableForeignKeyReference[existing.Length + 1];
            Array.Copy(existing, updated, existing.Length);
            updated[^1] = reference;
            _referencingForeignKeysByParentTableId[referencedTableSchemaId] = updated;
            return;
        }

        _referencingForeignKeysByParentTableId[referencedTableSchemaId] = new[] { reference };
    }

    private static TableSchema AdoptStructurallyEquivalentIdentities(
        TableSchema live,
        TableSchema identitySource)
    {
        EnsureStructurallyEquivalent(live, identitySource);

        Guid tableId = identitySource.SchemaId != Guid.Empty
            ? identitySource.SchemaId
            : live.SchemaId;
        var columns = new ColumnDefinition[live.Columns.Count];
        for (int i = 0; i < columns.Length; i++)
        {
            ColumnDefinition current = live.Columns[i];
            ColumnDefinition source = identitySource.Columns[i];
            columns[i] = new ColumnDefinition
            {
                SchemaId = source.SchemaId != Guid.Empty
                    ? source.SchemaId
                    : current.SchemaId,
                Name = current.Name,
                Type = current.Type,
                DeclaredType = current.DeclaredType,
                Nullable = current.Nullable,
                IsPrimaryKey = current.IsPrimaryKey,
                IsIdentity = current.IsIdentity,
                IsRowVersion = current.IsRowVersion,
                Collation = current.Collation,
                DefaultSql = current.DefaultSql,
            };
        }

        var checks = new CheckConstraintDefinition[live.CheckConstraints.Count];
        var usedSourceChecks = new bool[identitySource.CheckConstraints.Count];
        for (int i = 0; i < checks.Length; i++)
        {
            CheckConstraintDefinition current = live.CheckConstraints[i];
            int sourceIndex = TakeMatchingCheckIndex(
                current,
                identitySource,
                usedSourceChecks);
            CheckConstraintDefinition source =
                identitySource.CheckConstraints[sourceIndex];
            checks[i] = new CheckConstraintDefinition
            {
                SchemaId = source.SchemaId != Guid.Empty
                    ? source.SchemaId
                    : current.SchemaId,
                ConstraintName = current.ConstraintName,
                ExpressionSql = current.ExpressionSql,
                ColumnName = current.ColumnName,
            };
        }

        var keys = new KeyConstraintDefinition[live.KeyConstraints.Count];
        var usedSourceKeys = new bool[identitySource.KeyConstraints.Count];
        for (int i = 0; i < keys.Length; i++)
        {
            KeyConstraintDefinition current = live.KeyConstraints[i];
            int sourceIndex = TakeMatchingKeyIndex(
                current,
                identitySource,
                usedSourceKeys);
            KeyConstraintDefinition source =
                identitySource.KeyConstraints[sourceIndex];
            keys[i] = new KeyConstraintDefinition
            {
                SchemaId = source.SchemaId != Guid.Empty
                    ? source.SchemaId
                    : current.SchemaId,
                ConstraintName = current.ConstraintName,
                Kind = current.Kind,
                Columns = current.Columns,
                BackingIndexName = current.BackingIndexName,
            };
        }

        var foreignKeys = new ForeignKeyDefinition[live.ForeignKeys.Count];
        var usedSourceForeignKeys = new bool[identitySource.ForeignKeys.Count];
        for (int i = 0; i < foreignKeys.Length; i++)
        {
            ForeignKeyDefinition current = live.ForeignKeys[i];
            int sourceIndex = TakeMatchingForeignKeyIndex(
                current,
                live.TableName,
                identitySource,
                usedSourceForeignKeys);
            ForeignKeyDefinition source = identitySource.ForeignKeys[sourceIndex];
            IReadOnlyList<string> childColumnNames =
                GetForeignKeyColumnNames(current);
            IReadOnlyList<string> referencedColumnNames =
                GetForeignKeyReferencedColumnNames(current);
            bool selfReference = string.Equals(
                current.ReferencedTableName,
                live.TableName,
                StringComparison.OrdinalIgnoreCase);
            Guid referencedTableId = selfReference
                ? tableId
                : current.ReferencedTableSchemaId;
            Guid[] referencedColumnIds = selfReference
                ? ResolveColumnIds(columns, referencedColumnNames)
                : current.ReferencedColumnSchemaIds.ToArray();
            Guid referencedKeyId = current.ReferencedKeySchemaId;
            if (selfReference)
            {
                if (source.ReferencedKeySchemaId != Guid.Empty)
                {
                    KeyConstraintDefinition? adoptedReferencedKey =
                        keys.FirstOrDefault(key =>
                            key.SchemaId ==
                            source.ReferencedKeySchemaId &&
                            OrderedNamesEqual(
                                key.Columns,
                                referencedColumnNames));
                    if (adoptedReferencedKey is null)
                        ThrowIdentitySourceShapeMismatch(live.TableName);
                    referencedKeyId =
                        adoptedReferencedKey.SchemaId;
                }
                else
                {
                    referencedKeyId = keys.FirstOrDefault(key =>
                        OrderedNamesEqual(
                            key.Columns,
                            referencedColumnNames))?.SchemaId ?? Guid.Empty;
                }
            }

            foreignKeys[i] = new ForeignKeyDefinition
            {
                SchemaId = source.SchemaId != Guid.Empty
                    ? source.SchemaId
                    : current.SchemaId,
                ColumnSchemaIds = ResolveColumnIds(
                    columns,
                    childColumnNames),
                ReferencedTableSchemaId = referencedTableId,
                ReferencedColumnSchemaIds = referencedColumnIds,
                ReferencedKeySchemaId = referencedKeyId,
                ConstraintName = current.ConstraintName,
                ColumnName = current.ColumnName,
                ReferencedTableName = current.ReferencedTableName,
                ReferencedColumnName = current.ReferencedColumnName,
                ColumnNames = current.ColumnNames,
                ReferencedColumnNames = current.ReferencedColumnNames,
                OnDelete = current.OnDelete,
                OnUpdate = current.OnUpdate,
                SupportingIndexName = current.SupportingIndexName,
            };
        }

        return new TableSchema
        {
            SchemaId = tableId,
            TableName = live.TableName,
            Columns = columns,
            ForeignKeys = foreignKeys,
            CheckConstraints = checks,
            KeyConstraints = keys,
            QualifiedMappings = live.QualifiedMappings,
            NextRowId = live.NextRowId,
        };
    }

    private static void EnsureStructurallyEquivalent(
        TableSchema live,
        TableSchema identitySource)
    {
        if (live.Columns.Count != identitySource.Columns.Count ||
            live.ForeignKeys.Count != identitySource.ForeignKeys.Count ||
            live.CheckConstraints.Count != identitySource.CheckConstraints.Count ||
            live.KeyConstraints.Count != identitySource.KeyConstraints.Count)
        {
            ThrowIdentitySourceShapeMismatch(live.TableName);
        }

        for (int i = 0; i < live.Columns.Count; i++)
        {
            ColumnDefinition left = live.Columns[i];
            ColumnDefinition right = identitySource.Columns[i];
            if (!string.Equals(left.Name, right.Name, StringComparison.OrdinalIgnoreCase) ||
                left.Type != right.Type ||
                left.Nullable != right.Nullable ||
                left.IsPrimaryKey != right.IsPrimaryKey ||
                left.IsIdentity != right.IsIdentity ||
                left.IsRowVersion != right.IsRowVersion ||
                !string.Equals(left.Collation, right.Collation, StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(left.DefaultSql, right.DefaultSql, StringComparison.Ordinal))
            {
                ThrowIdentitySourceShapeMismatch(live.TableName);
            }
        }

        var usedSourceForeignKeys = new bool[identitySource.ForeignKeys.Count];
        for (int i = 0; i < live.ForeignKeys.Count; i++)
            TakeMatchingForeignKeyIndex(
                live.ForeignKeys[i],
                live.TableName,
                identitySource,
                usedSourceForeignKeys);

        var usedSourceChecks = new bool[identitySource.CheckConstraints.Count];
        for (int i = 0; i < live.CheckConstraints.Count; i++)
            TakeMatchingCheckIndex(
                live.CheckConstraints[i],
                identitySource,
                usedSourceChecks);

        var usedSourceKeys = new bool[identitySource.KeyConstraints.Count];
        for (int i = 0; i < live.KeyConstraints.Count; i++)
            TakeMatchingKeyIndex(
                live.KeyConstraints[i],
                identitySource,
                usedSourceKeys);
    }

    private static int TakeMatchingForeignKeyIndex(
        ForeignKeyDefinition liveForeignKey,
        string liveTableName,
        TableSchema identitySource,
        bool[] used)
    {
        for (int i = 0; i < identitySource.ForeignKeys.Count; i++)
        {
            if (used[i] ||
                !ForeignKeysStructurallyEquivalent(
                    liveForeignKey,
                    liveTableName,
                    identitySource.ForeignKeys[i],
                    identitySource.TableName))
            {
                continue;
            }

            used[i] = true;
            return i;
        }

        ThrowIdentitySourceShapeMismatch(liveTableName);
        return -1;
    }

    private static int TakeMatchingCheckIndex(
        CheckConstraintDefinition liveCheck,
        TableSchema identitySource,
        bool[] used)
    {
        for (int i = 0; i < identitySource.CheckConstraints.Count; i++)
        {
            if (used[i] ||
                !ChecksStructurallyEquivalent(
                    liveCheck,
                    identitySource.CheckConstraints[i]))
            {
                continue;
            }

            used[i] = true;
            return i;
        }

        ThrowIdentitySourceShapeMismatch(identitySource.TableName);
        return -1;
    }

    private static int TakeMatchingKeyIndex(
        KeyConstraintDefinition liveKey,
        TableSchema identitySource,
        bool[] used)
    {
        for (int i = 0; i < identitySource.KeyConstraints.Count; i++)
        {
            if (used[i] ||
                !KeysStructurallyEquivalent(
                    liveKey,
                    identitySource.KeyConstraints[i]))
            {
                continue;
            }

            used[i] = true;
            return i;
        }

        ThrowIdentitySourceShapeMismatch(identitySource.TableName);
        return -1;
    }

    private static bool ForeignKeysStructurallyEquivalent(
        ForeignKeyDefinition left,
        string leftTableName,
        ForeignKeyDefinition right,
        string rightTableName)
    {
        bool leftSelfReference = string.Equals(
            left.ReferencedTableName,
            leftTableName,
            StringComparison.OrdinalIgnoreCase);
        bool rightSelfReference = string.Equals(
            right.ReferencedTableName,
            rightTableName,
            StringComparison.OrdinalIgnoreCase);
        bool referencedTablesEqual =
            leftSelfReference && rightSelfReference ||
            string.Equals(
                left.ReferencedTableName,
                right.ReferencedTableName,
                StringComparison.OrdinalIgnoreCase);
        return string.Equals(
                left.ConstraintName,
                right.ConstraintName,
                StringComparison.OrdinalIgnoreCase) &&
            OrderedNamesEqual(
                GetForeignKeyColumnNames(left),
                GetForeignKeyColumnNames(right)) &&
            referencedTablesEqual &&
            OrderedNamesEqual(
                GetForeignKeyReferencedColumnNames(left),
                GetForeignKeyReferencedColumnNames(right)) &&
            left.OnDelete == right.OnDelete &&
            left.OnUpdate == right.OnUpdate;
    }

    private static bool ChecksStructurallyEquivalent(
        CheckConstraintDefinition left,
        CheckConstraintDefinition right) =>
        string.Equals(
            left.ConstraintName,
            right.ConstraintName,
            StringComparison.OrdinalIgnoreCase) &&
        string.Equals(
            left.ExpressionSql,
            right.ExpressionSql,
            StringComparison.Ordinal) &&
        string.Equals(
            left.ColumnName,
            right.ColumnName,
            StringComparison.OrdinalIgnoreCase);

    private static bool KeysStructurallyEquivalent(
        KeyConstraintDefinition left,
        KeyConstraintDefinition right) =>
        string.Equals(
            left.ConstraintName,
            right.ConstraintName,
            StringComparison.OrdinalIgnoreCase) &&
        left.Kind == right.Kind &&
        OrderedNamesEqual(left.Columns, right.Columns);

    [DoesNotReturn]
    private static void ThrowIdentitySourceShapeMismatch(string tableName)
    {
        throw new CSharpDbException(
            ErrorCode.ConstraintViolation,
            $"Stable identities can only be applied to a structurally equivalent schema for table '{tableName}'.");
    }

    private TableSchema NormalizeNewTableSchema(TableSchema schema)
    {
        long normalizedNextRowId = schema.NextRowId > 0 ? schema.NextRowId : 1;
        return NormalizeSchemaIdentities(
            schema,
            previous: null,
            normalizedNextRowId);
    }

    private TableSchema NormalizeUpdatedTableSchema(
        TableSchema schema,
        TableSchema previous)
    {
        // Legacy catalog entries use 0 to mean "unknown"; preserve that sentinel on ALTER/RENAME
        // so the allocator recomputes max(rowid)+1 instead of resetting to 1 after a schema rewrite.
        long normalizedNextRowId = schema.NextRowId > 0
            ? schema.NextRowId
            : previous.NextRowId;
        if (normalizedNextRowId < 0)
            normalizedNextRowId = 0;

        return NormalizeSchemaIdentities(
            schema,
            previous,
            normalizedNextRowId);
    }

    private TableSchema NormalizeSchemaIdentities(
        TableSchema schema,
        TableSchema? previous,
        long normalizedNextRowId)
    {
        Guid tableId = schema.SchemaId != Guid.Empty
            ? schema.SchemaId
            : previous?.SchemaId is { } previousId && previousId != Guid.Empty
                ? previousId
                : SchemaIdentity.Create();

        ColumnDefinition[] columns = schema.Columns
            .Select(column =>
            {
                ColumnDefinition? prior = column.SchemaId != Guid.Empty
                    ? previous?.Columns.FirstOrDefault(
                        candidate => candidate.SchemaId == column.SchemaId)
                    : null;
                prior ??= previous?.Columns.FirstOrDefault(
                    candidate => string.Equals(
                        candidate.Name,
                        column.Name,
                        StringComparison.OrdinalIgnoreCase));
                Guid id = column.SchemaId != Guid.Empty
                    ? column.SchemaId
                    : prior?.SchemaId is { } priorId && priorId != Guid.Empty
                        ? priorId
                        : SchemaIdentity.Create();
                return new ColumnDefinition
                {
                    SchemaId = id,
                    Name = column.Name,
                    Type = column.Type,
                    DeclaredType = column.DeclaredType,
                    Nullable = column.Nullable,
                    IsPrimaryKey = column.IsPrimaryKey,
                    IsIdentity = column.IsIdentity,
                    IsRowVersion = column.IsRowVersion,
                    Collation = column.Collation,
                    DefaultSql = column.DefaultSql,
                };
            })
            .ToArray();

        KeyConstraintDefinition[] keys = schema.KeyConstraints
            .Select(constraint =>
            {
                KeyConstraintDefinition? prior = constraint.SchemaId != Guid.Empty
                    ? previous?.KeyConstraints.FirstOrDefault(
                        candidate => candidate.SchemaId == constraint.SchemaId)
                    : null;
                prior ??= previous?.KeyConstraints.FirstOrDefault(
                    candidate => KeyConstraintsRepresentSameObject(
                        candidate,
                        constraint));
                return new KeyConstraintDefinition
                {
                    SchemaId = ResolveIdentity(constraint.SchemaId, prior?.SchemaId),
                    ConstraintName = constraint.ConstraintName,
                    Kind = constraint.Kind,
                    Columns = constraint.Columns,
                    BackingIndexName = constraint.BackingIndexName,
                };
            })
            .ToArray();

        ForeignKeyDefinition[] foreignKeys = schema.ForeignKeys
            .Select(constraint =>
            {
                ForeignKeyDefinition? prior = constraint.SchemaId != Guid.Empty
                    ? previous?.ForeignKeys.FirstOrDefault(
                        candidate => candidate.SchemaId == constraint.SchemaId)
                    : null;
                prior ??= previous?.ForeignKeys.FirstOrDefault(
                    candidate => string.Equals(
                        candidate.ConstraintName,
                        constraint.ConstraintName,
                        StringComparison.OrdinalIgnoreCase));
                IReadOnlyList<string> childColumnNames =
                    constraint.ColumnNames.Count > 0
                        ? constraint.ColumnNames
                        : [constraint.ColumnName];
                IReadOnlyList<string> referencedColumnNames =
                    constraint.ReferencedColumnNames.Count > 0
                        ? constraint.ReferencedColumnNames
                        : [constraint.ReferencedColumnName];
                Guid[] childColumnIds =
                    constraint.ColumnSchemaIds.Count > 0
                        ? constraint.ColumnSchemaIds.ToArray()
                        : ResolveColumnIds(
                            columns,
                            childColumnNames);

                bool selfReference = string.Equals(
                    constraint.ReferencedTableName,
                    schema.TableName,
                    StringComparison.OrdinalIgnoreCase);
                TableSchema? referencedSchema = selfReference
                    ? null
                    : _cache.GetValueOrDefault(constraint.ReferencedTableName);
                Guid referencedTableId =
                    constraint.ReferencedTableSchemaId != Guid.Empty
                        ? constraint.ReferencedTableSchemaId
                        : selfReference
                            ? tableId
                            : referencedSchema?.SchemaId ?? Guid.Empty;
                IReadOnlyList<ColumnDefinition> referencedColumns = selfReference
                    ? columns
                    : referencedSchema?.Columns ?? [];
                IReadOnlyList<KeyConstraintDefinition> referencedKeys = selfReference
                    ? keys
                    : referencedSchema?.KeyConstraints ?? [];
                Guid[] referencedColumnIds =
                    constraint.ReferencedColumnSchemaIds.Count > 0
                        ? constraint.ReferencedColumnSchemaIds.ToArray()
                        : ResolveColumnIds(
                            referencedColumns,
                            referencedColumnNames);
                Guid referencedKeyId =
                    constraint.ReferencedKeySchemaId != Guid.Empty
                        ? constraint.ReferencedKeySchemaId
                        : referencedKeys.FirstOrDefault(key =>
                            OrderedNamesEqual(
                                key.Columns,
                                referencedColumnNames))?.SchemaId ??
                          Guid.Empty;
                return new ForeignKeyDefinition
                {
                    SchemaId = ResolveIdentity(constraint.SchemaId, prior?.SchemaId),
                    ColumnSchemaIds = childColumnIds,
                    ReferencedTableSchemaId = referencedTableId,
                    ReferencedColumnSchemaIds = referencedColumnIds,
                    ReferencedKeySchemaId = referencedKeyId,
                    ConstraintName = constraint.ConstraintName,
                    ColumnName = constraint.ColumnName,
                    ReferencedTableName = constraint.ReferencedTableName,
                    ReferencedColumnName = constraint.ReferencedColumnName,
                    ColumnNames = constraint.ColumnNames,
                    ReferencedColumnNames = constraint.ReferencedColumnNames,
                    OnDelete = constraint.OnDelete,
                    OnUpdate = constraint.OnUpdate,
                    SupportingIndexName = constraint.SupportingIndexName,
                };
            })
            .ToArray();

        CheckConstraintDefinition[] checks = schema.CheckConstraints
            .Select(constraint =>
            {
                CheckConstraintDefinition? prior = constraint.SchemaId != Guid.Empty
                    ? previous?.CheckConstraints.FirstOrDefault(
                        candidate => candidate.SchemaId == constraint.SchemaId)
                    : null;
                prior ??= previous?.CheckConstraints.FirstOrDefault(
                    candidate => CheckConstraintsRepresentSameObject(
                        candidate,
                        constraint));
                return new CheckConstraintDefinition
                {
                    SchemaId = ResolveIdentity(constraint.SchemaId, prior?.SchemaId),
                    ConstraintName = constraint.ConstraintName,
                    ExpressionSql = constraint.ExpressionSql,
                    ColumnName = constraint.ColumnName,
                };
            })
            .ToArray();

        return new TableSchema
        {
            SchemaId = tableId,
            TableName = schema.TableName,
            Columns = columns,
            ForeignKeys = foreignKeys,
            CheckConstraints = checks,
            KeyConstraints = keys,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = normalizedNextRowId,
        };
    }

    private static Guid ResolveIdentity(Guid requested, Guid? previous) =>
        requested != Guid.Empty
            ? requested
            : previous is { } previousId && previousId != Guid.Empty
                ? previousId
                : SchemaIdentity.Create();

    private static Guid[] ResolveColumnIds(
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<string> names) =>
        names.Select(name =>
            columns.FirstOrDefault(column => string.Equals(
                column.Name,
                name,
                StringComparison.OrdinalIgnoreCase))?.SchemaId ?? Guid.Empty)
            .ToArray();

    private static bool KeyConstraintsRepresentSameObject(
        KeyConstraintDefinition left,
        KeyConstraintDefinition right)
    {
        if (left.ConstraintName is not null ||
            right.ConstraintName is not null)
        {
            return left.ConstraintName is not null &&
                right.ConstraintName is not null &&
                string.Equals(
                    left.ConstraintName,
                    right.ConstraintName,
                    StringComparison.OrdinalIgnoreCase);
        }

        return left.Kind == right.Kind &&
            OrderedNamesEqual(left.Columns, right.Columns);
    }

    private static bool CheckConstraintsRepresentSameObject(
        CheckConstraintDefinition left,
        CheckConstraintDefinition right)
    {
        if (left.ConstraintName is not null ||
            right.ConstraintName is not null)
        {
            return left.ConstraintName is not null &&
                right.ConstraintName is not null &&
                string.Equals(
                    left.ConstraintName,
                    right.ConstraintName,
                    StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(
                left.ExpressionSql,
                right.ExpressionSql,
                StringComparison.Ordinal) &&
            string.Equals(
                left.ColumnName,
                right.ColumnName,
                StringComparison.OrdinalIgnoreCase);
    }

    private static bool OrderedNamesEqual(
        IReadOnlyList<string> left,
        IReadOnlyList<string> right) =>
        left.Count == right.Count &&
        left.Zip(right).All(pair => string.Equals(
            pair.First,
            pair.Second,
            StringComparison.OrdinalIgnoreCase));

    private static bool HaveMatchingColumnNames(TableSchema left, TableSchema right)
    {
        if (left.Columns.Count != right.Columns.Count)
            return false;

        for (int i = 0; i < left.Columns.Count; i++)
        {
            if (!string.Equals(left.Columns[i].Name, right.Columns[i].Name, StringComparison.OrdinalIgnoreCase))
                return false;
        }

        return true;
    }

    private async ValueTask UpsertTableStatisticsAsync(TableStatistics stats, bool isExact, CancellationToken ct)
    {
        await EnsureTableStatsCatalogTreeAsync(ct);

        TableStatistics storedStats = isExact
            ? new TableStatistics
            {
                TableName = stats.TableName,
                RowCount = stats.RowCount,
                RowCountIsExact = true,
                HasStaleColumns = stats.HasStaleColumns,
                LastPersistedChangeCounter = unchecked(_pager.ChangeCounter + 1),
            }
            : stats;

        byte[] payload = SerializeTableStatistics(storedStats);
        long key = _schemaSerializer.TableNameToKey(storedStats.TableName);

        if (!await _tableStatsCatalogTree!.ReplaceAsync(key, payload, ct))
            await _tableStatsCatalogTree.InsertAsync(key, payload, ct);

        CacheTableStatistics(storedStats, isExact, markDirty: false);
    }

    private void CacheTableStatistics(TableStatistics stats, bool isExact, bool markDirty)
    {
        var normalized = new TableStatistics
        {
            TableName = stats.TableName,
            RowCount = stats.RowCount,
            RowCountIsExact = isExact,
            HasStaleColumns = stats.HasStaleColumns,
            LastPersistedChangeCounter = stats.LastPersistedChangeCounter,
        };

        _tableStatsCache[normalized.TableName] = normalized;
        if (isExact)
            _exactTableRowCounts.Add(normalized.TableName);
        else
            _exactTableRowCounts.Remove(normalized.TableName);

        if (markDirty)
            _dirtyTableStatistics.Add(normalized.TableName);

        _tableStatisticsSnapshotDirty = true;
        if (_tableTrees.TryGetValue(normalized.TableName, out var tree))
            tree.SetCachedEntryCount(normalized.RowCount);
    }

    private async ValueTask DeleteTableStatisticsAsync(string tableName, CancellationToken ct)
    {
        _dirtyTableStatistics.Remove(tableName);
        _pendingTableRowCountDeltas.Remove(tableName);
        _exactTableRowCounts.Remove(tableName);
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

        bool isExact = _exactTableRowCounts.Contains(oldTableName);
        _dirtyTableStatistics.Remove(oldTableName);
        _pendingTableRowCountDeltas.Remove(oldTableName);
        await DeleteTableStatisticsAsync(oldTableName, ct);
        await UpsertTableStatisticsAsync(
            new TableStatistics
            {
                TableName = newTableName,
                RowCount = stats.RowCount,
                HasStaleColumns = stats.HasStaleColumns,
                LastPersistedChangeCounter = stats.LastPersistedChangeCounter,
            },
            isExact,
            ct);
    }

    private void AccumulateTableRowCountDelta(string tableName, long delta)
    {
        if (delta == 0)
            return;

        if (_pendingTableRowCountDeltas.TryGetValue(tableName, out long existing))
            _pendingTableRowCountDeltas[tableName] = checked(existing + delta);
        else
            _pendingTableRowCountDeltas[tableName] = delta;
    }

    private async ValueTask SetTableHasStaleColumnsAsync(string tableName, bool hasStaleColumns, CancellationToken ct)
    {
        if (_tableStatsCache.TryGetValue(tableName, out var stats))
        {
            if (stats.HasStaleColumns == hasStaleColumns)
                return;

            CacheTableStatistics(
                new TableStatistics
                {
                    TableName = tableName,
                    RowCount = stats.RowCount,
                    HasStaleColumns = hasStaleColumns,
                    LastPersistedChangeCounter = stats.LastPersistedChangeCounter,
                },
                isExact: _exactTableRowCounts.Contains(tableName),
                markDirty: true);
            return;
        }

        long rowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
        CacheTableStatistics(
            new TableStatistics
            {
                TableName = tableName,
                RowCount = rowCount,
                HasStaleColumns = hasStaleColumns,
                LastPersistedChangeCounter = 0,
            },
            isExact: true,
            markDirty: true);
    }

    private async ValueTask UpsertColumnStatisticsAsync(ColumnStatistics stats, CancellationToken ct)
    {
        await EnsureColumnStatsCatalogTreeAsync(ct);

        byte[] payload = SerializeColumnStatistics(stats);
        long key = GetColumnStatisticsStorageKey(stats.TableName, stats.ColumnName);

        try { await _columnStatsCatalogTree!.DeleteAsync(key, ct); } catch { }
        await _columnStatsCatalogTree!.InsertAsync(key, payload, ct);

        CacheColumnStatistics(stats, markDirty: false);
    }

    private async ValueTask DeleteColumnStatisticsAsync(string tableName, CancellationToken ct)
    {
        if (!_columnStatsByTableSnapshot.TryGetValue(tableName, out var stats) || stats.Length == 0)
            return;

        if (_columnStatsCatalogTree != null)
        {
            for (int i = 0; i < stats.Length; i++)
            {
                long key = GetColumnStatisticsStorageKey(stats[i].TableName, stats[i].ColumnName);
                try { await _columnStatsCatalogTree.DeleteAsync(key, ct); } catch { }
            }
        }

        for (int i = 0; i < stats.Length; i++)
            RemoveColumnStatisticsFromCache(stats[i].TableName, stats[i].ColumnName);

        if (_tableStatsCache.ContainsKey(tableName))
            await SetTableHasStaleColumnsAsync(tableName, hasStaleColumns: false, ct);
    }

    private async ValueTask UpsertColumnDistributionStatisticsAsync(ColumnDistributionStatistics stats, CancellationToken ct)
    {
        await EnsureColumnDistributionStatsCatalogTreeAsync(ct);

        byte[] payload = SerializeColumnDistributionStatistics(stats);
        long key = GetColumnDistributionStatisticsStorageKey(stats.TableName, stats.ColumnName);

        try { await _columnDistributionStatsCatalogTree!.DeleteAsync(key, ct); } catch { }
        await _columnDistributionStatsCatalogTree!.InsertAsync(key, payload, ct);

        CacheColumnDistributionStatistics(stats);
    }

    private async ValueTask DeleteColumnDistributionStatisticsAsync(string tableName, CancellationToken ct)
    {
        if (!_columnDistributionStatsByTable.TryGetValue(tableName, out var stats) || stats.Length == 0)
            return;

        if (_columnDistributionStatsCatalogTree != null)
        {
            for (int i = 0; i < stats.Length; i++)
            {
                long key = GetColumnDistributionStatisticsStorageKey(stats[i].TableName, stats[i].ColumnName);
                try { await _columnDistributionStatsCatalogTree.DeleteAsync(key, ct); } catch { }
            }
        }

        for (int i = 0; i < stats.Length; i++)
            RemoveColumnDistributionStatisticsFromCache(stats[i].TableName, stats[i].ColumnName);
    }

    private void CacheColumnDistributionStatistics(ColumnDistributionStatistics stats)
    {
        var normalized = new ColumnDistributionStatistics
        {
            TableName = stats.TableName,
            ColumnName = stats.ColumnName,
            HistogramBuckets = stats.HistogramBuckets
                .Select(static bucket => new HistogramBucketStatistics
                {
                    LowerBound = bucket.LowerBound,
                    UpperBound = bucket.UpperBound,
                    RowCount = bucket.RowCount,
                })
                .ToArray(),
            FrequentValues = stats.FrequentValues
                .Select(static value => new FrequentValueStatistics
                {
                    Value = value.Value,
                    RowCount = value.RowCount,
                })
                .ToArray(),
        };

        string cacheKey = GetColumnDistributionStatisticsCacheKey(normalized.TableName, normalized.ColumnName);
        _columnDistributionStatsCache[cacheKey] = normalized;

        if (_columnDistributionStatsByTable.TryGetValue(normalized.TableName, out var existing))
        {
            _columnDistributionStatsByTable[normalized.TableName] = existing
                .Where(item => !string.Equals(item.ColumnName, normalized.ColumnName, StringComparison.OrdinalIgnoreCase))
                .Concat([normalized])
                .ToArray();
        }
        else
        {
            _columnDistributionStatsByTable[normalized.TableName] = [normalized];
        }
    }

    private void RemoveColumnDistributionStatisticsFromCache(string tableName, string columnName)
    {
        string cacheKey = GetColumnDistributionStatisticsCacheKey(tableName, columnName);
        _columnDistributionStatsCache.Remove(cacheKey);

        if (_columnDistributionStatsByTable.TryGetValue(tableName, out var existing))
        {
            var updated = existing
                .Where(item => !string.Equals(item.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
                .ToArray();

            if (updated.Length == 0)
                _columnDistributionStatsByTable.Remove(tableName);
            else
                _columnDistributionStatsByTable[tableName] = updated;
        }
    }

    private static string GetColumnDistributionStatisticsCacheKey(string tableName, string columnName)
        => $"{tableName}\u001F{columnName}";

    private long GetColumnDistributionStatisticsStorageKey(string tableName, string columnName)
        => _schemaSerializer.TableNameToKey($"{tableName}\u001F{columnName}");

    private async ValueTask UpsertIndexPrefixStatisticsAsync(IndexPrefixStatistics stats, CancellationToken ct)
    {
        await EnsureIndexPrefixStatsCatalogTreeAsync(ct);

        byte[] payload = SerializeIndexPrefixStatistics(stats);
        long key = _schemaSerializer.IndexNameToKey(stats.IndexName);

        try { await _indexPrefixStatsCatalogTree!.DeleteAsync(key, ct); } catch { }
        await _indexPrefixStatsCatalogTree!.InsertAsync(key, payload, ct);

        CacheIndexPrefixStatistics(stats);
    }

    private async ValueTask DeleteIndexPrefixStatisticsAsync(string indexName, CancellationToken ct)
    {
        if (!_indexPrefixStatsCache.TryGetValue(indexName, out var stats))
            return;

        if (_indexPrefixStatsCatalogTree != null)
        {
            long key = _schemaSerializer.IndexNameToKey(indexName);
            try { await _indexPrefixStatsCatalogTree.DeleteAsync(key, ct); } catch { }
        }

        RemoveIndexPrefixStatisticsFromCache(indexName, stats.TableName);
    }

    private async ValueTask DeleteIndexPrefixStatisticsForTableAsync(string tableName, CancellationToken ct)
    {
        if (!_indexPrefixStatsByTable.TryGetValue(tableName, out var stats) || stats.Length == 0)
            return;

        if (_indexPrefixStatsCatalogTree != null)
        {
            for (int i = 0; i < stats.Length; i++)
            {
                long key = _schemaSerializer.IndexNameToKey(stats[i].IndexName);
                try { await _indexPrefixStatsCatalogTree.DeleteAsync(key, ct); } catch { }
            }
        }

        for (int i = 0; i < stats.Length; i++)
            RemoveIndexPrefixStatisticsFromCache(stats[i].IndexName, tableName);
    }

    private void CacheIndexPrefixStatistics(IndexPrefixStatistics stats)
    {
        var normalized = new IndexPrefixStatistics
        {
            IndexName = stats.IndexName,
            TableName = stats.TableName,
            PrefixColumns = stats.PrefixColumns.ToArray(),
            PrefixDistinctCounts = stats.PrefixDistinctCounts.ToArray(),
        };

        _indexPrefixStatsCache[normalized.IndexName] = normalized;

        if (_indexPrefixStatsByTable.TryGetValue(normalized.TableName, out var existing))
        {
            _indexPrefixStatsByTable[normalized.TableName] = existing
                .Where(item => !string.Equals(item.IndexName, normalized.IndexName, StringComparison.OrdinalIgnoreCase))
                .Concat([normalized])
                .ToArray();
        }
        else
        {
            _indexPrefixStatsByTable[normalized.TableName] = [normalized];
        }
    }

    private void RemoveIndexPrefixStatisticsFromCache(string indexName, string tableName)
    {
        _indexPrefixStatsCache.Remove(indexName);

        if (_indexPrefixStatsByTable.TryGetValue(tableName, out var existing))
        {
            var updated = existing
                .Where(item => !string.Equals(item.IndexName, indexName, StringComparison.OrdinalIgnoreCase))
                .ToArray();

            if (updated.Length == 0)
                _indexPrefixStatsByTable.Remove(tableName);
            else
                _indexPrefixStatsByTable[tableName] = updated;
        }
    }

    private async ValueTask RenameColumnStatisticsAsync(string oldTableName, string newTableName, CancellationToken ct)
    {
        if (!_columnStatsByTableSnapshot.TryGetValue(oldTableName, out var stats) || stats.Length == 0)
            return;

        await DeleteColumnStatisticsAsync(oldTableName, ct);
        for (int i = 0; i < stats.Length; i++)
        {
            await UpsertColumnStatisticsAsync(
                new ColumnStatistics
                {
                    TableName = newTableName,
                    ColumnName = stats[i].ColumnName,
                    DistinctCount = stats[i].DistinctCount,
                    NonNullCount = stats[i].NonNullCount,
                    MinValue = stats[i].MinValue,
                    MaxValue = stats[i].MaxValue,
                    IsStale = stats[i].IsStale,
                },
                ct);
        }

        await SetTableHasStaleColumnsAsync(newTableName, stats.Any(item => item.IsStale), ct);
    }

    private void CacheColumnStatistics(ColumnStatistics stats, bool markDirty = false)
    {
        string cacheKey = GetColumnStatisticsCacheKey(stats.TableName, stats.ColumnName);
        _columnStatsCache[cacheKey] = stats;
        if (markDirty)
            _dirtyColumnStatistics.Add(cacheKey);

        if (_columnStatsByTableSnapshot.TryGetValue(stats.TableName, out var existing))
        {
            var updated = existing
                .Where(item => !string.Equals(item.ColumnName, stats.ColumnName, StringComparison.OrdinalIgnoreCase))
                .Concat([stats])
                .ToArray();
            _columnStatsByTableSnapshot[stats.TableName] = updated;
        }
        else
        {
            _columnStatsByTableSnapshot[stats.TableName] = [stats];
        }

        _columnStatisticsSnapshotDirty = true;
    }

    private void RemoveColumnStatisticsFromCache(string tableName, string columnName)
    {
        string cacheKey = GetColumnStatisticsCacheKey(tableName, columnName);
        _columnStatsCache.Remove(cacheKey);
        _dirtyColumnStatistics.Remove(cacheKey);

        if (_columnStatsByTableSnapshot.TryGetValue(tableName, out var existing))
        {
            var updated = existing
                .Where(item => !string.Equals(item.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
                .ToArray();

            if (updated.Length == 0)
                _columnStatsByTableSnapshot.Remove(tableName);
            else
                _columnStatsByTableSnapshot[tableName] = updated;
        }

        _columnStatisticsSnapshotDirty = true;
    }

    private static string GetColumnStatisticsCacheKey(string tableName, string columnName)
        => $"{tableName}\u001F{columnName}";

    private long GetColumnStatisticsStorageKey(string tableName, string columnName)
        => _schemaSerializer.TableNameToKey($"{tableName}\u001F{columnName}");

    private static byte[] SerializeTableStatistics(TableStatistics stats)
    {
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(stats.TableName);
        byte[] payload = new byte[4 + tableNameBytes.Length + 8 + 1 + 4];
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(0, 4), tableNameBytes.Length);
        tableNameBytes.CopyTo(payload.AsSpan(4));
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(4 + tableNameBytes.Length, 8), stats.RowCount);
        int staleOffset = 4 + tableNameBytes.Length + 8;
        payload[staleOffset] = stats.HasStaleColumns ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(staleOffset + 1, 4), stats.LastPersistedChangeCounter);
        return payload;
    }

    private static TableStatistics DeserializeTableStatistics(ReadOnlySpan<byte> payload)
    {
        int tableNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
        string tableName = Encoding.UTF8.GetString(payload.Slice(4, tableNameLength));
        long rowCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(4 + tableNameLength, 8));
        int staleOffset = 4 + tableNameLength + 8;
        bool hasStaleColumns = payload[staleOffset] != 0;
        uint lastPersistedChangeCounter = payload.Length >= staleOffset + 1 + 4
            ? BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(staleOffset + 1, 4))
            : 0;
        return new TableStatistics
        {
            TableName = tableName,
            RowCount = rowCount,
            HasStaleColumns = hasStaleColumns,
            LastPersistedChangeCounter = lastPersistedChangeCounter,
        };
    }

    private void ReconcileLoadedStatisticsFreshness()
    {
        if (_advisoryStatisticsPersistenceMode != AdvisoryStatisticsPersistenceMode.Deferred)
        {
            foreach (var stats in _tableStatsCache.Values.ToArray())
                CacheTableStatistics(stats, isExact: true, markDirty: false);

            return;
        }

        foreach (var stats in _tableStatsCache.Values.ToArray())
        {
            bool isExact = stats.LastPersistedChangeCounter == _pager.ChangeCounter;
            if (isExact)
            {
                CacheTableStatistics(stats, isExact: true, markDirty: false);
                continue;
            }

            CacheTableStatistics(stats, isExact: false, markDirty: false);
            if (!stats.HasStaleColumns)
            {
                CacheTableStatistics(
                    new TableStatistics
                    {
                        TableName = stats.TableName,
                        RowCount = stats.RowCount,
                        RowCountIsExact = false,
                        HasStaleColumns = true,
                        LastPersistedChangeCounter = stats.LastPersistedChangeCounter,
                    },
                    isExact: false,
                    markDirty: false);
            }

            if (_columnStatsByTableSnapshot.TryGetValue(stats.TableName, out var columnStats))
            {
                for (int i = 0; i < columnStats.Length; i++)
                {
                    if (columnStats[i].IsStale)
                        continue;

                    CacheColumnStatistics(
                        new ColumnStatistics
                        {
                            TableName = columnStats[i].TableName,
                            ColumnName = columnStats[i].ColumnName,
                            DistinctCount = columnStats[i].DistinctCount,
                            NonNullCount = columnStats[i].NonNullCount,
                            MinValue = columnStats[i].MinValue,
                            MaxValue = columnStats[i].MaxValue,
                            IsStale = true,
                        },
                        markDirty: false);
                }
            }
        }
    }

    private async ValueTask PopulateImmediateTableStatisticsAsync(CancellationToken ct)
    {
        foreach (string existingTableName in _tableStatsCache.Keys.ToArray())
        {
            if (!_cache.ContainsKey(existingTableName))
            {
                _tableStatsCache.Remove(existingTableName);
                _exactTableRowCounts.Remove(existingTableName);
            }
        }

        foreach (string tableName in _cache.Keys)
        {
            if (_tableStatsCache.TryGetValue(tableName, out var existing))
            {
                CacheTableStatistics(existing, isExact: true, markDirty: false);
                continue;
            }

            long rowCount = await GetTableTree(tableName).CountEntriesExactAsync(ct);
            bool hasStaleColumns =
                _columnStatsByTableSnapshot.TryGetValue(tableName, out var columnStats) &&
                columnStats.Any(static stats => stats.IsStale);

            CacheTableStatistics(
                new TableStatistics
                {
                    TableName = tableName,
                    RowCount = rowCount,
                    HasStaleColumns = hasStaleColumns,
                    LastPersistedChangeCounter = _pager.ChangeCounter,
                },
                isExact: true,
                markDirty: false);
        }
    }

    private async ValueTask PersistDirtyColumnStatisticsAsync(CancellationToken ct)
    {
        if (_dirtyColumnStatistics.Count == 0)
            return;

        string[] cacheKeys = _dirtyColumnStatistics.ToArray();
        for (int i = 0; i < cacheKeys.Length; i++)
        {
            if (!_columnStatsCache.TryGetValue(cacheKeys[i], out var stats))
            {
                _dirtyColumnStatistics.Remove(cacheKeys[i]);
                continue;
            }

            await UpsertColumnStatisticsAsync(stats, ct);
            _dirtyColumnStatistics.Remove(cacheKeys[i]);
        }
    }

    private static byte[] SerializeColumnStatistics(ColumnStatistics stats)
    {
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(stats.TableName);
        byte[] columnNameBytes = Encoding.UTF8.GetBytes(stats.ColumnName);
        byte[] minBytes = SerializeStatisticsValue(stats.MinValue);
        byte[] maxBytes = SerializeStatisticsValue(stats.MaxValue);

        byte[] payload = new byte[
            4 + tableNameBytes.Length +
            4 + columnNameBytes.Length +
            8 +
            8 +
            1 +
            4 + minBytes.Length +
            4 + maxBytes.Length];

        int offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), tableNameBytes.Length);
        offset += 4;
        tableNameBytes.CopyTo(payload.AsSpan(offset));
        offset += tableNameBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), columnNameBytes.Length);
        offset += 4;
        columnNameBytes.CopyTo(payload.AsSpan(offset));
        offset += columnNameBytes.Length;

        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), stats.DistinctCount);
        offset += 8;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), stats.NonNullCount);
        offset += 8;
        payload[offset++] = stats.IsStale ? (byte)1 : (byte)0;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), minBytes.Length);
        offset += 4;
        minBytes.CopyTo(payload.AsSpan(offset));
        offset += minBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), maxBytes.Length);
        offset += 4;
        maxBytes.CopyTo(payload.AsSpan(offset));

        return payload;
    }

    private static ColumnStatistics DeserializeColumnStatistics(ReadOnlySpan<byte> payload)
    {
        int offset = 0;
        int tableNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string tableName = Encoding.UTF8.GetString(payload.Slice(offset, tableNameLength));
        offset += tableNameLength;

        int columnNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string columnName = Encoding.UTF8.GetString(payload.Slice(offset, columnNameLength));
        offset += columnNameLength;

        long distinctCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, 8));
        offset += 8;
        long nonNullCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, 8));
        offset += 8;
        bool isStale = payload[offset++] != 0;

        int minLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        DbValue minValue = DeserializeStatisticsValue(payload.Slice(offset, minLength));
        offset += minLength;

        int maxLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        DbValue maxValue = DeserializeStatisticsValue(payload.Slice(offset, maxLength));

        return new ColumnStatistics
        {
            TableName = tableName,
            ColumnName = columnName,
            DistinctCount = distinctCount,
            NonNullCount = nonNullCount,
            MinValue = minValue,
            MaxValue = maxValue,
            IsStale = isStale,
        };
    }

    private static byte[] SerializeColumnDistributionStatistics(ColumnDistributionStatistics stats)
    {
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(stats.TableName);
        byte[] columnNameBytes = Encoding.UTF8.GetBytes(stats.ColumnName);
        int totalSize = 4 + tableNameBytes.Length + 4 + columnNameBytes.Length + 4;

        for (int i = 0; i < stats.FrequentValues.Count; i++)
        {
            byte[] valueBytes = SerializeStatisticsValue(stats.FrequentValues[i].Value);
            totalSize += 4 + valueBytes.Length + 8;
        }

        totalSize += 4;
        for (int i = 0; i < stats.HistogramBuckets.Count; i++)
        {
            byte[] lowerBytes = SerializeStatisticsValue(stats.HistogramBuckets[i].LowerBound);
            byte[] upperBytes = SerializeStatisticsValue(stats.HistogramBuckets[i].UpperBound);
            totalSize += 4 + lowerBytes.Length + 4 + upperBytes.Length + 8;
        }

        byte[] payload = new byte[totalSize];
        int offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), tableNameBytes.Length);
        offset += 4;
        tableNameBytes.CopyTo(payload.AsSpan(offset));
        offset += tableNameBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), columnNameBytes.Length);
        offset += 4;
        columnNameBytes.CopyTo(payload.AsSpan(offset));
        offset += columnNameBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), stats.FrequentValues.Count);
        offset += 4;
        for (int i = 0; i < stats.FrequentValues.Count; i++)
        {
            byte[] valueBytes = SerializeStatisticsValue(stats.FrequentValues[i].Value);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), valueBytes.Length);
            offset += 4;
            valueBytes.CopyTo(payload.AsSpan(offset));
            offset += valueBytes.Length;
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), stats.FrequentValues[i].RowCount);
            offset += 8;
        }

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), stats.HistogramBuckets.Count);
        offset += 4;
        for (int i = 0; i < stats.HistogramBuckets.Count; i++)
        {
            byte[] lowerBytes = SerializeStatisticsValue(stats.HistogramBuckets[i].LowerBound);
            byte[] upperBytes = SerializeStatisticsValue(stats.HistogramBuckets[i].UpperBound);

            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), lowerBytes.Length);
            offset += 4;
            lowerBytes.CopyTo(payload.AsSpan(offset));
            offset += lowerBytes.Length;

            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), upperBytes.Length);
            offset += 4;
            upperBytes.CopyTo(payload.AsSpan(offset));
            offset += upperBytes.Length;

            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), stats.HistogramBuckets[i].RowCount);
            offset += 8;
        }

        return payload;
    }

    private static ColumnDistributionStatistics DeserializeColumnDistributionStatistics(ReadOnlySpan<byte> payload)
    {
        int offset = 0;
        int tableNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string tableName = Encoding.UTF8.GetString(payload.Slice(offset, tableNameLength));
        offset += tableNameLength;

        int columnNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string columnName = Encoding.UTF8.GetString(payload.Slice(offset, columnNameLength));
        offset += columnNameLength;

        int frequentValueCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        var frequentValues = new FrequentValueStatistics[frequentValueCount];
        for (int i = 0; i < frequentValueCount; i++)
        {
            int valueLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
            offset += 4;
            DbValue value = DeserializeStatisticsValue(payload.Slice(offset, valueLength));
            offset += valueLength;
            long rowCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, 8));
            offset += 8;
            frequentValues[i] = new FrequentValueStatistics
            {
                Value = value,
                RowCount = rowCount,
            };
        }

        int bucketCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        var buckets = new HistogramBucketStatistics[bucketCount];
        for (int i = 0; i < bucketCount; i++)
        {
            int lowerLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
            offset += 4;
            DbValue lowerBound = DeserializeStatisticsValue(payload.Slice(offset, lowerLength));
            offset += lowerLength;

            int upperLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
            offset += 4;
            DbValue upperBound = DeserializeStatisticsValue(payload.Slice(offset, upperLength));
            offset += upperLength;

            long rowCount = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, 8));
            offset += 8;
            buckets[i] = new HistogramBucketStatistics
            {
                LowerBound = lowerBound,
                UpperBound = upperBound,
                RowCount = rowCount,
            };
        }

        return new ColumnDistributionStatistics
        {
            TableName = tableName,
            ColumnName = columnName,
            FrequentValues = frequentValues,
            HistogramBuckets = buckets,
        };
    }

    private static byte[] SerializeIndexPrefixStatistics(IndexPrefixStatistics stats)
    {
        byte[] indexNameBytes = Encoding.UTF8.GetBytes(stats.IndexName);
        byte[] tableNameBytes = Encoding.UTF8.GetBytes(stats.TableName);
        int totalSize = 4 + indexNameBytes.Length + 4 + tableNameBytes.Length + 4 + 4 + (8 * stats.PrefixDistinctCounts.Count);

        for (int i = 0; i < stats.PrefixColumns.Count; i++)
            totalSize += 4 + Encoding.UTF8.GetByteCount(stats.PrefixColumns[i]);

        byte[] payload = new byte[totalSize];
        int offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), indexNameBytes.Length);
        offset += 4;
        indexNameBytes.CopyTo(payload.AsSpan(offset));
        offset += indexNameBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), tableNameBytes.Length);
        offset += 4;
        tableNameBytes.CopyTo(payload.AsSpan(offset));
        offset += tableNameBytes.Length;

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), stats.PrefixColumns.Count);
        offset += 4;
        for (int i = 0; i < stats.PrefixColumns.Count; i++)
        {
            byte[] prefixColumnBytes = Encoding.UTF8.GetBytes(stats.PrefixColumns[i]);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), prefixColumnBytes.Length);
            offset += 4;
            prefixColumnBytes.CopyTo(payload.AsSpan(offset));
            offset += prefixColumnBytes.Length;
        }

        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(offset, 4), stats.PrefixDistinctCounts.Count);
        offset += 4;
        for (int i = 0; i < stats.PrefixDistinctCounts.Count; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(offset, 8), stats.PrefixDistinctCounts[i]);
            offset += 8;
        }

        return payload;
    }

    private static IndexPrefixStatistics DeserializeIndexPrefixStatistics(ReadOnlySpan<byte> payload)
    {
        int offset = 0;
        int indexNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string indexName = Encoding.UTF8.GetString(payload.Slice(offset, indexNameLength));
        offset += indexNameLength;

        int tableNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        string tableName = Encoding.UTF8.GetString(payload.Slice(offset, tableNameLength));
        offset += tableNameLength;

        int prefixColumnCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        var prefixColumns = new string[prefixColumnCount];
        for (int i = 0; i < prefixColumnCount; i++)
        {
            int prefixColumnLength = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
            offset += 4;
            prefixColumns[i] = Encoding.UTF8.GetString(payload.Slice(offset, prefixColumnLength));
            offset += prefixColumnLength;
        }

        int prefixDistinctCountCount = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(offset, 4));
        offset += 4;
        var prefixDistinctCounts = new long[prefixDistinctCountCount];
        for (int i = 0; i < prefixDistinctCountCount; i++)
        {
            prefixDistinctCounts[i] = BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(offset, 8));
            offset += 8;
        }

        return new IndexPrefixStatistics
        {
            IndexName = indexName,
            TableName = tableName,
            PrefixColumns = prefixColumns,
            PrefixDistinctCounts = prefixDistinctCounts,
        };
    }

    private static byte[] SerializeStatisticsValue(DbValue value)
    {
        const DbType bitStringStatisticsType = (DbType)0x80;
        return value.Type switch
        {
            DbType.Null => [(byte)DbType.Null],
            DbType.Integer => SerializeStatisticsFixedValue(value.Type, value.AsInteger),
            DbType.Real => SerializeStatisticsFixedValue(value.Type, BitConverter.DoubleToInt64Bits(value.AsReal)),
            DbType.Decimal => SerializeStatisticsDecimalValue(value),
            DbType.Text => SerializeStatisticsVariableValue(value.Type, Encoding.UTF8.GetBytes(value.AsText)),
            DbType.Blob when value.IsBitString => SerializeStatisticsVariableValue(
                bitStringStatisticsType,
                SerializeStatisticsBitString(value)),
            DbType.Blob => SerializeStatisticsVariableValue(value.Type, value.AsBlob),
            _ => throw new InvalidOperationException($"Unsupported statistics value type '{value.Type}'."),
        };
    }

    private static byte[] SerializeStatisticsFixedValue(DbType type, long bits)
    {
        byte[] payload = new byte[1 + 8];
        payload[0] = (byte)type;
        BinaryPrimitives.WriteInt64LittleEndian(payload.AsSpan(1, 8), bits);
        return payload;
    }

    private static byte[] SerializeStatisticsVariableValue(DbType type, byte[] data)
    {
        byte[] payload = new byte[1 + 4 + data.Length];
        payload[0] = (byte)type;
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(1, 4), data.Length);
        data.CopyTo(payload.AsSpan(5));
        return payload;
    }

    private static byte[] SerializeStatisticsDecimalValue(DbValue value)
    {
        byte[] payload = new byte[1 + sizeof(long) + sizeof(byte)];
        payload[0] = (byte)DbType.Decimal;
        BinaryPrimitives.WriteInt64LittleEndian(
            payload.AsSpan(1, sizeof(long)),
            value.DecimalCoefficient);
        payload[^1] = checked((byte)value.DecimalScale);
        return payload;
    }

    private static byte[] SerializeStatisticsBitString(DbValue value)
    {
        byte[] packedBytes = value.AsBlob;
        byte[] payload = new byte[sizeof(int) + packedBytes.Length];
        BinaryPrimitives.WriteInt32LittleEndian(payload, value.BitLength);
        packedBytes.CopyTo(payload.AsSpan(sizeof(int)));
        return payload;
    }

    private static DbValue DeserializeStatisticsValue(ReadOnlySpan<byte> payload)
    {
        const DbType bitStringStatisticsType = (DbType)0x80;
        DbType type = (DbType)payload[0];
        return type switch
        {
            DbType.Null => DbValue.Null,
            DbType.Integer => DbValue.FromInteger(BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(1, 8))),
            DbType.Real => DbValue.FromReal(BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(1, 8)))),
            DbType.Decimal => DbValue.FromDecimalParts(
                BinaryPrimitives.ReadInt64LittleEndian(payload.Slice(1, sizeof(long))),
                payload[1 + sizeof(long)]),
            DbType.Text => DbValue.FromText(ReadStatisticsString(payload)),
            DbType.Blob => DbValue.FromBlob(ReadStatisticsBytes(payload)),
            bitStringStatisticsType => DeserializeStatisticsBitString(ReadStatisticsBytes(payload)),
            _ => throw new InvalidOperationException($"Unsupported statistics value type '{type}'."),
        };
    }

    private static DbValue DeserializeStatisticsBitString(byte[] payload)
    {
        if (payload.Length < sizeof(int))
            throw new InvalidDataException("Malformed bit-string statistics payload.");

        int bitLength = BinaryPrimitives.ReadInt32LittleEndian(payload);
        try
        {
            return DbValue.FromBitString(payload.AsSpan(sizeof(int)).ToArray(), bitLength);
        }
        catch (ArgumentException ex)
        {
            throw new InvalidDataException("Malformed bit-string statistics payload.", ex);
        }
    }

    private static string ReadStatisticsString(ReadOnlySpan<byte> payload)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(1, 4));
        return Encoding.UTF8.GetString(payload.Slice(5, length));
    }

    private static byte[] ReadStatisticsBytes(ReadOnlySpan<byte> payload)
    {
        int length = BinaryPrimitives.ReadInt32LittleEndian(payload.Slice(1, 4));
        return payload.Slice(5, length).ToArray();
    }

    private void IncrementSchemaVersion()
    {
        Interlocked.Increment(ref _schemaVersion);
    }

    internal void MarkLogicalSchemaChanged()
    {
        IncrementSchemaVersion();
    }

    private async ValueTask<bool> PersistAuxiliaryCatalogRootPageChangesAsync(CancellationToken ct)
    {
        bool changed = false;
        changed |= await PersistIndexCatalogRootPageChangeAsync(ct);
        changed |= await PersistViewCatalogRootPageChangeAsync(ct);
        changed |= await PersistTriggerCatalogRootPageChangeAsync(ct);
        changed |= await PersistTableStatsCatalogRootPageChangeAsync(ct);
        changed |= await PersistColumnStatsCatalogRootPageChangeAsync(ct);
        changed |= await PersistColumnDistributionStatsCatalogRootPageChangeAsync(ct);
        changed |= await PersistIndexPrefixStatsCatalogRootPageChangeAsync(ct);
        return changed;
    }

    private ValueTask<bool> PersistIndexCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_indexCatalogTree, IndexCatalogSentinel, _persistedIndexCatalogRootPage, ct, rootPage => _persistedIndexCatalogRootPage = rootPage);

    private ValueTask<bool> PersistViewCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_viewCatalogTree, ViewCatalogSentinel, _persistedViewCatalogRootPage, ct, rootPage => _persistedViewCatalogRootPage = rootPage);

    private ValueTask<bool> PersistTriggerCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_triggerCatalogTree, TriggerCatalogSentinel, _persistedTriggerCatalogRootPage, ct, rootPage => _persistedTriggerCatalogRootPage = rootPage);

    private ValueTask<bool> PersistTableStatsCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_tableStatsCatalogTree, TableStatsCatalogSentinel, _persistedTableStatsCatalogRootPage, ct, rootPage => _persistedTableStatsCatalogRootPage = rootPage);

    private ValueTask<bool> PersistColumnStatsCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_columnStatsCatalogTree, ColumnStatsCatalogSentinel, _persistedColumnStatsCatalogRootPage, ct, rootPage => _persistedColumnStatsCatalogRootPage = rootPage);

    private ValueTask<bool> PersistColumnDistributionStatsCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_columnDistributionStatsCatalogTree, ColumnDistributionStatsCatalogSentinel, _persistedColumnDistributionStatsCatalogRootPage, ct, rootPage => _persistedColumnDistributionStatsCatalogRootPage = rootPage);

    private ValueTask<bool> PersistIndexPrefixStatsCatalogRootPageChangeAsync(CancellationToken ct) =>
        PersistAuxiliaryCatalogRootPageChangeAsync(_indexPrefixStatsCatalogTree, IndexPrefixStatsCatalogSentinel, _persistedIndexPrefixStatsCatalogRootPage, ct, rootPage => _persistedIndexPrefixStatsCatalogRootPage = rootPage);

    private async ValueTask<bool> PersistAuxiliaryCatalogRootPageChangeAsync(
        BTree? tree,
        long sentinelKey,
        uint persistedRootPage,
        CancellationToken ct,
        Action<uint> setPersistedRootPage)
    {
        if (tree == null)
            return false;

        uint currentRootPage = tree.RootPageId;
        if (persistedRootPage == currentRootPage)
            return false;

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, currentRootPage);
        try { await _catalogTree!.DeleteAsync(sentinelKey, ct); } catch { }
        await _catalogTree!.InsertAsync(sentinelKey, payload, ct);

        setPersistedRootPage(currentRootPage);
        _pager.SchemaRootPage = _catalogTree.RootPageId;
        return true;
    }

    private async ValueTask<bool> PersistTableRootPageChangeAsync(string tableName, CancellationToken ct)
    {
        if (!_tableTrees.TryGetValue(tableName, out var tree))
            return false;

        if (!_tableRootPages.TryGetValue(tableName, out uint persistedRootPage))
            return false;

        if (!_cache.TryGetValue(tableName, out var schema))
            return false;

        uint currentRootPage = tree.RootPageId;
        _persistedTableNextRowIds.TryGetValue(tableName, out long persistedNextRowId);
        bool metadataChanged = persistedNextRowId != schema.NextRowId;
        if (currentRootPage == persistedRootPage && !metadataChanged)
            return false;

        var schemaBytes = _schemaSerializer.Serialize(schema);
        var payload = _catalogStore.WriteRootPayload(currentRootPage, schemaBytes);

        long key = _schemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await _catalogTree.InsertAsync(key, payload, ct);

        _tableRootPages[tableName] = currentRootPage;
        _persistedTableNextRowIds[tableName] = schema.NextRowId;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
        return currentRootPage != persistedRootPage;
    }

    private async ValueTask<bool> PersistIndexRootPageChangeAsync(string indexName, CancellationToken ct)
    {
        if (!_indexStores.TryGetValue(indexName, out var store))
            return false;

        if (!_indexRootPages.TryGetValue(indexName, out uint persistedRootPage))
            return false;

        uint currentRootPage = store.RootPageId;
        if (currentRootPage == persistedRootPage)
            return false;

        var schema = _indexCache[indexName];
        var schemaBytes = _schemaSerializer.SerializeIndex(schema);
        var payload = _catalogStore.WriteRootPayload(currentRootPage, schemaBytes);

        long key = _schemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);
        await _indexCatalogTree.InsertAsync(key, payload, ct);

        _indexRootPages[indexName] = currentRootPage;
        return true;
    }

}

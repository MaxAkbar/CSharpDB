using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Storage.Catalog;

internal readonly record struct TableAndIndexStorageReplacement(
    uint PreviousTableRootPage,
    IReadOnlyDictionary<string, IIndexStore> PreviousIndexStores);

/// <summary>
/// Public schema-catalog facade that preserves the existing API surface.
/// Internal behavior is delegated to CatalogService.
/// </summary>
public sealed class SchemaCatalog
{
    private readonly CatalogService? _service;
    private readonly CatalogReadSnapshot? _readSnapshot;
    private readonly Pager? _readPager;
    private CatalogService Service => _service
        ?? throw new InvalidOperationException("Snapshot catalog views are read-only.");

    internal SchemaCatalog(CatalogReadSnapshot snapshot, Pager pager)
    {
        _readSnapshot = snapshot;
        _readPager = pager;
    }

    private SchemaCatalog(CatalogService service)
    {
        _service = service;
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(
            pager,
            AdvisoryStatisticsPersistenceMode.Immediate,
            ct);
        return new SchemaCatalog(service);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Immediate,
        CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(
            pager,
            schemaSerializer,
            indexProvider,
            advisoryStatisticsPersistenceMode,
            ct);
        return new SchemaCatalog(service);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Immediate,
        CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(
            pager,
            schemaSerializer,
            indexProvider,
            catalogStore,
            advisoryStatisticsPersistenceMode,
            ct);
        return new SchemaCatalog(service);
    }

    internal CatalogReadSnapshot CaptureReadSnapshot(CatalogReadSnapshot? previous = null)
        => _readSnapshot ?? Service.CaptureReadSnapshot(previous);

    internal bool TryGetReadTable(string name, out CatalogReadTable table)
    {
        if (_readSnapshot is not null)
            return _readSnapshot.TryGetReadTable(name, out table);
        TableSchema? schema = Service.GetTable(name);
        if (schema is null)
        {
            table = default;
            return false;
        }
        table = new(schema, Service.GetTableRootPage(name), Service.GetForeignKeysForTable(name), Service.GetReferencingForeignKeys(name));
        return true;
    }

    internal static async ValueTask<CatalogReadSnapshot> LoadReadSnapshotAsync(
        Pager snapshotPager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        AdvisoryStatisticsPersistenceMode advisoryStatisticsPersistenceMode,
        CancellationToken ct = default)
    {
        await snapshotPager.LoadSnapshotHeaderAsync(ct);
        var catalog = await CreateAsync(snapshotPager, schemaSerializer, indexProvider,
            catalogStore, advisoryStatisticsPersistenceMode, ct);
        // Explicit writers can publish data before merging their counts into the
        // shared catalog, even with immediate advisory persistence. Keep persisted
        // estimates for planning, but scan the snapshot for an exact COUNT.
        return catalog.Service.CaptureReadSnapshot(trustExactRowCounts: false);
    }

    public long SchemaVersion => _readSnapshot?.SchemaVersion ?? Service.SchemaVersion;

    public ulong RowVersionHighWater => _readSnapshot?.RowVersionHighWater ?? Service.RowVersionHighWater;

    public ValueTask PersistRowVersionHighWaterAsync(
        ulong rowVersionHighWater,
        CancellationToken ct = default) =>
        Service.PersistRowVersionHighWaterAsync(rowVersionHighWater, ct);

    internal void MarkLogicalSchemaChanged() => Service.MarkLogicalSchemaChanged();

    public TableSchema? GetTable(string tableName) => _readSnapshot is null ? Service.GetTable(tableName) : _readSnapshot.GetTable(tableName);

    public IReadOnlyList<ForeignKeyDefinition> GetForeignKeysForTable(string tableName) =>
        _readSnapshot is null ? Service.GetForeignKeysForTable(tableName) : _readSnapshot.GetForeignKeysForTable(tableName);

    public IReadOnlyList<TableForeignKeyReference> GetReferencingForeignKeys(string parentTableName) =>
        _readSnapshot is null ? Service.GetReferencingForeignKeys(parentTableName) : _readSnapshot.GetReferencingForeignKeys(parentTableName);

    public TableStatistics? GetTableStatistics(string tableName) => _readSnapshot is null ? Service.GetTableStatistics(tableName) : _readSnapshot.GetTableStatistics(tableName);

    public IReadOnlyCollection<TableStatistics> GetTableStatistics() => _readSnapshot is null ? Service.GetTableStatistics() : _readSnapshot.GetTableStatistics();

    public IReadOnlyCollection<TableStatistics> GetDirtyTableStatistics() => _readSnapshot is null ? Service.GetDirtyTableStatistics() : Array.Empty<TableStatistics>();

    public IReadOnlyCollection<KeyValuePair<string, long>> GetPendingTableRowCountDeltas() =>
        _readSnapshot is null ? Service.GetPendingTableRowCountDeltas() : Array.Empty<KeyValuePair<string, long>>();

    public ColumnStatistics? GetColumnStatistics(string tableName, string columnName) =>
        _readSnapshot is null ? Service.GetColumnStatistics(tableName, columnName) : _readSnapshot.GetColumnStatistics(tableName, columnName);

    public IReadOnlyCollection<ColumnStatistics> GetColumnStatistics(string tableName) =>
        _readSnapshot is null ? Service.GetColumnStatistics(tableName) : _readSnapshot.GetColumnStatistics(tableName);

    public IReadOnlyCollection<ColumnStatistics> GetColumnStatistics() =>
        _readSnapshot is null ? Service.GetColumnStatistics() : _readSnapshot.GetColumnStatistics();

    public IReadOnlyCollection<ColumnStatistics> GetDirtyColumnStatistics() =>
        _readSnapshot is null ? Service.GetDirtyColumnStatistics() : Array.Empty<ColumnStatistics>();

    public void ApplyCommittedAdvisoryStatisticsSnapshot(
        IReadOnlyCollection<TableStatistics> tableStatistics,
        IReadOnlyCollection<ColumnStatistics> columnStatistics,
        bool markDirty = false) =>
        Service.ApplyCommittedAdvisoryStatisticsSnapshot(tableStatistics, columnStatistics, markDirty);

    public void ApplyCommittedTableRowCountDeltas(IReadOnlyCollection<KeyValuePair<string, long>> rowCountDeltas) =>
        Service.ApplyCommittedTableRowCountDeltas(rowCountDeltas);

    public void ApplyCommittedTableMetadataSnapshot(IReadOnlyCollection<KeyValuePair<string, long>> nextRowIds) =>
        Service.ApplyCommittedTableMetadataSnapshot(nextRowIds);

    public bool TryGetFreshColumnStatistics(string tableName, string columnName, out ColumnStatistics stats) =>
        _readSnapshot is null ? Service.TryGetFreshColumnStatistics(tableName, columnName, out stats) : _readSnapshot.TryGetFreshColumnStatistics(tableName, columnName, out stats);

    public bool TryGetFreshColumnDistributionStatistics(string tableName, string columnName, out ColumnDistributionStatistics stats) =>
        _readSnapshot is null ? Service.TryGetFreshColumnDistributionStatistics(tableName, columnName, out stats) : _readSnapshot.TryGetFreshColumnDistributionStatistics(tableName, columnName, out stats);

    public bool TryGetFreshIndexPrefixStatistics(string indexName, out IndexPrefixStatistics stats) =>
        _readSnapshot is null ? Service.TryGetFreshIndexPrefixStatistics(indexName, out stats) : _readSnapshot.TryGetFreshIndexPrefixStatistics(indexName, out stats);

    internal IReadOnlyCollection<ColumnDistributionStatistics> GetColumnDistributionStatistics() =>
        _readSnapshot is null ? Service.GetColumnDistributionStatistics() : _readSnapshot.GetColumnDistributionStatistics();

    internal IReadOnlyCollection<IndexPrefixStatistics> GetIndexPrefixStatistics() =>
        _readSnapshot is null ? Service.GetIndexPrefixStatistics() : _readSnapshot.GetIndexPrefixStatistics();

    public bool TryGetTableRowCount(string tableName, out long rowCount) =>
        _readSnapshot is null ? Service.TryGetEstimatedTableRowCount(tableName, out rowCount) : _readSnapshot.TryGetEstimatedTableRowCount(tableName, out rowCount);

    public bool TryGetEstimatedTableRowCount(string tableName, out long rowCount) =>
        _readSnapshot is null ? Service.TryGetEstimatedTableRowCount(tableName, out rowCount) : _readSnapshot.TryGetEstimatedTableRowCount(tableName, out rowCount);

    public bool TryGetExactTableRowCount(string tableName, out long rowCount) =>
        _readSnapshot is null ? Service.TryGetExactTableRowCount(tableName, out rowCount) : _readSnapshot.TryGetExactTableRowCount(tableName, out rowCount);

    public ValueTask<long> GetExactTableRowCountAsync(string tableName, CancellationToken ct = default) =>
        _readSnapshot is null ? Service.GetExactTableRowCountAsync(tableName, ct) : _readSnapshot.GetExactTableRowCountAsync(tableName, _readPager!, ct);

    public bool HasDirtyAdvisoryStatistics => _readSnapshot is null && Service.HasDirtyAdvisoryStatistics;
    public bool HasAdvisoryCatalogContentChanges => _readSnapshot is null && Service.HasAdvisoryCatalogContentChanges;

    public uint GetTableRootPage(string tableName) => _readSnapshot is null ? Service.GetTableRootPage(tableName) : _readSnapshot.GetTableRootPage(tableName);

    public IReadOnlyCollection<string> GetTableNames() => _readSnapshot is null ? Service.GetTableNames() : _readSnapshot.GetTableNames();

    public ValueTask PersistRootPageChangesAsync(string tableName, CancellationToken ct = default) =>
        Service.PersistRootPageChangesAsync(tableName, ct);

    public ValueTask PersistAllRootPageChangesAsync(CancellationToken ct = default) =>
        Service.PersistAllRootPageChangesAsync(ct);

    public ValueTask<bool> PersistAllRootPageChangesAndDetectChangesAsync(CancellationToken ct = default) =>
        Service.PersistAllRootPageChangesAndDetectChangesAsync(ct);

    public ValueTask ReloadAsync(CancellationToken ct = default) =>
        Service.ReloadAsync(ct);

    public ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct = default) =>
        Service.CreateTableAsync(schema, ct);

    public ValueTask CreateTableExactAsync(TableSchema schema, CancellationToken ct = default) =>
        Service.CreateTableExactAsync(schema, ct);

    public ValueTask DropTableAsync(string tableName, CancellationToken ct = default) =>
        Service.DropTableAsync(tableName, ct);

    public ValueTask UpdateTableSchemaAsync(string oldTableName, TableSchema newSchema, CancellationToken ct = default) =>
        Service.UpdateTableSchemaAsync(oldTableName, newSchema, ct);

    internal ValueTask ApplyTableSchemaIdentitiesAsync(
        string tableName,
        TableSchema identitySource,
        CancellationToken ct = default) =>
        Service.ApplyTableSchemaIdentitiesAsync(tableName, identitySource, ct);

    public ValueTask<uint> ReplaceTableStorageAsync(
        string tableName,
        TableSchema newSchema,
        uint replacementRootPage,
        long exactRowCount,
        CancellationToken ct = default) =>
        Service.ReplaceTableStorageAsync(tableName, newSchema, replacementRootPage, exactRowCount, ct);

    internal ValueTask<IIndexStore> CreateDetachedIndexStoreAsync(
        IndexSchema schema,
        CancellationToken ct = default) =>
        Service.CreateDetachedIndexStoreAsync(schema, ct);

    internal ValueTask<TableAndIndexStorageReplacement> ReplaceTableAndIndexStorageAsync(
        string tableName,
        TableSchema newSchema,
        uint replacementTableRootPage,
        long exactRowCount,
        IReadOnlyDictionary<string, IIndexStore> replacementIndexes,
        CancellationToken ct = default) =>
        Service.ReplaceTableAndIndexStorageAsync(
            tableName,
            newSchema,
            replacementTableRootPage,
            exactRowCount,
            replacementIndexes,
            ct);

    public ValueTask SetTableRowCountAsync(string tableName, long rowCount, CancellationToken ct = default) =>
        Service.SetTableRowCountAsync(tableName, rowCount, ct);

    public ValueTask AdjustTableRowCountAsync(string tableName, long delta, CancellationToken ct = default) =>
        Service.AdjustTableRowCountAsync(tableName, delta, ct);

    public ValueTask AdjustTableRowCountKnownExactAsync(string tableName, long delta, CancellationToken ct = default) =>
        Service.AdjustTableRowCountKnownExactAsync(tableName, delta, ct);

    public ValueTask PersistDirtyTableStatisticsAsync(CancellationToken ct = default) =>
        Service.PersistDirtyTableStatisticsAsync(ct);

    public ValueTask PersistDirtyAdvisoryStatisticsAsync(CancellationToken ct = default) =>
        Service.PersistDirtyAdvisoryStatisticsAsync(ct);

    public ValueTask ReplaceColumnStatisticsAsync(
        string tableName,
        IReadOnlyList<ColumnStatistics> columnStatistics,
        CancellationToken ct = default) =>
        Service.ReplaceColumnStatisticsAsync(tableName, columnStatistics, ct);

    public ValueTask ReplaceColumnDistributionStatisticsAsync(
        string tableName,
        IReadOnlyList<ColumnDistributionStatistics> columnStatistics,
        CancellationToken ct = default) =>
        Service.ReplaceColumnDistributionStatisticsAsync(tableName, columnStatistics, ct);

    public ValueTask ReplaceIndexPrefixStatisticsForTableAsync(
        string tableName,
        IReadOnlyList<IndexPrefixStatistics> indexStatistics,
        CancellationToken ct = default) =>
        Service.ReplaceIndexPrefixStatisticsForTableAsync(tableName, indexStatistics, ct);

    public ValueTask MarkTableColumnStatisticsStaleAsync(string tableName, CancellationToken ct = default) =>
        Service.MarkTableColumnStatisticsStaleAsync(tableName, ct);

    public BTree GetTableTree(string tableName) => _readSnapshot is null ? Service.GetTableTree(tableName) : _readSnapshot.GetTableTree(tableName, _readPager!);

    public BTree GetTableTree(string tableName, Pager pager) => _readSnapshot is null ? Service.GetTableTree(tableName, pager) : _readSnapshot.GetTableTree(tableName, pager);

    public IndexSchema? GetIndex(string indexName) => _readSnapshot is null ? Service.GetIndex(indexName) : _readSnapshot.GetIndex(indexName);

    public IReadOnlyCollection<IndexSchema> GetIndexes() => _readSnapshot is null ? Service.GetIndexes() : _readSnapshot.GetIndexes();

    public IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName) =>
        _readSnapshot is null ? Service.GetIndexesForTable(tableName) : _readSnapshot.GetIndexesForTable(tableName);

    public IReadOnlyList<IndexSchema> GetSqlIndexesForTable(string tableName) =>
        _readSnapshot is null ? Service.GetSqlIndexesForTable(tableName) : _readSnapshot.GetSqlIndexesForTable(tableName);

    public IIndexStore GetIndexStore(string indexName) => _readSnapshot is null ? Service.GetIndexStore(indexName) : _readSnapshot.GetIndexStore(indexName, _readPager!);

    public IIndexStore GetIndexStore(string indexName, Pager pager) => _readSnapshot is null ? Service.GetIndexStore(indexName, pager) : _readSnapshot.GetIndexStore(indexName, pager);

    public ValueTask CreateIndexAsync(IndexSchema schema, CancellationToken ct = default) =>
        Service.CreateIndexAsync(schema, ct);

    public ValueTask UpdateIndexSchemaAsync(string oldIndexName, IndexSchema newSchema, CancellationToken ct = default) =>
        Service.UpdateIndexSchemaAsync(oldIndexName, newSchema, ct);

    public ValueTask DropIndexAsync(string indexName, CancellationToken ct = default) =>
        Service.DropIndexAsync(indexName, ct);

    public ValueTask<bool> DropIndexAllowCorruptReclaimAsync(string indexName, CancellationToken ct = default) =>
        Service.DropIndexAllowCorruptReclaimAsync(indexName, ct);

    public ValueTask DropForeignKeyOwnedIndexAsync(string indexName, CancellationToken ct = default) =>
        Service.DropForeignKeyOwnedIndexAsync(indexName, ct);

    public ValueTask DropConstraintOwnedIndexAsync(string indexName, CancellationToken ct = default) =>
        Service.DropConstraintOwnedIndexAsync(indexName, ct);

    public string? GetViewSql(string viewName) => _readSnapshot is null ? Service.GetViewSql(viewName) : _readSnapshot.GetViewSql(viewName);

    public IReadOnlyCollection<string> GetViewNames() => _readSnapshot is null ? Service.GetViewNames() : _readSnapshot.GetViewNames();

    public bool IsView(string name) => _readSnapshot is null ? Service.IsView(name) : _readSnapshot.IsView(name);

    public ValueTask CreateViewAsync(string viewName, string sql, CancellationToken ct = default) =>
        Service.CreateViewAsync(viewName, sql, ct);

    public ValueTask DropViewAsync(string viewName, CancellationToken ct = default) =>
        Service.DropViewAsync(viewName, ct);

    public TriggerSchema? GetTrigger(string triggerName) => _readSnapshot is null ? Service.GetTrigger(triggerName) : _readSnapshot.GetTrigger(triggerName);

    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _readSnapshot is null ? Service.GetTriggers() : _readSnapshot.GetTriggers();

    public IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName) =>
        _readSnapshot is null ? Service.GetTriggersForTable(tableName) : _readSnapshot.GetTriggersForTable(tableName);

    public ValueTask CreateTriggerAsync(TriggerSchema schema, CancellationToken ct = default) =>
        Service.CreateTriggerAsync(schema, ct);

    public ValueTask DropTriggerAsync(string triggerName, CancellationToken ct = default) =>
        Service.DropTriggerAsync(triggerName, ct);
}

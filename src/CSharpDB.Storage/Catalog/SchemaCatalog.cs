using CSharpDB.Core;

namespace CSharpDB.Storage.Catalog;

/// <summary>
/// Public schema-catalog facade that preserves the existing API surface.
/// Internal behavior is delegated to CatalogService.
/// </summary>
public sealed class SchemaCatalog
{
    private readonly CatalogService _service;

    private SchemaCatalog(CatalogService service)
    {
        _service = service;
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(pager, ct);
        return new SchemaCatalog(service);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(pager, schemaSerializer, indexProvider, ct);
        return new SchemaCatalog(service);
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(
        Pager pager,
        ISchemaSerializer schemaSerializer,
        IIndexProvider indexProvider,
        ICatalogStore catalogStore,
        CancellationToken ct = default)
    {
        var service = await CatalogService.CreateAsync(pager, schemaSerializer, indexProvider, catalogStore, ct);
        return new SchemaCatalog(service);
    }

    public long SchemaVersion => _service.SchemaVersion;

    public TableSchema? GetTable(string tableName) => _service.GetTable(tableName);

    public uint GetTableRootPage(string tableName) => _service.GetTableRootPage(tableName);

    public IReadOnlyCollection<string> GetTableNames() => _service.GetTableNames();

    public ValueTask PersistRootPageChangesAsync(string tableName, CancellationToken ct = default) =>
        _service.PersistRootPageChangesAsync(tableName, ct);

    public ValueTask PersistAllRootPageChangesAsync(CancellationToken ct = default) =>
        _service.PersistAllRootPageChangesAsync(ct);

    public ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct = default) =>
        _service.CreateTableAsync(schema, ct);

    public ValueTask DropTableAsync(string tableName, CancellationToken ct = default) =>
        _service.DropTableAsync(tableName, ct);

    public ValueTask UpdateTableSchemaAsync(string oldTableName, TableSchema newSchema, CancellationToken ct = default) =>
        _service.UpdateTableSchemaAsync(oldTableName, newSchema, ct);

    public BTree GetTableTree(string tableName) => _service.GetTableTree(tableName);

    public BTree GetTableTree(string tableName, Pager pager) => _service.GetTableTree(tableName, pager);

    public IndexSchema? GetIndex(string indexName) => _service.GetIndex(indexName);

    public IReadOnlyCollection<IndexSchema> GetIndexes() => _service.GetIndexes();

    public IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName) =>
        _service.GetIndexesForTable(tableName);

    public IIndexStore GetIndexStore(string indexName) => _service.GetIndexStore(indexName);

    public IIndexStore GetIndexStore(string indexName, Pager pager) => _service.GetIndexStore(indexName, pager);

    public ValueTask CreateIndexAsync(IndexSchema schema, CancellationToken ct = default) =>
        _service.CreateIndexAsync(schema, ct);

    public ValueTask DropIndexAsync(string indexName, CancellationToken ct = default) =>
        _service.DropIndexAsync(indexName, ct);

    public string? GetViewSql(string viewName) => _service.GetViewSql(viewName);

    public IReadOnlyCollection<string> GetViewNames() => _service.GetViewNames();

    public bool IsView(string name) => _service.IsView(name);

    public ValueTask CreateViewAsync(string viewName, string sql, CancellationToken ct = default) =>
        _service.CreateViewAsync(viewName, sql, ct);

    public ValueTask DropViewAsync(string viewName, CancellationToken ct = default) =>
        _service.DropViewAsync(viewName, ct);

    public TriggerSchema? GetTrigger(string triggerName) => _service.GetTrigger(triggerName);

    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _service.GetTriggers();

    public IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName) =>
        _service.GetTriggersForTable(tableName);

    public ValueTask CreateTriggerAsync(TriggerSchema schema, CancellationToken ct = default) =>
        _service.CreateTriggerAsync(schema, ct);

    public ValueTask DropTriggerAsync(string triggerName, CancellationToken ct = default) =>
        _service.DropTriggerAsync(triggerName, ct);
}

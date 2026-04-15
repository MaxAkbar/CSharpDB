namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Opened storage engine component graph.
/// </summary>
public sealed class StorageEngineContext
{
    public required Pager Pager { get; init; }
    public required SchemaCatalog Catalog { get; init; }
    public required IRecordSerializer RecordSerializer { get; init; }
    public required ISchemaSerializer SchemaSerializer { get; init; }
    public required IIndexProvider IndexProvider { get; init; }
    public required ICatalogStore CatalogStore { get; init; }
    public required IPageChecksumProvider ChecksumProvider { get; init; }
    public required AdvisoryStatisticsPersistenceMode AdvisoryStatisticsPersistenceMode { get; init; }
}

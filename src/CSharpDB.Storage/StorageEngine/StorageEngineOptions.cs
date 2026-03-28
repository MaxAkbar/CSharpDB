namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Top-level options used to compose storage engine components.
/// </summary>
public sealed class StorageEngineOptions
{
    /// <summary>
    /// Durability policy applied to file-backed WAL commits.
    /// Buffered maps to SQLite WAL NORMAL semantics; Durable maps to FULL semantics.
    /// </summary>
    public DurabilityMode DurabilityMode { get; init; } = DurabilityMode.Durable;

    /// <summary>
    /// Optional delay used by durable WAL group commit to collect additional
    /// pending commits before forcing the OS flush. Ignored unless file-backed
    /// durable commit flushing allows concurrent writers.
    /// </summary>
    public TimeSpan DurableCommitBatchWindow { get; init; } = TimeSpan.Zero;

    /// <summary>
    /// Controls when advisory planner statistics are persisted.
    /// </summary>
    public AdvisoryStatisticsPersistenceMode AdvisoryStatisticsPersistenceMode { get; init; }
        = AdvisoryStatisticsPersistenceMode.Immediate;

    /// <summary>
    /// Optional WAL file growth chunk used to reserve additional on-disk space
    /// ahead of future appends. Applies only to file-backed WAL instances.
    /// Set to zero to disable preallocation.
    /// </summary>
    public long WalPreallocationChunkBytes { get; init; }

    /// <summary>
    /// Pager behavior options (cache, lock timeout, checkpoint policy).
    /// </summary>
    public PagerOptions PagerOptions { get; init; } = new();

    /// <summary>
    /// Serializer provider used by catalog and execution paths.
    /// </summary>
    public ISerializerProvider SerializerProvider { get; init; } = new DefaultSerializerProvider();

    /// <summary>
    /// Provider used for index-store creation.
    /// </summary>
    public IIndexProvider IndexProvider { get; init; } = new BTreeIndexProvider();

    /// <summary>
    /// Catalog payload codec used by schema metadata storage.
    /// </summary>
    public ICatalogStore CatalogStore { get; init; } = new CatalogStore();

    /// <summary>
    /// Checksum provider used by WAL frame/header verification.
    /// </summary>
    public IPageChecksumProvider ChecksumProvider { get; init; } = new AdditiveChecksumProvider();
}

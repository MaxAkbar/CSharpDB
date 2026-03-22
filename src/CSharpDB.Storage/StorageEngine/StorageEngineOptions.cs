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

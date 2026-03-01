namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Helper for registering storage-engine providers in a composable style.
/// </summary>
public sealed class StorageEngineOptionsBuilder
{
    private PagerOptions _pagerOptions;
    private ISerializerProvider _serializerProvider;
    private IIndexProvider _indexProvider;
    private ICatalogStore _catalogStore;
    private IPageChecksumProvider _checksumProvider;

    public StorageEngineOptionsBuilder()
        : this(new StorageEngineOptions())
    {
    }

    public StorageEngineOptionsBuilder(StorageEngineOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _pagerOptions = options.PagerOptions;
        _serializerProvider = options.SerializerProvider;
        _indexProvider = options.IndexProvider;
        _catalogStore = options.CatalogStore;
        _checksumProvider = options.ChecksumProvider;
    }

    public StorageEngineOptionsBuilder UsePagerOptions(PagerOptions pagerOptions)
    {
        ArgumentNullException.ThrowIfNull(pagerOptions);
        _pagerOptions = pagerOptions;
        return this;
    }

    public StorageEngineOptionsBuilder UseMaxWalBytesWhenReadersActive(long maxWalBytes)
    {
        if (maxWalBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxWalBytes), "Value must be greater than zero.");

        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = _pagerOptions.CheckpointPolicy,
            MaxCachedPages = _pagerOptions.MaxCachedPages,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = maxWalBytes,
        };

        return this;
    }

    public StorageEngineOptionsBuilder UseSerializerProvider(ISerializerProvider serializerProvider)
    {
        ArgumentNullException.ThrowIfNull(serializerProvider);
        _serializerProvider = serializerProvider;
        return this;
    }

    public StorageEngineOptionsBuilder UseSerializerProvider<TProvider>()
        where TProvider : ISerializerProvider, new()
    {
        _serializerProvider = new TProvider();
        return this;
    }

    public StorageEngineOptionsBuilder UseIndexProvider(IIndexProvider indexProvider)
    {
        ArgumentNullException.ThrowIfNull(indexProvider);
        _indexProvider = indexProvider;
        return this;
    }

    public StorageEngineOptionsBuilder UseIndexProvider<TProvider>()
        where TProvider : IIndexProvider, new()
    {
        _indexProvider = new TProvider();
        return this;
    }

    public StorageEngineOptionsBuilder UseBTreeIndexes() =>
        UseIndexProvider(new BTreeIndexProvider());

    public StorageEngineOptionsBuilder UseCachingBTreeIndexes(int findCacheCapacity = 2048) =>
        UseIndexProvider(new CachingBTreeIndexProvider(findCacheCapacity));

    public StorageEngineOptionsBuilder UseCatalogStore(ICatalogStore catalogStore)
    {
        ArgumentNullException.ThrowIfNull(catalogStore);
        _catalogStore = catalogStore;
        return this;
    }

    public StorageEngineOptionsBuilder UseCatalogStore<TStore>()
        where TStore : ICatalogStore, new()
    {
        _catalogStore = new TStore();
        return this;
    }

    public StorageEngineOptionsBuilder UseChecksumProvider(IPageChecksumProvider checksumProvider)
    {
        ArgumentNullException.ThrowIfNull(checksumProvider);
        _checksumProvider = checksumProvider;
        return this;
    }

    public StorageEngineOptionsBuilder UseChecksumProvider<TProvider>()
        where TProvider : IPageChecksumProvider, new()
    {
        _checksumProvider = new TProvider();
        return this;
    }

    public StorageEngineOptions Build()
    {
        return new StorageEngineOptions
        {
            PagerOptions = _pagerOptions,
            SerializerProvider = _serializerProvider,
            IndexProvider = _indexProvider,
            CatalogStore = _catalogStore,
            ChecksumProvider = _checksumProvider,
        };
    }
}

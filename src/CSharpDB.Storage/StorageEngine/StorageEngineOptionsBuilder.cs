namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Helper for registering storage-engine providers in a composable style.
/// </summary>
public sealed class StorageEngineOptionsBuilder
{
    private PagerOptions _pagerOptions;
    private DurabilityMode _durabilityMode;
    private TimeSpan _durableCommitBatchWindow;
    private AdvisoryStatisticsPersistenceMode _advisoryStatisticsPersistenceMode;
    private long _walPreallocationChunkBytes;
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
        _durabilityMode = options.DurabilityMode;
        _durableCommitBatchWindow = options.DurableCommitBatchWindow;
        _advisoryStatisticsPersistenceMode = options.AdvisoryStatisticsPersistenceMode;
        _walPreallocationChunkBytes = options.WalPreallocationChunkBytes;
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

    public StorageEngineOptionsBuilder UseDurabilityMode(DurabilityMode durabilityMode)
    {
        _durabilityMode = durabilityMode;
        return this;
    }

    public StorageEngineOptionsBuilder UseAdvisoryStatisticsPersistenceMode(
        AdvisoryStatisticsPersistenceMode persistenceMode)
    {
        _advisoryStatisticsPersistenceMode = persistenceMode;
        return this;
    }

    public StorageEngineOptionsBuilder UseDurableCommitBatchWindow(TimeSpan batchWindow)
    {
        if (batchWindow < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(batchWindow), "Value must be non-negative.");

        _durableCommitBatchWindow = batchWindow;
        return this;
    }

    public StorageEngineOptionsBuilder UseWalPreallocationChunkBytes(long chunkBytes)
    {
        if (chunkBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(chunkBytes), "Value must be non-negative.");

        _walPreallocationChunkBytes = chunkBytes;
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
            MaxCachedWalReadPages = _pagerOptions.MaxCachedWalReadPages,
            AutoCheckpointExecutionMode = _pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = maxWalBytes,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = _pagerOptions.UseMemoryMappedReads,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
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

    /// <summary>
    /// Applies the current recommended preset for direct file-backed lookup workloads.
    /// Keeps the existing page-cache shape and caller-selected pager read path so
    /// hot local workloads do not pay for extra bounded-cache or mapped-read behavior
    /// unless they opt in explicitly.
    /// </summary>
    public StorageEngineOptionsBuilder UseDirectLookupOptimizedPreset()
    {
        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = _pagerOptions.CheckpointPolicy,
            MaxCachedPages = _pagerOptions.MaxCachedPages,
            MaxCachedWalReadPages = _pagerOptions.MaxCachedWalReadPages,
            AutoCheckpointExecutionMode = _pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = _pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = _pagerOptions.UseMemoryMappedReads,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
        };

        _indexProvider = new BTreeIndexProvider();
        return this;
    }

    /// <summary>
    /// Applies a direct file-backed preset for cache-pressured or cold-file lookup workloads.
    /// Keeps the existing cache shape while enabling memory-mapped reads for clean main-file pages.
    /// </summary>
    public StorageEngineOptionsBuilder UseDirectColdFileLookupPreset()
    {
        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = _pagerOptions.CheckpointPolicy,
            MaxCachedPages = _pagerOptions.MaxCachedPages,
            MaxCachedWalReadPages = _pagerOptions.MaxCachedWalReadPages,
            AutoCheckpointExecutionMode = _pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = _pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = true,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
        };

        _indexProvider = new BTreeIndexProvider();
        return this;
    }

    /// <summary>
    /// Applies the current recommended preset for explicit bounded file-cache workloads
    /// that should keep only a portion of the database resident in process memory.
    /// </summary>
    public StorageEngineOptionsBuilder UseHybridFileCachePreset(int maxCachedPages = 2048)
    {
        if (maxCachedPages <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxCachedPages), "Value must be greater than zero.");

        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = _pagerOptions.CheckpointPolicy,
            MaxCachedPages = maxCachedPages,
            MaxCachedWalReadPages = 256,
            AutoCheckpointExecutionMode = _pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = _pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = true,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
        };

        _indexProvider = new BTreeIndexProvider();
        return this;
    }

    /// <summary>
    /// Backward-compatible alias for the bounded hybrid file-cache preset.
    /// Prefer <see cref="UseDirectLookupOptimizedPreset"/> for direct local opens
    /// and <see cref="UseHybridFileCachePreset"/> for explicit bounded-cache scenarios.
    /// </summary>
    public StorageEngineOptionsBuilder UseLookupOptimizedPreset(int maxCachedPages = 2048)
        => UseHybridFileCachePreset(maxCachedPages);

    /// <summary>
    /// Applies the current recommended preset for file-backed write-heavy workloads.
    /// Keeps the current cache and index configuration, raises the auto-checkpoint frame threshold,
    /// and schedules auto-checkpoints in the background instead of blocking the triggering commit.
    /// </summary>
    public StorageEngineOptionsBuilder UseWriteOptimizedPreset(int checkpointFrameThreshold = 4096)
    {
        if (checkpointFrameThreshold <= 0)
            throw new ArgumentOutOfRangeException(nameof(checkpointFrameThreshold), "Value must be greater than zero.");

        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = new FrameCountCheckpointPolicy(checkpointFrameThreshold),
            AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            MaxCachedPages = _pagerOptions.MaxCachedPages,
            MaxCachedWalReadPages = _pagerOptions.MaxCachedWalReadPages,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = _pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = _pagerOptions.UseMemoryMappedReads,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
        };

        return this;
    }

    /// <summary>
    /// Applies the write-optimized checkpoint preset and defers advisory planner-stat persistence
    /// to maintenance boundaries so ordinary durable commits do less metadata work.
    /// </summary>
    public StorageEngineOptionsBuilder UseLowLatencyDurableWritePreset(int checkpointFrameThreshold = 4096)
    {
        UseWriteOptimizedPreset(checkpointFrameThreshold);
        _advisoryStatisticsPersistenceMode = AdvisoryStatisticsPersistenceMode.Deferred;
        return this;
    }

    public StorageEngineOptionsBuilder UseMemoryMappedReads(bool enabled = true)
    {
        _pagerOptions = new PagerOptions
        {
            WriterLockTimeout = _pagerOptions.WriterLockTimeout,
            CheckpointPolicy = _pagerOptions.CheckpointPolicy,
            AutoCheckpointExecutionMode = _pagerOptions.AutoCheckpointExecutionMode,
            AutoCheckpointMaxPagesPerStep = _pagerOptions.AutoCheckpointMaxPagesPerStep,
            MaxCachedPages = _pagerOptions.MaxCachedPages,
            MaxCachedWalReadPages = _pagerOptions.MaxCachedWalReadPages,
            PageCacheFactory = _pagerOptions.PageCacheFactory,
            Interceptors = _pagerOptions.Interceptors,
            MaxWalBytesWhenReadersActive = _pagerOptions.MaxWalBytesWhenReadersActive,
            OnCachePageEvicted = _pagerOptions.OnCachePageEvicted,
            UseMemoryMappedReads = enabled,
            EnableSequentialLeafReadAhead = _pagerOptions.EnableSequentialLeafReadAhead,
            PreserveOwnedPagesOnCheckpoint = _pagerOptions.PreserveOwnedPagesOnCheckpoint,
        };

        return this;
    }

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
            DurabilityMode = _durabilityMode,
            DurableCommitBatchWindow = _durableCommitBatchWindow,
            AdvisoryStatisticsPersistenceMode = _advisoryStatisticsPersistenceMode,
            WalPreallocationChunkBytes = _walPreallocationChunkBytes,
            PagerOptions = _pagerOptions,
            SerializerProvider = _serializerProvider,
            IndexProvider = _indexProvider,
            CatalogStore = _catalogStore,
            ChecksumProvider = _checksumProvider,
        };
    }
}

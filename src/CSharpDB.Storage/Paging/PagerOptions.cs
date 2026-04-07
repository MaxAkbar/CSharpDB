namespace CSharpDB.Storage.Paging;

/// <summary>
/// Configuration for Pager behavior.
/// </summary>
public sealed class PagerOptions
{
    /// <summary>
    /// Maximum duration to wait when acquiring the single-writer lock.
    /// </summary>
    public TimeSpan WriterLockTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Auto-checkpoint policy used after successful commits.
    /// </summary>
    public ICheckpointPolicy CheckpointPolicy { get; init; } =
        new FrameCountCheckpointPolicy(PageConstants.DefaultCheckpointThreshold);

    /// <summary>
    /// Controls whether auto-checkpoints run inline with the triggering commit
    /// or are scheduled to complete in the background.
    /// </summary>
    public AutoCheckpointExecutionMode AutoCheckpointExecutionMode { get; init; } =
        AutoCheckpointExecutionMode.Foreground;

    /// <summary>
    /// Maximum number of pages copied during a single background auto-checkpoint step.
    /// Ignored for foreground checkpoints, which always run to completion.
    /// </summary>
    public int AutoCheckpointMaxPagesPerStep { get; init; } = 64;

    /// <summary>
    /// Optional maximum number of pages retained in cache.
    /// When null, cache behavior is unbounded.
    /// </summary>
    public int? MaxCachedPages { get; init; }

    /// <summary>
    /// Optional maximum number of immutable WAL frame pages retained in the
    /// dedicated WAL read cache. When zero, WAL reads always materialize from
    /// the WAL stream/index path.
    /// </summary>
    public int MaxCachedWalReadPages { get; init; }

    /// <summary>
    /// Factory used to create pager-local page cache instances.
    /// If unset, falls back to <see cref="MaxCachedPages"/> and defaults to dictionary behavior.
    /// </summary>
    public Func<IPageCache>? PageCacheFactory { get; init; }

    /// <summary>
    /// Optional lifecycle interceptors for page/transaction/checkpoint/recovery events.
    /// </summary>
    public IReadOnlyList<IPageOperationInterceptor> Interceptors { get; init; } =
        Array.Empty<IPageOperationInterceptor>();

    /// <summary>
    /// Optional WAL growth cap (in bytes) enforced only while snapshot readers are active.
    /// When set, commits that would grow committed WAL bytes beyond this limit fail with <see cref="CSharpDB.Primitives.ErrorCode.Busy"/>.
    /// </summary>
    public long? MaxWalBytesWhenReadersActive { get; init; }

    /// <summary>
    /// Optional callback invoked when a page cache entry is evicted/replaced/removed.
    /// This is intended for diagnostics or deferred reclamation pipelines.
    /// </summary>
    public Action<uint, byte[]>? OnCachePageEvicted { get; init; }

    /// <summary>
    /// Enables an opt-in memory-mapped read path for clean main-file pages when
    /// the underlying storage device supports it.
    /// </summary>
    public bool UseMemoryMappedReads { get; init; }

    /// <summary>
    /// Enables cursor-local speculative reads of the next B-tree leaf page during
    /// sequential forward scans. This bypasses the shared page cache and is only
    /// used when the pager can safely issue side-effect-free speculative reads.
    /// </summary>
    public bool EnableSequentialLeafReadAhead { get; init; } = true;

    /// <summary>
    /// When true, owned main-file pages already materialized into the shared page
    /// cache remain resident after checkpoint completes. Transient WAL and
    /// memory-mapped read views are still invalidated. Enabled by default so
    /// checkpoints do not discard hot local working sets from the shared cache.
    /// </summary>
    public bool PreserveOwnedPagesOnCheckpoint { get; init; } = true;

    internal IPageCache CreatePageCache()
    {
        if (PageCacheFactory != null)
            return PageCacheFactory();

        if (MaxCachedPages is > 0)
            return new LruPageCache(MaxCachedPages.Value);

        return new DictionaryPageCache();
    }

    internal IPageOperationInterceptor CreateInterceptor()
    {
        if (Interceptors.Count == 0)
            return NoOpPageOperationInterceptor.Instance;
        if (Interceptors.Count == 1)
            return Interceptors[0];
        return new CompositePageOperationInterceptor(Interceptors);
    }
}

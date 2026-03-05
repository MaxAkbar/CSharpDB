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
    /// Optional maximum number of pages retained in cache.
    /// When null, cache behavior is unbounded.
    /// </summary>
    public int? MaxCachedPages { get; init; }

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
    /// When set, commits that would grow committed WAL bytes beyond this limit fail with <see cref="CSharpDB.Core.ErrorCode.Busy"/>.
    /// </summary>
    public long? MaxWalBytesWhenReadersActive { get; init; }

    /// <summary>
    /// Optional callback invoked when a page cache entry is evicted/replaced/removed.
    /// This is intended for diagnostics or deferred reclamation pipelines.
    /// </summary>
    public Action<uint, byte[]>? OnCachePageEvicted { get; init; }

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

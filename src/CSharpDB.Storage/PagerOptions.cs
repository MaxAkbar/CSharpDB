namespace CSharpDB.Storage;

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
    /// Factory used to create pager-local page cache instances.
    /// Defaults to the existing unbounded dictionary cache behavior.
    /// </summary>
    public Func<IPageCache> PageCacheFactory { get; init; } = static () => new DictionaryPageCache();
}

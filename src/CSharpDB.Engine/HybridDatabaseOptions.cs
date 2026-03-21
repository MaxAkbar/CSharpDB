namespace CSharpDB.Engine;

public enum HybridPersistenceMode
{
    IncrementalDurable = 0,
    Snapshot = 1,
}

[Flags]
public enum HybridPersistenceTriggers
{
    None = 0,
    Commit = 1,
    Checkpoint = 2,
    Dispose = 4,
}

/// <summary>
/// Persistence behavior for a lazy-resident hybrid database that snapshots
/// or incrementally persists its committed state to a backing file.
/// </summary>
public sealed class HybridDatabaseOptions
{
    /// <summary>
    /// Controls how the hybrid database persists committed state.
    /// Incremental durable mode is the default and uses an on-disk WAL plus
    /// page-level checkpoints while keeping touched pages resident by cache
    /// policy. Snapshot mode preserves the older full-image export behavior.
    /// </summary>
    public HybridPersistenceMode PersistenceMode { get; init; } =
        HybridPersistenceMode.IncrementalDurable;

    /// <summary>
    /// Lifecycle points that trigger automatic persistence to the backing file.
    /// This is used only when <see cref="PersistenceMode"/> is
    /// <see cref="HybridPersistenceMode.Snapshot"/>.
    /// </summary>
    public HybridPersistenceTriggers PersistenceTriggers { get; init; } =
        HybridPersistenceTriggers.Commit | HybridPersistenceTriggers.Dispose;

    /// <summary>
    /// Optional SQL table names to preload into the hybrid pager cache at open time.
    /// Supported only for incremental-durable hybrid opens that use the default
    /// unbounded pager cache shape.
    /// </summary>
    public IReadOnlyList<string> HotTableNames { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Optional collection names to preload into the hybrid pager cache at open time.
    /// Supported only for incremental-durable hybrid opens that use the default
    /// unbounded pager cache shape.
    /// </summary>
    public IReadOnlyList<string> HotCollectionNames { get; init; } = Array.Empty<string>();
}

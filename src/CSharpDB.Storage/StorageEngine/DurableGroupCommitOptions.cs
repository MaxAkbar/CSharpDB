namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Opt-in durable WAL group-commit settings.
/// </summary>
public readonly record struct DurableGroupCommitOptions
{
    public static DurableGroupCommitOptions Disabled { get; } = new(TimeSpan.Zero);

    public DurableGroupCommitOptions(TimeSpan batchWindow)
    {
        if (batchWindow < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(batchWindow), "Value must be non-negative.");

        BatchWindow = batchWindow;
    }

    /// <summary>
    /// Brief delay used to collect additional durable commits before one OS flush.
    /// Set to zero to disable group commit.
    /// </summary>
    public TimeSpan BatchWindow { get; }

    /// <summary>
    /// True when durable commits are allowed to wait briefly for additional peers.
    /// </summary>
    public bool Enabled => BatchWindow > TimeSpan.Zero;
}

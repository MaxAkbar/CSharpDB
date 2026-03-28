namespace CSharpDB.Storage.Paging;

/// <summary>
/// Cumulative commit-path diagnostics used by focused benchmarks.
/// Tick values use <see cref="System.Diagnostics.Stopwatch"/> timestamps.
/// </summary>
public readonly record struct CommitPathDiagnosticsSnapshot(
    long WalAppendCount,
    long WalAppendTicks,
    long BufferedFlushCount,
    long BufferedFlushTicks,
    long DurableFlushCount,
    long DurableFlushTicks,
    long PublishBatchCount,
    long PublishBatchTicks,
    long FinalizeCommitCount,
    long FinalizeCommitTicks,
    long CheckpointDecisionCount,
    long CheckpointDecisionTicks,
    long BackgroundCheckpointStartCount,
    long MaxPendingCommitCount,
    long MaxPendingCommitBytes)
{
    public static CommitPathDiagnosticsSnapshot Empty { get; } = new();
}

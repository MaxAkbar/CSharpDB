namespace CSharpDB.Storage.Paging;

/// <summary>
/// Cumulative commit-path diagnostics used by focused benchmarks.
/// Tick values use <see cref="System.Diagnostics.Stopwatch"/> timestamps.
/// </summary>
public readonly record struct CommitPathDiagnosticsSnapshot(
    long WalAppendCount,
    long WalAppendTicks,
    long ExplicitCommitLockWaitCount,
    long ExplicitCommitLockWaitTicks,
    long ExplicitCommitLockHoldCount,
    long ExplicitCommitLockHoldTicks,
    long ExplicitConflictResolutionCount,
    long ExplicitConflictResolutionTicks,
    long ExplicitLeafRebaseAttemptCount,
    long ExplicitLeafRebaseSuccessCount,
    long ExplicitLeafRebaseStructuralRejectCount,
    long ExplicitLeafRebaseCapacityRejectCount,
    long ExplicitInteriorRebaseAttemptCount,
    long ExplicitInteriorRebaseSuccessCount,
    long ExplicitInteriorRebaseStructuralRejectCount,
    long ExplicitInteriorRebaseCapacityRejectCount,
    long ExplicitPendingCommitWaitCount,
    long ExplicitPendingCommitWaitTicks,
    long ExplicitHeaderPreparationCount,
    long ExplicitHeaderPreparationTicks,
    long ExplicitPendingCommitReservationCount,
    long ExplicitPendingCommitReservationTicks,
    long DurableBatchWindowWaitCount,
    long DurableBatchWindowWaitTicks,
    long PendingCommitWriteCount,
    long PendingCommitWriteTicks,
    long PendingCommitDrainCount,
    long PendingCommitDrainTicks,
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
    long BTreeLeafSplitCount,
    long BTreeRightEdgeLeafSplitCount,
    long BTreeInteriorInsertCount,
    long BTreeRightEdgeInteriorInsertCount,
    long BTreeInteriorSplitCount,
    long BTreeRightEdgeInteriorSplitCount,
    long BTreeRootSplitCount,
    long MaxPendingCommitCount,
    long MaxPendingCommitBytes)
{
    public static CommitPathDiagnosticsSnapshot Empty { get; } = new();
}

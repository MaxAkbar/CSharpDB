namespace CSharpDB.Storage.Diagnostics;

internal enum StorageCheckpointPhaseRaw
{
    Idle = 0,
    Requested,
    Copying,
    CopyCompleteAwaitingReaders,
    Finalizing,
    Faulted,
}

internal enum StorageRecoveryPhaseRaw
{
    Unknown = 0,
    Scanning,
    Checkpointing,
    Completed,
}

internal enum StorageRecoveryTruncationReasonRaw
{
    Unknown = 0,
    None,
    IncompleteTail,
    SaltMismatch,
    ChecksumMismatch,
    UncommittedTail,
}

internal enum StorageCheckpointOriginRaw
{
    Unknown = 0,
    Manual,
    ForegroundAuto,
    BackgroundAuto,
    StartupRecovery,
    Backup,
    Shutdown,
}

internal enum StorageCheckpointRetentionReasonRaw
{
    Unknown = 0,
    None,
    ActiveReaders,
    NewerCommits,
    ActiveReadersAndNewerCommits,
}

internal enum StorageRuntimeOperationOutcomeRaw
{
    Unknown = 0,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

internal enum StorageRuntimeFailureKindRaw
{
    None = 0,
    Unknown,
    OperationCanceled,
    TimedOut,
    AccessDenied,
    NotFound,
    Busy,
    ResourceLimit,
    Corrupt,
    Io,
    Operation,
}

internal readonly record struct StorageRecoveryRuntimeRawSnapshot(
    StorageRecoveryPhaseRaw Phase,
    long ScannedFrameCount,
    long ScannedBytes,
    long RecoveredFrameCount,
    long RecoveredBytes,
    long DiscardedFrameCount,
    long DiscardedBytes,
    StorageRecoveryTruncationReasonRaw TruncationReason,
    long AttemptCount,
    long RetryCount,
    StorageRuntimeFailureKindRaw LastRetryFailureKind,
    StorageRuntimeOperationOutcomeRaw Outcome,
    StorageRuntimeFailureKindRaw FailureKind);

internal readonly record struct StorageCheckpointRuntimeRawSnapshot(
    StorageCheckpointPhaseRaw Phase,
    StorageCheckpointOriginRaw Origin,
    long? CompletedPageCount,
    long? TotalPageCount,
    StorageCheckpointRetentionReasonRaw RetentionReason,
    StorageRuntimeOperationOutcomeRaw Outcome,
    StorageRuntimeFailureKindRaw FailureKind);

internal readonly record struct WalCheckpointProgressRawSnapshot(
    long CompletedPageCount,
    long TotalPageCount,
    bool HasNewerCommits);

internal readonly record struct StorageRuntimeRawSnapshot(
    int PageSize,
    long PageCount,
    long LogicalBytes,
    long? AllocatedBytes,
    int? DirtyPageCount,
    int ActiveReaderCount,
    int ActiveWriterCount)
{
    internal long TerminalConflictCount { get; init; }
}

internal readonly record struct WalRuntimeRawSnapshot(
    long LogicalBytes,
    long? AllocatedBytes,
    long FrameCount,
    long CommittedFrameBytes,
    long RetainedBytes,
    int PendingCommitCount,
    StorageCheckpointPhaseRaw CheckpointPhase,
    long? LogicalCommitCount,
    long? CommitFlushBatchCount,
    long? CommittedFrameBytesWritten)
{
    internal long LogicalPageWriteCount { get; init; }
    internal long? FlushedCommitCount { get; init; }
    internal long? DurableFlushCount { get; init; }
    internal long? GroupCommitBatchCount { get; init; }
    internal long? GroupCommitCount { get; init; }
}

internal readonly record struct WalRuntimeRawCaptureState(
    long LogicalBytes,
    long? AllocatedBytes,
    long RetainedWalStartOffset,
    int PendingCommitCount,
    int FrameCount,
    long? LogicalCommitCount,
    long LogicalPageWriteCount,
    long? CommitFlushBatchCount,
    long? CommittedFrameBytesWritten)
{
    internal long? FlushedCommitCount { get; init; }
    internal long? DurableFlushCount { get; init; }
    internal long? GroupCommitBatchCount { get; init; }
    internal long? GroupCommitCount { get; init; }

    internal bool TryCreateSnapshot(out WalRuntimeRawSnapshot snapshot)
    {
        long committedFrameBytes = (long)FrameCount * PageConstants.WalFrameSize;
        long committedExtentBytes = LogicalBytes - PageConstants.WalHeaderSize;
        if (LogicalBytes < PageConstants.WalHeaderSize ||
            AllocatedBytes is < 0 ||
            AllocatedBytes is { } allocatedBytes && allocatedBytes < LogicalBytes ||
            RetainedWalStartOffset < -1 ||
            RetainedWalStartOffset > LogicalBytes ||
            PendingCommitCount < 0 ||
            FrameCount < 0 ||
            LogicalCommitCount is < 0 ||
            LogicalPageWriteCount < 0 ||
            CommitFlushBatchCount is < 0 ||
            CommittedFrameBytesWritten is < 0 ||
            FlushedCommitCount is < 0 ||
            DurableFlushCount is < 0 ||
            GroupCommitBatchCount is < 0 ||
            GroupCommitCount is < 0 ||
            CommitFlushBatchCount.HasValue != CommittedFrameBytesWritten.HasValue ||
            !HasCoherentFileLifetimeShape() ||
            FlushedCommitCount is { } flushedCommitCount &&
                (CommitFlushBatchCount > flushedCommitCount ||
                    LogicalCommitCount is not { } logicalCommitCount ||
                    flushedCommitCount > logicalCommitCount) ||
            GroupCommitBatchCount is { } groupCommitBatchCount &&
                CommitFlushBatchCount < groupCommitBatchCount ||
            GroupCommitCount is { } groupCommitCount &&
                FlushedCommitCount < groupCommitCount ||
            GroupCommitBatchCount is { } groupedBatchCount &&
                GroupCommitCount is { } groupedCommitCount &&
                (groupedCommitCount < SaturatingDouble(groupedBatchCount) ||
                    (groupedBatchCount == 0) != (groupedCommitCount == 0)) ||
            committedFrameBytes > committedExtentBytes)
        {
            snapshot = default;
            return false;
        }

        long retainedBytes = RetainedWalStartOffset >= 0
            ? LogicalBytes - RetainedWalStartOffset
            : 0;
        snapshot = new WalRuntimeRawSnapshot(
            LogicalBytes,
            AllocatedBytes,
            FrameCount,
            committedFrameBytes,
            retainedBytes,
            PendingCommitCount,
            StorageCheckpointPhaseRaw.Idle,
            LogicalCommitCount,
            CommitFlushBatchCount,
            CommittedFrameBytesWritten)
        {
            LogicalPageWriteCount = this.LogicalPageWriteCount,
            FlushedCommitCount = this.FlushedCommitCount,
            DurableFlushCount = this.DurableFlushCount,
            GroupCommitBatchCount = this.GroupCommitBatchCount,
            GroupCommitCount = this.GroupCommitCount,
        };
        return true;
    }

    private bool HasCoherentFileLifetimeShape()
    {
        bool hasFileExtent = AllocatedBytes.HasValue;
        return CommitFlushBatchCount.HasValue == hasFileExtent &&
            CommittedFrameBytesWritten.HasValue == hasFileExtent &&
            FlushedCommitCount.HasValue == hasFileExtent &&
            DurableFlushCount.HasValue == hasFileExtent &&
            GroupCommitBatchCount.HasValue == hasFileExtent &&
            GroupCommitCount.HasValue == hasFileExtent;
    }

    private static long SaturatingDouble(long value)
        => value > long.MaxValue / 2
            ? long.MaxValue
            : value * 2;
}

internal readonly record struct PagerRuntimeRawSnapshot(
    StorageRuntimeRawSnapshot Storage,
    WalRuntimeRawSnapshot Wal);

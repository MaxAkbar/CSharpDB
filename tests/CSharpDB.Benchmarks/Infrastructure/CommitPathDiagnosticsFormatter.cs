using System.Diagnostics;
using System.Globalization;
using System.Text;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class CommitPathDiagnosticsFormatter
{
    public static string BuildSummary(CommitPathDiagnosticsSnapshot diagnostics)
    {
        double maxPendingCommitKiB = diagnostics.MaxPendingCommitBytes / 1024.0;
        string hotBTreeResources = BuildHotBTreeResourceSummary(diagnostics.BTreeResourceDiagnostics);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"walAppends={diagnostics.WalAppendCount}, avgWalAppendMs={AverageMilliseconds(diagnostics.WalAppendTicks, diagnostics.WalAppendCount):F3}, explicitCommitLockWaits={diagnostics.ExplicitCommitLockWaitCount}, avgExplicitCommitLockWaitMs={AverageMilliseconds(diagnostics.ExplicitCommitLockWaitTicks, diagnostics.ExplicitCommitLockWaitCount):F3}, explicitCommitLockHolds={diagnostics.ExplicitCommitLockHoldCount}, avgExplicitCommitLockHoldMs={AverageMilliseconds(diagnostics.ExplicitCommitLockHoldTicks, diagnostics.ExplicitCommitLockHoldCount):F3}, explicitConflictResolutions={diagnostics.ExplicitConflictResolutionCount}, avgExplicitConflictResolutionMs={AverageMilliseconds(diagnostics.ExplicitConflictResolutionTicks, diagnostics.ExplicitConflictResolutionCount):F3}, leafRebases={diagnostics.ExplicitLeafRebaseAttemptCount}/{diagnostics.ExplicitLeafRebaseSuccessCount}/{diagnostics.ExplicitLeafRebaseStructuralRejectCount}/{diagnostics.ExplicitLeafRebaseCapacityRejectCount}, leafRejectReasons={diagnostics.ExplicitLeafRebaseRejectNonInsertOnlyCount}/{diagnostics.ExplicitLeafRebaseRejectDuplicateKeyCount}/{diagnostics.ExplicitLeafRebaseRejectSplitFallbackPreconditionCount}/{diagnostics.ExplicitLeafRebaseRejectSplitFallbackShapeCount}/{diagnostics.ExplicitLeafRebaseRejectOtherCount}, leafSplitPreconditions={diagnostics.ExplicitLeafRebaseRejectSplitFallbackMissingTraversalCount}/{diagnostics.ExplicitLeafRebaseRejectSplitFallbackDirtyAncestorCount}/{diagnostics.ExplicitLeafRebaseRejectSplitFallbackParentBoundaryCount}/{diagnostics.ExplicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount}, dirtyParentRecoveries={diagnostics.ExplicitLeafRebaseRejectDirtyParentMissingParentPageCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentInsertionShapeCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentInsertionMismatchCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentLocalSplitShapeCount}/{diagnostics.ExplicitLeafRebaseRejectDirtyParentRebaseFailureCount}, dirtyParentDescribeMatches={diagnostics.ExplicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount}, interiorRebases={diagnostics.ExplicitInteriorRebaseAttemptCount}/{diagnostics.ExplicitInteriorRebaseSuccessCount}/{diagnostics.ExplicitInteriorRebaseStructuralRejectCount}/{diagnostics.ExplicitInteriorRebaseCapacityRejectCount}, explicitPendingCommitWaits={diagnostics.ExplicitPendingCommitWaitCount}, avgExplicitPendingCommitWaitMs={AverageMilliseconds(diagnostics.ExplicitPendingCommitWaitTicks, diagnostics.ExplicitPendingCommitWaitCount):F3}, explicitHeaderPreparations={diagnostics.ExplicitHeaderPreparationCount}, avgExplicitHeaderPreparationMs={AverageMilliseconds(diagnostics.ExplicitHeaderPreparationTicks, diagnostics.ExplicitHeaderPreparationCount):F3}, explicitPendingCommitReservations={diagnostics.ExplicitPendingCommitReservationCount}, avgExplicitPendingCommitReservationMs={AverageMilliseconds(diagnostics.ExplicitPendingCommitReservationTicks, diagnostics.ExplicitPendingCommitReservationCount):F3}, durableBatchWindowWaits={diagnostics.DurableBatchWindowWaitCount}, avgDurableBatchWindowWaitMs={AverageMilliseconds(diagnostics.DurableBatchWindowWaitTicks, diagnostics.DurableBatchWindowWaitCount):F3}, pendingCommitWrites={diagnostics.PendingCommitWriteCount}, avgPendingCommitWriteMs={AverageMilliseconds(diagnostics.PendingCommitWriteTicks, diagnostics.PendingCommitWriteCount):F3}, pendingCommitDrains={diagnostics.PendingCommitDrainCount}, avgPendingCommitDrainMs={AverageMilliseconds(diagnostics.PendingCommitDrainTicks, diagnostics.PendingCommitDrainCount):F3}, bufferedFlushes={diagnostics.BufferedFlushCount}, avgBufferedFlushMs={AverageMilliseconds(diagnostics.BufferedFlushTicks, diagnostics.BufferedFlushCount):F3}, durableFlushes={diagnostics.DurableFlushCount}, avgDurableFlushMs={AverageMilliseconds(diagnostics.DurableFlushTicks, diagnostics.DurableFlushCount):F3}, publishBatches={diagnostics.PublishBatchCount}, avgPublishBatchMs={AverageMilliseconds(diagnostics.PublishBatchTicks, diagnostics.PublishBatchCount):F3}, finalizations={diagnostics.FinalizeCommitCount}, avgFinalizeMs={AverageMilliseconds(diagnostics.FinalizeCommitTicks, diagnostics.FinalizeCommitCount):F3}, checkpointDecisions={diagnostics.CheckpointDecisionCount}, avgCheckpointDecisionMs={AverageMilliseconds(diagnostics.CheckpointDecisionTicks, diagnostics.CheckpointDecisionCount):F3}, backgroundCheckpointStarts={diagnostics.BackgroundCheckpointStartCount}, btreeLeafSplits={diagnostics.BTreeLeafSplitCount}, btreeRightEdgeLeafSplits={diagnostics.BTreeRightEdgeLeafSplitCount}, btreeInteriorInserts={diagnostics.BTreeInteriorInsertCount}, btreeRightEdgeInteriorInserts={diagnostics.BTreeRightEdgeInteriorInsertCount}, btreeInteriorSplits={diagnostics.BTreeInteriorSplitCount}, btreeRightEdgeInteriorSplits={diagnostics.BTreeRightEdgeInteriorSplitCount}, btreeRootSplits={diagnostics.BTreeRootSplitCount}, hotBTreeResources={hotBTreeResources}, hashedAppendContext={diagnostics.HashedIndexAppendContextHitCount}/{diagnostics.HashedIndexAppendContextMissCount}, hashedAppendExternalMetadataReads={diagnostics.HashedIndexAppendExternalMetadataReadCount}, hashedAppendPromotions={diagnostics.HashedIndexAppendPromotionCount}, hashedAppendNotApplicable={diagnostics.HashedIndexAppendNotApplicableCount}, hashedAppendDeferred={diagnostics.HashedIndexDeferredAppendCount}/{diagnostics.HashedIndexDeferredFlushCount}, maxPendingCommits={diagnostics.MaxPendingCommitCount}, maxPendingCommitKiB={maxPendingCommitKiB:F1}");
    }

    private static string BuildHotBTreeResourceSummary(CommitPathBTreeResourceDiagnosticsSnapshot[]? resources)
    {
        if (resources is null || resources.Length == 0)
            return "none";

        var builder = new StringBuilder();
        int emitted = 0;
        for (int i = 0; i < resources.Length && emitted < 3; i++)
        {
            CommitPathBTreeResourceDiagnosticsSnapshot resource = resources[i];
            if (resource.StructuralEventCount == 0)
                continue;

            if (emitted > 0)
                builder.Append(';');

            builder.Append(ShortenResourceName(resource.ResourceName));
            builder.Append("[nre=");
            builder.Append(resource.NonRightEdgeStructuralEventCount.ToString(CultureInfo.InvariantCulture));
            builder.Append('/');
            builder.Append(resource.StructuralEventCount.ToString(CultureInfo.InvariantCulture));
            builder.Append(",leaf=");
            builder.Append(resource.LeafSplitCount.ToString(CultureInfo.InvariantCulture));
            builder.Append('/');
            builder.Append(resource.NonRightEdgeLeafSplitCount.ToString(CultureInfo.InvariantCulture));
            builder.Append(",ins=");
            builder.Append(resource.InteriorInsertCount.ToString(CultureInfo.InvariantCulture));
            builder.Append('/');
            builder.Append(resource.NonRightEdgeInteriorInsertCount.ToString(CultureInfo.InvariantCulture));
            builder.Append(",split=");
            builder.Append(resource.InteriorSplitCount.ToString(CultureInfo.InvariantCulture));
            builder.Append('/');
            builder.Append(resource.NonRightEdgeInteriorSplitCount.ToString(CultureInfo.InvariantCulture));
            if (resource.RootSplitCount > 0)
            {
                builder.Append(",root=");
                builder.Append(resource.RootSplitCount.ToString(CultureInfo.InvariantCulture));
            }

            builder.Append(']');
            emitted++;
        }

        return emitted == 0 ? "none" : builder.ToString();
    }

    private static string ShortenResourceName(string resourceName)
    {
        const string indexPrefix = "index:";
        return resourceName.StartsWith(indexPrefix, StringComparison.Ordinal)
            ? resourceName[indexPrefix.Length..]
            : resourceName;
    }

    public static double AverageMilliseconds(long ticks, long count)
    {
        if (ticks <= 0 || count <= 0)
            return 0;

        return ticks * 1000.0 / Stopwatch.Frequency / count;
    }
}

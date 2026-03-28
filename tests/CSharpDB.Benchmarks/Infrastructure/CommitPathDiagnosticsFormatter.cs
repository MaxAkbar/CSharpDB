using System.Diagnostics;
using System.Globalization;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Infrastructure;

internal static class CommitPathDiagnosticsFormatter
{
    public static string BuildSummary(CommitPathDiagnosticsSnapshot diagnostics)
    {
        double maxPendingCommitKiB = diagnostics.MaxPendingCommitBytes / 1024.0;
        return string.Create(
            CultureInfo.InvariantCulture,
            $"walAppends={diagnostics.WalAppendCount}, avgWalAppendMs={AverageMilliseconds(diagnostics.WalAppendTicks, diagnostics.WalAppendCount):F3}, bufferedFlushes={diagnostics.BufferedFlushCount}, avgBufferedFlushMs={AverageMilliseconds(diagnostics.BufferedFlushTicks, diagnostics.BufferedFlushCount):F3}, durableFlushes={diagnostics.DurableFlushCount}, avgDurableFlushMs={AverageMilliseconds(diagnostics.DurableFlushTicks, diagnostics.DurableFlushCount):F3}, publishBatches={diagnostics.PublishBatchCount}, avgPublishBatchMs={AverageMilliseconds(diagnostics.PublishBatchTicks, diagnostics.PublishBatchCount):F3}, finalizations={diagnostics.FinalizeCommitCount}, avgFinalizeMs={AverageMilliseconds(diagnostics.FinalizeCommitTicks, diagnostics.FinalizeCommitCount):F3}, checkpointDecisions={diagnostics.CheckpointDecisionCount}, avgCheckpointDecisionMs={AverageMilliseconds(diagnostics.CheckpointDecisionTicks, diagnostics.CheckpointDecisionCount):F3}, backgroundCheckpointStarts={diagnostics.BackgroundCheckpointStartCount}, maxPendingCommits={diagnostics.MaxPendingCommitCount}, maxPendingCommitKiB={maxPendingCommitKiB:F1}");
    }

    public static double AverageMilliseconds(long ticks, long count)
    {
        if (ticks <= 0 || count <= 0)
            return 0;

        return ticks * 1000.0 / Stopwatch.Frequency / count;
    }
}

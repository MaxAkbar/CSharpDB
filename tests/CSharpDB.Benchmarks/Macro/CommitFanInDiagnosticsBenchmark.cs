using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Compares shared auto-commit against overlapping explicit WriteTransaction
/// commits under the same low-conflict workload so commit fan-in can be
/// measured directly.
/// </summary>
public static class CommitFanInDiagnosticsBenchmark
{
    private const int SeedRowCount = 4096;

    private static readonly CommitFanInScenario[] s_scenarios =
    [
        new("AutoCommit_DisjointUpdate_W4_Batch250us", CommitMode.AutoCommit, 4, TimeSpan.FromMilliseconds(0.25)),
        new("AutoCommit_DisjointUpdate_W8_Batch250us", CommitMode.AutoCommit, 8, TimeSpan.FromMilliseconds(0.25)),
        new("ExplicitTx_DisjointUpdate_W4_Batch250us", CommitMode.ExplicitWriteTransaction, 4, TimeSpan.FromMilliseconds(0.25)),
        new("ExplicitTx_DisjointUpdate_W8_Batch250us", CommitMode.ExplicitWriteTransaction, 8, TimeSpan.FromMilliseconds(0.25)),
    ];

    private static readonly WriteTransactionOptions s_retryOptions = new()
    {
        MaxRetries = 20,
    };

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (CommitFanInScenario scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));

        return results;
    }

    public static Task<BenchmarkResult> RunNamedScenarioAsync(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);

        CommitFanInScenario? scenario = s_scenarios.FirstOrDefault(
            scenario => scenario.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown commit fan-in scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(CommitFanInScenario scenario)
    {
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(4096),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = 256,
            });
            builder.UseDurableGroupCommit(scenario.BatchWindow);
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(seedRowCount: SeedRowCount, options: options);
        await bench.ReopenAsync();

        await RunPhaseAsync(bench.Db, scenario, TimeSpan.FromSeconds(2), recordLatencies: false);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();
        RunStats stats = await RunPhaseAsync(bench.Db, scenario, TimeSpan.FromSeconds(5), recordLatencies: true);
        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

        var histogram = new LatencyHistogram();
        foreach (double latencyMs in stats.CommitLatenciesMs)
            histogram.Record(latencyMs);

        double elapsedSeconds = stats.ElapsedMs <= 0 ? 0 : stats.ElapsedMs / 1000.0;
        double flushesPerSecond = elapsedSeconds <= 0 ? 0 : walDiagnostics.FlushCount / elapsedSeconds;
        double commitsPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : (double)walDiagnostics.FlushedCommitCount / walDiagnostics.FlushCount;
        double kibPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : walDiagnostics.FlushedByteCount / (double)walDiagnostics.FlushCount / 1024.0;
        int extraAttempts = Math.Max(0, stats.AttemptCount - stats.SuccessfulCommits - stats.TerminalFailureCount);
        string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitDiagnostics);

        var result = new BenchmarkResult
        {
            Name = $"CommitFanIn_{scenario.Name}_5s",
            TotalOps = stats.SuccessfulCommits,
            ElapsedMs = stats.ElapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo =
                $"mode={scenario.Mode}, workload=DisjointUpdate, seededRows={SeedRowCount}, writers={scenario.WriterCount}, rowsPerCommit=1, batchWindow={FormatBatchWindow(scenario.BatchWindow)}, successfulCommits={stats.SuccessfulCommits}, attempts={stats.AttemptCount}, extraAttempts={extraAttempts}, terminalFailures={stats.TerminalFailureCount}, flushes={walDiagnostics.FlushCount}, flushesPerSec={flushesPerSecond:F1}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, {commitSummary}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, commits/flush={commitsPerFlush:F2}, maxPending={commitDiagnostics.MaxPendingCommitCount}");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static async Task<RunStats> RunPhaseAsync(
        Database db,
        CommitFanInScenario scenario,
        TimeSpan duration,
        bool recordLatencies)
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerTasks = new Task<WriterStats>[scenario.WriterCount];
        long durationTicks = (long)(duration.TotalSeconds * Stopwatch.Frequency);

        for (int writerIndex = 0; writerIndex < scenario.WriterCount; writerIndex++)
        {
            int localWriterIndex = writerIndex;
            writerTasks[writerIndex] = Task.Run(async () =>
            {
                var localLatencies = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int attemptCount = 0;
                int terminalFailureCount = 0;
                int updateCursor = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    int rowId = GetNextRowId(localWriterIndex, scenario.WriterCount, ref updateCursor);
                    string sql = $"UPDATE bench SET value = value + 1 WHERE id = {rowId}";
                    var sw = Stopwatch.StartNew();

                    try
                    {
                        if (scenario.Mode == CommitMode.AutoCommit)
                        {
                            attemptCount++;
                            await db.ExecuteAsync(sql, CancellationToken.None).ConfigureAwait(false);
                        }
                        else
                        {
                            int transactionAttempts = 0;
                            await db.RunWriteTransactionAsync(
                                async (tx, innerCt) =>
                                {
                                    transactionAttempts++;
                                    await tx.ExecuteAsync(sql, innerCt).ConfigureAwait(false);
                                },
                                s_retryOptions).ConfigureAwait(false);
                            attemptCount += transactionAttempts;
                        }

                        sw.Stop();
                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbConflictException)
                    {
                        sw.Stop();
                        terminalFailureCount++;
                    }
                    catch (CSharpDbException ex) when (ex.Code == ErrorCode.Busy)
                    {
                        sw.Stop();
                        terminalFailureCount++;
                    }
                    catch
                    {
                        terminalFailureCount++;
                        throw;
                    }
                }

                return new WriterStats(
                    localLatencies ?? [],
                    successfulCommits,
                    attemptCount,
                    terminalFailureCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks);
        totalSw.Stop();

        var combinedLatencies = new List<double>(completed.Sum(static writer => writer.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int attemptCount = 0;
        int terminalFailureCount = 0;

        foreach (WriterStats writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            attemptCount += writer.AttemptCount;
            terminalFailureCount += writer.TerminalFailureCount;
        }

        return new RunStats(
            combinedLatencies,
            successfulCommits,
            attemptCount,
            terminalFailureCount,
            totalSw.Elapsed.TotalMilliseconds);
    }

    private static int GetNextRowId(int writerIndex, int writerCount, ref int updateCursor)
    {
        int partitionSize = SeedRowCount / writerCount;
        int partitionStart = (writerIndex * partitionSize) + 1;
        int rowId = partitionStart + (updateCursor % partitionSize);
        updateCursor++;
        return rowId;
    }

    private static string FormatBatchWindow(TimeSpan batchWindow)
    {
        if (batchWindow == TimeSpan.Zero)
            return "0";

        double microseconds = batchWindow.TotalMilliseconds * 1000.0;
        if (microseconds < 1000.0)
            return $"{microseconds:F0}us";

        return $"{batchWindow.TotalMilliseconds:F3}ms";
    }

    private sealed record CommitFanInScenario(
        string Name,
        CommitMode Mode,
        int WriterCount,
        TimeSpan BatchWindow);

    private enum CommitMode
    {
        AutoCommit,
        ExplicitWriteTransaction,
    }

    private sealed record WriterStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int AttemptCount,
        int TerminalFailureCount);

    private sealed record RunStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int AttemptCount,
        int TerminalFailureCount,
        double ElapsedMs);
}

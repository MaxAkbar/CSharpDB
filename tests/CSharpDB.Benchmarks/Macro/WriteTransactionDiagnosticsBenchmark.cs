using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused file-backed benchmark for the phase-1 explicit WriteTransaction API.
/// Scenarios cover both single-writer batching and shared-database multi-writer contention.
/// </summary>
public static class WriteTransactionDiagnosticsBenchmark
{
    private static int _nextId;

    private static readonly WriteTransactionScenario[] s_scenarios =
    [
        new("W1_Rows1_Batch0", 1, 1, TimeSpan.Zero),
        new("W1_Rows10_Batch0", 1, 10, TimeSpan.Zero),
        new("W1_Rows100_Batch0", 1, 100, TimeSpan.Zero),
        new("W4_Rows1_Batch0", 4, 1, TimeSpan.Zero),
        new("W4_Rows10_Batch0", 4, 10, TimeSpan.Zero),
        new("W8_Rows1_Batch0", 8, 1, TimeSpan.Zero),
        new("W8_Rows10_Batch0", 8, 10, TimeSpan.Zero),
        new("W8_Rows10_Batch250us_Prealloc1MiB", 8, 10, TimeSpan.FromMilliseconds(0.25), 1L * 1024 * 1024),
        new("UpdateDisjoint_W4_Rows1_Batch250us", 4, 1, TimeSpan.FromMilliseconds(0.25), Workload: WriteTransactionWorkload.DisjointUpdate, SeedRowCount: 4096),
        new("UpdateDisjoint_W8_Rows1_Batch250us_Prealloc1MiB", 8, 1, TimeSpan.FromMilliseconds(0.25), 1L * 1024 * 1024, Workload: WriteTransactionWorkload.DisjointUpdate, SeedRowCount: 4096),
        new("UpdateDisjoint_W8_Rows10_Batch250us_Prealloc1MiB", 8, 10, TimeSpan.FromMilliseconds(0.25), 1L * 1024 * 1024, Workload: WriteTransactionWorkload.DisjointUpdate, SeedRowCount: 4096),
    ];

    private static readonly WriteTransactionOptions s_retryOptions = new()
    {
        MaxRetries = 20,
    };

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (var scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));

        return results;
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(WriteTransactionScenario scenario)
    {
        _nextId = 0;
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(4096),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = 256,
            });
            builder.UseDurableCommitBatchWindow(scenario.BatchWindow);
            builder.UseWalPreallocationChunkBytes(scenario.WalPreallocationChunkBytes);
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(
            seedRowCount: scenario.SeedRowCount > 0 ? scenario.SeedRowCount : null,
            options: options);
        await bench.ReopenAsync();

        await RunPhaseAsync(bench.Db, scenario, TimeSpan.FromSeconds(2), recordLatencies: false);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();
        var stats = await RunPhaseAsync(bench.Db, scenario, TimeSpan.FromSeconds(10), recordLatencies: true);
        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

        var histogram = new LatencyHistogram();
        foreach (double latencyMs in stats.CommitLatenciesMs)
            histogram.Record(latencyMs);

        double elapsedSeconds = stats.ElapsedMs <= 0 ? 0 : stats.ElapsedMs / 1000.0;
        double rowsPerSecond = elapsedSeconds <= 0
            ? 0
            : stats.SuccessfulCommits * (double)scenario.RowsPerTransaction / elapsedSeconds;
        double flushesPerSecond = elapsedSeconds <= 0 ? 0 : walDiagnostics.FlushCount / elapsedSeconds;
        double commitsPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : (double)walDiagnostics.FlushedCommitCount / walDiagnostics.FlushCount;
        double kibPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : walDiagnostics.FlushedByteCount / (double)walDiagnostics.FlushCount / 1024.0;
        double preallocatedKiB = walDiagnostics.PreallocatedByteCount / 1024.0;
        int retryCount = stats.AttemptCount - stats.SuccessfulCommits;
        double retriesPerCommit = stats.SuccessfulCommits == 0
            ? 0
            : retryCount / (double)stats.SuccessfulCommits;
        string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics);

        var result = new BenchmarkResult
        {
            Name = $"WriteTransactionDiagnostics_{scenario.Name}_10s",
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
                $"workload={scenario.Workload}, seededRows={scenario.SeedRowCount}, writers={scenario.WriterCount}, rowsPerTx={scenario.RowsPerTransaction}, checkpoint=FrameCount(4096)+Background(256), batchWindow={FormatBatchWindow(scenario.BatchWindow)}, walPrealloc={FormatPreallocationChunk(scenario.WalPreallocationChunkBytes)}, retryBudget={s_retryOptions.MaxRetries}, rowsPerSec={rowsPerSecond:F1}, attempts={stats.AttemptCount}, retries={retryCount}, retriesPerCommit={retriesPerCommit:F2}, exhaustedConflicts={stats.ExhaustedConflictCount}, fatalErrors={stats.FatalErrorCount}, flushes={walDiagnostics.FlushCount}, flushesPerSec={flushesPerSecond:F1}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, preallocations={walDiagnostics.PreallocationCount}, preallocatedKiB={preallocatedKiB:F1}, {commitSummary}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, rows/sec={rowsPerSecond:N0}, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static async Task<WriteTransactionPhaseStats> RunPhaseAsync(
        Database db,
        WriteTransactionScenario scenario,
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
                var ids = new int[scenario.RowsPerTransaction];
                int updateCursor = 0;
                int successfulCommits = 0;
                int attemptCount = 0;
                int exhaustedConflictCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    PopulateIds(ids, scenario, localWriterIndex, ref updateCursor);
                    int txAttempts = 0;
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        await db.RunWriteTransactionAsync(
                            async (tx, innerCt) =>
                            {
                                txAttempts++;
                                for (int i = 0; i < ids.Length; i++)
                                {
                                    int id = ids[i];
                                    string sql = scenario.Workload == WriteTransactionWorkload.HotInsert
                                        ? $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {localWriterIndex}, 'write_tx', 'Alpha')"
                                        : $"UPDATE bench SET value = value + 1 WHERE id = {id}";
                                    await tx.ExecuteAsync(sql, innerCt).ConfigureAwait(false);
                                }
                            },
                            s_retryOptions).ConfigureAwait(false);
                        sw.Stop();

                        attemptCount += txAttempts;
                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbConflictException)
                    {
                        sw.Stop();
                        attemptCount += txAttempts;
                        exhaustedConflictCount++;
                    }
                    catch
                    {
                        fatalErrorCount++;
                        throw;
                    }
                }

                return new WriterStats(
                    localLatencies ?? [],
                    successfulCommits,
                    attemptCount,
                    exhaustedConflictCount,
                    fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks);
        totalSw.Stop();

        var combinedLatencies = new List<double>(completed.Sum(static s => s.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int attemptCount = 0;
        int exhaustedConflictCount = 0;
        int fatalErrorCount = 0;

        foreach (var writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            attemptCount += writer.AttemptCount;
            exhaustedConflictCount += writer.ExhaustedConflictCount;
            fatalErrorCount += writer.FatalErrorCount;
        }

        return new WriteTransactionPhaseStats(
            combinedLatencies,
            successfulCommits,
            attemptCount,
            exhaustedConflictCount,
            fatalErrorCount,
            totalSw.Elapsed.TotalMilliseconds);
    }

    private static void PopulateIds(
        int[] ids,
        WriteTransactionScenario scenario,
        int writerIndex,
        ref int updateCursor)
    {
        if (scenario.Workload == WriteTransactionWorkload.HotInsert)
        {
            for (int i = 0; i < ids.Length; i++)
                ids[i] = Interlocked.Increment(ref _nextId);
            return;
        }

        int seededRows = scenario.SeedRowCount;
        int partitionSize = Math.Max(seededRows / scenario.WriterCount, ids.Length);
        int partitionStart = writerIndex * partitionSize;
        for (int i = 0; i < ids.Length; i++)
            ids[i] = partitionStart + ((updateCursor + i) % partitionSize);

        updateCursor += ids.Length;
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

    private static string FormatPreallocationChunk(long chunkBytes)
    {
        if (chunkBytes <= 0)
            return "0";

        return $"{chunkBytes / 1024.0 / 1024.0:F1}MiB";
    }

    private sealed record WriteTransactionScenario(
        string Name,
        int WriterCount,
        int RowsPerTransaction,
        TimeSpan BatchWindow,
        long WalPreallocationChunkBytes = 0,
        WriteTransactionWorkload Workload = WriteTransactionWorkload.HotInsert,
        int SeedRowCount = 0);

    private enum WriteTransactionWorkload
    {
        HotInsert,
        DisjointUpdate,
    }

    private sealed record WriterStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int AttemptCount,
        int ExhaustedConflictCount,
        int FatalErrorCount);

    private sealed record WriteTransactionPhaseStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int AttemptCount,
        int ExhaustedConflictCount,
        int FatalErrorCount,
        double ElapsedMs);
}

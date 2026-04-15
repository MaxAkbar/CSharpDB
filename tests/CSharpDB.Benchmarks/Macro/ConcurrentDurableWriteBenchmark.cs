using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures durable commit throughput when multiple in-process writer tasks share
/// one Database instance and issue auto-commit inserts concurrently.
/// This models engine-side write contention on a shared WAL/pager without
/// adding cross-process transport overhead.
/// </summary>
public static class ConcurrentDurableWriteBenchmark
{
    private static int _nextId;

    private static readonly ConcurrentWriteScenario[] s_scenarios =
    [
        new("W4_Batch0", 4, TimeSpan.Zero),
        new("W4_Batch250us", 4, TimeSpan.FromMilliseconds(0.25)),
        new("W4_Batch500us", 4, TimeSpan.FromMilliseconds(0.5)),
        new("W8_Batch0", 8, TimeSpan.Zero),
        new("W8_Batch250us", 8, TimeSpan.FromMilliseconds(0.25)),
        new("W8_Batch500us", 8, TimeSpan.FromMilliseconds(0.5)),
        new("W8_Batch0_Prealloc1MiB", 8, TimeSpan.Zero, 1L * 1024 * 1024),
        new("W8_Batch250us_Prealloc1MiB", 8, TimeSpan.FromMilliseconds(0.25), 1L * 1024 * 1024),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (var scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));

        return results;
    }

    public static Task<BenchmarkResult> RunNamedScenarioAsync(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);

        ConcurrentWriteScenario? scenario = s_scenarios.FirstOrDefault(
            scenario => scenario.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
            throw new ArgumentException(
                $"Unknown concurrent durable-write scenario '{scenarioName}'.",
                nameof(scenarioName));

        return RunScenarioAsync(scenario);
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(ConcurrentWriteScenario scenario)
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
            builder.UseDurableGroupCommit(scenario.BatchWindow);
            builder.UseWalPreallocationChunkBytes(scenario.WalPreallocationChunkBytes);
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(options: options);
        await bench.ReopenAsync();

        await RunPhaseAsync(bench.Db, scenario.WriterCount, TimeSpan.FromSeconds(2), recordLatencies: false);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();
        var stats = await RunPhaseAsync(bench.Db, scenario.WriterCount, TimeSpan.FromSeconds(10), recordLatencies: true);
        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

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
        double preallocatedKiB = walDiagnostics.PreallocatedByteCount / 1024.0;
        string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics);

        var result = new BenchmarkResult
        {
            Name = $"ConcurrentDurableWrite_{scenario.Name}_10s",
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
                $"writers={scenario.WriterCount}, checkpoint=FrameCount(4096)+Background(256), batchWindow={FormatBatchWindow(scenario.BatchWindow)}, walPrealloc={FormatPreallocationChunk(scenario.WalPreallocationChunkBytes)}, successfulCommits={stats.SuccessfulCommits}, busy={stats.BusyCount}, fatalErrors={stats.FatalErrorCount}, flushes={walDiagnostics.FlushCount}, flushesPerSec={flushesPerSecond:F1}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, preallocations={walDiagnostics.PreallocationCount}, preallocatedKiB={preallocatedKiB:F1}, {commitSummary}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static async Task<ConcurrentPhaseStats> RunPhaseAsync(
        Database db,
        int writerCount,
        TimeSpan duration,
        bool recordLatencies)
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerTasks = new Task<WriterStats>[writerCount];
        long durationTicks = (long)(duration.TotalSeconds * Stopwatch.Frequency);

        for (int writerIndex = 0; writerIndex < writerCount; writerIndex++)
        {
            int localWriterIndex = writerIndex;
            // Each logical writer runs as its own Task against the same Database
            // instance so the benchmark captures in-process contention.
            writerTasks[writerIndex] = Task.Run(async () =>
            {
                var localLatencies = recordLatencies ? new List<double>(capacity: 4096) : null;
                int successfulCommits = 0;
                int busyCount = 0;
                int fatalErrorCount = 0;

                await startGate.Task.ConfigureAwait(false);
                long startedAt = Stopwatch.GetTimestamp();

                while (Stopwatch.GetTimestamp() - startedAt < durationTicks)
                {
                    int id = Interlocked.Increment(ref _nextId);
                    string sql =
                        $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {localWriterIndex}, 'durable', 'Alpha')";
                    var sw = Stopwatch.StartNew();
                    try
                    {
                        await db.ExecuteAsync(sql, CancellationToken.None).ConfigureAwait(false);
                        sw.Stop();

                        successfulCommits++;
                        localLatencies?.Add(sw.Elapsed.TotalMilliseconds);
                    }
                    catch (CSharpDbException ex) when (ex.Code == ErrorCode.Busy)
                    {
                        sw.Stop();
                        busyCount++;
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
                    busyCount,
                    fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks);
        totalSw.Stop();

        var combinedLatencies = new List<double>(completed.Sum(static s => s.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int busyCount = 0;
        int fatalErrorCount = 0;

        foreach (var writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            busyCount += writer.BusyCount;
            fatalErrorCount += writer.FatalErrorCount;
        }

        return new ConcurrentPhaseStats(
            combinedLatencies,
            successfulCommits,
            busyCount,
            fatalErrorCount,
            totalSw.Elapsed.TotalMilliseconds);
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

    private sealed record ConcurrentWriteScenario(
        string Name,
        int WriterCount,
        TimeSpan BatchWindow,
        long WalPreallocationChunkBytes = 0);

    private sealed record WriterStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount);

    private sealed record ConcurrentPhaseStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount,
        double ElapsedMs);
}

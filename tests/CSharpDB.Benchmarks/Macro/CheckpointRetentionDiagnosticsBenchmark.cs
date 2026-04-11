using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures how much background checkpoint progress is pulled forward while an
/// explicit write transaction keeps WAL finalization deferred.
/// </summary>
public static class CheckpointRetentionDiagnosticsBenchmark
{
    private static int _nextId;

    private static readonly CheckpointRetentionScenario[] s_scenarios =
    [
        new("W8_NoBlocker_Batch250us", 8, TimeSpan.FromMilliseconds(0.25), 0, TimeSpan.Zero),
        new("W8_Blocker3s_Batch250us", 8, TimeSpan.FromMilliseconds(0.25), 0, TimeSpan.FromSeconds(3)),
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

        CheckpointRetentionScenario? scenario = s_scenarios.FirstOrDefault(
            scenario => scenario.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown checkpoint-retention scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(CheckpointRetentionScenario scenario)
    {
        _nextId = 0;
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                CheckpointPolicy = new FrameCountCheckpointPolicy(64),
                AutoCheckpointExecutionMode = AutoCheckpointExecutionMode.Background,
                AutoCheckpointMaxPagesPerStep = 64,
            });
            builder.UseDurableGroupCommit(scenario.BatchWindow);
            builder.UseWalPreallocationChunkBytes(scenario.WalPreallocationChunkBytes);
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(options: options);
        await bench.ReopenAsync();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();

        WriteTransaction? blocker = null;
        try
        {
            if (scenario.BlockerHoldDuration > TimeSpan.Zero)
            {
                blocker = await bench.Db.BeginWriteTransactionAsync();
                await using var blockerRead = await blocker.ExecuteSnapshotReadAsync("SELECT COUNT(*) FROM bench");
                await blockerRead.ToListAsync();
            }

            TimeSpan loadDuration = scenario.BlockerHoldDuration > TimeSpan.Zero
                ? scenario.BlockerHoldDuration
                : TimeSpan.FromSeconds(3);
            var stats = await RunLoadPhaseAsync(bench.Db, scenario.WriterCount, loadDuration, recordLatencies: true);
            WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
            CommitPathDiagnosticsSnapshot commitDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

            string walPath = bench.FilePath + ".wal";
            long walLengthBeforeCheckpoint = GetWalLength(walPath);

            if (blocker is not null)
            {
                await blocker.DisposeAsync();
                blocker = null;
            }

            var checkpointSw = Stopwatch.StartNew();
            await bench.Db.CheckpointAsync();
            checkpointSw.Stop();

            long walLengthAfterCheckpoint = GetWalLength(walPath);
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
            double walBeforeCheckpointKiB = walLengthBeforeCheckpoint / 1024.0;
            double walAfterCheckpointKiB = walLengthAfterCheckpoint / 1024.0;
            string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitDiagnostics);

            var result = new BenchmarkResult
            {
                Name = $"CheckpointRetention_{scenario.Name}",
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
                    $"writers={scenario.WriterCount}, blockerHold={FormatDuration(scenario.BlockerHoldDuration)}, checkpoint=FrameCount(64)+Background(64), batchWindow={FormatBatchWindow(scenario.BatchWindow)}, walPrealloc={FormatPreallocationChunk(scenario.WalPreallocationChunkBytes)}, successfulCommits={stats.SuccessfulCommits}, busy={stats.BusyCount}, fatalErrors={stats.FatalErrorCount}, flushes={walDiagnostics.FlushCount}, flushesPerSec={flushesPerSecond:F1}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, preallocations={walDiagnostics.PreallocationCount}, preallocatedKiB={preallocatedKiB:F1}, backgroundCheckpointStarts={commitDiagnostics.BackgroundCheckpointStartCount}, walBeforeCheckpointKiB={walBeforeCheckpointKiB:F1}, postReleaseCheckpointMs={checkpointSw.Elapsed.TotalMilliseconds:F3}, walAfterCheckpointKiB={walAfterCheckpointKiB:F1}, {commitSummary}",
            };

            Console.WriteLine(
                $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, post-release checkpoint={checkpointSw.Elapsed.TotalMilliseconds:F2}ms");
            Console.WriteLine($"    {result.ExtraInfo}");
            return result;
        }
        finally
        {
            if (blocker is not null)
                await blocker.DisposeAsync();
        }
    }

    private static async Task<LoadPhaseStats> RunLoadPhaseAsync(
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
                        $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {localWriterIndex}, 'checkpoint_retention', 'Alpha')";
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

                return new WriterStats(localLatencies ?? [], successfulCommits, busyCount, fatalErrorCount);
            });
        }

        var totalSw = Stopwatch.StartNew();
        startGate.TrySetResult();
        WriterStats[] completed = await Task.WhenAll(writerTasks);
        totalSw.Stop();

        var combinedLatencies = new List<double>(completed.Sum(static writer => writer.CommitLatenciesMs.Count));
        int successfulCommits = 0;
        int busyCount = 0;
        int fatalErrorCount = 0;

        foreach (WriterStats writer in completed)
        {
            combinedLatencies.AddRange(writer.CommitLatenciesMs);
            successfulCommits += writer.SuccessfulCommits;
            busyCount += writer.BusyCount;
            fatalErrorCount += writer.FatalErrorCount;
        }

        return new LoadPhaseStats(
            combinedLatencies,
            successfulCommits,
            busyCount,
            fatalErrorCount,
            totalSw.Elapsed.TotalMilliseconds);
    }

    private static long GetWalLength(string walPath)
    {
        if (!File.Exists(walPath))
            return 0;

        return new FileInfo(walPath).Length;
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

    private static string FormatDuration(TimeSpan duration)
    {
        if (duration == TimeSpan.Zero)
            return "0";

        if (duration.TotalMilliseconds < 1000.0)
            return $"{duration.TotalMilliseconds:F0}ms";

        return $"{duration.TotalSeconds:F1}s";
    }

    private static string FormatPreallocationChunk(long chunkBytes)
    {
        if (chunkBytes <= 0)
            return "0";

        return $"{chunkBytes / 1024.0 / 1024.0:F1}MiB";
    }

    private sealed record CheckpointRetentionScenario(
        string Name,
        int WriterCount,
        TimeSpan BatchWindow,
        long WalPreallocationChunkBytes,
        TimeSpan BlockerHoldDuration);

    private sealed record WriterStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount);

    private sealed record LoadPhaseStats(
        IReadOnlyList<double> CommitLatenciesMs,
        int SuccessfulCommits,
        int BusyCount,
        int FatalErrorCount,
        double ElapsedMs);
}

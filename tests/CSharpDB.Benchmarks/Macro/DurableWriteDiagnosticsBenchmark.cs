using System.Diagnostics;
using System.Globalization;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused diagnostics for file-backed durable-write variance in the shared pager/WAL path.
/// Reports how often commits pay checkpoint cost under different checkpoint policies and execution modes.
/// </summary>
public static class DurableWriteDiagnosticsBenchmark
{
    private static readonly DiagnosticScenario[] s_scenarios =
    [
        new(
            "Frame1000",
            "FrameCount(1000)",
            static () => new FrameCountCheckpointPolicy(1000),
            AutoCheckpointExecutionMode.Foreground),
        new(
            "Frame2048",
            "FrameCount(2048)",
            static () => new FrameCountCheckpointPolicy(2048),
            AutoCheckpointExecutionMode.Foreground),
        new(
            "Frame4096",
            "FrameCount(4096)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Foreground),
        new(
            "Frame4096Background16",
            "FrameCount(4096)+Background(16 pages/step)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            16),
        new(
            "Frame4096Background64",
            "FrameCount(4096)+Background(64 pages/step)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            64),
        new(
            "Frame4096Background256",
            "FrameCount(4096)+Background(256 pages/step)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            256),
        new(
            "Frame4096Background256Prealloc1MiB",
            "FrameCount(4096)+Background(256 pages/step)+WalPrealloc(1MiB)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            256,
            WalPreallocationChunkBytes: 1L * 1024 * 1024),
        new(
            "Frame4096Background256Batch250us",
            "FrameCount(4096)+Background(256 pages/step)+BatchWindow(250us)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            256,
            TimeSpan.FromMilliseconds(0.25)),
        new(
            "Frame4096Background256Batch1ms",
            "FrameCount(4096)+Background(256 pages/step)+BatchWindow(1ms)",
            static () => new FrameCountCheckpointPolicy(4096),
            AutoCheckpointExecutionMode.Background,
            256,
            TimeSpan.FromMilliseconds(1)),
        new(
            "Wal4MiB",
            "WalSize(4 MiB)",
            static () => new WalSizeCheckpointPolicy(4L * 1024 * 1024),
            AutoCheckpointExecutionMode.Foreground),
        new(
            "Wal8MiB",
            "WalSize(8 MiB)",
            static () => new WalSizeCheckpointPolicy(8L * 1024 * 1024),
            AutoCheckpointExecutionMode.Foreground),
    ];

    private static int _idCounter;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (var scenario in s_scenarios)
        {
            var interceptor = new WritePathDiagnosticInterceptor();
            var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    CheckpointPolicy = scenario.CreatePolicy(),
                    AutoCheckpointExecutionMode = scenario.ExecutionMode,
                    AutoCheckpointMaxPagesPerStep = scenario.AutoCheckpointMaxPagesPerStep,
                    Interceptors = [interceptor],
                });
                builder.UseDurableGroupCommit(scenario.DurableCommitBatchWindow);
                builder.UseWalPreallocationChunkBytes(scenario.WalPreallocationChunkBytes);
            });

            await using var bench = await BenchmarkDatabase.CreateAsync(options: options);
            var result = await RunScenarioAsync(
                $"WriteDiagnostics_SingleRow_{scenario.Name}_10s",
                bench,
                interceptor,
                scenario.Description,
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(10));
            results.Add(result);
        }

        return results;
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(
        string name,
        BenchmarkDatabase bench,
        WritePathDiagnosticInterceptor interceptor,
        string policyDescription,
        TimeSpan warmupDuration,
        TimeSpan measuredDuration)
    {
        async Task OperationAsync()
        {
            int id = Interlocked.Increment(ref _idCounter);
            await bench.Db.ExecuteAsync(
                $"INSERT INTO bench VALUES ({id}, {id}, 'durable', 'Alpha')");
        }

        var warmupEnd = DateTime.UtcNow + warmupDuration;
        while (DateTime.UtcNow < warmupEnd)
        {
            await OperationAsync();
        }

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();
        interceptor.Reset();

        var histogram = new LatencyHistogram();
        var totalSw = Stopwatch.StartNew();
        var measuredEnd = DateTime.UtcNow + measuredDuration;

        while (DateTime.UtcNow < measuredEnd)
        {
            var sw = Stopwatch.StartNew();
            await OperationAsync();
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        totalSw.Stop();
        WalFlushDiagnosticsSnapshot walDiagnostics = bench.Db.GetWalFlushDiagnosticsSnapshot();
        CommitPathDiagnosticsSnapshot commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();

        var result = CreateResult(
            name,
            histogram,
            totalSw.Elapsed.TotalMilliseconds,
            interceptor.BuildSummary(
                policyDescription,
                walDiagnostics,
                commitPathDiagnostics,
                totalSw.Elapsed.TotalMilliseconds));

        Console.WriteLine($"  {name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private static BenchmarkResult CreateResult(
        string name,
        LatencyHistogram histogram,
        double elapsedMs,
        string extraInfo)
    {
        return new BenchmarkResult
        {
            Name = name,
            TotalOps = histogram.Count,
            ElapsedMs = elapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev,
            ExtraInfo = extraInfo,
        };
    }

    private sealed class WritePathDiagnosticInterceptor : IPageOperationInterceptor
    {
        private readonly object _sync = new();

        private LatencyHistogram _commitLatencyMs = new();
        private LatencyHistogram _commitWithCheckpointLatencyMs = new();
        private LatencyHistogram _commitWithoutCheckpointLatencyMs = new();
        private LatencyHistogram _checkpointLatencyMs = new();

        private long _currentCommitStartTicks;
        private long _currentCheckpointStartTicks;
        private bool _currentCommitSawCheckpoint;

        private long _totalDirtyPages;
        private int _commitCount;
        private int _commitCountWithCheckpoint;
        private int _checkpointCount;
        private int _maxDirtyPages;

        public void Reset()
        {
            lock (_sync)
            {
                _commitLatencyMs = new LatencyHistogram();
                _commitWithCheckpointLatencyMs = new LatencyHistogram();
                _commitWithoutCheckpointLatencyMs = new LatencyHistogram();
                _checkpointLatencyMs = new LatencyHistogram();
                _currentCommitStartTicks = 0;
                _currentCheckpointStartTicks = 0;
                _currentCommitSawCheckpoint = false;
                _totalDirtyPages = 0;
                _commitCount = 0;
                _commitCountWithCheckpoint = 0;
                _checkpointCount = 0;
                _maxDirtyPages = 0;
            }
        }

        public string BuildSummary(
            string policyDescription,
            WalFlushDiagnosticsSnapshot walDiagnostics,
            CommitPathDiagnosticsSnapshot commitPathDiagnostics,
            double elapsedMs)
        {
            lock (_sync)
            {
                double avgDirtyPages = _commitCount == 0 ? 0 : (double)_totalDirtyPages / _commitCount;
                double elapsedSeconds = elapsedMs <= 0 ? 0 : elapsedMs / 1000.0;
                double flushesPerSecond = elapsedSeconds <= 0 ? 0 : walDiagnostics.FlushCount / elapsedSeconds;
                double commitsPerFlush = walDiagnostics.FlushCount == 0
                    ? 0
                    : (double)walDiagnostics.FlushedCommitCount / walDiagnostics.FlushCount;
                double kibPerFlush = walDiagnostics.FlushCount == 0
                    ? 0
                    : walDiagnostics.FlushedByteCount / (double)walDiagnostics.FlushCount / 1024.0;
                double preallocatedKiB = walDiagnostics.PreallocatedByteCount / 1024.0;
                string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics);
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"policy={policyDescription}, commits={_commitCount}, avgDirtyPages={avgDirtyPages:F2}, maxDirtyPages={_maxDirtyPages}, checkpoints={_checkpointCount}, commitsWithCheckpoint={_commitCountWithCheckpoint}, flushes={walDiagnostics.FlushCount}, flushesPerSec={flushesPerSecond:F1}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, preallocations={walDiagnostics.PreallocationCount}, preallocatedKiB={preallocatedKiB:F1}, avgCommitMs={_commitLatencyMs.Mean:F3}, p99CommitMs={_commitLatencyMs.Percentile(0.99):F3}, avgCommitNoCheckpointMs={_commitWithoutCheckpointLatencyMs.Mean:F3}, avgCommitWithCheckpointMs={_commitWithCheckpointLatencyMs.Mean:F3}, avgCheckpointMs={_checkpointLatencyMs.Mean:F3}, p99CheckpointMs={_checkpointLatencyMs.Percentile(0.99):F3}, {commitSummary}");
            }
        }

        public ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
        {
            lock (_sync)
            {
                _currentCommitStartTicks = Stopwatch.GetTimestamp();
                _currentCommitSawCheckpoint = false;
                _totalDirtyPages += dirtyPageCount;
                if (dirtyPageCount > _maxDirtyPages)
                    _maxDirtyPages = dirtyPageCount;
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
        {
            long endTicks = Stopwatch.GetTimestamp();

            lock (_sync)
            {
                double commitMs = ElapsedMilliseconds(_currentCommitStartTicks, endTicks);
                _commitLatencyMs.Record(commitMs);
                _commitCount++;

                if (_currentCommitSawCheckpoint)
                {
                    _commitWithCheckpointLatencyMs.Record(commitMs);
                    _commitCountWithCheckpoint++;
                }
                else
                {
                    _commitWithoutCheckpointLatencyMs.Record(commitMs);
                }
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
        {
            lock (_sync)
            {
                _currentCommitSawCheckpoint = true;
                _currentCheckpointStartTicks = Stopwatch.GetTimestamp();
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
        {
            long endTicks = Stopwatch.GetTimestamp();

            lock (_sync)
            {
                _checkpointLatencyMs.Record(ElapsedMilliseconds(_currentCheckpointStartTicks, endTicks));
                _checkpointCount++;
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask OnRecoveryStartAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

        public ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default) => ValueTask.CompletedTask;

        private static double ElapsedMilliseconds(long startTicks, long endTicks)
        {
            if (startTicks == 0 || endTicks <= startTicks)
                return 0;

            return (endTicks - startTicks) * 1000.0 / Stopwatch.Frequency;
        }
    }

    private sealed record DiagnosticScenario(
        string Name,
        string Description,
        Func<ICheckpointPolicy> CreatePolicy,
        AutoCheckpointExecutionMode ExecutionMode,
        int AutoCheckpointMaxPagesPerStep = 64,
        TimeSpan DurableCommitBatchWindow = default,
        long WalPreallocationChunkBytes = 0);
}

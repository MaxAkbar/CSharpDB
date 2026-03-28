using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Focused file-backed durable SQL ingest benchmark that compares single-row auto-commit
/// against explicit transaction batching using the existing PrepareInsertBatch API.
/// </summary>
public static class DurableSqlBatchingBenchmark
{
    private static readonly SqlBatchScenario[] s_scenarios =
    [
        new("AutoCommitSingle_WriteOptimized", RowsPerCommit: 1, UseLowLatencyPreset: false, AnalyzeBeforeRun: false),
        new("AutoCommitSingle_WriteOptimized_Analyzed", RowsPerCommit: 1, UseLowLatencyPreset: false, AnalyzeBeforeRun: true),
        new("AutoCommitSingle_LowLatency_Analyzed", RowsPerCommit: 1, UseLowLatencyPreset: true, AnalyzeBeforeRun: true),
        new("TxBatch10_LowLatency", RowsPerCommit: 10, UseLowLatencyPreset: true, AnalyzeBeforeRun: false),
        new("TxBatch100_LowLatency", RowsPerCommit: 100, UseLowLatencyPreset: true, AnalyzeBeforeRun: false),
        new("TxBatch1000_LowLatency", RowsPerCommit: 1000, UseLowLatencyPreset: true, AnalyzeBeforeRun: false),
    ];

    private static int _nextId;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);

        foreach (var scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));

        return results;
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(SqlBatchScenario scenario)
    {
        _nextId = 0;
        var options = new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            if (scenario.UseLowLatencyPreset)
                builder.UseLowLatencyDurableWritePreset();
            else
                builder.UseWriteOptimizedPreset();
        });

        await using var bench = await BenchmarkDatabase.CreateAsync(options: options);

        if (scenario.AnalyzeBeforeRun)
        {
            await bench.Db.ExecuteAsync("INSERT INTO bench VALUES (0, 0, 'seed', 'Alpha')");
            await bench.Db.ExecuteAsync("ANALYZE bench");
        }

        InsertBatch? batch = scenario.RowsPerCommit > 1
            ? bench.Db.PrepareInsertBatch("bench", initialCapacity: scenario.RowsPerCommit)
            : null;
        var row = new DbValue[4];
        DbValue textValue = DbValue.FromText("durable_batch");
        DbValue categoryValue = DbValue.FromText("Alpha");

        async Task OperationAsync()
        {
            if (scenario.RowsPerCommit == 1)
            {
                int id = Interlocked.Increment(ref _nextId);
                await bench.Db.ExecuteAsync(
                    $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {id}, 'durable_batch', 'Alpha')");
                return;
            }

            batch!.Clear();
            await bench.Db.BeginTransactionAsync();
            for (int i = 0; i < scenario.RowsPerCommit; i++)
            {
                int id = Interlocked.Increment(ref _nextId);
                row[0] = DbValue.FromInteger(id);
                row[1] = DbValue.FromInteger(id);
                row[2] = textValue;
                row[3] = categoryValue;
                batch.AddRow(row);
            }

            await batch.ExecuteAsync();
            await bench.Db.CommitAsync();
        }

        TimeSpan warmupDuration = TimeSpan.FromSeconds(2);
        TimeSpan measuredDuration = TimeSpan.FromSeconds(10);

        var warmupEnd = DateTime.UtcNow + warmupDuration;
        while (DateTime.UtcNow < warmupEnd)
            await OperationAsync();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        bench.Db.ResetWalFlushDiagnostics();
        bench.Db.ResetCommitPathDiagnostics();

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
        var commitPathDiagnostics = bench.Db.GetCommitPathDiagnosticsSnapshot();
        double elapsedSeconds = totalSw.Elapsed.TotalSeconds <= 0 ? 0 : totalSw.Elapsed.TotalSeconds;
        double rowsPerSecond = elapsedSeconds <= 0 ? 0 : histogram.Count * (double)scenario.RowsPerCommit / elapsedSeconds;
        double commitsPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : (double)walDiagnostics.FlushedCommitCount / walDiagnostics.FlushCount;
        double kibPerFlush = walDiagnostics.FlushCount == 0
            ? 0
            : walDiagnostics.FlushedByteCount / (double)walDiagnostics.FlushCount / 1024.0;
        string presetName = scenario.UseLowLatencyPreset ? "LowLatencyDurableWritePreset" : "WriteOptimizedPreset";
        string commitSummary = CommitPathDiagnosticsFormatter.BuildSummary(commitPathDiagnostics);

        var result = new BenchmarkResult
        {
            Name = $"DurableSqlBatching_{scenario.Name}_10s",
            TotalOps = histogram.Count,
            ElapsedMs = totalSw.Elapsed.TotalMilliseconds,
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
                $"preset={presetName}, analyzed={scenario.AnalyzeBeforeRun}, rowsPerCommit={scenario.RowsPerCommit}, rowsPerSec={rowsPerSecond:F1}, flushes={walDiagnostics.FlushCount}, commitsPerFlush={commitsPerFlush:F2}, KiBPerFlush={kibPerFlush:F1}, batchWindowWaits={walDiagnostics.BatchWindowWaitCount}, batchWindowBypasses={walDiagnostics.BatchWindowThresholdBypassCount}, {commitSummary}",
        };

        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} commits/sec, rows/sec={rowsPerSecond:N0}, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
        return result;
    }

    private sealed record SqlBatchScenario(
        string Name,
        int RowsPerCommit,
        bool UseLowLatencyPreset,
        bool AnalyzeBeforeRun);
}

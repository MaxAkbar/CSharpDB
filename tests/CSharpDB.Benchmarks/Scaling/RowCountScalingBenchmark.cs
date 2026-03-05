using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Scaling;

/// <summary>
/// Measures how point lookup, insert, and full scan performance scale
/// with row count from 100 to 1M rows.
/// </summary>
public static class RowCountScalingBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (int targetRowCount in new[] { 100, 1_000, 10_000, 100_000 })
        {
            Console.WriteLine($"  Testing at {targetRowCount:N0} rows...");

            var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_scale_bench_{Guid.NewGuid():N}.db");
            var walPath = filePath + ".wal";

            try
            {
                await using var db = await Database.OpenAsync(filePath);
                db.PreferSyncPointLookups = true;
                await db.ExecuteAsync(
                    "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, data TEXT)");

                // Seed rows
                var rng = new Random(42);
                const int batchSize = 1000;
                for (int i = 0; i < targetRowCount; i += batchSize)
                {
                    await db.BeginTransactionAsync();
                    int end = Math.Min(i + batchSize, targetRowCount);
                    for (int j = i; j < end; j++)
                    {
                        var text = DataGenerator.RandomString(rng, 50);
                        await db.ExecuteAsync(
                            $"INSERT INTO t VALUES ({j}, {rng.Next()}, '{text}')");
                    }
                    await db.CommitAsync();
                }

                // Checkpoint to ensure clean state
                await db.CheckpointAsync();

                // --- Point Lookup Benchmark ---
                var lookupHist = new LatencyHistogram();
                var lookupRng = new Random(42);
                const int lookupWarmupIters = 1_000;
                const int minLookupIters = 2_000;
                const int maxLookupIters = 100_000;
                const double minLookupDurationMs = 250;

                for (int i = 0; i < lookupWarmupIters; i++)
                {
                    int id = lookupRng.Next(0, targetRowCount);
                    await using var warmup = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {id}");
                    await warmup.ToListAsync();
                }

                var lookupSw = Stopwatch.StartNew();
                int lookupIters = 0;
                while (lookupIters < maxLookupIters)
                {
                    int id = lookupRng.Next(0, targetRowCount);
                    var sw = Stopwatch.StartNew();
                    await using var result = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {id}");
                    await result.ToListAsync();
                    sw.Stop();
                    lookupHist.Record(sw.Elapsed.TotalMilliseconds);
                    lookupIters++;

                    if (lookupIters >= minLookupIters && lookupSw.Elapsed.TotalMilliseconds >= minLookupDurationMs)
                        break;
                }
                lookupSw.Stop();

                results.Add(BenchmarkResult.FromHistogram(
                    $"RowScale_{targetRowCount}_PointLookup", lookupHist, lookupSw.Elapsed.TotalMilliseconds));

                // --- Insert Benchmark ---
                var insertHist = new LatencyHistogram();
                var insertSw = Stopwatch.StartNew();
                int insertIters = 500;

                for (int i = 0; i < insertIters; i++)
                {
                    var sw = Stopwatch.StartNew();
                    await db.ExecuteAsync(
                        $"INSERT INTO t VALUES ({targetRowCount + i + 1_000_000}, {i}, 'scaling')");
                    sw.Stop();
                    insertHist.Record(sw.Elapsed.TotalMilliseconds);
                }
                insertSw.Stop();

                results.Add(BenchmarkResult.FromHistogram(
                    $"RowScale_{targetRowCount}_Insert", insertHist, insertSw.Elapsed.TotalMilliseconds));

                // Normalize scan phase against a checkpointed state to reduce WAL-related jitter.
                await db.CheckpointAsync();

                // --- Full Scan Benchmark (only for ≤ 100K rows) ---
                if (targetRowCount <= 100_000)
                {
                    var scanHist = new LatencyHistogram();
                    // Use COUNT(val) to force row visitation instead of metadata COUNT(*).
                    const string scanSql = "SELECT COUNT(val) FROM t";
                    int scanWarmupIters = targetRowCount switch
                    {
                        <= 1_000 => 8,
                        <= 10_000 => 6,
                        _ => 4
                    };
                    int minScanIters = targetRowCount switch
                    {
                        <= 1_000 => 120,
                        <= 10_000 => 80,
                        _ => 30
                    };
                    const int maxScanIters = 5_000_000;
                    double minScanDurationMs = targetRowCount switch
                    {
                        <= 1_000 => 300,
                        <= 10_000 => 500,
                        _ => 1_000
                    };

                    for (int i = 0; i < scanWarmupIters; i++)
                    {
                        await using var warmup = await db.ExecuteAsync(scanSql);
                        await warmup.ToListAsync();
                    }

                    // Reduce GC interference right before the measured scan window.
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GC.Collect();

                    var scanSw = Stopwatch.StartNew();
                    int scanIters = 0;

                    while (scanIters < maxScanIters)
                    {
                        var sw = Stopwatch.StartNew();
                        await using var result = await db.ExecuteAsync(scanSql);
                        await result.ToListAsync();
                        sw.Stop();
                        scanHist.Record(sw.Elapsed.TotalMilliseconds);
                        scanIters++;

                        if (scanSw.Elapsed.TotalMilliseconds >= minScanDurationMs && scanIters >= minScanIters)
                            break;
                    }
                    scanSw.Stop();

                    results.Add(BenchmarkResult.FromHistogram(
                        $"RowScale_{targetRowCount}_CountScan", scanHist, scanSw.Elapsed.TotalMilliseconds));
                }

                Console.WriteLine($"    Lookup: {results[^(targetRowCount <= 100_000 ? 3 : 2)].OpsPerSecond:N0} ops/sec, " +
                                  $"Insert: {results[^(targetRowCount <= 100_000 ? 2 : 1)].OpsPerSecond:N0} ops/sec");
            }
            finally
            {
                try { File.Delete(filePath); } catch { }
                try { File.Delete(walPath); } catch { }
            }
        }

        return results;
    }
}

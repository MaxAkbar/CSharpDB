using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Scaling;

/// <summary>
/// Measures point lookup latency at different B+tree depths.
/// Inserts increasing numbers of rows to force deeper trees,
/// then measures lookup performance at each depth.
/// </summary>
public static class BTreeDepthBenchmark
{
    // Approximate row counts for different B+tree depths (4KB pages, ~100-byte records):
    // Depth 1: ~40 rows (single leaf)
    // Depth 2: ~1,600 rows (root + leaves)
    // Depth 3: ~64,000 rows (root + interior + leaves)
    // Depth 4: ~2,500,000 rows (root + 2 interior levels + leaves)
    private static readonly (string Label, int RowCount)[] DepthTargets =
    [
        ("Depth2_1600rows", 1_600),
        ("Depth3_50Krows", 50_000),
        ("Depth3_100Krows", 100_000),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (var (label, targetRows) in DepthTargets)
        {
            Console.WriteLine($"  Testing {label} ({targetRows:N0} rows)...");

            var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_btree_bench_{Guid.NewGuid():N}.db");
            var walPath = filePath + ".wal";

            try
            {
                await using var db = await Database.OpenAsync(filePath);
                db.PreferSyncPointLookups = true;
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)");

                // Seed rows in batches
                const int batchSize = 2000;
                for (int i = 0; i < targetRows; i += batchSize)
                {
                    await db.BeginTransactionAsync();
                    int end = Math.Min(i + batchSize, targetRows);
                    for (int j = i; j < end; j++)
                    {
                        await db.ExecuteAsync($"INSERT INTO t VALUES ({j}, {j * 7})");
                    }
                    await db.CommitAsync();
                }

                // Checkpoint so reads go through B+tree, not WAL
                await db.CheckpointAsync();

                // Measure point lookup latency
                var hist = new LatencyHistogram();
                var rng = new Random(42);
                const int lookupCount = 1000;
                var totalSw = Stopwatch.StartNew();

                for (int i = 0; i < lookupCount; i++)
                {
                    int id = rng.Next(0, targetRows);
                    var sw = Stopwatch.StartNew();
                    await using var result = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {id}");
                    await result.ToListAsync();
                    sw.Stop();
                    hist.Record(sw.Elapsed.TotalMilliseconds);
                }
                totalSw.Stop();

                var benchResult = BenchmarkResult.FromHistogram(
                    $"BTreeDepth_{label}", hist, totalSw.Elapsed.TotalMilliseconds);
                results.Add(benchResult);

                Console.WriteLine($"    {label}: {benchResult.OpsPerSecond:N0} lookups/sec, " +
                                  $"P50={benchResult.P50Ms:F3}ms, P99={benchResult.P99Ms:F3}ms");
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

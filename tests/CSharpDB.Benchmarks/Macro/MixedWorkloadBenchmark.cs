using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Simulates a realistic mixed workload: 80% reads / 20% writes for a fixed duration.
/// Reports separate read and write latency distributions.
/// </summary>
public static class MixedWorkloadBenchmark
{
    private static int _idCounter = 1_000_000;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        await using var bench = await BenchmarkDatabase.CreateAsync(10_000);

        var rng = new Random(42);
        var readHistogram = new LatencyHistogram();
        var writeHistogram = new LatencyHistogram();

        // Warmup
        for (int i = 0; i < 100; i++)
        {
            if (rng.NextDouble() < 0.8)
            {
                int id = rng.Next(0, 10_000);
                await using var r = await bench.Db.ExecuteAsync($"SELECT * FROM bench WHERE id = {id}");
                await r.ToListAsync();
            }
            else
            {
                int id = Interlocked.Increment(ref _idCounter);
                await bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'mixed', 'Alpha')");
            }
        }

        // Measured run: 15 seconds
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var end = DateTime.UtcNow + TimeSpan.FromSeconds(15);

        while (DateTime.UtcNow < end)
        {
            var opSw = System.Diagnostics.Stopwatch.StartNew();

            if (rng.NextDouble() < 0.8)
            {
                // Read
                int id = rng.Next(0, 10_000);
                await using var r = await bench.Db.ExecuteAsync($"SELECT * FROM bench WHERE id = {id}");
                await r.ToListAsync();
                opSw.Stop();
                readHistogram.Record(opSw.Elapsed.TotalMilliseconds);
            }
            else
            {
                // Write
                int id = Interlocked.Increment(ref _idCounter);
                await bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'mixed', 'Beta')");
                opSw.Stop();
                writeHistogram.Record(opSw.Elapsed.TotalMilliseconds);
            }
        }

        sw.Stop();

        if (readHistogram.Count > 0)
        {
            var readResult = BenchmarkResult.FromHistogram("MixedWorkload_Reads_80pct", readHistogram, sw.Elapsed.TotalMilliseconds);
            results.Add(readResult);
            Console.WriteLine($"  Reads:  {readResult.OpsPerSecond:N0} ops/sec, P50={readResult.P50Ms:F3}ms, P99={readResult.P99Ms:F3}ms");
        }

        if (writeHistogram.Count > 0)
        {
            var writeResult = BenchmarkResult.FromHistogram("MixedWorkload_Writes_20pct", writeHistogram, sw.Elapsed.TotalMilliseconds);
            results.Add(writeResult);
            Console.WriteLine($"  Writes: {writeResult.OpsPerSecond:N0} ops/sec, P50={writeResult.P50Ms:F3}ms, P99={writeResult.P99Ms:F3}ms");
        }

        return results;
    }
}

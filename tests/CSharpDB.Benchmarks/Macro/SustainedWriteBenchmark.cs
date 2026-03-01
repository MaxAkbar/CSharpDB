using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Inserts rows continuously for a fixed duration, measuring throughput over time
/// and detecting checkpoint-induced latency spikes.
/// </summary>
public static class SustainedWriteBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        // Test sustained single-row inserts for 15 seconds
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "SustainedWrite_SingleRow_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    int id = Environment.TickCount; // good enough for unique IDs
                    await bench.Db.ExecuteAsync(
                        $"INSERT INTO bench VALUES ({Interlocked.Increment(ref _idCounter)}, {id}, 'sustained', 'Alpha')");
                });
            results.Add(result);
        }

        // Test sustained batch inserts (100 per tx) for 15 seconds
        await using (var bench = await BenchmarkDatabase.CreateAsync())
        {
            var result = await MacroBenchmarkRunner.RunForDurationAsync(
                "SustainedWrite_Batch100_15s",
                warmupDuration: TimeSpan.FromSeconds(2),
                measuredDuration: TimeSpan.FromSeconds(15),
                async () =>
                {
                    await bench.Db.BeginTransactionAsync();
                    for (int i = 0; i < 100; i++)
                    {
                        int id = Interlocked.Increment(ref _idCounter);
                        await bench.Db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id}, 'batch', 'Beta')");
                    }
                    await bench.Db.CommitAsync();
                });
            results.Add(result);
        }

        return results;
    }

    private static int _idCounter = 0;
}

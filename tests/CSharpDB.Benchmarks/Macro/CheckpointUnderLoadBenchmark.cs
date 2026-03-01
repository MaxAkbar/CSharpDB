using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures how checkpoint duration and write latency are affected
/// when checkpoint is triggered at various WAL sizes.
/// </summary>
public static class CheckpointUnderLoadBenchmark
{
    private static int _idCounter = 1_000_000;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (int walFrameTarget in new[] { 100, 500, 1000, 2000 })
        {
            await using var bench = await BenchmarkDatabase.CreateAsync();

            // Insert rows to build up WAL (each auto-commit INSERT = ~1 WAL frame)
            var insertHist = new LatencyHistogram();
            var insertSw = Stopwatch.StartNew();

            for (int i = 0; i < walFrameTarget; i++)
            {
                var opSw = Stopwatch.StartNew();
                int id = Interlocked.Increment(ref _idCounter);
                await bench.Db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({id}, {id}, 'ckpt_load', 'Alpha')");
                opSw.Stop();
                insertHist.Record(opSw.Elapsed.TotalMilliseconds);
            }
            insertSw.Stop();

            var insertResult = BenchmarkResult.FromHistogram(
                $"CheckpointLoad_{walFrameTarget}frames_InsertLatency",
                insertHist, insertSw.Elapsed.TotalMilliseconds);
            results.Add(insertResult);

            // Measure checkpoint time
            var ckptSw = Stopwatch.StartNew();
            await bench.Db.CheckpointAsync();
            ckptSw.Stop();

            var ckptResult = new BenchmarkResult
            {
                Name = $"CheckpointLoad_{walFrameTarget}frames_CheckpointTime",
                TotalOps = 1,
                ElapsedMs = ckptSw.Elapsed.TotalMilliseconds,
                P50Ms = ckptSw.Elapsed.TotalMilliseconds,
                P90Ms = ckptSw.Elapsed.TotalMilliseconds,
                P95Ms = ckptSw.Elapsed.TotalMilliseconds,
                P99Ms = ckptSw.Elapsed.TotalMilliseconds,
                P999Ms = ckptSw.Elapsed.TotalMilliseconds,
                MinMs = ckptSw.Elapsed.TotalMilliseconds,
                MaxMs = ckptSw.Elapsed.TotalMilliseconds,
                MeanMs = ckptSw.Elapsed.TotalMilliseconds,
                StdDevMs = 0,
                ExtraInfo = $"WAL frames={walFrameTarget}"
            };
            results.Add(ckptResult);

            Console.WriteLine($"  {walFrameTarget} frames: insert P99={insertResult.P99Ms:F3}ms, " +
                              $"checkpoint={ckptSw.Elapsed.TotalMilliseconds:F2}ms");
        }

        return results;
    }
}

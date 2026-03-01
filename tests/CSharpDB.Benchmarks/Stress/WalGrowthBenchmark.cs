using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Stress;

/// <summary>
/// Measures how read performance degrades as the WAL grows without checkpointing,
/// then measures improvement after checkpoint.
/// </summary>
public static class WalGrowthBenchmark
{
    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        var filePath = Path.Combine(Path.GetTempPath(), $"csharpdb_walg_bench_{Guid.NewGuid():N}.db");
        var walPath = filePath + ".wal";

        try
        {
            await using var db = await Database.OpenAsync(filePath);

            await db.ExecuteAsync(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, data TEXT)");

            var rng = new Random(42);
            int nextId = 0;

            foreach (int targetFrames in new[] { 100, 1_000, 5_000, 10_000 })
            {
                // Insert rows to reach the target WAL frame count
                // Each auto-commit INSERT creates ~2 WAL frames (data page + page 0 header)
                int rowsNeeded = targetFrames / 2;
                int rowsToInsert = rowsNeeded - nextId;

                if (rowsToInsert > 0)
                {
                    const int batchSize = 500;
                    for (int i = 0; i < rowsToInsert; i += batchSize)
                    {
                        await db.BeginTransactionAsync();
                        int end = Math.Min(i + batchSize, rowsToInsert);
                        for (int j = i; j < end; j++)
                        {
                            var text = DataGenerator.RandomString(rng, 50);
                            await db.ExecuteAsync(
                                $"INSERT INTO t VALUES ({nextId}, {rng.Next()}, '{text}')");
                            nextId++;
                        }
                        await db.CommitAsync();
                    }
                }

                // Measure read latency at this WAL size
                var readHist = new LatencyHistogram();
                var lookupRng = new Random(123);

                for (int i = 0; i < 200; i++)
                {
                    int lookupId = lookupRng.Next(0, nextId);
                    var sw = Stopwatch.StartNew();
                    await using var result = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {lookupId}");
                    await result.ToListAsync();
                    sw.Stop();
                    readHist.Record(sw.Elapsed.TotalMilliseconds);
                }

                long walSize = File.Exists(walPath) ? new FileInfo(walPath).Length : 0;
                var readResult = BenchmarkResult.FromHistogram(
                    $"WalGrowth_{targetFrames}frames_ReadLatency", readHist, 0);
                readResult = new BenchmarkResult
                {
                    Name = readResult.Name,
                    TotalOps = readResult.TotalOps,
                    ElapsedMs = readResult.ElapsedMs,
                    P50Ms = readResult.P50Ms,
                    P90Ms = readResult.P90Ms,
                    P95Ms = readResult.P95Ms,
                    P99Ms = readResult.P99Ms,
                    P999Ms = readResult.P999Ms,
                    MinMs = readResult.MinMs,
                    MaxMs = readResult.MaxMs,
                    MeanMs = readResult.MeanMs,
                    StdDevMs = readResult.StdDevMs,
                    ExtraInfo = $"WALSize={walSize:N0}B, Rows={nextId}"
                };
                results.Add(readResult);

                Console.WriteLine($"  {targetFrames} frames (WAL={walSize:N0}B): " +
                                  $"read P50={readResult.P50Ms:F3}ms, P99={readResult.P99Ms:F3}ms");
            }

            // Checkpoint and measure post-checkpoint read latency
            var ckptSw = Stopwatch.StartNew();
            await db.CheckpointAsync();
            ckptSw.Stop();

            var postCkptHist = new LatencyHistogram();
            var postRng = new Random(123);
            for (int i = 0; i < 200; i++)
            {
                int lookupId = postRng.Next(0, nextId);
                var sw = Stopwatch.StartNew();
                await using var result = await db.ExecuteAsync($"SELECT * FROM t WHERE id = {lookupId}");
                await result.ToListAsync();
                sw.Stop();
                postCkptHist.Record(sw.Elapsed.TotalMilliseconds);
            }

            var postResult = BenchmarkResult.FromHistogram(
                "WalGrowth_PostCheckpoint_ReadLatency", postCkptHist, 0);
            postResult = new BenchmarkResult
            {
                Name = postResult.Name,
                TotalOps = postResult.TotalOps,
                ElapsedMs = postResult.ElapsedMs,
                P50Ms = postResult.P50Ms,
                P90Ms = postResult.P90Ms,
                P95Ms = postResult.P95Ms,
                P99Ms = postResult.P99Ms,
                P999Ms = postResult.P999Ms,
                MinMs = postResult.MinMs,
                MaxMs = postResult.MaxMs,
                MeanMs = postResult.MeanMs,
                StdDevMs = postResult.StdDevMs,
                ExtraInfo = $"CheckpointTime={ckptSw.Elapsed.TotalMilliseconds:F2}ms"
            };
            results.Add(postResult);

            Console.WriteLine($"  Post-checkpoint: read P50={postResult.P50Ms:F3}ms, " +
                              $"P99={postResult.P99Ms:F3}ms (checkpoint took {ckptSw.Elapsed.TotalMilliseconds:F2}ms)");
        }
        finally
        {
            try { File.Delete(filePath); } catch { }
            try { File.Delete(walPath); } catch { }
        }

        return results;
    }
}

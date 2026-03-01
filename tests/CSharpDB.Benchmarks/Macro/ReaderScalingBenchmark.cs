using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures how concurrent reader sessions scale alongside a continuous writer.
/// Tests with 1, 2, 4, and 8 reader sessions.
/// </summary>
public static class ReaderScalingBenchmark
{
    private static int _idCounter = 1_000_000;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (int readerCount in new[] { 1, 2, 4, 8 })
        {
            await using var bench = await BenchmarkDatabase.CreateAsync(5_000);

            var duration = TimeSpan.FromSeconds(10);
            var cts = new CancellationTokenSource(duration);

            // Writer task
            var writeHistogram = new LatencyHistogram();
            var writerTask = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    var sw = Stopwatch.StartNew();
                    int id = Interlocked.Increment(ref _idCounter);
                    try
                    {
                        await bench.Db.ExecuteAsync(
                            $"INSERT INTO bench VALUES ({id}, {id}, 'scaling', 'Alpha')");
                    }
                    catch (OperationCanceledException) { break; }
                    catch { /* ignore busy errors during scaling test */ }
                    sw.Stop();
                    writeHistogram.Record(sw.Elapsed.TotalMilliseconds);
                }
            });

            // Reader tasks
            var readerHistograms = new LatencyHistogram[readerCount];
            var readerTasks = new Task[readerCount];

            for (int r = 0; r < readerCount; r++)
            {
                readerHistograms[r] = new LatencyHistogram();
                var hist = readerHistograms[r];

                readerTasks[r] = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var sw = Stopwatch.StartNew();
                        try
                        {
                            using var reader = bench.Db.CreateReaderSession();
                            await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench");
                            await result.ToListAsync();
                        }
                        catch (OperationCanceledException) { break; }
                        catch { /* ignore transient errors */ }
                        sw.Stop();
                        hist.Record(sw.Elapsed.TotalMilliseconds);
                    }
                });
            }

            await Task.WhenAll(new[] { writerTask }.Concat(readerTasks));

            var totalSw = duration.TotalMilliseconds;

            var writeResult = BenchmarkResult.FromHistogram(
                $"ReaderScaling_{readerCount}readers_Writer", writeHistogram, totalSw);
            results.Add(writeResult);
            Console.WriteLine($"  {readerCount} readers - Writer: {writeResult.OpsPerSecond:N0} ops/sec, P99={writeResult.P99Ms:F3}ms");

            // Aggregate reader results
            var combinedReaderHist = new LatencyHistogram();
            foreach (var h in readerHistograms)
            {
                // Merge all reader samples into one histogram
                // (simple approach: just report totals)
            }

            int totalReaderOps = readerHistograms.Sum(h => h.Count);
            double avgReaderP50 = readerHistograms.Average(h => h.Percentile(0.50));
            double avgReaderP99 = readerHistograms.Average(h => h.Percentile(0.99));

            var readerResult = new BenchmarkResult
            {
                Name = $"ReaderScaling_{readerCount}readers_Readers",
                TotalOps = totalReaderOps,
                ElapsedMs = totalSw,
                P50Ms = avgReaderP50,
                P90Ms = readerHistograms.Average(h => h.Percentile(0.90)),
                P95Ms = readerHistograms.Average(h => h.Percentile(0.95)),
                P99Ms = avgReaderP99,
                P999Ms = readerHistograms.Average(h => h.Percentile(0.999)),
                MinMs = readerHistograms.Min(h => h.Min),
                MaxMs = readerHistograms.Max(h => h.Max),
                MeanMs = readerHistograms.Average(h => h.Mean),
                StdDevMs = readerHistograms.Average(h => h.StdDev)
            };
            results.Add(readerResult);
            Console.WriteLine($"  {readerCount} readers - Total reader ops: {totalReaderOps:N0}, Avg P50={avgReaderP50:F3}ms, Avg P99={avgReaderP99:F3}ms");
        }

        return results;
    }
}

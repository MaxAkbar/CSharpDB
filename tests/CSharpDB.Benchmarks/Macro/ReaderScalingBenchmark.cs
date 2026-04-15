using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures how concurrent reader sessions scale alongside a continuous writer.
/// Tests with 1, 2, 4, and 8 reader sessions.
/// </summary>
public static class ReaderScalingBenchmark
{
    private const int ReusedSessionBurstReads = 32;
    private const int HighThroughputLatencySampleEvery = 128;
    private static int _idCounter = 1_000_000;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>();

        foreach (int readerCount in new[] { 1, 2, 4, 8 })
        {
            results.AddRange(await RunScenarioAsync(
                readerCount,
                sessionReadsPerSnapshot: 1,
                resultPrefix: $"ReaderScaling_{readerCount}readers",
                description: "per-query reader sessions"));

            results.AddRange(await RunScenarioAsync(
                readerCount,
                sessionReadsPerSnapshot: ReusedSessionBurstReads,
                resultPrefix: $"ReaderScalingBurst{ReusedSessionBurstReads}_{readerCount}readers",
                description: $"reader sessions reused for {ReusedSessionBurstReads} reads"));
        }

        return results;
    }

    private static async Task<List<BenchmarkResult>> RunScenarioAsync(
        int readerCount,
        int sessionReadsPerSnapshot,
        string resultPrefix,
        string description)
    {
        await using var bench = await BenchmarkDatabase.CreateAsync(5_000);

        var duration = TimeSpan.FromSeconds(10);
        var cts = new CancellationTokenSource(duration);

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

        var readerHistograms = new LatencyHistogram[readerCount];
        var readerTasks = new Task[readerCount];
        int latencySampleEvery = sessionReadsPerSnapshot > 1 ? HighThroughputLatencySampleEvery : 1;

        for (int r = 0; r < readerCount; r++)
        {
            readerHistograms[r] = new LatencyHistogram(latencySampleEvery);
            var hist = readerHistograms[r];

            readerTasks[r] = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    using var reader = bench.Db.CreateReaderSession();
                    for (int i = 0; i < sessionReadsPerSnapshot && !cts.Token.IsCancellationRequested; i++)
                    {
                        Stopwatch? sw = hist.ShouldSampleNext() ? Stopwatch.StartNew() : null;
                        try
                        {
                            await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench");
                            await result.ToListAsync();
                        }
                        catch (OperationCanceledException) { return; }
                        catch { /* ignore transient errors */ }

                        if (sw is null)
                        {
                            hist.RecordUnsampled();
                        }
                        else
                        {
                            sw.Stop();
                            hist.Record(sw.Elapsed.TotalMilliseconds);
                        }
                    }
                }
            });
        }

        await Task.WhenAll(new[] { writerTask }.Concat(readerTasks));

        var totalSw = duration.TotalMilliseconds;
        var results = new List<BenchmarkResult>(2);

        var writeResult = BenchmarkResult.FromHistogram(
            $"{resultPrefix}_Writer",
            writeHistogram,
            totalSw);
        results.Add(writeResult);
        Console.WriteLine($"  {readerCount} readers ({description}) - Writer: {writeResult.OpsPerSecond:N0} ops/sec, P99={writeResult.P99Ms:F3}ms");

        int totalReaderOps = readerHistograms.Sum(h => h.Count);
        double avgReaderP50 = readerHistograms.Average(h => h.Percentile(0.50));
        double avgReaderP99 = readerHistograms.Average(h => h.Percentile(0.99));

        var readerResult = new BenchmarkResult
        {
            Name = $"{resultPrefix}_Readers",
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
            StdDevMs = readerHistograms.Average(h => h.StdDev),
            ExtraInfo = sessionReadsPerSnapshot > 1
                ? $"session-mode=reused reader session; burst-reads={sessionReadsPerSnapshot}; readers={readerCount}; latency-sampling=1/{latencySampleEvery}"
                : $"session-mode=per-query reader session; readers={readerCount}"
        };
        results.Add(readerResult);
        Console.WriteLine($"  {readerCount} readers ({description}) - Total reader ops: {totalReaderOps:N0}, Avg P50={avgReaderP50:F3}ms, Avg P99={avgReaderP99:F3}ms");

        return results;
    }
}

using System.Diagnostics;

namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Custom benchmark harness for sustained workloads that need latency histograms
/// and throughput measurement over time — things BenchmarkDotNet doesn't handle well.
/// </summary>
public static class MacroBenchmarkRunner
{
    /// <summary>
    /// Run a benchmark with the given operation, collecting latency samples.
    /// </summary>
    /// <param name="name">Benchmark name for reporting.</param>
    /// <param name="warmupIterations">Number of warmup iterations (not measured).</param>
    /// <param name="measuredIterations">Number of measured iterations.</param>
    /// <param name="operation">The async operation to benchmark.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A BenchmarkResult with throughput and latency distribution.</returns>
    public static async Task<BenchmarkResult> RunAsync(
        string name,
        int warmupIterations,
        int measuredIterations,
        Func<Task> operation,
        CancellationToken ct = default)
    {
        // Warmup
        for (int i = 0; i < warmupIterations && !ct.IsCancellationRequested; i++)
        {
            await operation();
        }

        // Measured run
        var histogram = new LatencyHistogram();
        var totalSw = Stopwatch.StartNew();

        for (int i = 0; i < measuredIterations && !ct.IsCancellationRequested; i++)
        {
            var sw = Stopwatch.StartNew();
            await operation();
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        totalSw.Stop();

        var result = BenchmarkResult.FromHistogram(name, histogram, totalSw.Elapsed.TotalMilliseconds);
        Console.WriteLine($"  {name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, P999={result.P999Ms:F3}ms");
        return result;
    }

    /// <summary>
    /// Run a benchmark for a fixed duration, collecting latency samples.
    /// </summary>
    public static async Task<BenchmarkResult> RunForDurationAsync(
        string name,
        TimeSpan warmupDuration,
        TimeSpan measuredDuration,
        Func<Task> operation,
        CancellationToken ct = default)
    {
        // Warmup
        var warmupEnd = DateTime.UtcNow + warmupDuration;
        while (DateTime.UtcNow < warmupEnd && !ct.IsCancellationRequested)
        {
            await operation();
        }

        // Measured run
        var histogram = new LatencyHistogram();
        var totalSw = Stopwatch.StartNew();
        var measuredEnd = DateTime.UtcNow + measuredDuration;

        while (DateTime.UtcNow < measuredEnd && !ct.IsCancellationRequested)
        {
            var sw = Stopwatch.StartNew();
            await operation();
            sw.Stop();
            histogram.Record(sw.Elapsed.TotalMilliseconds);
        }

        totalSw.Stop();

        var result = BenchmarkResult.FromHistogram(name, histogram, totalSw.Elapsed.TotalMilliseconds);
        Console.WriteLine($"  {name}: {result.OpsPerSecond:N0} ops/sec, P50={result.P50Ms:F3}ms, P99={result.P99Ms:F3}ms, P999={result.P999Ms:F3}ms");
        return result;
    }
}

namespace CSharpDB.Benchmarks.Infrastructure;

/// <summary>
/// Represents the result of a macro-benchmark or stress test run.
/// </summary>
public sealed class BenchmarkResult
{
    public required string Name { get; init; }
    public int TotalOps { get; init; }
    public double ElapsedMs { get; init; }
    public double OpsPerSecond => TotalOps > 0 && ElapsedMs > 0
        ? TotalOps / (ElapsedMs / 1000.0)
        : 0;

    // Latency percentiles (ms)
    public double P50Ms { get; init; }
    public double P90Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double P999Ms { get; init; }
    public double MinMs { get; init; }
    public double MaxMs { get; init; }
    public double MeanMs { get; init; }
    public double StdDevMs { get; init; }

    /// <summary>
    /// Optional extra info (e.g., file sizes, amplification ratios).
    /// </summary>
    public string? ExtraInfo { get; init; }

    public static BenchmarkResult FromHistogram(string name, LatencyHistogram histogram, double elapsedMs)
    {
        return new BenchmarkResult
        {
            Name = name,
            TotalOps = histogram.Count,
            ElapsedMs = elapsedMs,
            P50Ms = histogram.Percentile(0.50),
            P90Ms = histogram.Percentile(0.90),
            P95Ms = histogram.Percentile(0.95),
            P99Ms = histogram.Percentile(0.99),
            P999Ms = histogram.Percentile(0.999),
            MinMs = histogram.Min,
            MaxMs = histogram.Max,
            MeanMs = histogram.Mean,
            StdDevMs = histogram.StdDev
        };
    }
}

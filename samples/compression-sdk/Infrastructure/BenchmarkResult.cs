namespace CSharpDB.Samples.CompressionSdk.Infrastructure;

public sealed class BenchmarkResult
{
    public required string Name { get; init; }
    public int TotalOps { get; init; }
    public double ElapsedMs { get; init; }
    public double OpsPerSecond => TotalOps > 0 && ElapsedMs > 0
        ? TotalOps / (ElapsedMs / 1000.0)
        : 0;

    public double P50Ms { get; init; }
    public double P90Ms { get; init; }
    public double P95Ms { get; init; }
    public double P99Ms { get; init; }
    public double P999Ms { get; init; }
    public double MinMs { get; init; }
    public double MaxMs { get; init; }
    public double MeanMs { get; init; }
    public double StdDevMs { get; init; }
    public string? ExtraInfo { get; init; }
}

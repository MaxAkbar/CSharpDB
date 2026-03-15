using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures generic scan paths that need both filtering and projection.
/// These shapes exercise the broader row-by-row executor path rather than
/// index-only or lookup-specific planner shortcuts.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ScanProjectionBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Filtered scan + column projection (20% selectivity)")]
    public async Task FilteredColumnProjection_20Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, category FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Filtered scan + expression projection (20% selectivity)")]
    public async Task FilteredExpressionProjection_20Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, value + id FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }
}

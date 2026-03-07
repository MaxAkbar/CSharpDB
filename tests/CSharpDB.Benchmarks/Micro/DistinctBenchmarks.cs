using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures row-level DISTINCT overhead vs equivalent non-DISTINCT queries.
/// Includes a TOP-N sort shape where DISTINCT changes planner behavior.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class DistinctBenchmarks
{
    [Params(1_000, 10_000)]
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

    [Benchmark(Baseline = true, Description = "Projection scan (value)")]
    public async Task Projection_Value()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT value FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT DISTINCT value")]
    public async Task Distinct_Value()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT DISTINCT value FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value + LIMIT 100")]
    public async Task OrderedTopN_Value()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT value FROM bench ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT DISTINCT value ORDER BY value + LIMIT 100")]
    public async Task DistinctOrderedTopN_Value()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT DISTINCT value FROM bench ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }
}

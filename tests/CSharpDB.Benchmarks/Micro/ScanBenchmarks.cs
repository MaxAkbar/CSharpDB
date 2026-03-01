using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scan performance: full table scan, filtered scans at various selectivity,
/// ordered scans, LIMIT, and aggregate queries.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ScanBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
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

    [Benchmark(Description = "Full table scan (SELECT *)")]
    public async Task FullTableScan()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT * FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Filtered scan ~20% selectivity")]
    public async Task FilteredScan_20Pct()
    {
        // value is 0..999999, so value < 200000 ≈ 20%
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Filtered scan ~1% selectivity")]
    public async Task FilteredScan_1Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE value < 10000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY (full sort)")]
    public async Task OrderedScan()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench ORDER BY value ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY expression (value + id)")]
    public async Task OrderedScan_Expression()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench ORDER BY value + id ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY + LIMIT 100 (top-N)")]
    public async Task OrderedScan_TopN()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT with LIMIT 100")]
    public async Task LimitedScan()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(*) aggregate")]
    public async Task AggregateCount()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY with COUNT + AVG")]
    public async Task AggregateGroupBy()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT category, COUNT(*), AVG(value) FROM bench GROUP BY category");
        await result.ToListAsync();
    }
}

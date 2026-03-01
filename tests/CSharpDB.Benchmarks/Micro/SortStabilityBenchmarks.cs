using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Higher-sample stability run for the 100k-row ORDER BY hot paths.
/// Use this to validate direction before moving to new optimization targets.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 4, iterationCount: 12)]
public class SortStabilityBenchmarks
{
    private const int RowCount = 100_000;
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

    [Benchmark(Description = "ORDER BY value (100k)")]
    public async Task OrderedScan_100k()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench ORDER BY value ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value + id (100k)")]
    public async Task OrderedScanExpression_100k()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench ORDER BY value + id ASC");
        await result.ToListAsync();
    }
}

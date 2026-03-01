using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scalar aggregate performance (no GROUP BY) and compares to a single-bucket GROUP BY baseline.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 6)]
public class ScalarAggregateBenchmarks
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

    [Benchmark(Description = "Scalar SUM(value)")]
    public async Task ScalarSum()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT SUM(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar AVG(value)")]
    public async Task ScalarAvg()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT AVG(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar COUNT(value)")]
    public async Task ScalarCountColumn()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar COUNT(DISTINCT value)")]
    public async Task ScalarCountDistinctValue()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(DISTINCT value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar SUM(DISTINCT value)")]
    public async Task ScalarSumDistinctValue()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT SUM(DISTINCT value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar MIN(value)")]
    public async Task ScalarMin()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT MIN(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Scalar MAX(value)")]
    public async Task ScalarMax()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT MAX(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Hash SUM(value) via GROUP BY 1")]
    public async Task HashSumSingleBucket()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT SUM(value) FROM bench GROUP BY 1");
        await result.ToListAsync();
    }
}

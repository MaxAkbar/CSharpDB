using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scalar aggregate fast paths that run directly on the INTEGER PRIMARY KEY B-tree key stream.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class PrimaryKeyAggregateBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private int _lowerBound;
    private int _upperBound;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
        _lowerBound = RowCount / 4;
        _upperBound = (RowCount * 3) / 4;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "MIN(id) via table key aggregate")]
    public async Task MinPrimaryKey()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT MIN(id) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MAX(id) via table key aggregate")]
    public async Task MaxPrimaryKey()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT MAX(id) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(id) via table key aggregate")]
    public async Task CountPrimaryKey()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(id) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(id) via table key aggregate")]
    public async Task SumPrimaryKey()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT SUM(id) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(*) WHERE id BETWEEN ... via table key aggregate")]
    public async Task CountPrimaryKeyRange()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT COUNT(*) FROM bench WHERE id BETWEEN {_lowerBound} AND {_upperBound}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(id) WHERE id BETWEEN ... via table key aggregate")]
    public async Task SumPrimaryKeyRange()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT SUM(id) FROM bench WHERE id BETWEEN {_lowerBound} AND {_upperBound}");
        await result.ToListAsync();
    }
}

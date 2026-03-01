using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures point lookup latency: by primary key, by indexed column,
/// by non-indexed column (full scan), and cache misses.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class PointLookupBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();

        // Create an index on the value column
        _bench.Db.ExecuteAsync("CREATE INDEX idx_value ON bench (value)")
            .AsTask().GetAwaiter().GetResult();

        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "SELECT by primary key")]
    public async Task SelectByPrimaryKey()
    {
        int id = _rng.Next(0, RowCount);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM bench WHERE id = {id}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT projected column by primary key")]
    public async Task SelectProjectedByPrimaryKey()
    {
        int id = _rng.Next(0, RowCount);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id FROM bench WHERE id = {id}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT by PK with residual conjunct")]
    public async Task SelectByPrimaryKeyWithResidualConjunct()
    {
        int id = _rng.Next(0, RowCount);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id FROM bench WHERE id = {id} AND category = 'Alpha'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT by indexed column")]
    public async Task SelectByIndexedColumn()
    {
        // Use a specific value — index lookup
        int val = _rng.Next(0, 1_000_000);
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM bench WHERE value = {val}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT by non-indexed column (full scan)")]
    public async Task SelectByNonIndexedColumn()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE category = 'Alpha'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT miss (non-existent key)")]
    public async Task SelectMiss()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM bench WHERE id = {RowCount + 999_999}");
        await result.ToListAsync();
    }
}

using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scalar aggregate performance on PK/index lookups.
/// Includes both numeric-only aggregates and text-column aggregates to highlight decode costs.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 8)]
public class ScalarAggregateLookupBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private int _targetId;
    private int _targetValue;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _bench = await BenchmarkDatabase.CreateAsync(RowCount);
        await _bench.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)");

        _targetId = 0;
        await using var valueResult = await _bench.Db.ExecuteAsync(
            "SELECT value FROM bench LIMIT 1");
        var rows = await valueResult.ToListAsync();
        _targetValue = (int)rows[0][0].AsInteger;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "PK lookup scalar SUM(value)")]
    public async Task PkScalarSumValue()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT SUM(value) FROM bench WHERE id = {_targetId}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "PK lookup scalar COUNT(text_col)")]
    public async Task PkScalarCountText()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT COUNT(text_col) FROM bench WHERE id = {_targetId}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Index lookup scalar SUM(id)")]
    public async Task IndexScalarSumId()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT SUM(id) FROM bench WHERE value = {_targetValue}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Index lookup scalar COUNT(text_col)")]
    public async Task IndexScalarCountText()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT COUNT(text_col) FROM bench WHERE value = {_targetValue}");
        await result.ToListAsync();
    }
}

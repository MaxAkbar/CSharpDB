using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scan shapes that benefit from pushing multiple simple conjuncts into encoded-row evaluation.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class PredicatePushdownBenchmarks
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

    [Benchmark(Description = "Single predicate (value < 200000)")]
    public async Task SinglePredicate_20Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compound range (10000 <= value < 20000)")]
    public async Task CompoundRange_1Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE value >= 10000 AND value < 20000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compound mixed (category = 'Alpha' AND value < 200000)")]
    public async Task CompoundMixed_4Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT * FROM bench WHERE category = 'Alpha' AND value < 200000");
        await result.ToListAsync();
    }
}

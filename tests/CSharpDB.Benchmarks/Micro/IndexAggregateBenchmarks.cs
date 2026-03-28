using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Compares scalar aggregates over an unindexed integer column to the new
/// direct integer index-key aggregate path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class IndexAggregateBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchWithIndex = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchNoIndex.Dispose();
        _benchWithIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "SUM(value) (no index)")]
    public async Task Sum_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT SUM(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(value) (direct index aggregate)")]
    public async Task Sum_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT SUM(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(value) (no index)")]
    public async Task CountColumn_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(value) (direct index aggregate)")]
    public async Task CountColumn_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MIN(value) (no index)")]
    public async Task Min_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT MIN(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MIN(value) (direct index aggregate)")]
    public async Task Min_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT MIN(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MAX(value) (no index)")]
    public async Task Max_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT MAX(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MAX(value) (direct index aggregate)")]
    public async Task Max_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT MAX(value) FROM bench");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(*) WHERE value BETWEEN ... (no index)")]
    public async Task CountStarRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(*) WHERE value BETWEEN ... (direct index aggregate)")]
    public async Task CountStarRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(*) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(value) WHERE value BETWEEN ... (no index)")]
    public async Task SumRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT SUM(value) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(value) WHERE value BETWEEN ... (direct index aggregate)")]
    public async Task SumRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT SUM(value) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(text_col) WHERE value BETWEEN ... (no index)")]
    public async Task CountTextRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(text_col) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task CountTextRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _benchNoIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        _benchWithIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)");
    }
}

using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Benchmarks payload-backed ordered range aggregates on row counts that stay above the
/// planner's current indexed-payload gate.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class PayloadRangeAggregateBenchmarks
{
    [Params(25_000, 100_000)]
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

    [Benchmark(Baseline = true, Description = "SUM(id) WHERE value BETWEEN ... (no index)")]
    public async Task SumIdRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT SUM(id) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(id) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task SumIdRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT SUM(id) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "AVG(id) WHERE value BETWEEN ... (no index)")]
    public async Task AvgIdRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT AVG(id) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "AVG(id) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task AvgIdRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT AVG(id) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MIN(text_col) WHERE value BETWEEN ... (no index)")]
    public async Task MinTextRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT MIN(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MIN(text_col) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task MinTextRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT MIN(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MAX(text_col) WHERE value BETWEEN ... (no index)")]
    public async Task MaxTextRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT MAX(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "MAX(text_col) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task MaxTextRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT MAX(text_col) FROM bench WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(DISTINCT category) WHERE value BETWEEN ... AND id >= ... (no index)")]
    public async Task CountDistinctCategoryRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(DISTINCT category) FROM bench WHERE value BETWEEN 250000 AND 750000 AND id >= 5000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(DISTINCT category) WHERE value BETWEEN ... AND id >= ... (payload-backed index aggregate)")]
    public async Task CountDistinctCategoryRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(DISTINCT category) FROM bench WHERE value BETWEEN 250000 AND 750000 AND id >= 5000");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _benchNoIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        _benchWithIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)");
    }
}

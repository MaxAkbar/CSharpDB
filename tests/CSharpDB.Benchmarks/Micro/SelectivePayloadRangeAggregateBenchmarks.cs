using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures the stats-driven selective ordered-range payload aggregate route on
/// smaller tables where the old size-only gate would have stayed on the table scan.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class SelectivePayloadRangeAggregateBenchmarks
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

    [Benchmark(Baseline = true, Description = "Selective SUM(id) WHERE value BETWEEN ... (no index)")]
    public async Task SelectiveSumIdRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT SUM(id) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective SUM(id) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task SelectiveSumIdRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT SUM(id) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective AVG(id) WHERE value BETWEEN ... (no index)")]
    public async Task SelectiveAvgIdRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT AVG(id) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective AVG(id) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task SelectiveAvgIdRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT AVG(id) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective COUNT(DISTINCT category) WHERE value BETWEEN ... AND id >= ... (no index)")]
    public async Task SelectiveCountDistinctCategoryRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            $"SELECT COUNT(DISTINCT category) FROM bench WHERE value BETWEEN 250000 AND 260000 AND id >= {RowCount / 2}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective COUNT(DISTINCT category) WHERE value BETWEEN ... AND id >= ... (payload-backed index aggregate)")]
    public async Task SelectiveCountDistinctCategoryRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            $"SELECT COUNT(DISTINCT category) FROM bench WHERE value BETWEEN 250000 AND 260000 AND id >= {RowCount / 2}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective COUNT(text_col) WHERE value BETWEEN ... (no index)")]
    public async Task SelectiveCountTextRange_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective COUNT(text_col) WHERE value BETWEEN ... (payload-backed index aggregate)")]
    public async Task SelectiveCountTextRange_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(text_col) FROM bench WHERE value BETWEEN 250000 AND 260000");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _benchNoIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        _benchWithIndex = await BenchmarkDatabase.CreateAsync(RowCount);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)");
        await _benchWithIndex.Db.ExecuteAsync("ANALYZE bench");
    }
}

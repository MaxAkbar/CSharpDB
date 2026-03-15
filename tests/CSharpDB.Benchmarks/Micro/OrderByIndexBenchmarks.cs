using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Compares ORDER BY on a non-indexed source vs an indexed source
/// where planner can stream rows in index order.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class OrderByIndexBenchmarks
{
    [Params(1_000, 10_000, 100_000)]
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

    [Benchmark(Baseline = true, Description = "ORDER BY value (no index)")]
    public async Task OrderedScan_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT * FROM bench_idx ORDER BY value ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value (index-order scan)")]
    public async Task OrderedScan_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT * FROM bench_idx ORDER BY value ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value (covered index-order scan)")]
    public async Task OrderedCoveredScan_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT id, value FROM bench_idx ORDER BY value ASC");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value + LIMIT 100 (no index)")]
    public async Task OrderedScan_NoIndex_TopN()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT * FROM bench_idx ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value + LIMIT 100 (index-order scan)")]
    public async Task OrderedScan_WithIndex_TopN()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT * FROM bench_idx ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "ORDER BY value + LIMIT 100 (covered index-order scan)")]
    public async Task OrderedCoveredScan_WithIndex_TopN()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT id, value FROM bench_idx ORDER BY value ASC LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Range scan WHERE value BETWEEN ... (row fetch)")]
    public async Task RangeScan_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT * FROM bench_idx WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Range scan WHERE value BETWEEN ... (covered projection)")]
    public async Task RangeCoveredScan_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT id, value FROM bench_idx WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Range scan WHERE value BETWEEN ... (compact row projection)")]
    public async Task RangeCompactProjection_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT id, category FROM bench_idx WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Range scan WHERE value BETWEEN ... (compact expression projection)")]
    public async Task RangeCompactExpressionProjection_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT id, value + id FROM bench_idx WHERE value BETWEEN 250000 AND 750000");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        const string createSql =
            "CREATE TABLE bench_idx (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, text_col TEXT, category TEXT)";

        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createSql);
        _benchWithIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createSql);

        await SeedBenchAsync(_benchNoIndex, RowCount);
        await SeedBenchAsync(_benchWithIndex, RowCount);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_idx_value ON bench_idx(value)");
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        var categories = new[] { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };
        var rng = new Random(42);

        await bench.SeedAsync("bench_idx", rowCount, id =>
        {
            var cat = categories[id % categories.Length];
            var text = DataGenerator.RandomString(rng, 50);
            return $"INSERT INTO bench_idx VALUES ({id}, {rng.Next(0, 1_000_000)}, '{text}', '{cat}')";
        });
    }
}

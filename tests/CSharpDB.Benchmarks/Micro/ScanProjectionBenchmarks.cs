using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures scan-heavy projection shapes that now route through two internal
/// batch lanes: compact table-scan plans and LIMIT-forced generic plans.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class ScanProjectionBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Compact scan batch plan: residual column projection")]
    public async Task CompactResidualColumnProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, category FROM bench WHERE category IS NOT NULL");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compact scan batch plan: expression projection (20% selectivity)")]
    public async Task CompactExpressionProjection_20Pct()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, value + id FROM bench WHERE value < 200000");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compact scan batch plan: IN column projection")]
    public async Task CompactInColumnProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, category FROM bench WHERE category IN ('Beta', 'Gamma')");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Compact scan batch plan: LIKE column projection")]
    public async Task CompactLikeColumnProjection()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "SELECT id, category FROM bench WHERE category LIKE 'B%'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: residual column projection + LIMIT")]
    public async Task GenericResidualColumnProjection_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, category FROM bench WHERE category IS NOT NULL LIMIT {RowCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: expression projection + LIMIT (20% selectivity)")]
    public async Task GenericExpressionProjection_20Pct_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, value + id FROM bench WHERE value < 200000 LIMIT {RowCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: IN expression projection + LIMIT")]
    public async Task GenericInExpressionProjection_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, value + id FROM bench WHERE category IN ('Beta', 'Gamma') LIMIT {RowCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Generic scan batch plan: NOT IN expression projection + LIMIT")]
    public async Task GenericNotInExpressionProjection_WithLimit()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id, value + id FROM bench WHERE category NOT IN ('Beta', 'Delta') LIMIT {RowCount}");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, text_col TEXT, category TEXT)");

        await SeedBenchAsync(_bench, RowCount);
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        var categories = new[] { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };
        var rng = new Random(42);

        await bench.SeedAsync("bench", rowCount, id =>
        {
            string categoryLiteral = (id % 5) == 0
                ? "NULL"
                : $"'{categories[id % categories.Length]}'";
            string text = DataGenerator.RandomString(rng, 50);
            return $"INSERT INTO bench VALUES ({id}, {rng.Next(0, 1_000_000)}, '{text}', {categoryLiteral})";
        });
    }
}

using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures the selective analyzed-range payload path for AVG(DISTINCT ...) over a duplicated
/// numeric payload column, which cannot use the direct key-aggregate path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class SelectiveDistinctAvgPayloadAggregateBenchmarks
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

    [Benchmark(Baseline = true, Description = "Selective AVG(DISTINCT metric) WHERE score BETWEEN ... AND id >= ... (no index)")]
    public async Task SelectiveAvgDistinctMetric_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            $"SELECT AVG(DISTINCT metric) FROM bench WHERE score BETWEEN 250000 AND 260000 AND id >= {RowCount / 2}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Selective AVG(DISTINCT metric) WHERE score BETWEEN ... AND id >= ... (payload-backed index aggregate)")]
    public async Task SelectiveAvgDistinctMetric_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            $"SELECT AVG(DISTINCT metric) FROM bench WHERE score BETWEEN 250000 AND 260000 AND id >= {RowCount / 2}");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, metric INTEGER NOT NULL)");
        _benchWithIndex = await BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, score INTEGER NOT NULL, metric INTEGER NOT NULL)");

        await SeedAsync(_benchNoIndex, RowCount);
        await SeedAsync(_benchWithIndex, RowCount);

        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_score ON bench(score)");
        await _benchWithIndex.Db.ExecuteAsync("ANALYZE bench");
    }

    private static Task SeedAsync(BenchmarkDatabase bench, int rowCount)
        => bench.SeedAsync("bench", rowCount, rowIndex =>
        {
            int id = rowIndex + 1;
            int score = id;
            int metric = (id % 4) * 10;
            return $"INSERT INTO bench VALUES ({id}, {score}, {metric})";
        });
}

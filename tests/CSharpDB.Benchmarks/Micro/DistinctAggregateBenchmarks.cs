using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Compares DISTINCT scalar aggregates over a duplicate-heavy integer column
/// to the direct integer index-key aggregate path.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class DistinctAggregateBenchmarks
{
    private const int DistinctValueCount = 1024;

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

    [Benchmark(Baseline = true, Description = "COUNT(DISTINCT value) (no index)")]
    public async Task CountDistinct_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT COUNT(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    [Benchmark(Description = "COUNT(DISTINCT value) (direct index aggregate)")]
    public async Task CountDistinct_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT COUNT(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(DISTINCT value) (no index)")]
    public async Task SumDistinct_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT SUM(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SUM(DISTINCT value) (direct index aggregate)")]
    public async Task SumDistinct_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT SUM(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    [Benchmark(Description = "AVG(DISTINCT value) (no index)")]
    public async Task AvgDistinct_NoIndex()
    {
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            "SELECT AVG(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    [Benchmark(Description = "AVG(DISTINCT value) (direct index aggregate)")]
    public async Task AvgDistinct_WithIndex()
    {
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            "SELECT AVG(DISTINCT value) FROM bench_distinct");
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        const string createTableSql = "CREATE TABLE bench_distinct (id INTEGER PRIMARY KEY, value INTEGER NOT NULL, payload TEXT)";
        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchWithIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);

        await SeedAsync(_benchNoIndex);
        await SeedAsync(_benchWithIndex);
        await _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_distinct_value ON bench_distinct(value)");
    }

    private async Task SeedAsync(BenchmarkDatabase bench)
    {
        await bench.SeedAsync("bench_distinct", RowCount, static rowIndex =>
        {
            int id = rowIndex + 1;
            int value = rowIndex % DistinctValueCount;
            return $"INSERT INTO bench_distinct VALUES ({id}, {value}, 'payload_{id}')";
        });
    }
}

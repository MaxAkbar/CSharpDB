using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CompositeGroupedIndexBenchmarks
{
    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _noIndex = null!;
    private BenchmarkDatabase _withCompositeIndex = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        const string createTableSql =
            "CREATE TABLE bench_comp_group (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL, payload TEXT)";

        _noIndex = BenchmarkDatabase.CreateWithSchemaAsync(createTableSql).GetAwaiter().GetResult();
        _withCompositeIndex = BenchmarkDatabase.CreateWithSchemaAsync(createTableSql).GetAwaiter().GetResult();

        SeedAsync(_noIndex, RowCount).GetAwaiter().GetResult();
        SeedAsync(_withCompositeIndex, RowCount).GetAwaiter().GetResult();

        _withCompositeIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_comp_group_ab ON bench_comp_group(a, b)")
            .GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _noIndex.Dispose();
        _withCompositeIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "GROUP BY a,b COUNT(*) (no index)")]
    public async Task GroupByCompositeCount_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*) FROM bench_comp_group GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a,b COUNT(*) (composite index)")]
    public async Task GroupByCompositeCount_CompositeIndex()
    {
        await using var result = await _withCompositeIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*) FROM bench_comp_group GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a,b COUNT+SUM+AVG (no index)")]
    public async Task GroupByCompositeCountSumAvg_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*), SUM(id), AVG(id) FROM bench_comp_group GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a,b COUNT+SUM+AVG (composite index)")]
    public async Task GroupByCompositeCountSumAvg_CompositeIndex()
    {
        await using var result = await _withCompositeIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*), SUM(id), AVG(id) FROM bench_comp_group GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a,b WHERE a BETWEEN COUNT+SUM+AVG (no index)")]
    public async Task GroupByCompositeCountSumAvg_Filtered_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*), SUM(id), AVG(id) " +
            "FROM bench_comp_group WHERE a BETWEEN 25 AND 74 GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a,b WHERE a BETWEEN COUNT+SUM+AVG (composite index)")]
    public async Task GroupByCompositeCountSumAvg_Filtered_CompositeIndex()
    {
        await using var result = await _withCompositeIndex.Db.ExecuteAsync(
            "SELECT a, b, COUNT(*), SUM(id), AVG(id) " +
            "FROM bench_comp_group WHERE a BETWEEN 25 AND 74 GROUP BY a, b");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a COUNT(*) (no index)")]
    public async Task GroupByPrefixCount_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT a, COUNT(*) FROM bench_comp_group GROUP BY a");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY a COUNT(*) (composite index prefix)")]
    public async Task GroupByPrefixCount_CompositeIndex()
    {
        await using var result = await _withCompositeIndex.Db.ExecuteAsync(
            "SELECT a, COUNT(*) FROM bench_comp_group GROUP BY a");
        await result.ToListAsync();
    }

    private static async Task SeedAsync(BenchmarkDatabase bench, int rowCount)
    {
        await bench.SeedAsync("bench_comp_group", rowCount, id =>
        {
            int a = id % 100;
            int b = (id / 100) % 50;
            return $"INSERT INTO bench_comp_group VALUES ({id}, {a}, {b}, 'payload_{id}')";
        });
    }
}

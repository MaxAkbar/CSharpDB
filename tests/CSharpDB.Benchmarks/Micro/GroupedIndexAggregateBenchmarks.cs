using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class GroupedIndexAggregateBenchmarks
{
    private const int GroupModulo = 1000;

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _noIndex = null!;
    private BenchmarkDatabase _withIndex = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _noIndex = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, payload TEXT)")
            .GetAwaiter().GetResult();
        _withIndex = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, payload TEXT)")
            .GetAwaiter().GetResult();

        _noIndex.SeedAsync("bench", RowCount, id =>
            $"INSERT INTO bench VALUES ({id}, {id % GroupModulo}, 'payload_{id}')").GetAwaiter().GetResult();
        _withIndex.SeedAsync("bench", RowCount, id =>
            $"INSERT INTO bench VALUES ({id}, {id % GroupModulo}, 'payload_{id}')").GetAwaiter().GetResult();

        _withIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_group_id ON bench(group_id)").GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _noIndex.Dispose();
        _withIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "GROUP BY group_id COUNT(*) (no index)")]
    public async Task GroupByCount_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id COUNT(*) (direct index aggregate)")]
    public async Task GroupByCount_DirectIndexAggregate()
    {
        await using var result = await _withIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id COUNT+SUM+AVG (no index)")]
    public async Task GroupByCountSumAvg_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id) FROM bench GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id COUNT+SUM+AVG (direct index aggregate)")]
    public async Task GroupByCountSumAvg_DirectIndexAggregate()
    {
        await using var result = await _withIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id) FROM bench GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id WHERE BETWEEN COUNT(*) (no index)")]
    public async Task GroupByCount_Range_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench WHERE group_id BETWEEN 250 AND 749 GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id WHERE BETWEEN COUNT(*) (direct index aggregate)")]
    public async Task GroupByCount_Range_DirectIndexAggregate()
    {
        await using var result = await _withIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench WHERE group_id BETWEEN 250 AND 749 GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id ORDER BY group_id LIMIT 100 (no index)")]
    public async Task GroupByCount_OrderLimit_NoIndex()
    {
        await using var result = await _noIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench GROUP BY group_id ORDER BY group_id LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id ORDER BY group_id LIMIT 100 (direct index aggregate)")]
    public async Task GroupByCount_OrderLimit_DirectIndexAggregate()
    {
        await using var result = await _withIndex.Db.ExecuteAsync(
            "SELECT group_id, COUNT(*) FROM bench GROUP BY group_id ORDER BY group_id LIMIT 100");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id WHERE = HAVING COUNT(*) (no index)")]
    public async Task GroupByCount_EqualityHaving_NoIndex()
    {
        int expectedCount = RowCount / GroupModulo;
        await using var result = await _noIndex.Db.ExecuteAsync(
            $"SELECT group_id, COUNT(*) FROM bench WHERE group_id = 250 GROUP BY group_id HAVING COUNT(*) >= {expectedCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id WHERE = HAVING COUNT(*) (direct index aggregate)")]
    public async Task GroupByCount_EqualityHaving_DirectIndexAggregate()
    {
        int expectedCount = RowCount / GroupModulo;
        await using var result = await _withIndex.Db.ExecuteAsync(
            $"SELECT group_id, COUNT(*) FROM bench WHERE group_id = 250 GROUP BY group_id HAVING COUNT(*) >= {expectedCount}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id HAVING COUNT+SUM+AVG (no index)")]
    public async Task GroupByCountSumAvg_Having_NoIndex()
    {
        int averageThreshold = RowCount / 2;
        await using var result = await _noIndex.Db.ExecuteAsync(
            $"SELECT group_id, COUNT(*), SUM(id), AVG(id) " +
            $"FROM bench GROUP BY group_id HAVING AVG(id) >= {averageThreshold}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id HAVING COUNT+SUM+AVG (direct index aggregate)")]
    public async Task GroupByCountSumAvg_Having_DirectIndexAggregate()
    {
        int averageThreshold = RowCount / 2;
        await using var result = await _withIndex.Db.ExecuteAsync(
            $"SELECT group_id, COUNT(*), SUM(id), AVG(id) " +
            $"FROM bench GROUP BY group_id HAVING AVG(id) >= {averageThreshold}");
        await result.ToListAsync();
    }
}

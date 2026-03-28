using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class GroupedDistinctAggregateBenchmarks
{
    private const int GroupModulo = 1000;
    private const int DistinctCycle = 50;

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _db = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _db = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, group_id INTEGER NOT NULL, metric INTEGER)")
            .GetAwaiter().GetResult();

        _db.SeedAsync(
            "bench",
            RowCount,
            id =>
            {
                int groupId = id % GroupModulo;
                int metric = (id / GroupModulo) % DistinctCycle;
                return $"INSERT INTO bench VALUES ({id}, {groupId}, {metric})";
            }).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _db.Dispose();

    [Benchmark(Baseline = true, Description = "GROUP BY group_id COUNT(DISTINCT)+AVG(DISTINCT)")]
    public async Task GroupByDistinctCountAvg()
    {
        await using var result = await _db.Db.ExecuteAsync(
            "SELECT group_id, COUNT(DISTINCT metric), AVG(DISTINCT metric) FROM bench GROUP BY group_id");
        await result.ToListAsync();
    }

    [Benchmark(Description = "GROUP BY group_id WHERE BETWEEN COUNT(DISTINCT)+AVG(DISTINCT)")]
    public async Task GroupByDistinctCountAvg_Filtered()
    {
        await using var result = await _db.Db.ExecuteAsync(
            "SELECT group_id, COUNT(DISTINCT metric), AVG(DISTINCT metric) " +
            "FROM bench WHERE metric BETWEEN 10 AND 35 GROUP BY group_id");
        await result.ToListAsync();
    }
}

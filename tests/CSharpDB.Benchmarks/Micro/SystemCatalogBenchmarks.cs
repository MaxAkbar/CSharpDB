using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures system-catalog query and metadata API overhead under varying schema sizes.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SystemCatalogBenchmarks
{
    [Params(10, 100)]
    public int TableCount { get; set; }

    private BenchmarkDatabase _bench = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync("CREATE TABLE seed (id INTEGER PRIMARY KEY, v INTEGER)");
        await _bench.Db.ExecuteAsync("CREATE TABLE audit (id INTEGER)");
        await _bench.Db.ExecuteAsync("CREATE TABLE planner_diag (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, v INTEGER)");
        await _bench.Db.ExecuteAsync("CREATE INDEX idx_planner_diag_ab ON planner_diag(a, b)");

        for (int i = 0; i < TableCount; i++)
        {
            await _bench.Db.ExecuteAsync($"CREATE TABLE t{i} (id INTEGER PRIMARY KEY, v INTEGER)");
            await _bench.Db.ExecuteAsync($"CREATE INDEX idx_t{i}_v ON t{i}(v)");
        }

        await _bench.Db.BeginTransactionAsync();
        for (int i = 1; i <= 200; i++)
        {
            int a = i % 5;
            int b = i % 10;
            int v = i <= 150 ? 1 : i;
            await _bench.Db.ExecuteAsync($"INSERT INTO planner_diag VALUES ({i}, {a}, {b}, {v})");
        }
        await _bench.Db.CommitAsync();
        await _bench.Db.ExecuteAsync("ANALYZE planner_diag");

        await _bench.Db.ExecuteAsync("CREATE VIEW v_seed AS SELECT id FROM seed");
        await _bench.Db.ExecuteAsync(
            "CREATE TRIGGER trg_seed AFTER INSERT ON seed BEGIN INSERT INTO audit VALUES (NEW.id); END");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.tables")]
    public async Task Sql_SysTablesCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.tables");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.columns")]
    public async Task Sql_SysColumnsCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.columns");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.indexes")]
    public async Task Sql_SysIndexesCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys_indexes");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.views")]
    public async Task Sql_SysViewsCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.views");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.triggers")]
    public async Task Sql_SysTriggersCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys_triggers");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.planner_histograms")]
    public async Task Sql_PlannerHistogramsCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.planner_histograms");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.planner_heavy_hitters")]
    public async Task Sql_PlannerHeavyHittersCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.planner_heavy_hitters");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: SELECT COUNT(*) FROM sys.planner_index_prefix_stats")]
    public async Task Sql_PlannerIndexPrefixStatsCount()
    {
        await using var result = await _bench.Db.ExecuteAsync("SELECT COUNT(*) FROM sys.planner_index_prefix_stats");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SQL: EXPLAIN ESTIMATE skewed lookup")]
    public async Task Sql_ExplainEstimateSkewedLookup()
    {
        await using var result = await _bench.Db.ExecuteAsync(
            "EXPLAIN ESTIMATE FOR SELECT * FROM planner_diag WHERE v = 1");
        await result.ToListAsync();
    }

    [Benchmark(Description = "API: GetIndexes().Count")]
    public int Api_GetIndexesCount()
    {
        return _bench.Db.GetIndexes().Count;
    }

    [Benchmark(Description = "API: GetViewNames().Count")]
    public int Api_GetViewNamesCount()
    {
        return _bench.Db.GetViewNames().Count;
    }

    [Benchmark(Description = "API: GetTriggers().Count")]
    public int Api_GetTriggersCount()
    {
        return _bench.Db.GetTriggers().Count;
    }
}

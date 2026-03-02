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

        for (int i = 0; i < TableCount; i++)
        {
            await _bench.Db.ExecuteAsync($"CREATE TABLE t{i} (id INTEGER PRIMARY KEY, v INTEGER)");
            await _bench.Db.ExecuteAsync($"CREATE INDEX idx_t{i}_v ON t{i}(v)");
        }

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

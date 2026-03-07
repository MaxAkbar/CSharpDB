using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Isolates query-plan reuse behavior for non-fast-path SELECT shapes.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class QueryPlanCacheBenchmarks
{
    [Params(10_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private string _stableSql = null!;
    private Statement _preParsedStable = null!;

    private string[] _varyingSql = null!;
    private int _varyingSqlIndex;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
        _bench.Db.PreferSyncPointLookups = false;
        _bench.Db.ExecuteAsync("CREATE INDEX idx_bench_value ON bench(value)").GetAwaiter().GetResult();
        _bench.Db.ResetSelectPlanCacheDiagnostics();

        _stableSql = "SELECT id, value FROM bench WHERE value >= 1000 ORDER BY value LIMIT 128";
        _preParsedStable = Parser.Parse(_stableSql);

        _varyingSql = BuildVaryingSqlSet(1024);
        _varyingSqlIndex = 0;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        var stats = _bench.Db.GetSelectPlanCacheDiagnostics();
        Console.WriteLine(
            $"Select plan cache stats: hits={stats.HitCount}, misses={stats.MissCount}, " +
            $"reclassifications={stats.ReclassificationCount}, stores={stats.StoreCount}, entries={stats.EntryCount}");
        _bench.Dispose();
    }

    [Benchmark(Description = "Stable SQL text (statement+plan cache hits)")]
    public async Task ExecuteStableSqlText()
    {
        await using var result = await _bench.Db.ExecuteAsync(_stableSql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Pre-parsed statement (plan cache hit)")]
    public async Task ExecutePreParsedStatement()
    {
        await using var result = await _bench.Db.ExecuteAsync(_preParsedStable);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Varying SQL text (limited plan reuse)")]
    public async Task ExecuteVaryingSqlText()
    {
        string sql = _varyingSql[_varyingSqlIndex];
        _varyingSqlIndex++;
        if (_varyingSqlIndex == _varyingSql.Length)
            _varyingSqlIndex = 0;

        await using var result = await _bench.Db.ExecuteAsync(sql);
        await result.ToListAsync();
    }

    private string[] BuildVaryingSqlSet(int count)
    {
        var sql = new string[count];
        for (int i = 0; i < count; i++)
        {
            string sp1 = new(' ', 1 + (i & 0x3));
            string sp2 = new(' ', 1 + ((i >> 2) & 0x3));
            string sp3 = new(' ', 1 + ((i >> 4) & 0x3));
            string sp4 = new(' ', 1 + ((i >> 6) & 0x3));
            string sp5 = new(' ', 1 + ((i >> 8) & 0x3));
            string sp6 = new(' ', 1 + ((i >> 10) & 0x3));
            string sp7 = new(' ', 1 + ((i >> 12) & 0x3));

            sql[i] =
                $"SELECT{sp1}id,{sp2}value FROM{sp3}bench WHERE{sp4}value >={sp5}1000 ORDER BY{sp6}value LIMIT{sp7}128";
        }

        return sql;
    }
}

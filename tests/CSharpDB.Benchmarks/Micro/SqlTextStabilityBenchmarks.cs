using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Isolates statement parsing/cache behavior for point lookups:
/// same SQL text, bounded varying text, cache-churning varying text,
/// and pre-parsed statement execution.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SqlTextStabilityBenchmarks
{
    [Params(10_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private string _stableSql = null!;
    private Statement _preParsedStable = null!;

    private string[] _varyingSqlWithinCache = null!;
    private string[] _varyingSqlBeyondCache = null!;
    private int _withinCacheIndex;
    private int _beyondCacheIndex;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(RowCount).GetAwaiter().GetResult();
        _bench.Db.PreferSyncPointLookups = false;

        int stableId = RowCount / 2;
        _stableSql = $"SELECT id FROM bench WHERE id = {stableId}";
        _preParsedStable = Parser.Parse(_stableSql);

        _varyingSqlWithinCache = BuildSqlSet(128);
        _varyingSqlBeyondCache = BuildSqlSet(2048);
        _withinCacheIndex = 0;
        _beyondCacheIndex = 0;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Stable SQL text (statement cache hit)")]
    public async Task ExecuteStableSqlText()
    {
        await using var result = await _bench.Db.ExecuteAsync(_stableSql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Varying SQL (<= cache capacity)")]
    public async Task ExecuteVaryingSqlWithinCacheCapacity()
    {
        string sql = _varyingSqlWithinCache[_withinCacheIndex];
        _withinCacheIndex++;
        if (_withinCacheIndex == _varyingSqlWithinCache.Length)
            _withinCacheIndex = 0;

        await using var result = await _bench.Db.ExecuteAsync(sql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Varying SQL (> cache capacity)")]
    public async Task ExecuteVaryingSqlBeyondCacheCapacity()
    {
        string sql = _varyingSqlBeyondCache[_beyondCacheIndex];
        _beyondCacheIndex++;
        if (_beyondCacheIndex == _varyingSqlBeyondCache.Length)
            _beyondCacheIndex = 0;

        await using var result = await _bench.Db.ExecuteAsync(sql);
        await result.ToListAsync();
    }

    [Benchmark(Description = "Pre-parsed statement (parse bypass)")]
    public async Task ExecutePreParsedStatement()
    {
        await using var result = await _bench.Db.ExecuteAsync(_preParsedStable);
        await result.ToListAsync();
    }

    private string[] BuildSqlSet(int count)
    {
        var sql = new string[count];
        for (int i = 0; i < count; i++)
        {
            int id = i % RowCount;
            sql[i] = $"SELECT id FROM bench WHERE id = {id}";
        }

        return sql;
    }
}

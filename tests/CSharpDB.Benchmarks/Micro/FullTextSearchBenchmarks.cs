using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures full-text index build cost and steady-state search latency
/// over a deterministic SQL document corpus.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class FullTextSearchBenchmarks
{
    [Params(10_000, 50_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private int _sink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => _bench.Dispose();

    [Benchmark(Baseline = true, Description = "FTS search common term")]
    public async Task<int> SearchCommonTerm()
        => await ExecuteSearchAsync(FullTextBenchmarkData.CommonQuery);

    [Benchmark(Description = "FTS search medium intersection")]
    public async Task<int> SearchMediumIntersection()
        => await ExecuteSearchAsync(FullTextBenchmarkData.IntersectionQuery);

    [Benchmark(Description = "FTS search rare term")]
    public async Task<int> SearchRareTerm()
        => await ExecuteSearchAsync(FullTextBenchmarkData.RareQuery);

    private async Task GlobalSetupAsync()
    {
        _bench = await BenchmarkDatabase.CreateWithSchemaAsync(FullTextBenchmarkData.CreateTableSql);
        await FullTextBenchmarkData.SeedAsync(_bench, FullTextBenchmarkData.TableName, RowCount);
        await _bench.Db.EnsureFullTextIndexAsync(
            FullTextBenchmarkData.IndexName,
            FullTextBenchmarkData.TableName,
            [FullTextBenchmarkData.TitleColumn, FullTextBenchmarkData.BodyColumn]);
    }

    private async Task<int> ExecuteSearchAsync(string query)
    {
        var hits = await _bench.Db.SearchAsync(FullTextBenchmarkData.IndexName, query);
        int count = hits.Count;
        if (count > 0)
            _sink ^= (int)hits[0].RowId;
        _sink ^= count;
        return count;
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 5)]
public class FullTextIndexBuildBenchmarks
{
    [Params(5_000, 20_000)]
    public int RowCount { get; set; }

    [Benchmark(Description = "FTS ensure-index backfill build")]
    public async Task<int> EnsureFullTextIndexBuild()
    {
        await using var bench = await BenchmarkDatabase.CreateWithSchemaAsync(FullTextBenchmarkData.CreateTableSql);
        await FullTextBenchmarkData.SeedAsync(bench, FullTextBenchmarkData.TableName, RowCount);
        await bench.Db.EnsureFullTextIndexAsync(
            FullTextBenchmarkData.IndexName,
            FullTextBenchmarkData.TableName,
            [FullTextBenchmarkData.TitleColumn, FullTextBenchmarkData.BodyColumn]);

        var hits = await bench.Db.SearchAsync(FullTextBenchmarkData.IndexName, FullTextBenchmarkData.RareQuery);
        return hits.Count;
    }
}

internal static class FullTextBenchmarkData
{
    public const string TableName = "bench_fts";
    public const string IndexName = "fts_bench_fts";
    public const string TitleColumn = "title";
    public const string BodyColumn = "body";
    public const string CommonQuery = "alpha";
    public const string IntersectionQuery = "alpha burst";
    public const string RareQuery = "raretermcentral";
    public const string CreateTableSql =
        "CREATE TABLE bench_fts (id INTEGER PRIMARY KEY, title TEXT, body TEXT)";

    public static Task SeedAsync(BenchmarkDatabase bench, string tableName, int rowCount)
    {
        return bench.SeedAsync(tableName, rowCount, id =>
        {
            string focus = (id & 1) == 0 ? "alpha" : "omega";
            string cadence = id % 10 == 0 ? "burst" : "steady";
            string category = $"group{id % 20:D2}";
            string segment = $"segment{id % 100:D2}";
            string rare = id == rowCount / 2 ? RareQuery : $"bucket{id % 1000:D4}";
            string title = $"{focus} {category} doc{id:D5}";
            string body = $"{cadence} signal {category} {segment} {rare} payload{id:D5}";
            return $"INSERT INTO {tableName} VALUES ({id}, '{title}', '{body}')";
        });
    }
}

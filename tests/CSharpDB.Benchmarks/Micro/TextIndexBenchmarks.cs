using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures TEXT equality lookup performance with and without an index.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class TextIndexBenchmarks
{
    private const int DistinctNameCount = 1_000;
    private const int TargetNameBucket = 123;

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchTextIndex = null!;
    private string _lookupSql = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        GlobalSetupAsync().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchNoIndex.Dispose();
        _benchTextIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "WHERE text eq (no index)")]
    public async Task LookupNoIndex()
    {
        await ExecuteLookupAsync(_benchNoIndex);
    }

    [Benchmark(Description = "WHERE text eq (text index)")]
    public async Task LookupTextIndex()
    {
        await ExecuteLookupAsync(_benchTextIndex);
    }

    private async Task ExecuteLookupAsync(BenchmarkDatabase bench)
    {
        await using var result = await bench.Db.ExecuteAsync(_lookupSql);
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        const string createTableSql =
            "CREATE TABLE bench_text_idx (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)";

        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchTextIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);

        await SeedBenchAsync(_benchNoIndex, RowCount);
        await SeedBenchAsync(_benchTextIndex, RowCount);

        await _benchTextIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_text_name ON bench_text_idx(name)");

        _lookupSql = $"SELECT id FROM bench_text_idx WHERE name = 'name_{TargetNameBucket:D4}'";
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        await bench.SeedAsync("bench_text_idx", rowCount, id =>
        {
            string name = $"name_{id % DistinctNameCount:D4}";
            return $"INSERT INTO bench_text_idx VALUES ({id}, '{name}', 'payload_{id}')";
        });
    }
}

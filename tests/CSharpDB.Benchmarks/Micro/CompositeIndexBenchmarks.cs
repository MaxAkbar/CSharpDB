using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures two-predicate equality lookups against no index, a single-column index,
/// and a composite index over the predicate columns.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CompositeIndexBenchmarks
{
    private const int TargetA = 123;
    private const int TargetB = 10;

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchSingleIndex = null!;
    private BenchmarkDatabase _benchCompositeIndex = null!;
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
        _benchSingleIndex.Dispose();
        _benchCompositeIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "WHERE a+b (no index)")]
    public async Task LookupNoIndex()
    {
        await ExecuteLookupAsync(_benchNoIndex);
    }

    [Benchmark(Description = "WHERE a+b (single-column index)")]
    public async Task LookupSingleColumnIndex()
    {
        await ExecuteLookupAsync(_benchSingleIndex);
    }

    [Benchmark(Description = "WHERE a+b (composite index)")]
    public async Task LookupCompositeIndex()
    {
        await ExecuteLookupAsync(_benchCompositeIndex);
    }

    private async Task ExecuteLookupAsync(BenchmarkDatabase bench)
    {
        await using var result = await bench.Db.ExecuteAsync(_lookupSql);
        await result.ToListAsync();
    }

    private async Task GlobalSetupAsync()
    {
        const string createTableSql =
            "CREATE TABLE bench_comp (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, payload TEXT)";

        _benchNoIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchSingleIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);
        _benchCompositeIndex = await BenchmarkDatabase.CreateWithSchemaAsync(createTableSql);

        await SeedBenchAsync(_benchNoIndex, RowCount);
        await SeedBenchAsync(_benchSingleIndex, RowCount);
        await SeedBenchAsync(_benchCompositeIndex, RowCount);

        await _benchSingleIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_comp_a ON bench_comp(a)");
        await _benchCompositeIndex.Db.ExecuteAsync("CREATE INDEX idx_bench_comp_ab ON bench_comp(a, b)");

        // Ensures each run executes the same logical lookup shape.
        _lookupSql = $"SELECT * FROM bench_comp WHERE a = {TargetA} AND b = {TargetB}";
    }

    private static async Task SeedBenchAsync(BenchmarkDatabase bench, int rowCount)
    {
        await bench.SeedAsync("bench_comp", rowCount, id =>
        {
            int a = id % 500;
            int b = (id / 500) % 500;
            return $"INSERT INTO bench_comp VALUES ({id}, {a}, {b}, 'payload_{id}')";
        });
    }
}

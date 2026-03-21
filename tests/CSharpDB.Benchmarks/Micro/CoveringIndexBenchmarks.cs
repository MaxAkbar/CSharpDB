using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures the gap between unique-index lookup shapes that could be answered
/// without touching the base row and shapes that still need the wide table payload.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CoveringIndexBenchmarks
{
    private static readonly string s_largePayload = new('x', 1024);

    [Params(10_000, 100_000)]
    public int RowCount { get; set; }

    private BenchmarkDatabase _bench = null!;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_cover (id INTEGER PRIMARY KEY, lookup_key TEXT, small_value INTEGER, payload TEXT)")
            .GetAwaiter().GetResult();

        _bench.SeedAsync(
            "t_cover",
            RowCount,
            i => $"INSERT INTO t_cover VALUES ({i}, 'key_{i}', {i % 1000}, '{s_largePayload}')")
            .GetAwaiter().GetResult();

        _bench.Db.ExecuteAsync("CREATE UNIQUE INDEX idx_t_cover_lookup ON t_cover (lookup_key)")
            .AsTask().GetAwaiter().GetResult();

        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Unique-index lookup SELECT *")]
    public async Task SelectStar_ByUniqueIndex()
    {
        string key = $"key_{_rng.Next(0, RowCount)}";
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT * FROM t_cover WHERE lookup_key = '{key}'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Unique-index lookup SELECT id")]
    public async Task SelectRowId_ByUniqueIndex()
    {
        string key = $"key_{_rng.Next(0, RowCount)}";
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT id FROM t_cover WHERE lookup_key = '{key}'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Unique-index lookup SELECT lookup_key")]
    public async Task SelectIndexedKey_ByUniqueIndex()
    {
        string key = $"key_{_rng.Next(0, RowCount)}";
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT lookup_key FROM t_cover WHERE lookup_key = '{key}'");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Unique-index lookup SELECT payload")]
    public async Task SelectWidePayload_ByUniqueIndex()
    {
        string key = $"key_{_rng.Next(0, RowCount)}";
        await using var result = await _bench.Db.ExecuteAsync(
            $"SELECT payload FROM t_cover WHERE lookup_key = '{key}'");
        await result.ToListAsync();
    }
}

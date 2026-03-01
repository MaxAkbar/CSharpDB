using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures the overhead of maintaining indexes on INSERT and the speedup on lookups.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class IndexBenchmarks
{
    private BenchmarkDatabase _benchNoIndex = null!;
    private BenchmarkDatabase _benchWithIndex = null!;
    private BenchmarkDatabase _benchWithUniqueIndex = null!;
    private int _nextId;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _rng = new Random(42);
        _nextId = 1_000_000;

        // Table without secondary index
        _benchNoIndex = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_noindex (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
            .GetAwaiter().GetResult();

        // Table with regular index
        _benchWithIndex = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_indexed (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
            .GetAwaiter().GetResult();
        _benchWithIndex.Db.ExecuteAsync("CREATE INDEX idx_val ON t_indexed (val)")
            .AsTask().GetAwaiter().GetResult();

        // Table with unique index
        _benchWithUniqueIndex = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_unique (id INTEGER PRIMARY KEY, val INTEGER, label TEXT)")
            .GetAwaiter().GetResult();
        _benchWithUniqueIndex.Db.ExecuteAsync("CREATE UNIQUE INDEX idx_uval ON t_unique (val)")
            .AsTask().GetAwaiter().GetResult();

        // Seed all three with 10K rows
        for (int i = 0; i < 10_000; i++)
        {
            var sql = $"INSERT INTO {{0}} VALUES ({i}, {i}, 'row_{i}')";
            _benchNoIndex.Db.ExecuteAsync(string.Format(sql, "t_noindex"))
                .AsTask().GetAwaiter().GetResult();
            _benchWithIndex.Db.ExecuteAsync(string.Format(sql, "t_indexed"))
                .AsTask().GetAwaiter().GetResult();
            _benchWithUniqueIndex.Db.ExecuteAsync(string.Format(sql, "t_unique"))
                .AsTask().GetAwaiter().GetResult();
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchNoIndex.Dispose();
        _benchWithIndex.Dispose();
        _benchWithUniqueIndex.Dispose();
    }

    [Benchmark(Baseline = true, Description = "INSERT without index")]
    public async Task InsertWithoutIndex()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchNoIndex.Db.ExecuteAsync(
            $"INSERT INTO t_noindex VALUES ({id}, {_rng.Next()}, 'new')");
    }

    [Benchmark(Description = "INSERT with secondary index")]
    public async Task InsertWithIndex()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchWithIndex.Db.ExecuteAsync(
            $"INSERT INTO t_indexed VALUES ({id}, {id}, 'new')");
    }

    [Benchmark(Description = "INSERT with UNIQUE index")]
    public async Task InsertWithUniqueIndex()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchWithUniqueIndex.Db.ExecuteAsync(
            $"INSERT INTO t_unique VALUES ({id}, {id}, 'new')");
    }

    [Benchmark(Description = "Indexed equality lookup")]
    public async Task IndexedLookup()
    {
        int val = _rng.Next(0, 10_000);
        await using var result = await _benchWithIndex.Db.ExecuteAsync(
            $"SELECT * FROM t_indexed WHERE val = {val}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "Non-indexed equality lookup (full scan)")]
    public async Task NonIndexedLookup()
    {
        int val = _rng.Next(0, 10_000);
        await using var result = await _benchNoIndex.Db.ExecuteAsync(
            $"SELECT * FROM t_noindex WHERE val = {val}");
        await result.ToListAsync();
    }
}

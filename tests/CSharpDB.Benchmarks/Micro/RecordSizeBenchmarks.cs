using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures how record (payload) size affects INSERT and SELECT performance.
/// Tests small, medium, and large records.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class RecordSizeBenchmarks
{
    private BenchmarkDatabase _benchSmall = null!;
    private BenchmarkDatabase _benchMedium = null!;
    private BenchmarkDatabase _benchLarge = null!;
    private int _nextId;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _rng = new Random(42);
        _nextId = 1_000_000;

        // Small: 3 columns (id, short text, int)
        _benchSmall = BenchmarkDatabase.CreateWithSchemaAsync(
            "CREATE TABLE t_small (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
            .GetAwaiter().GetResult();

        // Medium: 8 columns with 200-char text
        _benchMedium = BenchmarkDatabase.CreateWithSchemaAsync(
            DataGenerator.CreateTableSql("t_medium", 4, 3))
            .GetAwaiter().GetResult();

        // Large: 12 columns with 1000-char text
        _benchLarge = BenchmarkDatabase.CreateWithSchemaAsync(
            DataGenerator.CreateTableSql("t_large", 6, 5))
            .GetAwaiter().GetResult();

        // Seed each with 1000 rows
        var smallRng = new Random(42);
        _benchSmall.SeedAsync("t_small", 1000, i =>
            $"INSERT INTO t_small VALUES ({i}, '{DataGenerator.RandomString(smallRng, 20)}', {smallRng.Next()})")
            .GetAwaiter().GetResult();

        var medRng = new Random(42);
        _benchMedium.SeedAsync("t_medium", 1000, i =>
            DataGenerator.InsertSql("t_medium", i, 4, 3, medRng, 200))
            .GetAwaiter().GetResult();

        var largeRng = new Random(42);
        _benchLarge.SeedAsync("t_large", 1000, i =>
            DataGenerator.InsertSql("t_large", i, 6, 5, largeRng, 1000))
            .GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _benchSmall.Dispose();
        _benchMedium.Dispose();
        _benchLarge.Dispose();
    }

    [Benchmark(Description = "INSERT small record (3 cols, 20-char text)")]
    public async Task Insert_SmallRecord()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchSmall.Db.ExecuteAsync(
            $"INSERT INTO t_small VALUES ({id}, 'short text here!!', {_rng.Next()})");
    }

    [Benchmark(Description = "INSERT medium record (8 cols, 200-char text)")]
    public async Task Insert_MediumRecord()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchMedium.Db.ExecuteAsync(
            DataGenerator.InsertSql("t_medium", id, 4, 3, _rng, 200));
    }

    [Benchmark(Description = "INSERT large record (12 cols, 1000-char text)")]
    public async Task Insert_LargeRecord()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _benchLarge.Db.ExecuteAsync(
            DataGenerator.InsertSql("t_large", id, 6, 5, _rng, 1000));
    }

    [Benchmark(Description = "SELECT small record by PK")]
    public async Task Select_SmallRecord()
    {
        int id = _rng.Next(0, 1000);
        await using var result = await _benchSmall.Db.ExecuteAsync(
            $"SELECT * FROM t_small WHERE id = {id}");
        await result.ToListAsync();
    }

    [Benchmark(Description = "SELECT large record by PK")]
    public async Task Select_LargeRecord()
    {
        int id = _rng.Next(0, 1000);
        await using var result = await _benchLarge.Db.ExecuteAsync(
            $"SELECT * FROM t_large WHERE id = {id}");
        await result.ToListAsync();
    }
}

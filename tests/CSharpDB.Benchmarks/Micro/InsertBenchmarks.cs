using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures INSERT throughput: single-row auto-commit, single-row in transaction,
/// and batch inserts at various sizes.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class InsertBenchmarks
{
    [Params(100, 1_000, 10_000)]
    public int PreSeededRows { get; set; }

    private BenchmarkDatabase _bench = null!;
    private int _nextId;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(PreSeededRows).GetAwaiter().GetResult();
        _nextId = PreSeededRows + 1_000_000; // avoid PK conflicts
        _rng = new Random(42);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Single INSERT (auto-commit)")]
    public async Task SingleInsert()
    {
        int id = Interlocked.Increment(ref _nextId);
        var text = DataGenerator.RandomString(_rng, 50);
        await _bench.Db.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, '{text}', 'Alpha')");
    }

    [Benchmark(Description = "Single INSERT in explicit transaction")]
    public async Task SingleInsertInTransaction()
    {
        int id = Interlocked.Increment(ref _nextId);
        var text = DataGenerator.RandomString(_rng, 50);
        await _bench.Db.BeginTransactionAsync();
        await _bench.Db.ExecuteAsync(
            $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, '{text}', 'Beta')");
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x100 in transaction")]
    public async Task BatchInsert_100()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync(
                $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, 'batch100', 'Gamma')");
        }
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x1000 in transaction")]
    public async Task BatchInsert_1000()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 1000; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync(
                $"INSERT INTO bench VALUES ({id}, {_rng.Next()}, 'batch1000', 'Delta')");
        }
        await _bench.Db.CommitAsync();
    }
}

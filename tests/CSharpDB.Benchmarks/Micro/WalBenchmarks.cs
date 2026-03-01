using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Measures WAL-specific overhead: commit flush time, batch commit,
/// checkpoint duration, and auto-checkpoint impact.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
public class WalBenchmarks
{
    [Params(100, 500, 1000)]
    public int WalFramesBeforeCheckpoint { get; set; }

    private BenchmarkDatabase _bench = null!;
    private int _nextId;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync().GetAwaiter().GetResult();
        _nextId = 1_000_000;
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _bench.Dispose();
    }

    [Benchmark(Description = "Single-row commit (WAL flush)")]
    public async Task CommitSingleRow()
    {
        int id = Interlocked.Increment(ref _nextId);
        await _bench.Db.BeginTransactionAsync();
        await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'wal_test', 'Alpha')");
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "100-row batch commit")]
    public async Task CommitBatch_100()
    {
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'batch', 'Beta')");
        }
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Manual checkpoint after N writes")]
    public async Task ManualCheckpoint()
    {
        // Insert N rows to build up WAL frames
        await _bench.Db.BeginTransactionAsync();
        for (int i = 0; i < WalFramesBeforeCheckpoint; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            await _bench.Db.ExecuteAsync($"INSERT INTO bench VALUES ({id}, {id}, 'ckpt', 'Gamma')");
        }
        await _bench.Db.CommitAsync();

        // Measure checkpoint time
        await _bench.Db.CheckpointAsync();
    }
}

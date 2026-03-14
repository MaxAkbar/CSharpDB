using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using System.Text;

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
    private InsertBatch _preparedBatch100 = null!;
    private InsertBatch _preparedBatch1000 = null!;
    private int _nextId;
    private Random _rng = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _bench = BenchmarkDatabase.CreateAsync(PreSeededRows).GetAwaiter().GetResult();
        _preparedBatch100 = _bench.Db.PrepareInsertBatch("bench", 100);
        _preparedBatch1000 = _bench.Db.PrepareInsertBatch("bench", 1000);
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

    [Benchmark(Description = "Batch INSERT x100 in one statement")]
    public async Task BatchInsert_100_MultiRow()
    {
        await _bench.Db.ExecuteAsync(BuildMultiRowInsertSql(100, "batch100_multi", "Gamma"));
    }

    [Benchmark(Description = "Batch INSERT x1000 in one statement")]
    public async Task BatchInsert_1000_MultiRow()
    {
        await _bench.Db.ExecuteAsync(BuildMultiRowInsertSql(1000, "batch1000_multi", "Delta"));
    }

    [Benchmark(Description = "Batch INSERT x100 prepared batch")]
    public async Task BatchInsert_100_PreparedBatch()
    {
        _preparedBatch100.Clear();
        var row = new DbValue[4];
        for (int i = 0; i < 100; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(_rng.Next());
            row[2] = DbValue.FromText("batch100_prepared");
            row[3] = DbValue.FromText("Gamma");
            _preparedBatch100.AddRow(row);
        }

        await _bench.Db.BeginTransactionAsync();
        await _preparedBatch100.ExecuteAsync();
        await _bench.Db.CommitAsync();
    }

    [Benchmark(Description = "Batch INSERT x1000 prepared batch")]
    public async Task BatchInsert_1000_PreparedBatch()
    {
        _preparedBatch1000.Clear();
        var row = new DbValue[4];
        for (int i = 0; i < 1000; i++)
        {
            int id = Interlocked.Increment(ref _nextId);
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(_rng.Next());
            row[2] = DbValue.FromText("batch1000_prepared");
            row[3] = DbValue.FromText("Delta");
            _preparedBatch1000.AddRow(row);
        }

        await _bench.Db.BeginTransactionAsync();
        await _preparedBatch1000.ExecuteAsync();
        await _bench.Db.CommitAsync();
    }

    private string BuildMultiRowInsertSql(int rowCount, string text, string category)
    {
        var builder = new StringBuilder(rowCount * 40);
        builder.Append("INSERT INTO bench VALUES ");

        for (int i = 0; i < rowCount; i++)
        {
            if (i > 0)
                builder.Append(", ");

            int id = Interlocked.Increment(ref _nextId);
            builder
                .Append('(')
                .Append(id)
                .Append(", ")
                .Append(_rng.Next())
                .Append(", '")
                .Append(text)
                .Append("', '")
                .Append(category)
                .Append("')");
        }

        return builder.ToString();
    }
}

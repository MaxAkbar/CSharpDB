using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class WalPointReadBenchmarks
{
    private const int LookupBatchSize = 256;
    private const int ProbeSequenceLength = 32_768;
    private const int WalCheckpointThreshold = 1_000_000;

    private readonly SelectStatement[] _probeStatements = new SelectStatement[ProbeSequenceLength];

    private string _dbPath = null!;
    private Database _db = null!;
    private int _cursor;
    private long _sink;

    [Params(100, 1_000, 5_000, 10_000)]
    public int TargetFrames { get; set; }

    [Params(WalPointReadState.WalBacked, WalPointReadState.Checkpointed)]
    public WalPointReadState State { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(OperationsPerInvoke = LookupBatchSize, Description = "SQL primary-key point read")]
    public async Task SqlPrimaryKeyPointRead()
    {
        int offset = NextBatchOffset();
        for (int i = 0; i < LookupBatchSize; i++)
        {
            await using var result = await _db.ExecuteAsync(_probeStatements[(offset + i) % ProbeSequenceLength]);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    private async Task GlobalSetupAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_wal_point_read_bench_{Guid.NewGuid():N}.db");
        _db = await Database.OpenAsync(_dbPath, CreateOptions());
        await _db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, data TEXT)");

        int rowCount = Math.Max(1, TargetFrames / 2);
        await SeedRowsAsync(rowCount);

        if (State == WalPointReadState.Checkpointed)
            await _db.CheckpointAsync();

        var lookupRng = new Random(123);
        for (int i = 0; i < ProbeSequenceLength; i++)
            _probeStatements[i] = CreatePointLookupStatement(lookupRng.Next(0, rowCount));

        await WarmupAsync();
    }

    private async Task GlobalCleanupAsync()
    {
        if (_db != null)
            await _db.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_dbPath);
    }

    private async Task SeedRowsAsync(int rowCount)
    {
        const int batchSize = 500;
        var rng = new Random(42);

        for (int batchStart = 0; batchStart < rowCount; batchStart += batchSize)
        {
            await _db.BeginTransactionAsync();
            int batchEnd = Math.Min(batchStart + batchSize, rowCount);
            for (int id = batchStart; id < batchEnd; id++)
            {
                string text = DataGenerator.RandomString(rng, 50);
                await _db.ExecuteAsync($"INSERT INTO t VALUES ({id}, {rng.Next()}, '{text}')");
            }

            await _db.CommitAsync();
        }
    }

    private async Task WarmupAsync()
    {
        for (int i = 0; i < LookupBatchSize; i++)
        {
            await using var result = await _db.ExecuteAsync(_probeStatements[i]);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    private static DatabaseOptions CreateOptions()
    {
        return BenchmarkDurability.Apply(new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(WalCheckpointThreshold),
                },
            },
        });
    }

    private static SelectStatement CreatePointLookupStatement(int id)
    {
        return new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new ColumnRefExpression { ColumnName = "val" },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "t" },
            Where = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = "id" },
                Right = new LiteralExpression
                {
                    LiteralType = TokenType.IntegerLiteral,
                    Value = (long)id,
                },
            },
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };
    }

    private int NextBatchOffset()
    {
        int offset = _cursor;
        _cursor += LookupBatchSize;
        if (_cursor >= ProbeSequenceLength)
            _cursor = 0;
        return offset;
    }
}

public enum WalPointReadState
{
    WalBacked,
    Checkpointed,
}

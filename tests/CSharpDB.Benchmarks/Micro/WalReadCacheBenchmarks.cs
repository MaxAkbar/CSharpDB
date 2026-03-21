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
public class WalReadCacheBenchmarks
{
    private const int SeedRowCount = 100_000;
    private const int ProbeSequenceLength = 32_768;
    private const int LookupBatchSize = 256;
    private const int MaxCachedPages = 16;

    private readonly int[] _probeIds = new int[ProbeSequenceLength];
    private readonly SelectStatement[] _sqlProbeStatements = new SelectStatement[ProbeSequenceLength];

    private string _sqlPath = null!;
    private Database _sqlDb = null!;
    private int _sqlCursor;
    private long _sink;

    [Params(0, 128)]
    public int MaxCachedWalReadPages { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, OperationsPerInvoke = LookupBatchSize, Description = "SQL cold lookup (WAL-backed, WAL cache toggle)")]
    public async Task SqlColdLookup_WalBacked()
    {
        int offset = NextBatchOffset(ref _sqlCursor);
        for (int i = 0; i < LookupBatchSize; i++)
        {
            var statement = _sqlProbeStatements[(offset + i) % ProbeSequenceLength];
            await using var result = await _sqlDb.ExecuteAsync(statement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    private async Task GlobalSetupAsync()
    {
        var rng = new Random(42);
        for (int i = 0; i < ProbeSequenceLength; i++)
        {
            int probeId = rng.Next(0, SeedRowCount);
            _probeIds[i] = probeId;
            _sqlProbeStatements[i] = CreateSqlLookupStatement(probeId);
        }

        _sqlPath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("wal-read-cache-sql", SeedRowCount);
        _sqlDb = await Database.OpenAsync(_sqlPath, CreateOptions());

        // Keep the latest table pages in the WAL so lookup reads exercise the WAL path.
        await _sqlDb.ExecuteAsync("UPDATE bench SET value = value + 1");
    }

    private async Task GlobalCleanupAsync()
    {
        if (_sqlDb != null)
            await _sqlDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlPath);
    }

    private DatabaseOptions CreateOptions()
    {
        return new DatabaseOptions
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                PagerOptions = new PagerOptions
                {
                    MaxCachedPages = MaxCachedPages,
                    MaxCachedWalReadPages = MaxCachedWalReadPages,
                    CheckpointPolicy = new FrameCountCheckpointPolicy(1_000_000),
                    UseMemoryMappedReads = false,
                },
            },
        };
    }

    private static SelectStatement CreateSqlLookupStatement(int id)
    {
        return new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new ColumnRefExpression { ColumnName = "value" },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "bench" },
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

    private static int NextBatchOffset(ref int cursor)
    {
        int offset = cursor;
        cursor += LookupBatchSize;
        if (cursor >= ProbeSequenceLength)
            cursor = 0;
        return offset;
    }
}

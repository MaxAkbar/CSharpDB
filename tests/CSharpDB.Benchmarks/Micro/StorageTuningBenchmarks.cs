using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class StorageTuningBenchmarks
{
    private const int SeedRowCount = 100_000;
    private const int ProbeSequenceLength = 16_384;
    private const int LookupBatchSize = 128;

    private readonly int[] _probeIds = new int[ProbeSequenceLength];
    private readonly SelectStatement[] _sqlProbeStatements = new SelectStatement[ProbeSequenceLength];

    private string _sqlPath = null!;
    private string _collectionPath = null!;
    private Database _sqlDb = null!;
    private Database _collectionDb = null!;
    private Collection<BenchDoc> _collection = null!;
    private int _sqlCursor;
    private int _collectionCursor;
    private long _sink;

    [Params(16, 256, 2048)]
    public int MaxCachedPages { get; set; }

    [Params(false, true)]
    public bool UseCachingIndexes { get; set; }

    private sealed record BenchDoc(string Name, int Value, string Category);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, OperationsPerInvoke = LookupBatchSize, Description = "SQL indexed lookup tuning (file-backed)")]
    public async Task SqlIndexedLookup_FileBacked()
    {
        int offset = NextBatchOffset(ref _sqlCursor);
        for (int i = 0; i < LookupBatchSize; i++)
        {
            var statement = _sqlProbeStatements[(offset + i) % ProbeSequenceLength];
            using var reader = _sqlDb.CreateReaderSession();
            await using var result = await reader.ExecuteReadAsync(statement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = LookupBatchSize, Description = "SQL indexed lookup tuning (reused reader session)")]
    public async Task SqlIndexedLookup_ReusedReaderSession_FileBacked()
    {
        int offset = NextBatchOffset(ref _sqlCursor);
        using var reader = _sqlDb.CreateReaderSession();
        for (int i = 0; i < LookupBatchSize; i++)
        {
            var statement = _sqlProbeStatements[(offset + i) % ProbeSequenceLength];
            await using var result = await reader.ExecuteReadAsync(statement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = LookupBatchSize, Description = "Collection indexed lookup tuning (file-backed)")]
    public async Task CollectionIndexedLookup_FileBacked()
    {
        int offset = NextBatchOffset(ref _collectionCursor);
        for (int i = 0; i < LookupBatchSize; i++)
        {
            int id = _probeIds[(offset + i) % ProbeSequenceLength];
            await foreach (var match in _collection.FindByIndexAsync(d => d.Value, id))
                _sink ^= match.Value.Value;
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

        _sqlPath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("storage-tuning-sql", SeedRowCount);
        await EnsureSqlValueIndexAsync(_sqlPath);

        _collectionPath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("storage-tuning-collection", SeedRowCount);
        await EnsureCollectionValueIndexAsync(_collectionPath);

        var options = CreateOptions();
        _sqlDb = await Database.OpenAsync(_sqlPath, options);
        _collectionDb = await Database.OpenAsync(_collectionPath, options);
        _collection = await _collectionDb.GetCollectionAsync<BenchDoc>("bench_docs");
    }

    private async Task GlobalCleanupAsync()
    {
        if (_sqlDb != null)
            await _sqlDb.DisposeAsync();
        if (_collectionDb != null)
            await _collectionDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlPath);
        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_collectionPath);
    }

    private DatabaseOptions CreateOptions()
    {
        return new DatabaseOptions().ConfigureStorageEngine(builder =>
        {
            builder.UsePagerOptions(new PagerOptions
            {
                MaxCachedPages = MaxCachedPages,
            });

            if (UseCachingIndexes)
                builder.UseCachingBTreeIndexes(findCacheCapacity: 4096);
        });
    }

    private static async Task EnsureSqlValueIndexAsync(string path)
    {
        await using var db = await Database.OpenAsync(path);
        await db.ExecuteAsync("CREATE INDEX IF NOT EXISTS idx_bench_value ON bench (value)");
    }

    private static async Task EnsureCollectionValueIndexAsync(string path)
    {
        await using var db = await Database.OpenAsync(path);
        var collection = await db.GetCollectionAsync<BenchDoc>("bench_docs");
        await collection.EnsureIndexAsync(d => d.Value);
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
                Left = new ColumnRefExpression { ColumnName = "value" },
                Right = new LiteralExpression
                {
                    LiteralType = TokenType.IntegerLiteral,
                    Value = id * 10L,
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

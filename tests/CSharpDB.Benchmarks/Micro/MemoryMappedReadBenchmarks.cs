using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class MemoryMappedReadBenchmarks
{
    private const int SeedRowCount = 200_000;
    private const int ProbeSequenceLength = 65_536;
    private const int LookupBatchSize = 256;
    private const int MaxCachedPages = 16;

    private readonly int[] _probeIds = new int[ProbeSequenceLength];
    private readonly SelectStatement[] _sqlProbeStatements = new SelectStatement[ProbeSequenceLength];

    private string _sqlFilePath = null!;
    private string _collectionFilePath = null!;
    private Database _sqlFileDb = null!;
    private Database _collectionFileDb = null!;
    private Collection<BenchDoc> _fileCollection = null!;
    private int _sqlFileCursor;
    private int _collectionFileCursor;
    private long _sink;

    [Params(false, true)]
    public bool UseMemoryMappedReads { get; set; }

    private sealed record BenchDoc(string Name, int Value, string Category);

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, OperationsPerInvoke = LookupBatchSize, Description = "SQL cold lookup (file-backed, mmap toggle)")]
    public async Task SqlColdLookup_FileBacked()
    {
        int offset = NextBatchOffset(ref _sqlFileCursor);
        for (int i = 0; i < LookupBatchSize; i++)
        {
            var statement = _sqlProbeStatements[(offset + i) % ProbeSequenceLength];
            await using var result = await _sqlFileDb.ExecuteAsync(statement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = LookupBatchSize, Description = "Collection cold get (file-backed, mmap toggle)")]
    public async Task CollectionColdGet_FileBacked()
    {
        int offset = NextBatchOffset(ref _collectionFileCursor);
        for (int i = 0; i < LookupBatchSize; i++)
        {
            int id = _probeIds[(offset + i) % ProbeSequenceLength];
            var document = await _fileCollection.GetAsync($"doc:{id}");
            if (document != null)
                _sink ^= document.Value;
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

        var options = CreateOptions();

        _sqlFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("mmap-sql", SeedRowCount);
        _sqlFileDb = await Database.OpenAsync(_sqlFilePath, options);

        _collectionFilePath = await InMemoryBenchmarkDatabaseFactory.CreateSeededCollectionDatabaseAsync("mmap-collection", SeedRowCount);
        _collectionFileDb = await Database.OpenAsync(_collectionFilePath, options);
        _fileCollection = await _collectionFileDb.GetCollectionAsync<BenchDoc>("bench_docs");
    }

    private async Task GlobalCleanupAsync()
    {
        if (_sqlFileDb != null)
            await _sqlFileDb.DisposeAsync();
        if (_collectionFileDb != null)
            await _collectionFileDb.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlFilePath);
        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_collectionFilePath);
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
                    UseMemoryMappedReads = UseMemoryMappedReads,
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

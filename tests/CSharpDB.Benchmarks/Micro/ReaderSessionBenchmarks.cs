using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ReaderSessionBenchmarks
{
    private const int SeedRowCount = 100_000;
    private const int ProbeSequenceLength = 4_096;
    private const int QueryBatchSize = 128;

    private readonly SelectStatement[] _lookupStatements = new SelectStatement[ProbeSequenceLength];
    private readonly SelectStatement _countStatement = CreateCountStatement();

    private string _sqlPath = null!;
    private Database _db = null!;
    private int _countCursor;
    private int _lookupCursor;
    private long _sink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, OperationsPerInvoke = QueryBatchSize, Description = "COUNT(*) with per-query reader sessions")]
    public async Task Count_PerQueryReaderSession()
    {
        int offset = NextBatchOffset(ref _countCursor);
        for (int i = 0; i < QueryBatchSize; i++)
        {
            using var reader = _db.CreateReaderSession();
            await using var result = await reader.ExecuteReadAsync(_countStatement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = QueryBatchSize, Description = "COUNT(*) with reused reader session")]
    public async Task Count_ReusedReaderSession()
    {
        int offset = NextBatchOffset(ref _countCursor);
        using var reader = _db.CreateReaderSession();
        for (int i = 0; i < QueryBatchSize; i++)
        {
            await using var result = await reader.ExecuteReadAsync(_countStatement);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger + offset + i;
        }
    }

    [Benchmark(OperationsPerInvoke = QueryBatchSize, Description = "Point lookup with per-query reader sessions")]
    public async Task Lookup_PerQueryReaderSession()
    {
        int offset = NextBatchOffset(ref _lookupCursor);
        for (int i = 0; i < QueryBatchSize; i++)
        {
            using var reader = _db.CreateReaderSession();
            await using var result = await reader.ExecuteReadAsync(_lookupStatements[(offset + i) % ProbeSequenceLength]);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = QueryBatchSize, Description = "Point lookup with reused reader session")]
    public async Task Lookup_ReusedReaderSession()
    {
        int offset = NextBatchOffset(ref _lookupCursor);
        using var reader = _db.CreateReaderSession();
        for (int i = 0; i < QueryBatchSize; i++)
        {
            await using var result = await reader.ExecuteReadAsync(_lookupStatements[(offset + i) % ProbeSequenceLength]);
            if (await result.MoveNextAsync())
                _sink ^= result.Current[0].AsInteger;
        }
    }

    [Benchmark(OperationsPerInvoke = QueryBatchSize, Description = "Point lookup with direct ExecuteAsync")]
    public async Task Lookup_DirectExecute()
    {
        int offset = NextBatchOffset(ref _lookupCursor);
        for (int i = 0; i < QueryBatchSize; i++)
        {
            await using var result = await _db.ExecuteAsync(_lookupStatements[(offset + i) % ProbeSequenceLength]);
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
            _lookupStatements[i] = CreateLookupStatement(probeId);
        }

        _sqlPath = await InMemoryBenchmarkDatabaseFactory.CreateSeededSqlDatabaseAsync("reader-session", SeedRowCount);
        _db = await Database.OpenAsync(_sqlPath);
    }

    private async Task GlobalCleanupAsync()
    {
        if (_db != null)
            await _db.DisposeAsync();

        InMemoryBenchmarkDatabaseFactory.DeleteDatabaseFiles(_sqlPath);
    }

    private static SelectStatement CreateCountStatement()
    {
        return new SelectStatement
        {
            IsDistinct = false,
            Columns =
            [
                new SelectColumn
                {
                    IsStar = false,
                    Expression = new FunctionCallExpression
                    {
                        FunctionName = "COUNT",
                        Arguments = [],
                        IsStarArg = true,
                    },
                    Alias = null,
                },
            ],
            From = new SimpleTableRef { TableName = "bench" },
            Where = null,
            GroupBy = null,
            Having = null,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };
    }

    private static SelectStatement CreateLookupStatement(int id)
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
        cursor += QueryBatchSize;
        if (cursor >= ProbeSequenceLength)
            cursor = 0;
        return offset;
    }
}

using System.Text;
using BenchmarkDotNet.Attributes;
using CSharpDB.Core;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Sql;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class LayerComparisonLookupBenchmarks
{
    private const int SeedRowCount = 10_000;
    private const string LookupSql = "SELECT * FROM bench WHERE id = 5000";

    private string _adoPath = null!;
    private string _enginePath = null!;
    private string _corePath = null!;

    private CSharpDbConnection _adoConn = null!;
    private CSharpDbCommand _adoLookupCmd = null!;
    private Database _engineDb = null!;
    private Pager _corePager = null!;
    private QueryPlanner _corePlanner = null!;

    private long _sink;
    private string? _textSink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Point lookup via ADO.NET provider")]
    public async Task PointLookup_AdoNetProvider()
    {
        await using var reader = await _adoLookupCmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            _sink = reader.GetInt64(0);
            _textSink = reader.GetString(2);
        }
    }

    [Benchmark(Description = "Point lookup via Database API")]
    public async Task PointLookup_DatabaseApi()
    {
        await using var result = await _engineDb.ExecuteAsync(LookupSql);
        await ConsumeSingleRowAsync(result);
    }

    [Benchmark(Description = "Point lookup via SQL frontend + core")]
    public async Task PointLookup_SqlFrontendCore()
    {
        await using var result = await ExecuteCoreLookupAsync();
        await ConsumeSingleRowAsync(result);
    }

    private async Task GlobalSetupAsync()
    {
        _adoPath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-ado-lookup", SeedRowCount);
        _enginePath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-engine-lookup", SeedRowCount);
        _corePath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-core-lookup", SeedRowCount);

        _adoConn = new CSharpDbConnection($"Data Source={_adoPath}");
        await _adoConn.OpenAsync();
        _adoLookupCmd = (CSharpDbCommand)_adoConn.CreateCommand();
        _adoLookupCmd.CommandText = LookupSql;

        _engineDb = await Database.OpenAsync(_enginePath);

        var factory = new DefaultStorageEngineFactory();
        var coreContext = await factory.OpenAsync(_corePath, new StorageEngineOptions());
        _corePager = coreContext.Pager;
        _corePlanner = new QueryPlanner(_corePager, coreContext.Catalog, coreContext.RecordSerializer);
    }

    private async Task GlobalCleanupAsync()
    {
        _adoLookupCmd?.Dispose();
        if (_adoConn != null)
            await _adoConn.DisposeAsync();

        if (_engineDb != null)
            await _engineDb.DisposeAsync();

        if (_corePager != null)
            await _corePager.DisposeAsync();

        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_adoPath);
        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_enginePath);
        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_corePath);
    }

    private ValueTask<QueryResult> ExecuteCoreLookupAsync()
    {
        if (Parser.TryParseSimplePrimaryKeyLookup(LookupSql, out var simpleLookup) &&
            _corePlanner.TryExecuteSimplePrimaryKeyLookup(simpleLookup, out var fastResult))
        {
            return ValueTask.FromResult(fastResult);
        }

        return _corePlanner.ExecuteAsync(Parser.Parse(LookupSql));
    }

    private ValueTask ConsumeSingleRowAsync(QueryResult result)
    {
        return ConsumeSingleRowCoreAsync(result);
    }

    private async ValueTask ConsumeSingleRowCoreAsync(QueryResult result)
    {
        if (!await result.MoveNextAsync())
            return;

        var row = result.Current;
        _sink = row[0].AsInteger;
        _textSink = row[2].AsText;
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class LayerComparisonInsertBenchmarks
{
    private const int SeedRowCount = 10_000;
    private const int BatchSize = 100;

    private string _adoPath = null!;
    private string _enginePath = null!;
    private string _corePath = null!;

    private CSharpDbConnection _adoConn = null!;
    private CSharpDbCommand _adoInsertCmd = null!;
    private Database _engineDb = null!;
    private Pager _corePager = null!;
    private QueryPlanner _corePlanner = null!;

    private int _nextAdoId;
    private int _nextEngineId;
    private int _nextCoreId;
    private int _rowsAffectedSink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Batch insert x100 via ADO.NET provider")]
    public async Task BatchInsert100_AdoNetProvider()
    {
        _adoInsertCmd.CommandText = BuildMultiRowInsertSql(ref _nextAdoId, "ado");
        _rowsAffectedSink = await _adoInsertCmd.ExecuteNonQueryAsync();
    }

    [Benchmark(Description = "Batch insert x100 via Database API")]
    public async Task BatchInsert100_DatabaseApi()
    {
        string sql = BuildMultiRowInsertSql(ref _nextEngineId, "engine");
        await using var result = await _engineDb.ExecuteAsync(sql);
        _rowsAffectedSink = result.RowsAffected;
    }

    [Benchmark(Description = "Batch insert x100 via SQL frontend + core")]
    public async Task BatchInsert100_SqlFrontendCore()
    {
        string sql = BuildMultiRowInsertSql(ref _nextCoreId, "core");
        await _corePager.BeginTransactionAsync();
        try
        {
            QueryResult result;
            if (Parser.TryParseSimpleInsert(sql, out var simpleInsert))
            {
                result = await _corePlanner.ExecuteSimpleInsertAsync(simpleInsert);
            }
            else
            {
                result = await _corePlanner.ExecuteAsync(Parser.Parse(sql));
            }

            await using (result)
            {
                _rowsAffectedSink = result.RowsAffected;
            }

            await _corePager.CommitAsync();
        }
        catch
        {
            await _corePager.RollbackAsync();
            throw;
        }
    }

    private async Task GlobalSetupAsync()
    {
        _adoPath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-ado-insert", SeedRowCount);
        _enginePath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-engine-insert", SeedRowCount);
        _corePath = await LayerComparisonDatabaseFactory.CreateSeededBenchDatabaseAsync("layercmp-core-insert", SeedRowCount);

        _adoConn = new CSharpDbConnection($"Data Source={_adoPath}");
        await _adoConn.OpenAsync();
        _adoInsertCmd = (CSharpDbCommand)_adoConn.CreateCommand();

        _engineDb = await Database.OpenAsync(_enginePath);

        var factory = new DefaultStorageEngineFactory();
        var coreContext = await factory.OpenAsync(_corePath, new StorageEngineOptions());
        _corePager = coreContext.Pager;
        _corePlanner = new QueryPlanner(_corePager, coreContext.Catalog, coreContext.RecordSerializer);

        _nextAdoId = SeedRowCount + 1_000_000;
        _nextEngineId = SeedRowCount + 2_000_000;
        _nextCoreId = SeedRowCount + 3_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        _adoInsertCmd?.Dispose();
        if (_adoConn != null)
            await _adoConn.DisposeAsync();

        if (_engineDb != null)
            await _engineDb.DisposeAsync();

        if (_corePager != null)
            await _corePager.DisposeAsync();

        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_adoPath);
        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_enginePath);
        LayerComparisonDatabaseFactory.DeleteDatabaseFiles(_corePath);
    }

    private static string BuildMultiRowInsertSql(ref int nextId, string marker)
    {
        var builder = new StringBuilder(BatchSize * 48);
        builder.Append("INSERT INTO bench VALUES ");

        for (int i = 0; i < BatchSize; i++)
        {
            if (i > 0)
                builder.Append(", ");

            int id = nextId++;
            builder
                .Append('(')
                .Append(id)
                .Append(", ")
                .Append(id * 10L)
                .Append(", '")
                .Append(marker)
                .Append("_batch', 'Layer')");
        }

        return builder.ToString();
    }
}

file static class LayerComparisonDatabaseFactory
{
    private const int SeedBatchSize = 500;

    public static async Task<string> CreateSeededBenchDatabaseAsync(string prefix, int seedRowCount)
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");

        await using (var db = await Database.OpenAsync(filePath))
        {
            await db.ExecuteAsync(
                "CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");

            var batch = db.PrepareInsertBatch("bench", SeedBatchSize);
            var row = new DbValue[4];
            int nextId = 1;

            while (nextId <= seedRowCount)
            {
                batch.Clear();
                int batchEnd = Math.Min(nextId + SeedBatchSize - 1, seedRowCount);
                for (; nextId <= batchEnd; nextId++)
                {
                    row[0] = DbValue.FromInteger(nextId);
                    row[1] = DbValue.FromInteger(nextId * 10L);
                    row[2] = DbValue.FromText($"seed_{nextId}");
                    row[3] = DbValue.FromText(GetCategory(nextId));
                    batch.AddRow(row);
                }

                await db.BeginTransactionAsync();
                await batch.ExecuteAsync();
                await db.CommitAsync();
            }
        }

        return filePath;
    }

    public static void DeleteDatabaseFiles(string? filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return;

        try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
        try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
    }

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };
}

using BenchmarkDotNet.Attributes;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Benchmarks.Micro;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class SingleInsertPathBenchmarks
{
    private const string TableName = "bench";

    [Params(10_000)]
    public int PreSeededRows { get; set; }

    private BenchmarkDatabase _textBench = null!;
    private BenchmarkDatabase _simpleBench = null!;

    private string _plannerPath = null!;
    private string _storagePath = null!;
    private string _storageNoCatalogPath = null!;

    private Database _textDb = null!;
    private Database _simpleDb = null!;

    private Pager _plannerPager = null!;
    private SchemaCatalog _plannerCatalog = null!;
    private QueryPlanner _planner = null!;

    private Pager _storagePager = null!;
    private SchemaCatalog _storageCatalog = null!;
    private IRecordSerializer _storageRecordSerializer = null!;
    private BTree _storageTree = null!;

    private Pager _storageNoCatalogPager = null!;
    private IRecordSerializer _storageNoCatalogRecordSerializer = null!;
    private BTree _storageNoCatalogTree = null!;

    private DbValue[] _simpleValues = null!;
    private DbValue[] _plannerValues = null!;
    private DbValue[] _storageValues = null!;
    private DbValue[] _storageNoCatalogValues = null!;

    private SimpleInsertSql _simpleInsert;
    private SimpleInsertSql _plannerInsert;

    private int _nextTextId;
    private int _nextSimpleId;
    private int _nextPlannerId;
    private int _nextStorageId;
    private int _nextStorageNoCatalogId;

    private int _rowsAffectedSink;

    [GlobalSetup]
    public void GlobalSetup()
        => GlobalSetupAsync().GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
        => GlobalCleanupAsync().GetAwaiter().GetResult();

    [Benchmark(Baseline = true, Description = "Single insert via Database.ExecuteAsync(sql)")]
    public async Task SingleInsert_TextSql()
    {
        int id = Interlocked.Increment(ref _nextTextId);
        await using var result = await _textDb.ExecuteAsync(BuildInsertSql(id, "text"));
        _rowsAffectedSink = result.RowsAffected;
    }

    [Benchmark(Description = "Single insert via Database.ExecuteAsync(SimpleInsertSql)")]
    public async Task SingleInsert_PreparsedSimpleInsert()
    {
        int id = Interlocked.Increment(ref _nextSimpleId);
        PopulateValues(_simpleValues, id, "simple");
        await using var result = await _simpleDb.ExecuteAsync(_simpleInsert);
        _rowsAffectedSink = result.RowsAffected;
    }

    [Benchmark(Description = "Single insert via QueryPlanner + Pager")]
    public async Task SingleInsert_QueryPlannerCore()
    {
        int id = Interlocked.Increment(ref _nextPlannerId);
        PopulateValues(_plannerValues, id, "planner");

        await _plannerPager.BeginTransactionAsync();
        try
        {
            await using var result = await _planner.ExecuteSimpleInsertAsync(
                _plannerInsert,
                persistRootChanges: true);
            _rowsAffectedSink = result.RowsAffected;
            await _plannerPager.CommitAsync();
        }
        catch
        {
            await _plannerPager.RollbackAsync();
            throw;
        }
    }

    [Benchmark(Description = "Single insert via raw BTree + Pager")]
    public async Task SingleInsert_RawBTree()
    {
        int id = Interlocked.Increment(ref _nextStorageId);
        PopulateValues(_storageValues, id, "storage");

        await _storagePager.BeginTransactionAsync();
        try
        {
            await _storageTree.InsertAsync(id, _storageRecordSerializer.Encode(_storageValues));
            await _storageCatalog.AdjustTableRowCountKnownExactAsync(TableName, 1);
            await _storageCatalog.MarkTableColumnStatisticsStaleAsync(TableName);
            await _storageCatalog.PersistRootPageChangesAsync(TableName);
            await _storagePager.CommitAsync();
            _rowsAffectedSink = 1;
        }
        catch
        {
            await _storagePager.RollbackAsync();
            throw;
        }
    }

    [Benchmark(Description = "Single insert via raw BTree + Pager (no catalog sync)")]
    public async Task SingleInsert_RawBTree_NoCatalogSync()
    {
        int id = Interlocked.Increment(ref _nextStorageNoCatalogId);
        PopulateValues(_storageNoCatalogValues, id, "storage_nocatalog");

        await _storageNoCatalogPager.BeginTransactionAsync();
        try
        {
            await _storageNoCatalogTree.InsertAsync(id, _storageNoCatalogRecordSerializer.Encode(_storageNoCatalogValues));
            await _storageNoCatalogPager.CommitAsync();
            _rowsAffectedSink = 1;
        }
        catch
        {
            await _storageNoCatalogPager.RollbackAsync();
            throw;
        }
    }

    private async Task GlobalSetupAsync()
    {
        _textBench = await BenchmarkDatabase.CreateAsync(PreSeededRows);
        _simpleBench = await BenchmarkDatabase.CreateAsync(PreSeededRows);
        _plannerPath = await SingleInsertPathDatabaseFactory.CreateSeededBenchDatabaseAsync("single-insert-planner", PreSeededRows);
        _storagePath = await SingleInsertPathDatabaseFactory.CreateSeededBenchDatabaseAsync("single-insert-storage", PreSeededRows);
        _storageNoCatalogPath = await SingleInsertPathDatabaseFactory.CreateSeededBenchDatabaseAsync("single-insert-storage-nocatalog", PreSeededRows);

        _textDb = _textBench.Db;
        _simpleDb = _simpleBench.Db;

        var factory = new DefaultStorageEngineFactory();

        var plannerContext = await factory.OpenAsync(_plannerPath, new StorageEngineOptions());
        _plannerPager = plannerContext.Pager;
        _plannerCatalog = plannerContext.Catalog;
        _planner = new QueryPlanner(_plannerPager, _plannerCatalog, plannerContext.RecordSerializer);

        var storageContext = await factory.OpenAsync(_storagePath, new StorageEngineOptions());
        _storagePager = storageContext.Pager;
        _storageCatalog = storageContext.Catalog;
        _storageRecordSerializer = storageContext.RecordSerializer;
        _storageTree = _storageCatalog.GetTableTree(TableName);

        var storageNoCatalogContext = await factory.OpenAsync(_storageNoCatalogPath, new StorageEngineOptions());
        _storageNoCatalogPager = storageNoCatalogContext.Pager;
        _storageNoCatalogRecordSerializer = storageNoCatalogContext.RecordSerializer;
        _storageNoCatalogTree = storageNoCatalogContext.Catalog.GetTableTree(TableName);

        _simpleValues = CreateValueBuffer();
        _plannerValues = CreateValueBuffer();
        _storageValues = CreateValueBuffer();
        _storageNoCatalogValues = CreateValueBuffer();
        _simpleInsert = new SimpleInsertSql(TableName, [_simpleValues]);
        _plannerInsert = new SimpleInsertSql(TableName, [_plannerValues]);

        _nextTextId = PreSeededRows + 1_000_000;
        _nextSimpleId = PreSeededRows + 2_000_000;
        _nextPlannerId = PreSeededRows + 3_000_000;
        _nextStorageId = PreSeededRows + 4_000_000;
        _nextStorageNoCatalogId = PreSeededRows + 5_000_000;
    }

    private async Task GlobalCleanupAsync()
    {
        if (_textBench != null)
            await _textBench.DisposeAsync();

        if (_simpleBench != null)
            await _simpleBench.DisposeAsync();

        if (_plannerPager != null)
            await _plannerPager.DisposeAsync();

        if (_storagePager != null)
            await _storagePager.DisposeAsync();

        if (_storageNoCatalogPager != null)
            await _storageNoCatalogPager.DisposeAsync();

        SingleInsertPathDatabaseFactory.DeleteDatabaseFiles(_plannerPath);
        SingleInsertPathDatabaseFactory.DeleteDatabaseFiles(_storagePath);
        SingleInsertPathDatabaseFactory.DeleteDatabaseFiles(_storageNoCatalogPath);
    }

    private static DbValue[] CreateValueBuffer()
        => new DbValue[4];

    private static void PopulateValues(DbValue[] values, int id, string marker)
    {
        values[0] = DbValue.FromInteger(id);
        values[1] = DbValue.FromInteger(id * 10L);
        values[2] = DbValue.FromText($"{marker}_single");
        values[3] = DbValue.FromText("Layer");
    }

    private static string BuildInsertSql(int id, string marker)
        => $"INSERT INTO {TableName} VALUES ({id}, {id * 10L}, '{marker}_single', 'Layer')";
}

file static class SingleInsertPathDatabaseFactory
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

using System.Data.Common;
using BenchmarkDotNet.Attributes;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Stable engine-path baselines for calculating observability overhead. The
/// worker deliberately registers no ActivityListener, MeterListener, logging
/// bridge, exporter, or diagnostics-history consumer.
/// </summary>
[BenchmarkCategory("Observability", "Baseline", "NoListeners")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ObservabilityNoListenerEngineBenchmarks
{
    private const int SeedRowCount = 1_024;
    private const int StreamRowCount = 128;

    private Database _database = null!;
    private Statement _preparsedLookup = null!;
    private SimpleInsertSql _simpleInsert;
    private DbValue[] _simpleInsertValues = null!;
    private int _nextSqlInsertId;
    private int _nextSimpleInsertId;
    private int _nextTransactionalInsertId;
    private long _sink;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoActivityListeners();

        _database = await Database.OpenInMemoryAsync();
        await _database.ExecuteAsync(
            "CREATE TABLE observability_baseline (id INTEGER PRIMARY KEY, value INTEGER, text_value TEXT)");

        var batch = _database.PrepareInsertBatch("observability_baseline", SeedRowCount);
        var row = new DbValue[3];
        for (int id = 1; id <= SeedRowCount; id++)
        {
            row[0] = DbValue.FromInteger(id);
            row[1] = DbValue.FromInteger(id * 10L);
            row[2] = DbValue.FromText($"seed_{id}");
            batch.AddRow(row);
        }

        await batch.ExecuteAsync();

        _preparsedLookup = Parser.Parse(
            "SELECT value FROM observability_baseline WHERE id = 512");
        _simpleInsertValues = new DbValue[3];
        _simpleInsert = new SimpleInsertSql(
            "observability_baseline",
            _simpleInsertValues);

        _nextSqlInsertId = SeedRowCount + 1_000_000;
        _nextSimpleInsertId = SeedRowCount + 2_000_000;
        _nextTransactionalInsertId = SeedRowCount + 3_000_000;
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        if (_database is not null)
            await _database.DisposeAsync();
    }

    [Benchmark(Description = "No listeners: SQL primary-key fast lookup")]
    public async Task FastPrimaryKeyLookupSqlAsync()
    {
        await using var result = await _database.ExecuteAsync(
            "SELECT value FROM observability_baseline WHERE id = 512");
        if (await result.MoveNextAsync())
            _sink ^= result.Current[0].AsInteger;
    }

    [Benchmark(Description = "No listeners: pre-parsed primary-key lookup")]
    public async Task FastPrimaryKeyLookupPreparsedAsync()
    {
        await using var result = await _database.ExecuteAsync(_preparsedLookup);
        if (await result.MoveNextAsync())
            _sink ^= result.Current[0].AsInteger;
    }

    [Benchmark(Description = "No listeners: SQL simple insert (autocommit)")]
    public async Task SimpleInsertSqlTextAsync()
    {
        int id = Interlocked.Increment(ref _nextSqlInsertId);
        await using var result = await _database.ExecuteAsync(
            $"INSERT INTO observability_baseline VALUES ({id}, {id * 10L}, 'sql')");
        _sink ^= result.RowsAffected;
    }

    [Benchmark(Description = "No listeners: pre-parsed simple insert (autocommit)")]
    public async Task SimpleInsertPreparsedAsync()
    {
        int id = Interlocked.Increment(ref _nextSimpleInsertId);
        PopulateSimpleInsertValues(id, "preparsed");
        await using var result = await _database.ExecuteAsync(_simpleInsert);
        _sink ^= result.RowsAffected;
    }

    [Benchmark(Description = "No listeners: explicit transaction simple insert")]
    public async Task SimpleInsertExplicitTransactionAsync()
    {
        int id = Interlocked.Increment(ref _nextTransactionalInsertId);
        PopulateSimpleInsertValues(id, "transaction");

        await _database.BeginTransactionAsync();
        try
        {
            await using var result = await _database.ExecuteAsync(_simpleInsert);
            _sink ^= result.RowsAffected;
            await _database.CommitAsync();
        }
        catch
        {
            await _database.RollbackAsync();
            throw;
        }
    }

    [Benchmark(Description = "No listeners: stream 128 rows to exhaustion")]
    public async Task StreamResultToExhaustionAsync()
    {
        await using var result = await _database.ExecuteAsync(
            $"SELECT id, value FROM observability_baseline LIMIT {StreamRowCount}");

        long checksum = 0;
        while (await result.MoveNextAsync())
            checksum ^= result.Current[0].AsInteger;

        _sink ^= checksum;
    }

    private void PopulateSimpleInsertValues(int id, string marker)
    {
        _simpleInsertValues[0] = DbValue.FromInteger(id);
        _simpleInsertValues[1] = DbValue.FromInteger(id * 10L);
        _simpleInsertValues[2] = DbValue.FromText(marker);
    }
}

/// <summary>
/// Stable pooled ADO.NET connection-lifecycle baseline. The prepared physical
/// database remains in the pool; each invocation measures one logical
/// connection construction, open, close, and disposal cycle.
/// </summary>
[BenchmarkCategory("Observability", "Baseline", "NoListeners", "ConnectionPool")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ObservabilityNoListenerConnectionPoolBenchmarks
{
    private string _databasePath = null!;
    private string _connectionString = null!;

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoActivityListeners();
        await CSharpDbConnection.ClearAllPoolsAsync();

        _databasePath = Path.Combine(
            Path.GetTempPath(),
            $"observability_pool_baseline_{Guid.NewGuid():N}.db");

        await using (var database = await Database.OpenAsync(_databasePath))
        {
            await database.ExecuteAsync(
                "CREATE TABLE observability_baseline (id INTEGER PRIMARY KEY, value INTEGER)");
        }

        _connectionString =
            $"Data Source={_databasePath};Pooling=True;Max Pool Size=16;Storage Preset=WriteOptimized;Embedded Open Mode=Direct";

        await using DbConnection warmConnection = CreateConnection();
        await warmConnection.OpenAsync();
        await warmConnection.CloseAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_databasePath);
        DeleteIfExists(_databasePath + ".wal");
        DeleteIfExists(_databasePath + "-wal");
        DeleteIfExists(_databasePath + "-shm");
    }

    [Benchmark(Description = "No listeners: pooled connection open/close/dispose")]
    public async Task OpenClosePooledConnectionAsync()
    {
        await using DbConnection connection = CreateConnection();
        await connection.OpenAsync();
        await connection.CloseAsync();
    }

    private DbConnection CreateConnection()
        => new CSharpDbConnection(_connectionString);

    private static void DeleteIfExists(string? path)
    {
        if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
            File.Delete(path);
    }
}

file static class ObservabilityBaselineGuard
{
    public static void EnsureNoActivityListeners()
    {
        if (CSharpDbDiagnostics.ActivitySource.HasListeners())
        {
            throw new InvalidOperationException(
                "The no-listener observability baseline cannot run with an ActivityListener attached.");
        }
    }
}

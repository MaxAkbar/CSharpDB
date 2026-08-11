using System.Data.Common;
using BenchmarkDotNet.Attributes;
using CSharpDB.Client;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using Microsoft.Extensions.Logging;

namespace CSharpDB.Benchmarks.Micro;

/// <summary>
/// Paired engine-path modes for calculating observability overhead. Disabled
/// retains the Phase 0 no-listener baseline. HistoryCapture enables only the
/// Phase 2 bounded runtime ledger, while StructuredLogging also enables query
/// completion events with one active DiagnosticListener/logger bridge.
/// </summary>
[BenchmarkCategory("Observability", "Qualification", "PairedModes")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ObservabilityNoListenerEngineBenchmarks
{
    private const int SeedRowCount = 1_024;
    private const int StreamRowCount = 128;

    private Database _database = null!;
    private CSharpDbDiagnosticLoggerBridge? _loggerBridge;
    private Statement _preparsedLookup = null!;
    private SimpleInsertSql _simpleInsert;
    private DbValue[] _simpleInsertValues = null!;
    private int _nextSqlInsertId;
    private int _nextSimpleInsertId;
    private int _nextTransactionalInsertId;
    private long _sink;

    [Params(
        ObservabilityBenchmarkMode.Disabled,
        ObservabilityBenchmarkMode.HistoryCapture,
        ObservabilityBenchmarkMode.StructuredLogging)]
    public ObservabilityBenchmarkMode Mode { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoListeners();

        DatabaseOptions? databaseOptions = null;
        if (Mode != ObservabilityBenchmarkMode.Disabled)
        {
            CSharpDbObservabilityOptions observabilityOptions =
                Mode == ObservabilityBenchmarkMode.HistoryCapture
                    ? ObservabilityBenchmarkConfiguration.CreateHistoryCaptureOptions()
                    : ObservabilityBenchmarkConfiguration.CreateStructuredQueryLoggingOptions();
            if (Mode == ObservabilityBenchmarkMode.StructuredLogging)
            {
                _loggerBridge = new CSharpDbDiagnosticLoggerBridge(
                    ObservabilityBenchmarkLoggerFactory.Instance,
                    observabilityOptions);
                ObservabilityBaselineGuard.EnsureStructuredQueryLoggingListener();
            }
            databaseOptions = new DatabaseOptions
            {
                ObservabilityOptions = observabilityOptions,
            };
        }

        _database = databaseOptions is null
            ? await Database.OpenInMemoryAsync()
            : await Database.OpenInMemoryAsync(databaseOptions);
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
        _loggerBridge?.Dispose();
    }

    [Benchmark(Description = "SQL primary-key fast lookup")]
    public async Task FastPrimaryKeyLookupSqlAsync()
    {
        await using var result = await _database.ExecuteAsync(
            "SELECT value FROM observability_baseline WHERE id = 512");
        if (await result.MoveNextAsync())
            _sink ^= result.Current[0].AsInteger;
    }

    [Benchmark(Description = "Pre-parsed primary-key lookup")]
    public async Task FastPrimaryKeyLookupPreparsedAsync()
    {
        await using var result = await _database.ExecuteAsync(_preparsedLookup);
        if (await result.MoveNextAsync())
            _sink ^= result.Current[0].AsInteger;
    }

    [Benchmark(Description = "SQL simple insert (autocommit)")]
    public async Task SimpleInsertSqlTextAsync()
    {
        int id = Interlocked.Increment(ref _nextSqlInsertId);
        await using var result = await _database.ExecuteAsync(
            $"INSERT INTO observability_baseline VALUES ({id}, {id * 10L}, 'sql')");
        _sink ^= result.RowsAffected;
    }

    [Benchmark(Description = "Pre-parsed simple insert (autocommit)")]
    public async Task SimpleInsertPreparsedAsync()
    {
        int id = Interlocked.Increment(ref _nextSimpleInsertId);
        PopulateSimpleInsertValues(id, "preparsed");
        await using var result = await _database.ExecuteAsync(_simpleInsert);
        _sink ^= result.RowsAffected;
    }

    [Benchmark(Description = "Explicit transaction simple insert")]
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

    [Benchmark(Description = "Stream 128 rows to exhaustion")]
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
/// Paired pooled ADO.NET connection-lifecycle modes. The prepared physical
/// database remains in its mode-specific pool; each invocation measures one
/// logical connection construction, open, close, and disposal cycle.
/// </summary>
[BenchmarkCategory("Observability", "Qualification", "PairedModes", "ConnectionPool")]
[MemoryDiagnoser]
[MedianColumn]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class ObservabilityNoListenerConnectionPoolBenchmarks
{
    private string _databasePath = null!;
    private string _connectionString = null!;
    private DatabaseOptions? _databaseOptions;
    private CSharpDbDiagnosticLoggerBridge? _loggerBridge;

    [Params(
        ObservabilityBenchmarkMode.Disabled,
        ObservabilityBenchmarkMode.HistoryCapture,
        ObservabilityBenchmarkMode.StructuredLogging)]
    public ObservabilityBenchmarkMode Mode { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoListeners();
        await CSharpDbConnection.ClearAllPoolsAsync();

        if (Mode != ObservabilityBenchmarkMode.Disabled)
        {
            CSharpDbObservabilityOptions observabilityOptions =
                Mode == ObservabilityBenchmarkMode.HistoryCapture
                    ? ObservabilityBenchmarkConfiguration.CreateHistoryCaptureOptions()
                    : ObservabilityBenchmarkConfiguration.CreateStructuredQueryLoggingOptions();
            if (Mode == ObservabilityBenchmarkMode.StructuredLogging)
            {
                _loggerBridge = new CSharpDbDiagnosticLoggerBridge(
                    ObservabilityBenchmarkLoggerFactory.Instance,
                    observabilityOptions);
                ObservabilityBaselineGuard.EnsureStructuredQueryLoggingListener();
            }
            _databaseOptions = new DatabaseOptions
            {
                ObservabilityOptions = observabilityOptions,
            }.ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());
        }

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
        _loggerBridge?.Dispose();
        DeleteIfExists(_databasePath);
        DeleteIfExists(_databasePath + ".wal");
        DeleteIfExists(_databasePath + "-wal");
        DeleteIfExists(_databasePath + "-shm");
    }

    [Benchmark(Description = "Pooled connection open/close/dispose")]
    public async Task OpenClosePooledConnectionAsync()
    {
        await using DbConnection connection = CreateConnection();
        await connection.OpenAsync();
        await connection.CloseAsync();
    }

    private DbConnection CreateConnection()
        => _databaseOptions is null
            ? new CSharpDbConnection(_connectionString)
            : new CSharpDbConnection(_connectionString, _databaseOptions);

    private static void DeleteIfExists(string? path)
    {
        if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
            File.Delete(path);
    }
}

file static class ObservabilityBaselineGuard
{
    public static void EnsureNoListeners()
    {
        if (CSharpDbDiagnostics.ActivitySource.HasListeners())
        {
            throw new InvalidOperationException(
                "The no-listener observability baseline cannot run with an ActivityListener attached.");
        }

        if (CSharpDbDiagnostics.EventPublisher.IsEnabled(CSharpDbLogEvents.QueryCompleted))
        {
            throw new InvalidOperationException(
                "The disabled observability baseline cannot run with a query diagnostic listener attached.");
        }
    }

    public static void EnsureStructuredQueryLoggingListener()
    {
        if (!CSharpDbDiagnostics.EventPublisher.IsEnabled(CSharpDbLogEvents.QueryCompleted))
        {
            throw new InvalidOperationException(
                "The structured-query-logging mode requires an active diagnostic logger bridge.");
        }
    }
}

public enum ObservabilityBenchmarkMode
{
    Disabled = 0,
    StructuredLogging = 1,
    HistoryCapture = 2,
}

file static class ObservabilityBenchmarkConfiguration
{
    public static CSharpDbObservabilityOptions CreateHistoryCaptureOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "benchmark",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
            },
        };

    public static CSharpDbObservabilityOptions CreateStructuredQueryLoggingOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "benchmark",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = true,
                Queries = true,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };
}

file sealed class ObservabilityBenchmarkLoggerFactory : ILoggerFactory
{
    public static ObservabilityBenchmarkLoggerFactory Instance { get; } = new();

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public ILogger CreateLogger(string categoryName) => ObservabilityBenchmarkLogger.Instance;

    public void Dispose()
    {
    }
}

file sealed class ObservabilityBenchmarkLogger : ILogger
{
    private static int s_sink;

    public static ObservabilityBenchmarkLogger Instance { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        int fieldCount = state is IReadOnlyCollection<KeyValuePair<string, object?>> fields
            ? fields.Count
            : 0;
        Volatile.Write(ref s_sink, fieldCount);
        return ObservabilityBenchmarkScope.Instance;
    }

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        int stateFieldCount = state is IReadOnlyCollection<KeyValuePair<string, object?>> fields
            ? fields.Count
            : 0;
        Volatile.Write(ref s_sink, eventId.Id ^ stateFieldCount);
        GC.KeepAlive(state);
    }
}

file sealed class ObservabilityBenchmarkScope : IDisposable
{
    public static ObservabilityBenchmarkScope Instance { get; } = new();

    public void Dispose()
    {
    }
}

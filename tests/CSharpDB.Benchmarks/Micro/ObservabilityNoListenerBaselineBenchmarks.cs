using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.Metrics;
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
/// Engine-path modes for calculating observability overhead. Disabled retains
/// the Phase 0 no-listener baseline. HistoryCapture and StructuredLogging cover
/// the bounded runtime ledger and diagnostic logger bridge; MetricsOnly and
/// SampledTracing attach one in-process BCL listener for their measured signal.
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
    private ObservabilityBenchmarkListenerSet? _telemetryListeners;
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
        ObservabilityBenchmarkMode.StructuredLogging,
        ObservabilityBenchmarkMode.MetricsOnly,
        ObservabilityBenchmarkMode.SampledTracing)]
    public ObservabilityBenchmarkMode Mode { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoListeners();

        CSharpDbObservabilityOptions? observabilityOptions =
            ObservabilityBenchmarkConfiguration.CreateOptions(Mode);
        DatabaseOptions? databaseOptions = null;
        if (observabilityOptions is not null)
        {
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

        _telemetryListeners = ObservabilityBenchmarkListenerSet.Start(Mode);
        ObservabilityBaselineGuard.EnsureExpectedModeListeners(Mode, _telemetryListeners);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        try
        {
            if (_database is not null)
                await _database.DisposeAsync();
        }
        finally
        {
            try
            {
                _loggerBridge?.Dispose();
                _loggerBridge = null;
            }
            finally
            {
                _telemetryListeners?.Dispose();
                _telemetryListeners = null;
            }
        }
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
    private ObservabilityBenchmarkListenerSet? _telemetryListeners;

    [Params(
        ObservabilityBenchmarkMode.Disabled,
        ObservabilityBenchmarkMode.HistoryCapture,
        ObservabilityBenchmarkMode.StructuredLogging,
        ObservabilityBenchmarkMode.MetricsOnly,
        ObservabilityBenchmarkMode.SampledTracing)]
    public ObservabilityBenchmarkMode Mode { get; set; }

    [GlobalSetup]
    public async Task GlobalSetupAsync()
    {
        ObservabilityBaselineGuard.EnsureNoListeners();
        await CSharpDbConnection.ClearAllPoolsAsync();

        CSharpDbObservabilityOptions? observabilityOptions =
            ObservabilityBenchmarkConfiguration.CreateOptions(Mode);
        if (observabilityOptions is not null)
        {
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

        _telemetryListeners = ObservabilityBenchmarkListenerSet.Start(Mode);
        ObservabilityBaselineGuard.EnsureExpectedModeListeners(Mode, _telemetryListeners);
    }

    [GlobalCleanup]
    public async Task GlobalCleanupAsync()
    {
        try
        {
            await CSharpDbConnection.ClearAllPoolsAsync();
            DeleteIfExists(_databasePath);
            DeleteIfExists(_databasePath + ".wal");
            DeleteIfExists(_databasePath + "-wal");
            DeleteIfExists(_databasePath + "-shm");
        }
        finally
        {
            try
            {
                _loggerBridge?.Dispose();
                _loggerBridge = null;
            }
            finally
            {
                _telemetryListeners?.Dispose();
                _telemetryListeners = null;
            }
        }
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

    public static void EnsureExpectedModeListeners(
        ObservabilityBenchmarkMode mode,
        ObservabilityBenchmarkListenerSet? listeners)
    {
        switch (mode)
        {
            case ObservabilityBenchmarkMode.MetricsOnly
                when listeners is null ||
                     !listeners.HasMetricsListener ||
                     listeners.HasTracingListener:
                throw new InvalidOperationException(
                    "The metrics-only mode requires exactly one benchmark meter listener.");
            case ObservabilityBenchmarkMode.SampledTracing
                when listeners is null ||
                     listeners.HasMetricsListener ||
                     !listeners.HasTracingListener ||
                     !CSharpDbDiagnostics.ActivitySource.HasListeners():
                throw new InvalidOperationException(
                    "The sampled-tracing mode requires exactly one benchmark activity listener.");
            case ObservabilityBenchmarkMode.Disabled or
                 ObservabilityBenchmarkMode.StructuredLogging or
                 ObservabilityBenchmarkMode.HistoryCapture
                when listeners is not null:
                throw new InvalidOperationException(
                    "Only the metrics and tracing benchmark modes may attach telemetry listeners.");
        }
    }
}

public enum ObservabilityBenchmarkMode
{
    Disabled = 0,
    StructuredLogging = 1,
    HistoryCapture = 2,
    MetricsOnly = 3,
    SampledTracing = 4,
}

internal static class ObservabilityBenchmarkConfiguration
{
    public static CSharpDbObservabilityOptions? CreateOptions(ObservabilityBenchmarkMode mode)
        => mode switch
        {
            ObservabilityBenchmarkMode.Disabled => null,
            ObservabilityBenchmarkMode.HistoryCapture => CreateHistoryCaptureOptions(),
            ObservabilityBenchmarkMode.StructuredLogging => CreateStructuredQueryLoggingOptions(),
            ObservabilityBenchmarkMode.MetricsOnly or
                ObservabilityBenchmarkMode.SampledTracing => CreateOpenTelemetryOptions(),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown benchmark mode."),
        };

    private static CSharpDbObservabilityOptions CreateHistoryCaptureOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "benchmark",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
            },
        };

    private static CSharpDbObservabilityOptions CreateStructuredQueryLoggingOptions()
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

    private static CSharpDbObservabilityOptions CreateOpenTelemetryOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "benchmark",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
            },
            OpenTelemetry = new CSharpDbOpenTelemetryOptions
            {
                Enabled = true,
                SamplingRatio = 1,
            },
        };
}

internal sealed class ObservabilityBenchmarkListenerSet : IDisposable
{
    private readonly ActivityListener? _activityListener;
    private readonly MeterListener? _meterListener;
    private int _disposed;

    private ObservabilityBenchmarkListenerSet(
        ActivityListener? activityListener,
        MeterListener? meterListener)
    {
        _activityListener = activityListener;
        _meterListener = meterListener;
    }

    public bool HasTracingListener => _activityListener is not null && _disposed == 0;

    public bool HasMetricsListener => _meterListener is not null && _disposed == 0;

    public bool IsDisposed => _disposed != 0;

    public static ObservabilityBenchmarkListenerSet? Start(ObservabilityBenchmarkMode mode)
        => mode switch
        {
            ObservabilityBenchmarkMode.MetricsOnly => StartMetricsListener(),
            ObservabilityBenchmarkMode.SampledTracing => StartActivityListener(),
            ObservabilityBenchmarkMode.Disabled or
                ObservabilityBenchmarkMode.StructuredLogging or
                ObservabilityBenchmarkMode.HistoryCapture => null,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown benchmark mode."),
        };

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _meterListener?.Dispose();
        _activityListener?.Dispose();
    }

    private static ObservabilityBenchmarkListenerSet StartActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source =>
                source.Name == CSharpDbDiagnostics.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
            SampleUsingParentId = static (ref ActivityCreationOptions<string> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
            ActivityStarted = static activity =>
                ObservabilityBenchmarkTelemetrySink.ConsumeActivity(activity),
            ActivityStopped = static activity =>
                ObservabilityBenchmarkTelemetrySink.ConsumeActivity(activity),
        };
        ActivitySource.AddActivityListener(listener);
        return new ObservabilityBenchmarkListenerSet(listener, meterListener: null);
    }

    private static ObservabilityBenchmarkListenerSet StartMetricsListener()
    {
        var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                    meterListener.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>(
            static (_, measurement, tags, _) =>
                ObservabilityBenchmarkTelemetrySink.ConsumeMeasurement(
                    measurement,
                    tags.Length));
        listener.SetMeasurementEventCallback<double>(
            static (_, measurement, tags, _) =>
                ObservabilityBenchmarkTelemetrySink.ConsumeMeasurement(
                    BitConverter.DoubleToInt64Bits(measurement),
                    tags.Length));
        listener.Start();
        return new ObservabilityBenchmarkListenerSet(activityListener: null, listener);
    }
}

file static class ObservabilityBenchmarkTelemetrySink
{
    private static long s_sink;

    public static void ConsumeActivity(Activity activity)
        => Volatile.Write(
            ref s_sink,
            activity.Duration.Ticks ^ activity.SpanId.GetHashCode());

    public static void ConsumeMeasurement(long measurement, int tagCount)
        => Volatile.Write(ref s_sink, measurement ^ tagCount);
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

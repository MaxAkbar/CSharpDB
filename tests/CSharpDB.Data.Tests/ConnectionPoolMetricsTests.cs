using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ConnectionPoolMetricsTests : IAsyncLifetime
{
    private readonly string _databasePath = Path.Combine(
        Path.GetTempPath(),
        $"csharpdb-pool-metrics-{Guid.NewGuid():N}.db");
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_databasePath);
        DeleteIfExists(_databasePath + ".wal");
    }

    [Fact]
    public async Task ContendedOpen_PublishesPoolGaugesAndCanceledWaitDuration()
    {
        using var listener = new MeterListener();
        var latest = new ConcurrentDictionary<string, ObservedLong>(StringComparer.Ordinal);
        var waitDurations = new ConcurrentBag<ObservedDouble>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            KeyValuePair<string, object?>[] copiedTags = tags.ToArray();
            if (HasAlias(copiedTags, "pool-metrics"))
                latest[instrument.Name] = new ObservedLong(value, copiedTags);
        });
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            KeyValuePair<string, object?>[] copiedTags = tags.ToArray();
            if (instrument.Name == CSharpDbMetricInstrumentNames.PoolWaitDuration &&
                HasAlias(copiedTags, "pool-metrics"))
            {
                waitDurations.Add(new ObservedDouble(value, copiedTags));
            }
        });
        listener.Start();

        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "pool-metrics",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        observability.OpenTelemetry.Enabled = true;
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        };
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1";
        await using var blocker = new CSharpDbConnection(
            connectionString,
            databaseOptions);
        await using var queued = new CSharpDbConnection(
            connectionString,
            databaseOptions);
        await blocker.OpenAsync(Ct);

        listener.RecordObservableInstruments();
        AssertGauge(latest, CSharpDbMetricInstrumentNames.SessionsActive, 1);
        AssertGauge(latest, CSharpDbMetricInstrumentNames.ReadersActive, 0);
        AssertGauge(latest, CSharpDbMetricInstrumentNames.PoolWaiters, 0);
        AssertGauge(latest, CSharpDbMetricInstrumentNames.ConnectionsAvailable, 0);

        using var waitCancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task queuedOpen = queued.OpenAsync(waitCancellation.Token);
        bool observedWaiter = false;
        for (int attempt = 0; attempt < 500 && !observedWaiter; attempt++)
        {
            listener.RecordObservableInstruments();
            observedWaiter = latest.TryGetValue(
                    CSharpDbMetricInstrumentNames.PoolWaiters,
                    out ObservedLong? waiters) &&
                waiters.Value == 1;
            if (!observedWaiter)
                await Task.Delay(1, Ct);
        }

        Assert.True(observedWaiter, "The contended open never entered the bounded pool waiter gauge.");
        waitCancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedOpen);

        ObservedDouble wait = Assert.Single(waitDurations);
        Assert.True(wait.Value >= 0);
        AssertTag(wait.Tags, CSharpDbMetricTagNames.Outcome, "canceled");
        AssertTag(wait.Tags, CSharpDbMetricTagNames.Transport, "direct");
        AssertTag(wait.Tags, CSharpDbMetricTagNames.DatabaseAlias, "pool-metrics");

        await blocker.CloseAsync();
        listener.RecordObservableInstruments();
        AssertGauge(latest, CSharpDbMetricInstrumentNames.SessionsActive, 0);
        AssertGauge(latest, CSharpDbMetricInstrumentNames.ConnectionsAvailable, 1);
    }

    [Fact]
    public async Task NonPooledSession_PublishesSessionAndReaderGaugesOnly()
    {
        const string alias = "direct-session-metrics";
        using var listener = new MeterListener();
        var latest = new ConcurrentDictionary<string, ObservedLong>(
            StringComparer.Ordinal);
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            KeyValuePair<string, object?>[] copiedTags = tags.ToArray();
            if (HasAlias(copiedTags, alias))
                latest[instrument.Name] = new ObservedLong(value, copiedTags);
        });
        listener.Start();

        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        observability.Prometheus.Enabled = true;
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            new DatabaseOptions { ObservabilityOptions = observability });
        await connection.OpenAsync(Ct);

        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.SessionsActive,
            1,
            alias);
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.ReadersActive,
            0,
            alias);
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.PoolWaiters,
            latest.Keys);
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.ConnectionsAvailable,
            latest.Keys);

        await using (CSharpDbCommand create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE metric_rows (id INTEGER PRIMARY KEY)";
            await create.ExecuteNonQueryAsync(Ct);
        }
        await using (CSharpDbCommand insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO metric_rows (id) VALUES (1)";
            await insert.ExecuteNonQueryAsync(Ct);
        }
        await using CSharpDbCommand select = connection.CreateCommand();
        select.CommandText = "SELECT id FROM metric_rows";
        await using var reader = await select.ExecuteReaderAsync(Ct);

        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.ReadersActive,
            1,
            alias);
        await reader.DisposeAsync();
        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.ReadersActive,
            0,
            alias);
    }

    [Fact]
    public async Task SharedMemoryHost_AggregatesLogicalSessionsAndRetiresProvider()
    {
        const string alias = "shared-session-metrics";
        string sharedName = $"metrics-{Guid.NewGuid():N}";
        using var listener = new MeterListener();
        var latest = new ConcurrentDictionary<string, ObservedLong>(
            StringComparer.Ordinal);
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            KeyValuePair<string, object?>[] copiedTags = tags.ToArray();
            if (HasAlias(copiedTags, alias))
                latest[instrument.Name] = new ObservedLong(value, copiedTags);
        });
        listener.Start();

        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        observability.Prometheus.Enabled = true;
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        };
        string connectionString =
            $"Data Source=:memory:{sharedName};Pooling=false";
        await using var first = new CSharpDbConnection(
            connectionString,
            databaseOptions);
        await using var second = new CSharpDbConnection(
            connectionString,
            databaseOptions);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.SessionsActive,
            2,
            alias);
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.ReadersActive,
            0,
            alias);
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.PoolWaiters,
            latest.Keys);
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.ConnectionsAvailable,
            latest.Keys);

        await first.CloseAsync();
        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.SessionsActive,
            1,
            alias);
        await second.CloseAsync();
        listener.RecordObservableInstruments();
        AssertGauge(
            latest,
            CSharpDbMetricInstrumentNames.SessionsActive,
            0,
            alias);

        await CSharpDbConnection.ClearAllPoolsAsync();
        latest.Clear();
        listener.RecordObservableInstruments();
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.SessionsActive,
            latest.Keys);
        Assert.DoesNotContain(
            CSharpDbMetricInstrumentNames.ReadersActive,
            latest.Keys);
    }

    private static void AssertGauge(
        IReadOnlyDictionary<string, ObservedLong> latest,
        string name,
        long expected,
        string alias = "pool-metrics")
    {
        ObservedLong measurement = Assert.IsType<ObservedLong>(latest[name]);
        Assert.Equal(expected, measurement.Value);
        AssertTag(measurement.Tags, CSharpDbMetricTagNames.Transport, "direct");
        AssertTag(
            measurement.Tags,
            CSharpDbMetricTagNames.DatabaseAlias,
            alias);
        Assert.All(
            measurement.Tags,
            static tag => Assert.True(CSharpDbMetricTagNames.IsAllowed(tag.Key)));
    }

    private static bool HasAlias(
        IEnumerable<KeyValuePair<string, object?>> tags,
        string alias)
        => tags.Any(tag =>
            tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
            Equals(tag.Value, alias));

    private static void AssertTag(
        IEnumerable<KeyValuePair<string, object?>> tags,
        string key,
        string value)
        => Assert.Contains(tags, tag => tag.Key == key && Equals(tag.Value, value));

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed record ObservedLong(
        long Value,
        KeyValuePair<string, object?>[] Tags);

    private sealed record ObservedDouble(
        double Value,
        KeyValuePair<string, object?>[] Tags);
}

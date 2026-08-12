using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

[Collection(MetricsContractCollection.Name)]
public sealed class HealthMetricsContractTests
{
    [Fact]
    public void HealthStatusGauge_EmitsOnlyCurrentBoundedLivenessAndReadinessSeries()
    {
        using var listener = new MeterListener();
        var measurements = new ConcurrentQueue<ObservedHealth>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName &&
                instrument.Name == CSharpDbMetricInstrumentNames.HealthStatus)
            {
                Assert.IsAssignableFrom<ObservableGauge<long>>(instrument);
                Assert.Equal(CSharpDbMetricUnits.Status, instrument.Unit);
                currentListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) =>
            {
                if (instrument.Name == CSharpDbMetricInstrumentNames.HealthStatus)
                {
                    measurements.Enqueue(new ObservedHealth(
                        value,
                        tags.ToArray()));
                }
            });
        listener.Start();

        var state = new CSharpDbHostState(
            TimeProvider.System,
            static _ => { });
        using CSharpDbHealthMetricSource source = Assert.IsType<
            CSharpDbHealthMetricSource>(
                CSharpDbHealthMetricSource.TryCreate(
                    state,
                    "health-primary"));

        listener.RecordObservableInstruments();
        AssertCurrent(
            measurements,
            ("liveness", "healthy"),
            ("readiness", "unhealthy"));

        state.MarkRunning(CSharpDbReadinessReason.ExclusiveMaintenance);
        Clear(measurements);
        listener.RecordObservableInstruments();
        AssertCurrent(
            measurements,
            ("liveness", "healthy"),
            ("readiness", "unhealthy"));

        state.MarkReady();
        Clear(measurements);
        listener.RecordObservableInstruments();
        AssertCurrent(
            measurements,
            ("liveness", "healthy"),
            ("readiness", "healthy"));

        state.MarkStopping();
        state.MarkStopped();
        Clear(measurements);
        listener.RecordObservableInstruments();
        AssertCurrent(
            measurements,
            ("liveness", "unhealthy"),
            ("readiness", "unhealthy"));
    }

    [Fact]
    public void HealthMetricRegistry_IsAliasBoundedAndDisposalUnrootsSources()
    {
        Assert.Equal(0, CSharpDbHealthMetricsRegistry.RegisteredCount);
        var sources = new List<CSharpDbHealthMetricSource>();
        try
        {
            CSharpDbHealthMetricSource first = Assert.IsType<
                CSharpDbHealthMetricSource>(
                    CSharpDbHealthMetricSource.TryCreate(
                        CreateState(),
                        "health-0"));
            sources.Add(first);
            Assert.Null(CSharpDbHealthMetricSource.TryCreate(
                CreateState(),
                "health-0"));
            Assert.Throws<ArgumentException>(() =>
                CSharpDbHealthMetricSource.TryCreate(
                    CreateState(),
                    "C:\\private\\database.db"));

            for (int index = 1;
                 index < CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases;
                 index++)
            {
                CSharpDbHealthMetricSource? source =
                    CSharpDbHealthMetricSource.TryCreate(
                        CreateState(),
                        $"health-{index}");
                sources.Add(Assert.IsType<CSharpDbHealthMetricSource>(source));
            }

            Assert.Equal(
                CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases,
                CSharpDbHealthMetricsRegistry.RegisteredCount);
            Assert.Null(CSharpDbHealthMetricSource.TryCreate(
                CreateState(),
                "health-overflow"));
        }
        finally
        {
            foreach (CSharpDbHealthMetricSource source in sources)
                source.Dispose();
        }

        Assert.Equal(0, CSharpDbHealthMetricsRegistry.RegisteredCount);
        Assert.Empty(CSharpDbHealthMetricsRegistry.Observe());
    }

    [Fact]
    public void HostState_DefaultObserverPublishesTypedDistinctTransitionEvents()
    {
        var received = new List<KeyValuePair<string, object?>>();
        using IDisposable subscription =
            CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                new CapturingObserver(received),
                static (name, _, _) =>
                    name == CSharpDbLogEvents.HealthTransition.Name);

        var state = new CSharpDbHostState();
        state.MarkReady();
        state.MarkReady();
        state.MarkNotReady(CSharpDbReadinessReason.ReadOnly);

        Assert.Equal(3, received.Count);
        CSharpDbHealthTransitionEvent[] transitions = received
            .Select(static item =>
                Assert.IsType<CSharpDbHealthTransitionEvent>(item.Value))
            .ToArray();
        Assert.All(
            received,
            static item => Assert.Equal(
                CSharpDbLogEvents.HealthTransition.Name,
                item.Key));
        Assert.Equal(
            [
                CSharpDbReadinessReason.Starting,
                CSharpDbReadinessReason.None,
                CSharpDbReadinessReason.ReadOnly,
            ],
            transitions.Select(static item => item.State.ReadinessReason));
    }

    private static void AssertCurrent(
        ConcurrentQueue<ObservedHealth> measurements,
        params (string Check, string Status)[] expected)
    {
        ObservedHealth[] observed = measurements.ToArray();
        Assert.Equal(expected.Length, observed.Length);
        foreach ((string check, string status) in expected)
        {
            ObservedHealth measurement = Assert.Single(
                observed,
                item => Tag(item, CSharpDbMetricTagNames.CheckKind) == check);
            Assert.Equal(1, measurement.Value);
            Assert.Equal(
                status,
                Tag(measurement, CSharpDbMetricTagNames.Status));
            Assert.Equal(
                "health-primary",
                Tag(measurement, CSharpDbMetricTagNames.DatabaseAlias));
            Assert.Equal(3, measurement.Tags.Length);
            Assert.All(
                measurement.Tags,
                static tag => Assert.Contains(
                    tag.Key,
                    (IReadOnlySet<string>)CSharpDbMetricTagNames.Allowed));
        }
    }

    private static string? Tag(ObservedHealth measurement, string name)
        => measurement.Tags.SingleOrDefault(tag => tag.Key == name).Value as string;

    private static void Clear(ConcurrentQueue<ObservedHealth> measurements)
    {
        while (measurements.TryDequeue(out _))
        {
        }
    }

    private static CSharpDbHostState CreateState()
        => new(TimeProvider.System, static _ => { });

    private sealed class CapturingObserver(
        List<KeyValuePair<string, object?>> received) :
        IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
            => received.Add(value);
    }

    private sealed record ObservedHealth(
        long Value,
        KeyValuePair<string, object?>[] Tags);
}

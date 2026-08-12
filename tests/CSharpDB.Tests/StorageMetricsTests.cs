using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class StorageMetricsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentComponentCandidates_RegisterMetricsOnRetainedWinner()
    {
        const int contenderCount = 8;
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-metrics-component-race",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        options.OpenTelemetry.Enabled = true;
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        using var constructorsEntered = new CountdownEvent(contenderCount);
        using var releaseConstructors = new ManualResetEventSlim();
        StorageRuntimeDiagnostics.BeforeConstructionForTest = () =>
        {
            constructorsEntered.Signal();
            releaseConstructors.Wait(TimeSpan.FromSeconds(10), Ct);
        };

        StorageRuntimeDiagnostics.Registration?[] registrations;
        try
        {
            Task<StorageRuntimeDiagnostics.Registration?>[] contenders =
                Enumerable.Range(0, contenderCount)
                    .Select(_ => Task.Run(
                        () => StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                            state,
                            recoveryApplicable: false),
                        Ct))
                    .ToArray();

            Assert.True(
                constructorsEntered.Wait(TimeSpan.FromSeconds(10), Ct),
                "All component candidates must reach construction before release.");
            releaseConstructors.Set();
            registrations = await Task.WhenAll(contenders);
        }
        finally
        {
            releaseConstructors.Set();
            StorageRuntimeDiagnostics.BeforeConstructionForTest = null;
        }

        Assert.All(registrations, static registration => Assert.NotNull(registration));
        Assert.True(state.TryGetComponent(out StorageRuntimeDiagnostics? retained));
        Assert.NotNull(retained);
        Assert.True(retained.HasMetricsRegistrationForTest);

        foreach (StorageRuntimeDiagnostics.Registration? registration in registrations)
            registration?.Dispose();
    }

    [Fact]
    public async Task InMemoryOpen_PublishesAvailableStorageAndWalScalarsWithoutPoolActivity()
    {
        using var listener = new MeterListener();
        var measurements = new ConcurrentBag<ObservedLong>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => measurements.Add(
                new ObservedLong(instrument.Name, value, tags.ToArray())));
        listener.Start();

        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-metrics-memory",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        options.OpenTelemetry.Enabled = true;
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = options,
                RuntimeDiagnosticsState = state,
            },
            Ct);

        listener.RecordObservableInstruments();

        AssertMeasurement(
            measurements,
            CSharpDbMetricInstrumentNames.StorageLogicalBytes,
            "storage-metrics-memory",
            static value => value > 0);
        AssertMeasurement(
            measurements,
            CSharpDbMetricInstrumentNames.StoragePageCount,
            "storage-metrics-memory",
            static value => value > 0);
        AssertMeasurement(
            measurements,
            CSharpDbMetricInstrumentNames.StorageCommits,
            "storage-metrics-memory",
            static value => value >= 0);
        AssertMeasurement(
            measurements,
            CSharpDbMetricInstrumentNames.WalLogicalBytes,
            "storage-metrics-memory",
            static value => value > 0);
        AssertMeasurement(
            measurements,
            CSharpDbMetricInstrumentNames.WalFrameCount,
            "storage-metrics-memory",
            static value => value >= 0);

        Assert.DoesNotContain(
            measurements,
            static measurement =>
                measurement.Name ==
                    CSharpDbMetricInstrumentNames.StorageAllocatedBytes &&
                measurement.Tags.Any(static tag =>
                    tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                    Equals(tag.Value, "storage-metrics-memory")));
        Assert.DoesNotContain(
            measurements,
            static measurement =>
                measurement.Name ==
                    CSharpDbMetricInstrumentNames.WalAllocatedBytes &&
                measurement.Tags.Any(static tag =>
                    tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                    Equals(tag.Value, "storage-metrics-memory")));
    }

    [Fact]
    public void StorageOperationEvents_RecordExactOnceAndClearActiveGauges()
    {
        using var listener = new MeterListener();
        var longMeasurements = new ConcurrentQueue<ObservedLong>();
        var doubleMeasurements = new ConcurrentQueue<ObservedDouble>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => longMeasurements.Enqueue(
                new ObservedLong(instrument.Name, value, tags.ToArray())));
        listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => doubleMeasurements.Enqueue(
                new ObservedDouble(instrument.Name, value, tags.ToArray())));
        listener.Start();

        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 11, 0, 0, TimeSpan.Zero));
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "storage-operation-metrics",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        options.Prometheus.Enabled = true;
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using StorageRuntimeDiagnostics.Registration registration =
            Assert.IsType<StorageRuntimeDiagnostics.Registration>(
                StorageRuntimeDiagnostics.TryBeginBuiltInOpen(
                    state,
                    recoveryApplicable: true));

        registration.Observer.OnRecoveryStarted();
        listener.RecordObservableInstruments();
        Assert.Equal(
            1,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.WalRecoveriesActive));
        clock.Advance(TimeSpan.FromSeconds(2));
        StorageRecoveryRuntimeRawSnapshot recovery = RecoveryRaw();
        registration.Observer.OnRecoveryCompleted(recovery);
        registration.Observer.OnRecoveryCompleted(recovery);
        listener.RecordObservableInstruments();
        Assert.Equal(
            0,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.WalRecoveriesActive));

        StorageCheckpointRuntimeRawSnapshot automaticStart = CheckpointRaw(
            StorageCheckpointPhaseRaw.Copying,
            StorageRuntimeOperationOutcomeRaw.Running,
            StorageCheckpointOriginRaw.BackgroundAuto);
        registration.Observer.OnCheckpointStarted(automaticStart, correlation: null);
        listener.RecordObservableInstruments();
        Assert.Equal(
            1,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.CheckpointsActive));
        clock.Advance(TimeSpan.FromSeconds(3));
        StorageCheckpointRuntimeRawSnapshot automaticCompleted = CheckpointRaw(
            StorageCheckpointPhaseRaw.Idle,
            StorageRuntimeOperationOutcomeRaw.Succeeded,
            StorageCheckpointOriginRaw.BackgroundAuto);
        registration.Observer.OnCheckpointCompleted(
            automaticCompleted,
            correlation: null);
        registration.Observer.OnCheckpointCompleted(
            automaticCompleted,
            correlation: null);

        StorageCheckpointRuntimeRawSnapshot manualStart = CheckpointRaw(
            StorageCheckpointPhaseRaw.Copying,
            StorageRuntimeOperationOutcomeRaw.Running,
            StorageCheckpointOriginRaw.Manual);
        registration.Observer.OnCheckpointStarted(manualStart, correlation: null);
        clock.Advance(TimeSpan.FromSeconds(1));
        registration.Observer.OnCheckpointCompleted(
            CheckpointRaw(
                StorageCheckpointPhaseRaw.Faulted,
                StorageRuntimeOperationOutcomeRaw.Failed,
                StorageCheckpointOriginRaw.Manual),
            correlation: null);
        registration.Observer.OnWalFlushCompleted(logicalCommitCount: 3);
        listener.RecordObservableInstruments();

        Assert.Equal(
            0,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.CheckpointsActive));
        Assert.Single(
            longMeasurements,
            static item =>
                item.Name == CSharpDbMetricInstrumentNames.WalRecoveries);
        Assert.Single(
            doubleMeasurements,
            static item =>
                item.Name == CSharpDbMetricInstrumentNames.WalRecoveryDuration);
        Assert.Equal(
            2,
            longMeasurements.Count(static item =>
                item.Name == CSharpDbMetricInstrumentNames.Checkpoints));
        Assert.Equal(
            2,
            doubleMeasurements.Count(static item =>
                item.Name == CSharpDbMetricInstrumentNames.CheckpointDuration));
        Assert.Equal(
            1,
            LastDoubleGauge(
                doubleMeasurements,
                CSharpDbMetricInstrumentNames.CheckpointAge),
            precision: 6);
        ObservedLong batch = Assert.Single(
            longMeasurements,
            static item =>
                item.Name == CSharpDbMetricInstrumentNames.WalCommitBatchSize);
        Assert.Equal(3, batch.Value);

        registration.Observer.OnRecoveryStarted();
        registration.Observer.OnCheckpointStarted(manualStart, correlation: null);
        listener.RecordObservableInstruments();
        Assert.Equal(
            1,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.WalRecoveriesActive));
        Assert.Equal(
            1,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.CheckpointsActive));
        registration.Dispose();
        listener.RecordObservableInstruments();
        Assert.Equal(
            0,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.WalRecoveriesActive));
        Assert.Equal(
            0,
            LastGauge(
                longMeasurements,
                CSharpDbMetricInstrumentNames.CheckpointsActive));
        Assert.All(
            longMeasurements.Concat(
                doubleMeasurements.Select(static item => new ObservedLong(
                    item.Name,
                    0,
                    item.Tags))),
            static measurement => Assert.All(
                measurement.Tags,
                tag => Assert.True(
                    CSharpDbMetricTagNames.Allowed.Contains(tag.Key),
                    $"Unexpected metric tag '{tag.Key}'.")));
    }

    private static void AssertMeasurement(
        IEnumerable<ObservedLong> measurements,
        string name,
        string alias,
        Func<long, bool> valuePredicate)
    {
        ObservedLong measurement = Assert.Single(
            measurements,
            item =>
                item.Name == name &&
                item.Tags.Any(tag =>
                    tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                    Equals(tag.Value, alias)));
        Assert.True(valuePredicate(measurement.Value));
        Assert.Collection(
            measurement.Tags,
            tag =>
            {
                Assert.Equal(CSharpDbMetricTagNames.DatabaseAlias, tag.Key);
                Assert.Equal(alias, tag.Value);
            });
    }

    private static long LastGauge(
        IEnumerable<ObservedLong> measurements,
        string name)
        => measurements.Last(item =>
            item.Name == name &&
            item.Tags.Any(tag =>
                tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                Equals(tag.Value, "storage-operation-metrics"))).Value;

    private static double LastDoubleGauge(
        IEnumerable<ObservedDouble> measurements,
        string name)
        => measurements.Last(item =>
            item.Name == name &&
            item.Tags.Any(tag =>
                tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                Equals(tag.Value, "storage-operation-metrics"))).Value;

    private static StorageRecoveryRuntimeRawSnapshot RecoveryRaw()
        => new(
            StorageRecoveryPhaseRaw.Completed,
            ScannedFrameCount: 4,
            ScannedBytes: 4096,
            RecoveredFrameCount: 4,
            RecoveredBytes: 4096,
            DiscardedFrameCount: 0,
            DiscardedBytes: 0,
            TruncationReason: StorageRecoveryTruncationReasonRaw.None,
            AttemptCount: 1,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            Outcome: StorageRuntimeOperationOutcomeRaw.Succeeded,
            FailureKind: StorageRuntimeFailureKindRaw.None);

    private static StorageCheckpointRuntimeRawSnapshot CheckpointRaw(
        StorageCheckpointPhaseRaw phase,
        StorageRuntimeOperationOutcomeRaw outcome,
        StorageCheckpointOriginRaw origin)
        => new(
            phase,
            origin,
            CompletedPageCount: phase == StorageCheckpointPhaseRaw.Idle ? 1 : 0,
            TotalPageCount: 1,
            RetentionReason: StorageCheckpointRetentionReasonRaw.None,
            Outcome: outcome,
            FailureKind: outcome == StorageRuntimeOperationOutcomeRaw.Failed
                ? StorageRuntimeFailureKindRaw.Io
                : StorageRuntimeFailureKindRaw.None);

    private sealed record ObservedLong(
        string Name,
        long Value,
        KeyValuePair<string, object?>[] Tags);

    private sealed record ObservedDouble(
        string Name,
        double Value,
        KeyValuePair<string, object?>[] Tags);

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
        {
            _utcNow = _utcNow.Add(elapsed);
            Interlocked.Add(ref _timestamp, elapsed.Ticks);
        }
    }
}

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Reflection;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MetricsContractCollection
{
    public const string Name = "MetricsContract";
}

[Collection(MetricsContractCollection.Name)]
public sealed class MetricsContractTests
{
    [Fact]
    public void MetricsEnabled_EagerlyPublishesStaticStorageInstrumentsWithoutPoolWork()
    {
        using var listener = new MeterListener();
        var published = new ConcurrentDictionary<string, Instrument>(
            StringComparer.Ordinal);
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                published[instrument.Name] = instrument;
        };
        listener.Start();

        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-eager-publication"));

        Instrument storage = Assert.IsAssignableFrom<ObservableGauge<long>>(
            published[CSharpDbMetricInstrumentNames.StorageLogicalBytes]);
        Assert.Equal(CSharpDbDiagnostics.InstrumentationVersion, storage.Meter.Version);
        Assert.Equal(CSharpDbMetricUnits.Bytes, storage.Unit);
        Assert.False(string.IsNullOrWhiteSpace(storage.Description));
        Assert.NotNull(state.RuntimeMetrics);
    }

    [Fact]
    public void MetricsGate_IsMasteredAndIndependentOfLogging()
    {
        var disabled = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "metrics-disabled",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        using var disabledState = new CSharpDbRuntimeDiagnosticsState(disabled);
        Assert.False(disabledState.MetricsEnabled);
        Assert.False(disabledState.TracingEnabled);
        Assert.Null(disabledState.RuntimeMetrics);

        CSharpDbObservabilityOptions prometheus = CreateMetricsOptions(
            "metrics-prometheus",
            openTelemetry: false,
            prometheus: true);
        using var prometheusState = new CSharpDbRuntimeDiagnosticsState(prometheus);
        Assert.True(prometheusState.MetricsEnabled);
        Assert.False(prometheusState.TracingEnabled);
        Assert.NotNull(prometheusState.RuntimeMetrics);

        CSharpDbObservabilityOptions openTelemetry = CreateMetricsOptions(
            "metrics-otel",
            openTelemetry: true,
            prometheus: false);
        using var openTelemetryState = new CSharpDbRuntimeDiagnosticsState(openTelemetry);
        Assert.True(openTelemetryState.MetricsEnabled);
        Assert.True(openTelemetryState.TracingEnabled);
        Assert.NotNull(openTelemetryState.RuntimeMetrics);
    }

    [Fact]
    public void MetricNamesAndCounterSemantics_AreCompleteStableAndUnique()
    {
        string[] names = typeof(CSharpDbMetricInstrumentNames)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(static field => field.IsLiteral && field.FieldType == typeof(string))
            .Select(static field => Assert.IsType<string>(field.GetRawConstantValue()))
            .ToArray();

        Assert.Equal("1.0", CSharpDbDiagnostics.MetricSchemaVersion);
        Assert.Equal(names.Length, names.Distinct(StringComparer.Ordinal).Count());
        var histograms = new HashSet<string>(StringComparer.Ordinal)
        {
            CSharpDbMetricInstrumentNames.QueryDuration,
            CSharpDbMetricInstrumentNames.TransactionDuration,
            CSharpDbMetricInstrumentNames.MaintenanceDuration,
            CSharpDbMetricInstrumentNames.CheckpointDuration,
            CSharpDbMetricInstrumentNames.WalRecoveryDuration,
            CSharpDbMetricInstrumentNames.WalCommitBatchSize,
            CSharpDbMetricInstrumentNames.PoolWaitDuration,
        };
        Assert.All(
            names.Where(name => !histograms.Contains(name)),
            static name => Assert.True(
                CSharpDbCounterSemantics.Instruments.ContainsKey(name),
                $"Counter semantics are missing for '{name}'."));
        Assert.All(
            histograms,
            static name => Assert.DoesNotContain(
                name,
                CSharpDbCounterSemantics.Instruments.Keys));
    }

    [Fact]
    public void ObservableRegistry_IsBoundedAndDisposalUnrootsEveryRuntime()
    {
        Assert.Equal(0, CSharpDbRuntimeMetricsRegistry.RegisteredCount);
        var states = new List<CSharpDbRuntimeDiagnosticsState>();
        try
        {
            for (int index = 0;
                index < CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies;
                 index++)
            {
                states.Add(new CSharpDbRuntimeDiagnosticsState(
                    CreateMetricsOptions("metrics-family-capacity")));
            }

            Assert.Equal(
                CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies,
                CSharpDbRuntimeMetricsRegistry.RegisteredCount);
            using var overflow = new CSharpDbRuntimeDiagnosticsState(
                CreateMetricsOptions("metrics-family-capacity"));
            Assert.True(overflow.MetricsEnabled);
            Assert.Null(overflow.RuntimeMetrics);
            Assert.Equal(
                CSharpDbDiagnostics.MaximumRuntimeDiagnosticsFamilies,
                CSharpDbRuntimeMetricsRegistry.RegisteredCount);
        }
        finally
        {
            foreach (CSharpDbRuntimeDiagnosticsState state in states)
                state.Dispose();
        }

        Assert.Equal(0, CSharpDbRuntimeMetricsRegistry.RegisteredCount);
    }

    [Fact]
    public void ObservableCollection_EmitsLegitimateZeroAndOmitsUnavailableFields()
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

        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-availability"));
        using IDisposable registration = Assert.IsAssignableFrom<IDisposable>(
            state.RuntimeMetrics?.RegisterStorageProvider(
                new ZeroAndUnavailableStorageProvider()));

        listener.RecordObservableInstruments();

        ObservedLong logical = Assert.Single(
            measurements,
            static item => item.Name ==
                CSharpDbMetricInstrumentNames.StorageLogicalBytes);
        Assert.Equal(0, logical.Value);
        Assert.Collection(
            logical.Tags,
            tag =>
            {
                Assert.Equal(CSharpDbMetricTagNames.DatabaseAlias, tag.Key);
                Assert.Equal("metrics-availability", tag.Value);
            });
        Assert.DoesNotContain(
            measurements,
            static item => item.Name ==
                CSharpDbMetricInstrumentNames.StorageAllocatedBytes);
    }

    [Fact]
    public void ObservableCollection_AggregatesSameAliasIntoOneDeterministicSeries()
    {
        using var listener = new MeterListener();
        var measurements = new ConcurrentQueue<ObservedLong>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => measurements.Enqueue(
                new ObservedLong(instrument.Name, value, tags.ToArray())));
        listener.Start();

        using var first = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-shared-alias"));
        using var second = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-shared-alias"));
        using var absentProviderSibling = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-shared-alias"));
        using IDisposable firstProvider = Assert.IsAssignableFrom<IDisposable>(
            first.RuntimeMetrics?.RegisterStorageProvider(
                new FixedStorageProvider(logicalBytes: 2, commits: 5)));
        using IDisposable secondProvider = Assert.IsAssignableFrom<IDisposable>(
            second.RuntimeMetrics?.RegisterStorageProvider(
                new FixedStorageProvider(logicalBytes: 3, commits: 7)));
        Assert.True(first.RuntimeMetrics?.QueryStarted() == true);
        Assert.True(second.RuntimeMetrics?.QueryStarted() == true);

        listener.RecordObservableInstruments();
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.StorageLogicalBytes,
            5);
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.StorageCommits,
            12);
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.QueriesActive,
            2);

        using (var unavailableSibling = new CSharpDbRuntimeDiagnosticsState(
                   CreateMetricsOptions("metrics-shared-alias")))
        using (IDisposable unavailableProvider = Assert.IsAssignableFrom<IDisposable>(
                   unavailableSibling.RuntimeMetrics?.RegisterStorageProvider(
                       new UnavailableStorageProvider())))
        {
            while (measurements.TryDequeue(out _))
            {
            }

            listener.RecordObservableInstruments();
            Assert.DoesNotContain(
                measurements,
                static item =>
                    (item.Name is
                        CSharpDbMetricInstrumentNames.StorageLogicalBytes or
                        CSharpDbMetricInstrumentNames.StorageCommits) &&
                    item.Tags.Any(static tag =>
                        tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                        Equals(tag.Value, "metrics-shared-alias")));
            AssertSingleValue(
                measurements,
                CSharpDbMetricInstrumentNames.QueriesActive,
                2);
        }

        first.RuntimeMetrics?.QueryAbandoned(metricsStarted: true);
        first.Dispose();
        while (measurements.TryDequeue(out _))
        {
        }

        listener.RecordObservableInstruments();
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.StorageLogicalBytes,
            3);
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.StorageCommits,
            12);
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.QueriesActive,
            1);

        using (var replacement = new CSharpDbRuntimeDiagnosticsState(
                   CreateMetricsOptions("metrics-shared-alias")))
        using (IDisposable replacementProvider = Assert.IsAssignableFrom<IDisposable>(
                   replacement.RuntimeMetrics?.RegisterStorageProvider(
                       new FixedStorageProvider(logicalBytes: 1, commits: 2))))
        {
            while (measurements.TryDequeue(out _))
            {
            }

            listener.RecordObservableInstruments();
            AssertSingleValue(
                measurements,
                CSharpDbMetricInstrumentNames.StorageLogicalBytes,
                4);
            AssertSingleValue(
                measurements,
                CSharpDbMetricInstrumentNames.StorageCommits,
                14);
        }

        while (measurements.TryDequeue(out _))
        {
        }
        listener.RecordObservableInstruments();
        AssertSingleValue(
            measurements,
            CSharpDbMetricInstrumentNames.StorageCommits,
            14);
        second.RuntimeMetrics?.QueryAbandoned(metricsStarted: true);
    }

    [Fact]
    public void DataCollection_MixesPoolAndDirectApplicabilityWithoutPoisoning()
    {
        using var listener = new MeterListener();
        var measurements = new ConcurrentQueue<ObservedLong>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => measurements.Enqueue(
                new ObservedLong(instrument.Name, value, tags.ToArray())));
        listener.Start();

        using var pool = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-mixed-data"));
        using var direct = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-mixed-data"));
        using IDisposable poolProvider = Assert.IsAssignableFrom<IDisposable>(
            pool.RuntimeMetrics?.RegisterDataProvider(
                new FixedDataProvider(
                    activeSessions: 2,
                    activeReaders: 0,
                    poolWaiters: 3,
                    availableConnections: 4,
                    poolMetricsApplicable: true),
                CSharpDbTransport.Direct));
        using IDisposable directProvider = Assert.IsAssignableFrom<IDisposable>(
            direct.RuntimeMetrics?.RegisterDataProvider(
                new FixedDataProvider(
                    activeSessions: 1,
                    activeReaders: 1,
                    poolWaiters: null,
                    availableConnections: null,
                    poolMetricsApplicable: false),
                CSharpDbTransport.Direct));

        listener.RecordObservableInstruments();

        AssertSingleMixedDataValue(
            measurements,
            CSharpDbMetricInstrumentNames.SessionsActive,
            3);
        AssertSingleMixedDataValue(
            measurements,
            CSharpDbMetricInstrumentNames.ReadersActive,
            1);
        AssertSingleMixedDataValue(
            measurements,
            CSharpDbMetricInstrumentNames.PoolWaiters,
            3);
        AssertSingleMixedDataValue(
            measurements,
            CSharpDbMetricInstrumentNames.ConnectionsAvailable,
            4);
    }

    [Fact]
    public void CounterRetirement_InvalidFinalCaptureAndDuplicateDisposeStayMonotonic()
    {
        using var listener = new MeterListener();
        var measurements = new ConcurrentQueue<ObservedLong>();
        listener.InstrumentPublished = (instrument, currentListener) =>
        {
            if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                currentListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => measurements.Enqueue(
                new ObservedLong(instrument.Name, value, tags.ToArray())));
        listener.Start();

        var state = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-invalid-final"));
        var provider = new MutableStorageProvider(commits: 9);
        using IDisposable registration = Assert.IsAssignableFrom<IDisposable>(
            state.RuntimeMetrics?.RegisterStorageProvider(provider));
        listener.RecordObservableInstruments();
        provider.FailCapture = true;
        state.Dispose();
        state.Dispose();
        listener.RecordObservableInstruments();

        long[] commits = measurements
            .Where(static item => item.Name ==
                CSharpDbMetricInstrumentNames.StorageCommits)
            .Where(static item => item.Tags.Any(tag =>
                tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                Equals(tag.Value, "metrics-invalid-final")))
            .Select(static item => item.Value)
            .ToArray();
        Assert.NotEmpty(commits);
        Assert.Equal(9, commits[^1]);
        Assert.All(
            commits.Zip(commits.Skip(1)),
            static pair => Assert.True(pair.Second >= pair.First));
        Assert.DoesNotContain(
            CSharpDbRuntimeMetricsRegistry.Observe(
                CSharpDbMetricId.StoragePageReads),
            static item =>
                HasMeasurementAlias(item, "metrics-invalid-final"));
    }

    [Fact]
    public async Task CounterRetirement_ProviderAndRuntimeDisposeRaceHasExactHandoff()
    {
        var state = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-retirement-race"));
        var provider = new BlockingStorageProvider(commits: 5);
        IDisposable registration = Assert.IsAssignableFrom<IDisposable>(
            state.RuntimeMetrics?.RegisterStorageProvider(provider));
        Assert.Equal(
            5,
            Assert.Single(
                CSharpDbRuntimeMetricsRegistry.Observe(
                    CSharpDbMetricId.StorageCommits),
                static item =>
                    HasMeasurementAlias(item, "metrics-retirement-race")).Value);
        provider.Arm();

        Task providerRetirement = Task.Run(
            registration.Dispose,
            TestContext.Current.CancellationToken);
        try
        {
            await provider.CaptureEntered.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            await Task.Run(
                state.Dispose,
                TestContext.Current.CancellationToken);
        }
        finally
        {
            provider.ReleaseCapture.TrySetResult();
        }

        await providerRetirement;
        registration.Dispose();
        state.Dispose();
        Assert.Equal(
            5,
            Assert.Single(
                CSharpDbRuntimeMetricsRegistry.Observe(
                    CSharpDbMetricId.StorageCommits),
                static item =>
                    HasMeasurementAlias(item, "metrics-retirement-race")).Value);
    }

    [Fact]
    public void CounterRetirement_ClearsPoisonedPartialAndDuplicateBuffers()
    {
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateMetricsOptions("metrics-retirement-buffers"));
        using IDisposable registration = Assert.IsAssignableFrom<IDisposable>(
            state.RuntimeMetrics?.RegisterStorageProvider(
                new FixedStorageProvider(logicalBytes: 7, commits: 5)));
        CSharpDbRuntimeMetrics source = Assert.IsType<CSharpDbRuntimeMetrics>(
            state.RuntimeMetrics);
        var values = Enumerable.Repeat(
            0x5a5a5a5a5a5a5a5aL,
            CSharpDbRuntimeMetricsRegistry.StorageCounterCount).ToArray();
        var available = Enumerable.Repeat(
            true,
            CSharpDbRuntimeMetricsRegistry.StorageCounterCount).ToArray();

        source.CaptureRetiredStorageCounters(values, available);
        Assert.True(CSharpDbRuntimeMetricsRegistry.TryGetStorageCounterIndex(
            CSharpDbMetricId.StorageCommits,
            out int commitsIndex));
        for (int index = 0; index < values.Length; index++)
        {
            if (index == commitsIndex)
            {
                Assert.True(available[index]);
                Assert.Equal(5, values[index]);
            }
            else
            {
                Assert.False(available[index]);
                Assert.Equal(0, values[index]);
            }
        }

        Array.Fill(values, 0x6b6b6b6b6b6b6b6bL);
        Array.Fill(available, true);
        source.CaptureRetiredStorageCounters(values, available);
        Assert.All(values, static value => Assert.Equal(0, value));
        Assert.All(available, static value => Assert.False(value));
    }

    private static void AssertSingleValue(
        IEnumerable<ObservedLong> measurements,
        string name,
        long expected)
    {
        ObservedLong measurement = Assert.Single(
            measurements,
            item =>
                item.Name == name &&
                item.Tags.Any(static tag =>
                    tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                    Equals(tag.Value, "metrics-shared-alias")));
        Assert.Equal(expected, measurement.Value);
        Assert.Collection(
            measurement.Tags,
            tag =>
            {
                Assert.Equal(CSharpDbMetricTagNames.DatabaseAlias, tag.Key);
                Assert.Equal("metrics-shared-alias", tag.Value);
            });
    }

    private static void AssertSingleMixedDataValue(
        IEnumerable<ObservedLong> measurements,
        string name,
        long expected)
    {
        ObservedLong measurement = Assert.Single(
            measurements,
            item =>
                item.Name == name &&
                item.Tags.Any(static tag =>
                    tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                    Equals(tag.Value, "metrics-mixed-data")));
        Assert.Equal(expected, measurement.Value);
        Assert.Collection(
            measurement.Tags,
            tag =>
            {
                Assert.Equal(CSharpDbMetricTagNames.Transport, tag.Key);
                Assert.Equal("direct", tag.Value);
            },
            tag =>
            {
                Assert.Equal(CSharpDbMetricTagNames.DatabaseAlias, tag.Key);
                Assert.Equal("metrics-mixed-data", tag.Value);
            });
    }

    private static bool HasMeasurementAlias(
        Measurement<long> measurement,
        string alias)
    {
        foreach (KeyValuePair<string, object?> tag in measurement.Tags)
        {
            if (tag.Key == CSharpDbMetricTagNames.DatabaseAlias &&
                Equals(tag.Value, alias))
            {
                return true;
            }
        }

        return false;
    }

    private static CSharpDbObservabilityOptions CreateMetricsOptions(
        string alias,
        bool openTelemetry = true,
        bool prometheus = false)
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions { Enabled = false },
        };
        options.OpenTelemetry.Enabled = openTelemetry;
        options.Prometheus.Enabled = prometheus;
        return options;
    }

    private sealed class ZeroAndUnavailableStorageProvider :
        ICSharpDbStorageMetricsProvider
    {
        public bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot)
        {
            snapshot = new CSharpDbStorageMetricSnapshot(
                LogicalBytes: 0,
                AllocatedBytes: null,
                PageCount: null,
                PageReads: null,
                PageWrites: null,
                BytesRead: null,
                BytesWritten: null,
                CacheHits: null,
                CacheMisses: null,
                DirtyPages: null,
                ActiveReaders: null,
                ActiveWriters: null,
                Commits: null,
                Conflicts: null,
                WalLogicalBytes: null,
                WalAllocatedBytes: null,
                WalCommittedBytes: null,
                WalRetainedBytes: null,
                WalFrameCount: null,
                WalCommitBatches: null,
                WalBytesWritten: null,
                WalPendingCommits: null,
                WalFlushedCommits: null,
                WalFlushes: null,
                WalGroupCommitBatches: null,
                WalGroupCommitCommits: null);
            return true;
        }
    }

    private sealed class FixedStorageProvider(long logicalBytes, long commits) :
        ICSharpDbStorageMetricsProvider
    {
        public bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot)
        {
            snapshot = new CSharpDbStorageMetricSnapshot(
                LogicalBytes: logicalBytes,
                AllocatedBytes: null,
                PageCount: null,
                PageReads: null,
                PageWrites: null,
                BytesRead: null,
                BytesWritten: null,
                CacheHits: null,
                CacheMisses: null,
                DirtyPages: null,
                ActiveReaders: null,
                ActiveWriters: null,
                Commits: commits,
                Conflicts: null,
                WalLogicalBytes: null,
                WalAllocatedBytes: null,
                WalCommittedBytes: null,
                WalRetainedBytes: null,
                WalFrameCount: null,
                WalCommitBatches: null,
                WalBytesWritten: null,
                WalPendingCommits: null,
                WalFlushedCommits: null,
                WalFlushes: null,
                WalGroupCommitBatches: null,
                WalGroupCommitCommits: null);
            return true;
        }
    }

    private sealed class UnavailableStorageProvider :
        ICSharpDbStorageMetricsProvider
    {
        public bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot)
        {
            snapshot = new CSharpDbStorageMetricSnapshot(
                LogicalBytes: null,
                AllocatedBytes: null,
                PageCount: null,
                PageReads: null,
                PageWrites: null,
                BytesRead: null,
                BytesWritten: null,
                CacheHits: null,
                CacheMisses: null,
                DirtyPages: null,
                ActiveReaders: null,
                ActiveWriters: null,
                Commits: null,
                Conflicts: null,
                WalLogicalBytes: null,
                WalAllocatedBytes: null,
                WalCommittedBytes: null,
                WalRetainedBytes: null,
                WalFrameCount: null,
                WalCommitBatches: null,
                WalBytesWritten: null,
                WalPendingCommits: null,
                WalFlushedCommits: null,
                WalFlushes: null,
                WalGroupCommitBatches: null,
                WalGroupCommitCommits: null);
            return true;
        }
    }

    private sealed class FixedDataProvider(
        long activeSessions,
        long activeReaders,
        long? poolWaiters,
        long? availableConnections,
        bool poolMetricsApplicable) : ICSharpDbDataMetricsProvider
    {
        public bool TryCaptureMetrics(out CSharpDbDataMetricSnapshot snapshot)
        {
            snapshot = new CSharpDbDataMetricSnapshot(
                activeSessions,
                activeReaders,
                poolWaiters,
                availableConnections,
                poolMetricsApplicable);
            return true;
        }
    }

    private sealed class MutableStorageProvider(long commits) :
        ICSharpDbStorageMetricsProvider
    {
        internal bool FailCapture { get; set; }

        public bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot)
        {
            if (FailCapture)
            {
                snapshot = default;
                return false;
            }

            snapshot = new CSharpDbStorageMetricSnapshot(
                LogicalBytes: null,
                AllocatedBytes: null,
                PageCount: null,
                PageReads: null,
                PageWrites: null,
                BytesRead: null,
                BytesWritten: null,
                CacheHits: null,
                CacheMisses: null,
                DirtyPages: null,
                ActiveReaders: null,
                ActiveWriters: null,
                Commits: commits,
                Conflicts: null,
                WalLogicalBytes: null,
                WalAllocatedBytes: null,
                WalCommittedBytes: null,
                WalRetainedBytes: null,
                WalFrameCount: null,
                WalCommitBatches: null,
                WalBytesWritten: null,
                WalPendingCommits: null,
                WalFlushedCommits: null,
                WalFlushes: null,
                WalGroupCommitBatches: null,
                WalGroupCommitCommits: null);
            return true;
        }
    }

    private sealed class BlockingStorageProvider(long commits) :
        ICSharpDbStorageMetricsProvider
    {
        private int _armed;
        private int _armedCaptures;

        internal TaskCompletionSource CaptureEntered { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource ReleaseCapture { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal void Arm() => Volatile.Write(ref _armed, 1);

        public bool TryCaptureMetrics(out CSharpDbStorageMetricSnapshot snapshot)
        {
            if (Volatile.Read(ref _armed) != 0 &&
                Interlocked.Increment(ref _armedCaptures) == 1)
            {
                CaptureEntered.TrySetResult();
                ReleaseCapture.Task.GetAwaiter().GetResult();
            }

            snapshot = new CSharpDbStorageMetricSnapshot(
                LogicalBytes: null,
                AllocatedBytes: null,
                PageCount: null,
                PageReads: null,
                PageWrites: null,
                BytesRead: null,
                BytesWritten: null,
                CacheHits: null,
                CacheMisses: null,
                DirtyPages: null,
                ActiveReaders: null,
                ActiveWriters: null,
                Commits: commits,
                Conflicts: null,
                WalLogicalBytes: null,
                WalAllocatedBytes: null,
                WalCommittedBytes: null,
                WalRetainedBytes: null,
                WalFrameCount: null,
                WalCommitBatches: null,
                WalBytesWritten: null,
                WalPendingCommits: null,
                WalFlushedCommits: null,
                WalFlushes: null,
                WalGroupCommitBatches: null,
                WalGroupCommitCommits: null);
            return true;
        }
    }

    private sealed record ObservedLong(
        string Name,
        long Value,
        KeyValuePair<string, object?>[] Tags);
}

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using CSharpDB.Client.Models;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class OperationMetricsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void QueryTerminal_RecordsExactOnceAndClearsActiveGauge()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 9, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateMetricsOptions(
            "query-metrics-exact");
        using var recorder = new MetricRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);

        const string secret = "MetricsSqlSecret_4c7648";
        QueryOperation operation = Assert.IsType<QueryOperation>(
            diagnostics.Start($"SELECT '{secret}'"));
        recorder.RecordObservableInstruments();
        Assert.Equal(
            1,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.QueriesActive,
                "query-metrics-exact"));

        clock.Advance(TimeSpan.FromSeconds(2));
        operation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 7));
        operation.Fail(new InvalidOperationException(
            "MetricsErrorSecret_18b68e"));
        recorder.RecordObservableInstruments();

        Assert.Equal(
            0,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.QueriesActive,
                "query-metrics-exact"));
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.Requests, 1);
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.Statements, 1);
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.RowsProduced, 7);
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.QueriesSlow, 1);
        Assert.Single(recorder.DoubleEvents(
            CSharpDbMetricInstrumentNames.QueryDuration));

        Assert.All(
            recorder.Events,
            measurement =>
            {
                Assert.All(
                    measurement.Tags,
                    tag => Assert.True(
                        CSharpDbMetricTagNames.Allowed.Contains(tag.Key),
                        $"Unexpected metric tag '{tag.Key}'."));
                string rendered = string.Join(
                    "|",
                    measurement.Tags.Select(static tag => $"{tag.Key}={tag.Value}"));
                Assert.DoesNotContain(secret, rendered, StringComparison.Ordinal);
                Assert.DoesNotContain("MetricsErrorSecret", rendered, StringComparison.Ordinal);
            });

        ObservedMetric terminal = Assert.Single(recorder.LongEvents(
            CSharpDbMetricInstrumentNames.Requests));
        AssertTags(
            terminal,
            (CSharpDbMetricTagNames.OperationClass, "query"),
            (CSharpDbMetricTagNames.Outcome, "succeeded"),
            (CSharpDbMetricTagNames.Transport, "embedded"),
            (CSharpDbMetricTagNames.DatabaseAlias, "query-metrics-exact"));
        Assert.Single(diagnostics.GetRecentSnapshot(8).Records);
    }

    [Fact]
    public void MetricsOnly_HistoryDisabled_EmitsOneTerminalWithoutRetainingQueryState()
    {
        CSharpDbObservabilityOptions options = CreateMetricsOptions(
            "query-metrics-no-history");
        options.History.Enabled = false;
        using var recorder = new MetricRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);
        Assert.Null(MaintenanceRuntimeDiagnostics.GetOrCreate(state));

        QueryOperation operation = Assert.IsType<QueryOperation>(
            diagnostics.Start("SELECT 'history-disabled-metrics-canary'"));
        Assert.True(operation.HasRuntimeOperationForTest);
        Assert.Empty(diagnostics.GetActiveSnapshot(8).Records);

        operation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 3));
        operation.Fail(new InvalidOperationException("must-not-complete-twice"));

        Assert.Single(recorder.LongEvents(CSharpDbMetricInstrumentNames.Requests));
        Assert.Single(recorder.LongEvents(CSharpDbMetricInstrumentNames.Statements));
        Assert.Single(recorder.LongEvents(CSharpDbMetricInstrumentNames.RowsProduced));
        Assert.Single(recorder.DoubleEvents(
            CSharpDbMetricInstrumentNames.QueryDuration));
        Assert.Empty(diagnostics.GetActiveSnapshot(8).Records);
        Assert.Empty(diagnostics.GetRecentSnapshot(8).Records);
        Assert.Equal(0, diagnostics.GetSummary().RequestCount);
        Assert.Equal(0, diagnostics.GetSummary().StatementExecutionCount);
        Assert.Null(diagnostics.GetPlanSnapshot(
            OpaqueDiagnosticsId.Create(Guid.NewGuid())));
    }

    [Fact]
    public void QueryLabels_RemainOneSeriesAcrossDistinctSqlAndErrors()
    {
        CSharpDbObservabilityOptions options = CreateMetricsOptions(
            "query-metrics-cardinality");
        using var recorder = new MetricRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(options);
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);

        for (int index = 0; index < 64; index++)
        {
            QueryOperation operation = Assert.IsType<QueryOperation>(
                diagnostics.Start(
                    $"SELECT 'UnboundedSqlSecret_{index:D3}' FROM private_{index:D3}"));
            operation.Fail(new InvalidOperationException(
                $"UnboundedErrorSecret_{index:D3}"));
        }

        ObservedMetric[] requests = recorder.LongEvents(
            CSharpDbMetricInstrumentNames.Requests).ToArray();
        Assert.Equal(64, requests.Length);
        Assert.Single(requests.Select(TagSignature).Distinct(StringComparer.Ordinal));
        Assert.All(
            requests,
            measurement => AssertTags(
                measurement,
                (CSharpDbMetricTagNames.OperationClass, "query"),
                (CSharpDbMetricTagNames.Outcome, "failed"),
                (CSharpDbMetricTagNames.Transport, "embedded"),
                (CSharpDbMetricTagNames.DatabaseAlias, "query-metrics-cardinality")));
    }

    [Fact]
    public void ScriptAndProcedureRoots_CountRequestsWhileChildrenCountStatements()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 9, 30, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateMetricsOptions(
            "query-metrics-command-model");
        using var recorder = new MetricRecorder();
        using var state = new CSharpDbRuntimeDiagnosticsState(options, clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);

        CSharpDbOperationContext script = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Script,
            CSharpDbTransport.Direct,
            options.DatabaseAlias,
            timeProvider: clock);
        CSharpDbOperationContext scriptStatement =
            CSharpDbOperationContext.CreateStatement(script);
        CSharpDbOperationContext procedure = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Procedure,
            CSharpDbTransport.Direct,
            options.DatabaseAlias,
            timeProvider: clock);
        CSharpDbOperationContext procedureStatement =
            CSharpDbOperationContext.CreateStatement(procedure);
        CSharpDbOperationContext internalAttempt =
            CSharpDbOperationContext.CreateInternal(
                script,
                CSharpDbOperationClass.Query,
                CSharpDbTransport.Direct,
                options.DatabaseAlias);
        QueryRuntimeDiagnostics.QueryRuntimeOperation[] operations =
        [
            Start(registry, script),
            Start(registry, scriptStatement),
            Start(registry, procedure),
            Start(registry, procedureStatement),
            Start(registry, internalAttempt),
        ];
        recorder.RecordObservableInstruments();
        Assert.Equal(
            5,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.QueriesActive,
                options.DatabaseAlias));

        Complete(operations[0], clock, rowsProduced: 0, rowsAffected: 0);
        Complete(operations[1], clock, rowsProduced: 2, rowsAffected: 0);
        Complete(operations[2], clock, rowsProduced: 0, rowsAffected: 0);
        Complete(operations[3], clock, rowsProduced: 0, rowsAffected: 3);
        Complete(operations[4], clock, rowsProduced: 99, rowsAffected: 99);
        recorder.RecordObservableInstruments();

        ObservedMetric[] requests = recorder.LongEvents(
            CSharpDbMetricInstrumentNames.Requests).ToArray();
        Assert.Equal(2, requests.Length);
        Assert.Contains(requests, static item =>
            TagValue(item, CSharpDbMetricTagNames.OperationClass) == "script");
        Assert.Contains(requests, static item =>
            TagValue(item, CSharpDbMetricTagNames.OperationClass) == "procedure");
        ObservedMetric[] statements = recorder.LongEvents(
            CSharpDbMetricInstrumentNames.Statements).ToArray();
        Assert.Equal(2, statements.Length);
        Assert.All(
            statements,
            static item => Assert.Equal(
                "query",
                TagValue(item, CSharpDbMetricTagNames.OperationClass)));
        Assert.Equal(
            4,
            recorder.DoubleEvents(
                CSharpDbMetricInstrumentNames.QueryDuration).Count());
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.RowsProduced, 2);
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.RowsAffected, 3);
        Assert.Equal(
            0,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.QueriesActive,
                options.DatabaseAlias));
    }

    [Fact]
    public async Task DirectClient_PreDispatchFailureAndEngineAdoption_RecordOneTerminalEach()
    {
        CSharpDbObservabilityOptions observability = CreateMetricsOptions(
            "query-metrics-composite");
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        };
        using var recorder = new MetricRecorder();
        await using var client = new EngineTransportClient(
            ":memory:query-metrics-composite",
            static (_, options, ct) =>
                Database.OpenInMemoryAsync(options, ct).AsTask(),
            databaseOptions);

        await Assert.ThrowsAnyAsync<Exception>(() => client.GetRowByPkAsync(
            "invalid-table-name",
            "id",
            1L,
            Ct));

        Assert.Single(
            recorder.LongEvents(CSharpDbMetricInstrumentNames.Requests),
            static measurement => TagValue(
                measurement,
                CSharpDbMetricTagNames.Outcome) == "failed");

        Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);

        ObservedMetric[] requests = recorder.LongEvents(
            CSharpDbMetricInstrumentNames.Requests).ToArray();
        Assert.Equal(2, requests.Length);
        Assert.Single(
            requests,
            static measurement => TagValue(
                measurement,
                CSharpDbMetricTagNames.Outcome) == "failed");
        Assert.Single(
            requests,
            static measurement => TagValue(
                measurement,
                CSharpDbMetricTagNames.Outcome) == "succeeded");
        Assert.Equal(
            2,
            recorder.LongEvents(CSharpDbMetricInstrumentNames.Statements).Count());
    }

    [Fact]
    public async Task ShardedMetricsOnly_CountsOneLogicalRequestAndStatement()
    {
        const string alias = "query-metrics-sharded";
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_metrics_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        using var recorder = new MetricRecorder();

        try
        {
            CSharpDbObservabilityOptions observability = CreateMetricsOptions(alias);
            var options = new CSharpDB.Client.CSharpDbShardingOptions
            {
                Keyspace = "metrics",
                MapVersion = 1,
                VirtualBucketCount = 2,
                Shards =
                [
                    new CSharpDB.Client.CSharpDbShardDefinition
                    {
                        ShardId = "shard-0",
                        DataSource = Path.Combine(directory, "shard-0.db"),
                    },
                    new CSharpDB.Client.CSharpDbShardDefinition
                    {
                        ShardId = "shard-1",
                        DataSource = Path.Combine(directory, "shard-1.db"),
                    },
                ],
                BucketRanges =
                [
                    new CSharpDB.Client.CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ShardId = "shard-0",
                    },
                    new CSharpDB.Client.CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 1,
                        EndBucketExclusive = 2,
                        ShardId = "shard-1",
                    },
                ],
                DirectDatabaseOptions = new DatabaseOptions
                {
                    ObservabilityOptions = observability,
                },
            };
            await using CSharpDB.Client.CSharpDbShardedClient client =
                await CSharpDB.Client.CSharpDbShardedClient.CreateAsync(
                    options,
                    ct: Ct);
            recorder.Clear();

            IReadOnlyList<CSharpDB.Client.CSharpDbShardSqlExecutionResult> results =
                await client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);

            Assert.Equal(2, results.Count);
            Assert.All(results, static result => Assert.Null(result.Error));
            Assert.Single(
                recorder.LongEvents(CSharpDbMetricInstrumentNames.Requests),
                measurement => TagValue(
                    measurement,
                    CSharpDbMetricTagNames.DatabaseAlias) == alias);
            Assert.Single(
                recorder.LongEvents(CSharpDbMetricInstrumentNames.Statements),
                measurement => TagValue(
                    measurement,
                    CSharpDbMetricTagNames.DatabaseAlias) == alias);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void PrometheusOnlyLifecycle_UsesExactSourceAndExcludesCheckpoint()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 10, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateMetricsOptions(
            "lifecycle-metrics-shared-alias");
        using var recorder = new MetricRecorder();
        using var disposableSibling = new CSharpDbRuntimeDiagnosticsState(
            options,
            clock);
        using var exactState = new CSharpDbRuntimeDiagnosticsState(options, clock);
        CSharpDbOperationContext transactionContext =
            CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Transaction,
                CSharpDbTransport.Direct,
                options.DatabaseAlias,
                timeProvider: clock);
        LifecycleOperation transaction = Assert.IsType<LifecycleOperation>(
            LifecycleObservability.StartExact(
                options,
                CSharpDbLogEvents.TransactionCompleted,
                CSharpDbOperationClass.Transaction,
                transactionContext,
                activityOperation: null,
                runtimeState: exactState));

        recorder.RecordObservableInstruments();
        Assert.Equal(
            1,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.TransactionsActive,
                options.DatabaseAlias));
        disposableSibling.Dispose();
        clock.Advance(TimeSpan.FromSeconds(3));
        transaction.Succeed();
        transaction.Fail(new InvalidOperationException("must-not-complete-twice"));

        CSharpDbOperationContext backupContext =
            CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Backup,
                CSharpDbTransport.Embedded,
                options.DatabaseAlias,
                timeProvider: clock);
        LifecycleOperation backup = Assert.IsType<LifecycleOperation>(
            LifecycleObservability.StartExact(
                options,
                CSharpDbLogEvents.BackupCompleted,
                CSharpDbOperationClass.Backup,
                backupContext,
                activityOperation: null,
                runtimeState: exactState));
        recorder.RecordObservableInstruments();
        Assert.Equal(
            1,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.MaintenanceActive,
                options.DatabaseAlias,
                operationClass: "backup"));
        backup.Succeed();

        CSharpDbOperationContext checkpointContext =
            CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Checkpoint,
                CSharpDbTransport.Embedded,
                options.DatabaseAlias,
                timeProvider: clock);
        Assert.Null(LifecycleObservability.StartExact(
            options,
            CSharpDbLogEvents.CheckpointCompleted,
            CSharpDbOperationClass.Checkpoint,
            checkpointContext,
            activityOperation: null,
            runtimeState: exactState));

        recorder.RecordObservableInstruments();
        Assert.Equal(
            0,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.TransactionsActive,
                options.DatabaseAlias));
        Assert.Equal(
            0,
            recorder.LastLong(
                CSharpDbMetricInstrumentNames.MaintenanceActive,
                options.DatabaseAlias,
                operationClass: "backup"));
        AssertLongEvent(recorder, CSharpDbMetricInstrumentNames.Transactions, 1);
        AssertLongEvent(
            recorder,
            CSharpDbMetricInstrumentNames.MaintenanceOperations,
            1);
        Assert.Single(recorder.DoubleEvents(
            CSharpDbMetricInstrumentNames.TransactionDuration));
        Assert.Single(recorder.DoubleEvents(
            CSharpDbMetricInstrumentNames.MaintenanceDuration));
        Assert.Empty(recorder.LongEvents(CSharpDbMetricInstrumentNames.Checkpoints));
    }

    [Fact]
    public async Task ExclusiveMaintenance_RetainsExactMetricSourceAcrossFamilyReset()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-maintenance-metrics-{Guid.NewGuid():N}.db");
        CSharpDbObservabilityOptions observability = CreateMetricsOptions(
            "maintenance-metrics-reset");
        var options = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        };
        using var recorder = new MetricRecorder();
        var disposeEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDispose = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var postResetEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePostReset = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int blockedPostReset = 0;

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                options);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE metric_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX idx_metric_items_value ON metric_items(value)",
                Ct)).Error);
            CSharpDbRuntimeDiagnosticsState originalState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);

            EngineTransportClient.DisposeExclusiveDatabaseForTests =
                async database =>
                {
                    disposeEntered.TrySetResult();
                    await releaseDispose.Task;
                    await database.DisposeAsync();
                };
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase != MaintenanceOperationPhase.Validating ||
                    Interlocked.Exchange(ref blockedPostReset, 1) != 0)
                {
                    return;
                }

                postResetEntered.TrySetResult();
                releasePostReset.Task.GetAwaiter().GetResult();
            };

            Task<ReindexResult> reindex = client.ReindexAsync(
                new ReindexRequest
                {
                    Scope = ReindexScope.Index,
                    Name = "idx_metric_items_value",
                },
                Ct);
            await disposeEntered.Task.WaitAsync(Ct);
            try
            {
                recorder.RecordObservableInstruments();
                Assert.Equal(
                    1,
                    recorder.LastLong(
                        CSharpDbMetricInstrumentNames.MaintenanceActive,
                        observability.DatabaseAlias,
                        operationClass: "reindex"));
            }
            finally
            {
                releaseDispose.TrySetResult();
            }

            await postResetEntered.Task.WaitAsync(Ct);
            try
            {
                Assert.NotSame(
                    originalState,
                    client.CurrentRuntimeDiagnosticsState);
                Assert.NotNull(originalState.RuntimeMetrics);
                recorder.RecordObservableInstruments();
                Assert.Equal(
                    1,
                    recorder.LastLong(
                        CSharpDbMetricInstrumentNames.MaintenanceActive,
                        observability.DatabaseAlias,
                        operationClass: "reindex"));
            }
            finally
            {
                releasePostReset.TrySetResult();
            }

            _ = await reindex;
            recorder.RecordObservableInstruments();

            Assert.Null(originalState.RuntimeMetrics);
            Assert.Equal(
                0,
                recorder.LastLong(
                    CSharpDbMetricInstrumentNames.MaintenanceActive,
                    observability.DatabaseAlias,
                    operationClass: "reindex"));
            Assert.Single(recorder.LongEvents(
                CSharpDbMetricInstrumentNames.MaintenanceOperations));
            Assert.Single(recorder.DoubleEvents(
                CSharpDbMetricInstrumentNames.MaintenanceDuration));
        }
        finally
        {
            releaseDispose.TrySetResult();
            releasePostReset.TrySetResult();
            EngineTransportClient.DisposeExclusiveDatabaseForTests = null;
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    private static void AssertLongEvent(
        MetricRecorder recorder,
        string name,
        long value)
    {
        ObservedMetric measurement = Assert.Single(recorder.LongEvents(name));
        Assert.Equal(value, measurement.LongValue);
    }

    private static QueryRuntimeDiagnostics.QueryRuntimeOperation Start(
        QueryRuntimeDiagnostics registry,
        CSharpDbOperationContext context)
        => Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
            registry.TryStart(context, QueryExecutionPhase.Queued));

    private static void Complete(
        QueryRuntimeDiagnostics.QueryRuntimeOperation operation,
        ManualTimeProvider clock,
        long rowsProduced,
        long rowsAffected)
    {
        clock.Advance(TimeSpan.FromMilliseconds(10));
        operation.Complete(
            CSharpDbOperationOutcome.Succeeded,
            clock.GetUtcNow(),
            TimeSpan.FromMilliseconds(10),
            timeToFirstResult: null,
            rowsProduced,
            rowsAffected,
            error: null,
            isSlow: false);
    }

    private static void AssertTags(
        ObservedMetric measurement,
        params (string Key, string Value)[] expected)
    {
        Assert.Equal(expected.Length, measurement.Tags.Length);
        for (int index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index].Key, measurement.Tags[index].Key);
            Assert.Equal(expected[index].Value, measurement.Tags[index].Value);
        }
    }

    private static string TagSignature(ObservedMetric measurement)
        => string.Join(
            "|",
            measurement.Tags.Select(static tag => $"{tag.Key}={tag.Value}"));

    private static string? TagValue(ObservedMetric measurement, string name)
        => measurement.Tags.FirstOrDefault(tag => tag.Key == name).Value as string;

    private static CSharpDbObservabilityOptions CreateMetricsOptions(string alias)
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                SqlText = SqlTextCaptureMode.Raw,
                SlowQueryThreshold = TimeSpan.FromSeconds(1),
            },
        };
        options.OpenTelemetry.Enabled = false;
        options.Prometheus.Enabled = true;
        return options;
    }

    private static void DeleteDatabaseArtifacts(string databasePath)
    {
        DeleteIfExists(databasePath);
        DeleteIfExists(databasePath + ".wal");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class MetricRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly ConcurrentQueue<ObservedMetric> _events = new();

        internal MetricRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == CSharpDbDiagnostics.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => _events.Enqueue(
                    new ObservedMetric(
                        instrument.Name,
                        value,
                        null,
                        tags.ToArray())));
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => _events.Enqueue(
                    new ObservedMetric(
                        instrument.Name,
                        null,
                        value,
                        tags.ToArray())));
            _listener.Start();
        }

        internal ObservedMetric[] Events => _events.ToArray();

        internal IEnumerable<ObservedMetric> LongEvents(string name)
            => Events.Where(item => item.Name == name && item.LongValue.HasValue);

        internal IEnumerable<ObservedMetric> DoubleEvents(string name)
            => Events.Where(item => item.Name == name && item.DoubleValue.HasValue);

        internal long LastLong(
            string name,
            string databaseAlias,
            string? operationClass = null)
        {
            ObservedMetric measurement = LongEvents(name)
                .Where(item => TagValue(
                    item,
                    CSharpDbMetricTagNames.DatabaseAlias) == databaseAlias)
                .Where(item => operationClass is null ||
                    TagValue(item, CSharpDbMetricTagNames.OperationClass) ==
                        operationClass)
                .Last();
            return Assert.IsType<long>(measurement.LongValue);
        }

        internal void RecordObservableInstruments()
            => _listener.RecordObservableInstruments();

        internal void Clear()
        {
            while (_events.TryDequeue(out _))
            {
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    private sealed record ObservedMetric(
        string Name,
        long? LongValue,
        double? DoubleValue,
        KeyValuePair<string, object?>[] Tags);

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
        {
            _utcNow = _utcNow.Add(duration);
            Interlocked.Add(ref _timestamp, duration.Ticks);
        }
    }
}

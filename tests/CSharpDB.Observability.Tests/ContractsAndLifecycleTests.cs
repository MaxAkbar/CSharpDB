using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Observability.Tests;

public sealed class ContractsAndLifecycleTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void MetricTagAllowlist_IsBoundedAndExcludesCorrelationAndSensitiveFields()
    {
        Assert.Equal(6, CSharpDbMetricTagNames.Allowed.Count);
        Assert.All(CSharpDbMetricTagNames.Allowed, tag => Assert.StartsWith("csharpdb.", tag));

        string[] prohibited =
        [
            "query",
            "fingerprint",
            "sql",
            "table",
            "path",
            "operation.id",
            "trace",
            "session",
            "exception",
            "message",
        ];

        Assert.All(
            prohibited,
            value => Assert.DoesNotContain(
                CSharpDbMetricTagNames.Allowed,
                tag => tag.Contains(value, StringComparison.OrdinalIgnoreCase)));

        Assert.True(CSharpDbMetricTagNames.IsAllowedValue(
            CSharpDbMetricTagNames.OperationClass,
            "query"));
        Assert.True(CSharpDbMetricTagNames.IsAllowedValue(
            CSharpDbMetricTagNames.DatabaseAlias,
            "shard-01"));
        Assert.False(CSharpDbMetricTagNames.IsAllowedValue(
            CSharpDbMetricTagNames.DatabaseAlias,
            "C:\\private\\database.db"));
        Assert.False(CSharpDbMetricTagNames.IsAllowedValue(
            CSharpDbMetricTagNames.Status,
            "Customer42-Canary"));
    }

    [Fact]
    public void SafeErrorProjection_DropsMessageStackAndData()
    {
        const string secret = "Password=CanarySecret;Data Source=C:\\private\\database.db";
        var exception = new InvalidOperationException(secret);
        exception.Data["sql"] = "SELECT 'CanarySecret'";

        SafeErrorProjection projection = SafeErrorProjector.Project(exception);
        string json = JsonSerializer.Serialize(
            projection,
            CSharpDbObservabilityJsonContext.Default.SafeErrorProjection);

        Assert.Equal("unexpected_error", projection.Code);
        Assert.Equal("unexpected", projection.ErrorType);
        Assert.DoesNotContain(nameof(InvalidOperationException), json, StringComparison.Ordinal);
        Assert.DoesNotContain("CanarySecret", json, StringComparison.Ordinal);
        Assert.DoesNotContain("private", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SELECT", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OrdinarySnapshotSerialization_HasNoSqlOrPathField()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        DiagnosticsSnapshotMetadata metadata = DiagnosticsSnapshotMetadata.Create(
            CSharpDbDiagnostics.CreateServerInstanceId(),
            counterEpoch: 3,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            source: DiagnosticsSource.Engine,
            databaseAlias: "primary",
            timeProvider: clock);
        QueryFingerprint fingerprint =
            SqlQueryNormalizer.CreateFingerprint("SELECT value FROM secrets WHERE id = 42", Ct);
        var active = new ActiveQuerySnapshot(
            metadata,
            OperationId: OpaqueDiagnosticsId.Create(),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Streaming,
            clock.GetUtcNow(),
            TimeSpan.FromSeconds(2),
            fingerprint,
            CSharpDbTransport.Direct,
            TraceId: new DiagnosticsTraceId("0123456789abcdef0123456789abcdef"),
            SessionId: OpaqueDiagnosticsId.Create());

        string json = JsonSerializer.Serialize(
            new[] { active },
            CSharpDbObservabilityJsonContext.Default.ActiveQuerySnapshotArray);

        Assert.DoesNotContain("SELECT", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("secrets", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("database.db", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sqlText", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("path", json, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(CSharpDbDiagnostics.SchemaVersion, json, StringComparison.Ordinal);
    }

    [Fact]
    public void OperationHierarchy_SeparatesRequestAndStatementCounters()
    {
        QueryFingerprint fingerprint =
            SqlQueryNormalizer.CreateFingerprint("SELECT id FROM users WHERE id = 1", Ct);
        CSharpDbOperationContext script = CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Script,
            CSharpDbTransport.Http,
            "primary",
            sessionId: OpaqueDiagnosticsId.Create());
        CSharpDbOperationContext statement =
            CSharpDbOperationContext.CreateStatement(script, fingerprint);
        CSharpDbOperationContext single = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary",
            queryFingerprint: fingerprint);
        CSharpDbOperationContext shardAttempt = CSharpDbOperationContext.CreateInternal(
            single,
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Grpc,
            "shard-1",
            fingerprint);

        Assert.True(script.CountsAsRequest);
        Assert.False(script.CountsAsStatement);
        Assert.False(statement.CountsAsRequest);
        Assert.True(statement.CountsAsStatement);
        Assert.Equal(script.OperationId, statement.ParentOperationId);
        Assert.True(single.CountsAsRequest);
        Assert.True(single.CountsAsStatement);
        Assert.False(shardAttempt.CountsAsRequest);
        Assert.False(shardAttempt.CountsAsStatement);
        Assert.Equal(single.OperationId, shardAttempt.ParentOperationId);
        Assert.Equal(single.TraceId, shardAttempt.TraceId);
        Assert.Equal(CSharpDbTransport.Grpc, shardAttempt.Transport);
        Assert.Equal("shard-1", shardAttempt.DatabaseAlias);
    }

    [Fact]
    public void OperationClass_AdditionsPreservePublishedNumericValues()
    {
        Assert.Equal(12, (int)CSharpDbOperationClass.Maintenance);
        Assert.Equal(13, (int)CSharpDbOperationClass.Pipeline);
    }

    [Fact]
    public void OperationTiming_UsesTheClockThatCreatedTheContext()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        CSharpDbOperationContext request = CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Script,
            CSharpDbTransport.Http,
            "primary",
            timeProvider: clock);
        CSharpDbOperationContext statement = CSharpDbOperationContext.CreateStatement(
            request,
            SqlQueryNormalizer.CreateFingerprint("SELECT 1", Ct));

        clock.Advance(TimeSpan.FromMilliseconds(125));

        Assert.Equal(TimeSpan.FromMilliseconds(125), request.GetElapsedTime());
        Assert.Equal(TimeSpan.FromMilliseconds(125), statement.GetElapsedTime());
        Assert.Equal(
            new DateTimeOffset(2026, 8, 9, 12, 0, 0, 125, TimeSpan.Zero),
            statement.GetUtcNow());
    }

    [Fact]
    public void OperationContext_CapturesOnlyAValidW3cTraceId()
    {
        using var activity = new Activity("observability-contract-test");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        activity.Start();

        CSharpDbOperationContext operation = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "primary");

        Assert.NotNull(operation.TraceId);
        Assert.Equal(activity.TraceId.ToHexString(), operation.TraceId.Value);
        Assert.True(DiagnosticsTraceId.IsValid(operation.TraceId.Value));
    }

    [Fact]
    public void StatementOperation_InheritsParentTraceWhenAmbientActivityChanges()
    {
        CSharpDbOperationContext request;
        using (var requestActivity = new Activity("request"))
        {
            requestActivity.SetIdFormat(ActivityIdFormat.W3C);
            requestActivity.SetParentId(
                "00-11111111111111111111111111111111-1111111111111111-01");
            requestActivity.Start();
            request = CSharpDbOperationContext.CreateRequest(
                CSharpDbOperationClass.Script,
                CSharpDbTransport.Http,
                "primary");
        }

        using var unrelatedActivity = new Activity("unrelated");
        unrelatedActivity.SetIdFormat(ActivityIdFormat.W3C);
        unrelatedActivity.SetParentId(
            "00-22222222222222222222222222222222-2222222222222222-01");
        unrelatedActivity.Start();

        CSharpDbOperationContext statement = CSharpDbOperationContext.CreateStatement(
            request,
            SqlQueryNormalizer.CreateFingerprint("SELECT 1", Ct));

        Assert.NotNull(request.TraceId);
        Assert.Equal(request.TraceId, statement.TraceId);
        Assert.NotEqual(unrelatedActivity.TraceId.ToHexString(), statement.TraceId?.Value);
    }

    [Fact]
    public void OperationContext_RejectsUndefinedBoundedEnums()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CSharpDbOperationContext.CreateRoot(
            (CSharpDbOperationClass)999,
            CSharpDbTransport.Direct,
            "primary"));
        Assert.Throws<ArgumentOutOfRangeException>(() => CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            (CSharpDbTransport)999,
            "primary"));
    }

    [Fact]
    public void HostState_UsesDeterministicTransitionsAndKeepsDatabaseFailureLive()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        var state = new CSharpDbHostState(clock);

        Assert.True(state.Snapshot.IsLive);
        Assert.False(state.Snapshot.IsReady);
        Assert.Equal(CSharpDbReadinessReason.Starting, state.Snapshot.ReadinessReason);

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(CSharpDbHostLifecyclePhase.Recovering, state.MarkRecovering().LifecyclePhase);

        clock.Advance(TimeSpan.FromSeconds(2));
        CSharpDbHostStateSnapshot ready = state.MarkReady();
        Assert.True(ready.IsLive);
        Assert.True(ready.IsReady);

        clock.Advance(TimeSpan.FromSeconds(3));
        SafeErrorProjection safeError = SafeErrorProjector.Project(
            new IOException("C:\\private\\database.db"),
            SafeErrorKind.DatabaseIo);
        CSharpDbHostStateSnapshot failed = state.MarkFailed(safeError);
        Assert.True(failed.IsLive);
        Assert.False(failed.IsReady);
        Assert.Equal("csharpdb.io", failed.Error?.Code);
        Assert.DoesNotContain("private", JsonSerializer.Serialize(failed), StringComparison.OrdinalIgnoreCase);
        Assert.Throws<InvalidOperationException>(state.MarkReady);

        state.MarkStopping();
        CSharpDbHostStateSnapshot stopped = state.MarkStopped();
        Assert.False(stopped.IsLive);
        Assert.False(stopped.IsReady);
        Assert.Throws<InvalidOperationException>(state.MarkReady);
    }

    [Fact]
    public void HostState_RetriesFailedInitializationAndPublishesDistinctTransitionsInOrder()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 12, 0, 0, TimeSpan.Zero));
        var observed = new List<CSharpDbHostStateSnapshot>();
        var state = new CSharpDbHostState(
            clock,
            snapshot =>
            {
                observed.Add(snapshot);
                if (snapshot.LifecyclePhase ==
                    CSharpDbHostLifecyclePhase.Recovering)
                {
                    throw new InvalidOperationException(
                        "An observer cannot stop initialization recovery.");
                }
            });

        clock.Advance(TimeSpan.FromSeconds(1));
        state.MarkFailed(SafeErrorProjector.Project(SafeErrorKind.DatabaseIo));
        clock.Advance(TimeSpan.FromSeconds(1));
        state.MarkRecovering();
        clock.Advance(TimeSpan.FromSeconds(1));
        CSharpDbHostStateSnapshot ready = state.MarkReady();
        clock.Advance(TimeSpan.FromSeconds(1));
        CSharpDbHostStateSnapshot repeatedReady = state.MarkReady();

        Assert.Same(ready, repeatedReady);
        Assert.Equal(ready.ChangedAtUtc, repeatedReady.ChangedAtUtc);
        Assert.Equal(
            [
                CSharpDbHostLifecyclePhase.Starting,
                CSharpDbHostLifecyclePhase.Failed,
                CSharpDbHostLifecyclePhase.Recovering,
                CSharpDbHostLifecyclePhase.Running,
            ],
            observed.Select(static snapshot => snapshot.LifecyclePhase));
        Assert.Null(state.Snapshot.Error);
        Assert.True(state.Snapshot.IsReady);
    }

    [Fact]
    public void HostState_MarkRunningPublishesInitialRuntimeReasonAtomically()
    {
        var observed = new List<CSharpDbHostStateSnapshot>();
        var state = new CSharpDbHostState(
            TimeProvider.System,
            observed.Add);

        CSharpDbHostStateSnapshot running = state.MarkRunning(
            CSharpDbReadinessReason.ReadOnly);

        Assert.True(running.IsLive);
        Assert.False(running.IsReady);
        Assert.Equal(CSharpDbHostLifecyclePhase.Running, running.LifecyclePhase);
        Assert.Equal(CSharpDbReadinessReason.ReadOnly, running.ReadinessReason);
        Assert.Equal(2, observed.Count);
        Assert.DoesNotContain(observed, static snapshot => snapshot.IsReady);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            state.MarkRunning(CSharpDbReadinessReason.Starting));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            state.MarkRunning((CSharpDbReadinessReason)999));
    }

    [Fact]
    public void CounterEpoch_AdvancesAtomically()
    {
        var epoch = new CSharpDbCounterEpoch();

        Parallel.For(0, 1_000, _ => epoch.Advance());

        Assert.Equal(1_000, epoch.Value);
    }

    [Fact]
    public void CounterEpoch_SaturatesWithoutOverflowingMetadataDomain()
    {
        var epoch = new CSharpDbCounterEpoch(long.MaxValue - 1);

        Assert.Equal(long.MaxValue, epoch.Advance());
        Parallel.For(0, 1_000, _ => epoch.Advance());

        Assert.Equal(long.MaxValue, epoch.Value);
    }

    [Fact]
    public void OpaqueIdentifiers_RejectCallerValuesAndSerializeAsStrings()
    {
        Assert.Throws<ArgumentException>(() => new OpaqueDiagnosticsId("BearerCapabilitySecret"));

        OpaqueDiagnosticsId id = OpaqueDiagnosticsId.Create();
        OpaqueDiagnosticsId roundTrippedText = new(id.Value);
        string json = JsonSerializer.Serialize(
            id,
            CSharpDbObservabilityJsonContext.Default.OpaqueDiagnosticsId);

        Assert.Equal(id, roundTrippedText);
        Assert.Equal(id.GetHashCode(), roundTrippedText.GetHashCode());
        Assert.Equal(32, id.Value.Length);
        Assert.Equal($"\"{id.Value}\"", json);
    }

    [Fact]
    public void OperationContext_RuntimeOwnershipDoesNotChangeRecordIdentity()
    {
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Embedded,
            "primary");
        CSharpDbOperationContext copied = context with { };
        var keyed = new Dictionary<CSharpDbOperationContext, string>
        {
            [context] = "present",
        };
        int hashBeforeClaim = context.GetHashCode();
        var owner = new object();

        Assert.True(context.TryClaimRuntimeDiagnostics(owner));

        Assert.Equal(hashBeforeClaim, context.GetHashCode());
        Assert.Equal(copied, context);
        Assert.False(copied.TryClaimRuntimeDiagnostics(new object()));
        Assert.Equal("present", keyed[context]);
        Assert.Equal("present", keyed[copied]);

        context.ReleaseRuntimeDiagnostics(owner);
        Assert.Equal(hashBeforeClaim, context.GetHashCode());
        Assert.Equal(copied, context);
    }

    [Fact]
    public void TraceIdentifiers_RejectCallerTextAndAllZeroW3cIds()
    {
        const string secret = "SELECT secret FROM C:\\private\\database.db";

        Assert.Throws<ArgumentException>(() => new DiagnosticsTraceId(secret));
        Assert.Throws<ArgumentException>(() => new DiagnosticsTraceId(new string('0', 32)));

        var traceId = new DiagnosticsTraceId("0123456789abcdef0123456789abcdef");
        string json = JsonSerializer.Serialize(
            traceId,
            CSharpDbObservabilityJsonContext.Default.DiagnosticsTraceId);

        Assert.Equal($"\"{traceId.Value}\"", json);
        Assert.DoesNotContain("SELECT", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SourceGeneratedContext_RoundTripsValidatedPublicContracts()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        DiagnosticsSnapshotMetadata metadata = DiagnosticsSnapshotMetadata.Create(
            CSharpDbDiagnostics.CreateServerInstanceId(),
            counterEpoch: 7,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            databaseAlias: "primary",
            recordsTruncated: true,
            fieldsTruncated: false,
            timeProvider: clock);
        SafeErrorProjection error = SafeErrorProjector.Project(SafeErrorKind.DatabaseIo);
        var hostState = new CSharpDbHostState(clock);
        CSharpDbHostStateSnapshot failedHost = hostState.MarkFailed(error);
        DiagnosticsSection<HealthDiagnosticsSnapshot> disabledSection =
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled);
        var active = new ActiveQuerySnapshot(
            metadata,
            OpaqueDiagnosticsId.Create(),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            clock.GetUtcNow(),
            TimeSpan.FromMilliseconds(12),
            Fingerprint: null,
            CSharpDbTransport.Direct,
            new DiagnosticsTraceId("0123456789abcdef0123456789abcdef"),
            SessionId: null);
        var bounded = new BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>(
            new[] { active },
            droppedCount: 2,
            isTruncated: true);

        string metadataJson = JsonSerializer.Serialize(
            metadata,
            CSharpDbObservabilityJsonContext.Default.DiagnosticsSnapshotMetadata);
        string compatibleMetadataJson = metadataJson.Replace(
            $"\"schemaVersion\":\"{CSharpDbDiagnostics.SchemaVersion}\"",
            "\"schemaVersion\":\"1.42\"",
            StringComparison.Ordinal);
        string errorJson = JsonSerializer.Serialize(
            error,
            CSharpDbObservabilityJsonContext.Default.SafeErrorProjection);
        string hostJson = JsonSerializer.Serialize(
            failedHost,
            CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot);
        string sectionJson = JsonSerializer.Serialize(
            disabledSection,
            CSharpDbObservabilityJsonContext.Default.DiagnosticsSectionHealthDiagnosticsSnapshot);
        var boundedTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>))!;
        string boundedJson = JsonSerializer.Serialize(bounded, boundedTypeInfo);

        Assert.Equal(
            metadata,
            JsonSerializer.Deserialize(
                metadataJson,
                CSharpDbObservabilityJsonContext.Default.DiagnosticsSnapshotMetadata));
        Assert.Equal(
            "1.42",
            JsonSerializer.Deserialize(
                compatibleMetadataJson,
                CSharpDbObservabilityJsonContext.Default.DiagnosticsSnapshotMetadata)?.SchemaVersion);
        Assert.Equal(
            error,
            JsonSerializer.Deserialize(
                errorJson,
                CSharpDbObservabilityJsonContext.Default.SafeErrorProjection));
        Assert.Equal(
            failedHost,
            JsonSerializer.Deserialize(
                hostJson,
                CSharpDbObservabilityJsonContext.Default.CSharpDbHostStateSnapshot));
        Assert.Equal(
            disabledSection,
            JsonSerializer.Deserialize(
                sectionJson,
                CSharpDbObservabilityJsonContext.Default.DiagnosticsSectionHealthDiagnosticsSnapshot));

        var boundedRoundTrip = Assert.IsType<BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>>(
            JsonSerializer.Deserialize(boundedJson, boundedTypeInfo));
        ActiveQuerySnapshot activeRoundTrip = Assert.Single(boundedRoundTrip.Records);
        Assert.Equal(active, activeRoundTrip);
        Assert.Equal(2, boundedRoundTrip.DroppedCount);
        Assert.True(boundedRoundTrip.IsTruncated);
    }

    [Fact]
    public void SourceGeneratedDeserialization_RejectsUnsafeErrorProjection()
    {
        const string malicious =
            """{"code":"unexpected_error","errorType":"unexpected","publicDetail":"SELECT secret FROM C:\\\\private\\\\database.db"}""";

        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(
            malicious,
            CSharpDbObservabilityJsonContext.Default.SafeErrorProjection));
    }

    [Fact]
    public void SourceGeneratedDeserialization_RejectsUnsafeSchemaVersion()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 8, 9, 12, 0, 0, TimeSpan.Zero));
        DiagnosticsSnapshotMetadata metadata = DiagnosticsSnapshotMetadata.Create(
            CSharpDbDiagnostics.CreateServerInstanceId(),
            counterEpoch: 0,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            databaseAlias: "primary",
            timeProvider: clock);
        string json = JsonSerializer.Serialize(
            metadata,
            CSharpDbObservabilityJsonContext.Default.DiagnosticsSnapshotMetadata).Replace(
                $"\"schemaVersion\":\"{CSharpDbDiagnostics.SchemaVersion}\"",
                "\"schemaVersion\":\"1.C:\\\\private\"",
                StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(
            json,
            CSharpDbObservabilityJsonContext.Default.DiagnosticsSnapshotMetadata));
    }

    [Fact]
    public void SourceGeneratedContext_CoversBoundedAndSectionEnvelopes()
    {
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(BoundedDiagnosticsSnapshot<ActiveQuerySnapshot>)));
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(BoundedDiagnosticsSnapshot<RecentQuerySnapshot>)));
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(BoundedDiagnosticsSnapshot<MaintenanceOperationSnapshot>)));
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>)));
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>)));
        Assert.NotNull(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>)));
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override DateTimeOffset GetUtcNow() => _utcNow;
        public override long GetTimestamp() => _timestamp;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public void Advance(TimeSpan elapsed)
        {
            _utcNow += elapsed;
            _timestamp += elapsed.Ticks;
        }
    }
}

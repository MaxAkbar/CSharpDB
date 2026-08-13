using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Observability;

namespace CSharpDB.Observability.Tests;

public sealed class RuntimeDiagnosticsValidationTests
{
    [Fact]
    public void RuntimeSnapshot_RequiresCompleteMatchingSectionsAndTruthfulUnavailableState()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        DiagnosticsSnapshotMetadata otherMetadata = Metadata(databaseAlias: "other");
        QueryDiagnosticsSummary queries = QuerySummary(metadata);
        QueryDiagnosticsSummary otherQueries = QuerySummary(otherMetadata);

        RuntimeDiagnosticsSnapshot valid = RuntimeSnapshot(metadata, queries);
        Assert.Equal(metadata, valid.Metadata);

        Assert.Throws<ArgumentException>(() => RuntimeSnapshot(metadata, otherQueries));
        Assert.Throws<ArgumentException>(() =>
            _ = valid with { Metadata = otherMetadata });
        Assert.Throws<ArgumentException>(() =>
            _ = valid with
            {
                Queries = DiagnosticsSection<QueryDiagnosticsSummary>.Available(otherQueries),
            });
        Assert.Throws<ArgumentNullException>(() => new RuntimeDiagnosticsSnapshot(
            metadata,
            Queries: null!,
            valid.Connections,
            valid.Storage,
            valid.Wal,
            valid.ActiveMaintenance,
            valid.Health));

        DiagnosticsSnapshotMetadata disabledMetadata = Metadata(
            availability: DiagnosticsAvailability.Disabled);
        RuntimeDiagnosticsSnapshot disabled = RuntimeSnapshotWithoutValues(
            disabledMetadata,
            DiagnosticsAvailability.Disabled);
        Assert.Equal(DiagnosticsAvailability.Disabled, disabled.Metadata.Availability);

        Assert.Throws<ArgumentException>(() => RuntimeSnapshotWithoutValues(
            disabledMetadata,
            DiagnosticsAvailability.Unavailable));
    }

    [Fact]
    public void QueryContracts_RejectUnsafeValuesAndRelationalWithExpressions()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        OpaqueDiagnosticsId operationId = Id('1');
        DateTimeOffset now = UtcNow;

        QueryDiagnosticsSummary summary = QuerySummary(metadata);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = summary with { RowsProduced = -1 });
        Assert.Throws<ArgumentException>(() => new QueryDiagnosticsSummary(
            Metadata(availability: DiagnosticsAvailability.Disabled),
            0, 0, 0, 0, 0, 0, 0, 0, 0));

        Assert.Throws<ArgumentException>(() => new ActiveQuerySnapshot(
            metadata,
            operationId,
            ParentOperationId: operationId,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            now,
            TimeSpan.Zero,
            Fingerprint: null,
            CSharpDbTransport.Direct,
            TraceId: null,
            SessionId: null));
        Assert.Throws<ArgumentException>(() =>
            _ = ActiveQuery(metadata) with { Phase = QueryExecutionPhase.Completed });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = ActiveQuery(metadata) with { Transport = (CSharpDbTransport)999 });
        Assert.Throws<ArgumentException>(() => new ActiveQuerySnapshot(
            metadata,
            operationId,
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            now.ToOffset(TimeSpan.FromHours(1)),
            TimeSpan.Zero,
            Fingerprint: null,
            CSharpDbTransport.Direct,
            TraceId: null,
            SessionId: null));

        Assert.Throws<ArgumentException>(() => RecentQuery(
            metadata,
            outcome: CSharpDbOperationOutcome.Succeeded,
            error: SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation)));
        Assert.Throws<ArgumentException>(() => RecentQuery(
            metadata,
            outcome: CSharpDbOperationOutcome.Failed,
            error: null));
        Assert.Throws<ArgumentException>(() => RecentQuery(
            metadata,
            timeToFirstResult: TimeSpan.FromMilliseconds(1),
            resultConsumptionDuration: null));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = RecentQuery(metadata) with { RowsAffected = -1 });

        RecentQuerySnapshot failed = RecentQuery(
            metadata,
            outcome: CSharpDbOperationOutcome.Failed,
            error: SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation));
        Assert.Throws<ArgumentException>(() =>
            _ = failed with { Outcome = CSharpDbOperationOutcome.Succeeded });
        Assert.Throws<ArgumentException>(() =>
            _ = RecentQuery(metadata) with { TimeToFirstResult = TimeSpan.Zero });
    }

    [Fact]
    public void PositionalRecordConstructionDeconstructionAndInitSurface_RemainCompatible()
    {
        QueryDiagnosticsSummary summary = QuerySummary(Metadata());
        (
            DiagnosticsSnapshotMetadata metadata,
            long requestCount,
            long statementExecutionCount,
            long succeededCount,
            long failedCount,
            long canceledCount,
            long slowCount,
            long rowsProduced,
            long rowsAffected,
            int activeCount) = summary;

        Assert.Equal(summary.Metadata, metadata);
        Assert.Equal(summary.RequestCount, requestCount);
        Assert.Equal(summary.StatementExecutionCount, statementExecutionCount);
        Assert.Equal(summary.SucceededCount, succeededCount);
        Assert.Equal(summary.FailedCount, failedCount);
        Assert.Equal(summary.CanceledCount, canceledCount);
        Assert.Equal(summary.SlowCount, slowCount);
        Assert.Equal(summary.RowsProduced, rowsProduced);
        Assert.Equal(summary.RowsAffected, rowsAffected);
        Assert.Equal(summary.ActiveCount, activeCount);
        Assert.NotNull(typeof(QueryDiagnosticsSummary).GetProperty(nameof(QueryDiagnosticsSummary.RowsProduced))?.SetMethod);
    }

    [Fact]
    public void QueryDetail_EnforcesAuthorizationShapeAbsoluteBoundAndTruncationSemantics()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        DiagnosticsSnapshotMetadata truncatedMetadata = Metadata(fieldsTruncated: true);
        OpaqueDiagnosticsId operationId = Id('2');

        var none = new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.None,
            CapturedSqlText: null);
        var normalized = new QueryDetailSnapshot(
            truncatedMetadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.Normalized,
            new string('x', QueryDetailSnapshot.MaximumCapturedSqlTextLength));

        Assert.Null(none.CapturedSqlText);
        Assert.Equal(QueryDetailSnapshot.MaximumCapturedSqlTextLength, normalized.CapturedSqlText!.Length);
        Assert.Throws<ArgumentException>(() => new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.None,
            "SELECT 1"));
        Assert.Throws<ArgumentException>(() => new QueryDetailSnapshot(
            truncatedMetadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.None,
            CapturedSqlText: null));
        Assert.Throws<ArgumentException>(() => new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.Normalized,
            "   "));
        Assert.Throws<ArgumentException>(() => new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.Raw,
            new string('x', QueryDetailSnapshot.MaximumCapturedSqlTextLength + 1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            (SqlTextCaptureMode)999,
            "SELECT 1"));
        Assert.Throws<ArgumentException>(() =>
            _ = normalized with
            {
                CapturedSqlText = new string(
                    'x',
                    QueryDetailSnapshot.MaximumCapturedSqlTextLength + 1),
            });
        Assert.Throws<ArgumentException>(() =>
            _ = none with { CaptureMode = SqlTextCaptureMode.Raw });
        Assert.Throws<ArgumentException>(() =>
            _ = none with { Metadata = truncatedMetadata });
        Assert.Throws<ArgumentException>(() =>
            _ = normalized with { CapturedSqlText = null });

        JsonTypeInfo<QueryDetailSnapshot> typeInfo =
            CSharpDbObservabilityJsonContext.Default.QueryDetailSnapshot;
        string json = JsonSerializer.Serialize(normalized, typeInfo);
        QueryDetailSnapshot roundTrip = Assert.IsType<QueryDetailSnapshot>(
            JsonSerializer.Deserialize(json, typeInfo));
        Assert.Equal(normalized, roundTrip);

        string malicious = JsonSerializer.Serialize(none, typeInfo).Replace(
            "\"captureMode\":\"None\"",
            "\"captureMode\":\"Normalized\"",
            StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(malicious, typeInfo));

        JsonObject oversized = Assert.IsType<JsonObject>(JsonNode.Parse(json));
        oversized["capturedSqlText"] = new string(
            'x',
            QueryDetailSnapshot.MaximumCapturedSqlTextLength + 1);
        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(
            oversized.ToJsonString(),
            typeInfo));
    }

    [Fact]
    public void PlanConnectionAndSessionContracts_RejectContradictionsAndUnsafeValues()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        QueryPlanDiagnosticsSnapshot plan = QueryPlan(metadata);

        Assert.Throws<ArgumentException>(() => new QueryPlanDiagnosticsSnapshot(
            metadata,
            Id('3'),
            Fingerprint: null,
            QueryAccessPathCategory.TableScan,
            PlanCacheHit: null,
            Reoptimized: null,
            EstimatedRows: null,
            ActualRows: null,
            PlanNodeCount: null,
            PlanTruncated: true));
        Assert.Throws<ArgumentException>(() =>
            _ = plan with { PlanTruncated = true });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = plan with { ActualRows = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = plan with { AccessPath = (QueryAccessPathCategory)999 });

        ConnectionDiagnosticsSnapshot connections = Connections(metadata);
        Assert.Throws<ArgumentException>(() => new ConnectionDiagnosticsSnapshot(
            metadata,
            PoolCapacity: 1,
            AvailableSlots: 2,
            WaiterCount: 0,
            ActiveLogicalSessions: 0,
            ActiveReaders: 0,
            ActiveTransactions: 0,
            RetiredPoolCount: 0,
            PoisonedPoolCount: 0,
            OldestTransactionAge: null));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = connections with { WarmEngineIdleCount = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = connections with { PoolState = (ConnectionPoolLifecycleState)999 });
        Assert.Throws<ArgumentException>(() =>
            _ = connections with { PoolCapacity = 2 });
        Assert.Throws<ArgumentException>(() =>
            _ = connections with { ActiveTransactions = 0 });

        SessionDiagnosticsSnapshot session = Session(metadata);
        Assert.Throws<ArgumentException>(() => new SessionDiagnosticsSnapshot(
            metadata,
            Id('4'),
            CreatedAtUtc: UtcNow,
            LastActiveAtUtc: UtcNow.AddTicks(-1),
            CurrentOperationId: null,
            HasActiveReader: false,
            HasActiveTransaction: false,
            CSharpDbTransport.Direct));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = session with { State = (DiagnosticsSessionState)999 });
        Assert.Throws<ArgumentException>(() =>
            _ = session with { LastActiveAtUtc = UtcNow.ToOffset(TimeSpan.FromHours(-1)) });
        Assert.Throws<ArgumentException>(() =>
            _ = session with { LastActiveAtUtc = UtcNow.AddMinutes(-2) });
    }

    [Fact]
    public void StorageWalMaintenanceAndHealthContracts_RejectUnsafeValues()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Storage(metadata) with { BytesRead = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Wal(metadata) with { CheckpointPhase = (CheckpointPhase)999 });
        Assert.Throws<ArgumentException>(() =>
            _ = Wal(metadata) with
            {
                LastSuccessfulFlushAtUtc = UtcNow.ToOffset(TimeSpan.FromHours(2)),
            });

        Assert.Throws<ArgumentException>(() => new MaintenanceOperationSnapshot(
            metadata,
            Id('5'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Copying,
            UtcNow,
            TimeSpan.Zero,
            CompletedUnits: 2,
            TotalUnits: 1,
            CSharpDbOperationOutcome.Unknown,
            WarningCount: 0,
            ErrorCount: 0,
            Error: null));
        Assert.Throws<ArgumentException>(() => new MaintenanceOperationSnapshot(
            metadata,
            Id('5'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Completed,
            UtcNow,
            TimeSpan.Zero,
            CompletedUnits: 1,
            TotalUnits: 1,
            CSharpDbOperationOutcome.Failed,
            WarningCount: 0,
            ErrorCount: 1,
            Error: null));

        MaintenanceOperationSnapshot activeMaintenance = new(
            metadata,
            Id('5'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Copying,
            UtcNow,
            TimeSpan.Zero,
            CompletedUnits: 1,
            TotalUnits: 2,
            CSharpDbOperationOutcome.Unknown,
            WarningCount: 0,
            ErrorCount: 0,
            Error: null);
        Assert.Throws<ArgumentException>(() =>
            _ = activeMaintenance with { Phase = MaintenanceOperationPhase.Completed });
        Assert.Throws<ArgumentException>(() =>
            _ = activeMaintenance with { CompletedUnits = 3 });

        MaintenanceOperationSnapshot failedMaintenance = new(
            metadata,
            Id('5'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Completed,
            UtcNow,
            TimeSpan.Zero,
            CompletedUnits: 1,
            TotalUnits: 1,
            CSharpDbOperationOutcome.Failed,
            WarningCount: 0,
            ErrorCount: 1,
            SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation));
        Assert.Throws<ArgumentException>(() =>
            _ = failedMaintenance with { Outcome = CSharpDbOperationOutcome.Succeeded });

        Assert.Throws<ArgumentException>(() => new HealthDiagnosticsSnapshot(
            metadata,
            CSharpDbHostLifecyclePhase.Failed,
            CSharpDbHealthStatus.Unhealthy,
            CSharpDbHealthStatus.Unhealthy,
            CSharpDbReadinessReason.InitializationFailed,
            UtcNow,
            Error: null));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = Health(metadata) with { Liveness = (CSharpDbHealthStatus)999 });
    }

    [Fact]
    public void EveryRuntimeDto_RoundTripsThroughItsSourceGeneratedContract()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        QueryDiagnosticsSummary queries = QuerySummary(metadata);
        ConnectionDiagnosticsSnapshot connections = Connections(metadata);
        StorageRuntimeDiagnosticsSnapshot storage = Storage(metadata);
        WalRuntimeDiagnosticsSnapshot wal = Wal(metadata);
        MaintenanceOperationSnapshot maintenance = Maintenance(metadata);
        HealthDiagnosticsSnapshot health = Health(metadata);
        RuntimeDiagnosticsSnapshot runtime = RuntimeSnapshot(
            metadata,
            queries,
            connections,
            storage,
            wal,
            maintenance,
            health);

        AssertRoundTrip(runtime, CSharpDbObservabilityJsonContext.Default.RuntimeDiagnosticsSnapshot);
        AssertRoundTrip(queries, CSharpDbObservabilityJsonContext.Default.QueryDiagnosticsSummary);
        AssertRoundTrip(ActiveQuery(metadata), CSharpDbObservabilityJsonContext.Default.ActiveQuerySnapshot);
        AssertRoundTrip(RecentQuery(metadata), CSharpDbObservabilityJsonContext.Default.RecentQuerySnapshot);
        AssertRoundTrip(
            new QueryDetailSnapshot(
                metadata,
                Id('1'),
                Fingerprint: null,
                SqlTextCaptureMode.Normalized,
                "SELECT value FROM runtime_contract WHERE id = ?"),
            CSharpDbObservabilityJsonContext.Default.QueryDetailSnapshot);
        AssertRoundTrip(QueryPlan(metadata), CSharpDbObservabilityJsonContext.Default.QueryPlanDiagnosticsSnapshot);
        AssertRoundTrip(connections, CSharpDbObservabilityJsonContext.Default.ConnectionDiagnosticsSnapshot);
        AssertRoundTrip(Session(metadata), CSharpDbObservabilityJsonContext.Default.SessionDiagnosticsSnapshot);
        AssertRoundTrip(storage, CSharpDbObservabilityJsonContext.Default.StorageRuntimeDiagnosticsSnapshot);
        AssertRoundTrip(wal, CSharpDbObservabilityJsonContext.Default.WalRuntimeDiagnosticsSnapshot);
        AssertRoundTrip(maintenance, CSharpDbObservabilityJsonContext.Default.MaintenanceOperationSnapshot);
        AssertRoundTrip(health, CSharpDbObservabilityJsonContext.Default.HealthDiagnosticsSnapshot);
    }

    [Fact]
    public void MaliciousSourceGeneratedPayloads_CannotBypassConstructors()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata();
        QueryDiagnosticsSummary summary = QuerySummary(metadata);
        string summaryJson = JsonSerializer.Serialize(
            summary,
            CSharpDbObservabilityJsonContext.Default.QueryDiagnosticsSummary).Replace(
                "\"requestCount\":1",
                "\"requestCount\":-1",
                StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>(() => JsonSerializer.Deserialize(
            summaryJson,
            CSharpDbObservabilityJsonContext.Default.QueryDiagnosticsSummary));

        SessionDiagnosticsSnapshot session = Session(metadata);
        string validSessionJson = JsonSerializer.Serialize(
            session,
            CSharpDbObservabilityJsonContext.Default.SessionDiagnosticsSnapshot);
        JsonObject maliciousSession = Assert.IsType<JsonObject>(
            JsonNode.Parse(validSessionJson));
        maliciousSession["lastActiveAtUtc"] = UtcNow.AddMinutes(-2);
        Assert.Throws<ArgumentException>(() => JsonSerializer.Deserialize(
            maliciousSession.ToJsonString(),
            CSharpDbObservabilityJsonContext.Default.SessionDiagnosticsSnapshot));

        ConnectionDiagnosticsSnapshot connections = Connections(metadata);
        JsonObject maliciousConnection = Assert.IsType<JsonObject>(JsonNode.Parse(
            JsonSerializer.Serialize(
                connections,
                CSharpDbObservabilityJsonContext.Default.ConnectionDiagnosticsSnapshot)));
        maliciousConnection["warmEngineIdleCount"] = -1;
        Assert.Throws<ArgumentOutOfRangeException>(() => JsonSerializer.Deserialize(
            maliciousConnection.ToJsonString(),
            CSharpDbObservabilityJsonContext.Default.ConnectionDiagnosticsSnapshot));
    }

    private static readonly DateTimeOffset UtcNow =
        new(2026, 8, 10, 12, 0, 0, TimeSpan.Zero);

    private static DiagnosticsSnapshotMetadata Metadata(
        DiagnosticsAvailability availability = DiagnosticsAvailability.Available,
        string databaseAlias = "primary",
        bool fieldsTruncated = false)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            UtcNow,
            "0123456789abcdef0123456789abcdef",
            counterEpoch: 0,
            DiagnosticsScope.Instance,
            availability,
            DiagnosticsSource.Engine,
            databaseAlias,
            recordsTruncated: false,
            fieldsTruncated);

    private static OpaqueDiagnosticsId Id(char digit)
        => new(new string(digit, 32));

    private static QueryDiagnosticsSummary QuerySummary(DiagnosticsSnapshotMetadata metadata)
        => new(metadata, 1, 1, 1, 0, 0, 0, 1, 0, 0);

    private static ActiveQuerySnapshot ActiveQuery(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('1'),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            UtcNow,
            TimeSpan.FromSeconds(1),
            Fingerprint: null,
            CSharpDbTransport.Direct,
            TraceId: null,
            SessionId: null);

    private static RecentQuerySnapshot RecentQuery(
        DiagnosticsSnapshotMetadata metadata,
        CSharpDbOperationOutcome outcome = CSharpDbOperationOutcome.Succeeded,
        SafeErrorProjection? error = null,
        TimeSpan? timeToFirstResult = null,
        TimeSpan? resultConsumptionDuration = null)
        => new(
            metadata,
            Id('1'),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            UtcNow,
            UtcNow.AddSeconds(2),
            TimeSpan.FromSeconds(2),
            timeToFirstResult,
            resultConsumptionDuration,
            outcome,
            Fingerprint: null,
            CSharpDbTransport.Direct,
            RowsProduced: 1,
            RowsAffected: 0,
            TraceId: null,
            SessionId: null,
            error);

    private static QueryPlanDiagnosticsSnapshot QueryPlan(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('1'),
            Fingerprint: null,
            QueryAccessPathCategory.PrimaryKeyLookup,
            PlanCacheHit: true,
            Reoptimized: false,
            EstimatedRows: 1,
            ActualRows: 1,
            PlanNodeCount: 1,
            PlanTruncated: false);

    private static ConnectionDiagnosticsSnapshot Connections(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            PoolCapacity: 4,
            AvailableSlots: 3,
            WaiterCount: 0,
            ActiveLogicalSessions: 1,
            ActiveReaders: 0,
            ActiveTransactions: 1,
            RetiredPoolCount: 0,
            PoisonedPoolCount: 0,
            OldestTransactionAge: TimeSpan.FromSeconds(1))
        {
            WarmEngineIdleCount = 1,
            DisabledPoolCount = 0,
            RetiringPoolCount = 0,
            TransactionOwnerSessionId = Id('6'),
            PoolState = ConnectionPoolLifecycleState.Enabled,
            ExclusiveMaintenanceActive = false,
        };

    private static SessionDiagnosticsSnapshot Session(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('6'),
            UtcNow.AddMinutes(-1),
            UtcNow,
            CurrentOperationId: Id('1'),
            HasActiveReader: false,
            HasActiveTransaction: true,
            CSharpDbTransport.Direct)
        {
            State = DiagnosticsSessionState.Transaction,
        };

    private static StorageRuntimeDiagnosticsSnapshot Storage(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            LogicalDatabaseBytes: 1,
            AllocatedDatabaseBytes: 1,
            PageCount: 1,
            PageReads: 1,
            PageWrites: 1,
            BytesRead: 1,
            BytesWritten: 1,
            CacheHits: 1,
            CacheMisses: 0,
            DirtyPages: 0,
            ActiveReaders: 0,
            ActiveWriters: 0,
            CommitCount: 1,
            ConflictCount: 0);

    private static WalRuntimeDiagnosticsSnapshot Wal(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            LogicalBytes: 1,
            AllocatedBytes: 1,
            CommittedFrameBytes: 1,
            RetainedBytes: 0,
            FrameCount: 1,
            FlushCount: 1,
            BytesWritten: 1,
            PendingCommitCount: 0,
            CheckpointPhase.Idle,
            LastSuccessfulFlushAtUtc: UtcNow,
            LastSuccessfulCheckpointAtUtc: UtcNow,
            LastError: null);

    private static MaintenanceOperationSnapshot Maintenance(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('5'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Completed,
            UtcNow,
            TimeSpan.FromSeconds(1),
            CompletedUnits: 1,
            TotalUnits: 1,
            CSharpDbOperationOutcome.Succeeded,
            WarningCount: 0,
            ErrorCount: 0,
            Error: null);

    private static HealthDiagnosticsSnapshot Health(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            CSharpDbHostLifecyclePhase.Running,
            CSharpDbHealthStatus.Healthy,
            CSharpDbHealthStatus.Healthy,
            CSharpDbReadinessReason.None,
            UtcNow,
            Error: null);

    private static RuntimeDiagnosticsSnapshot RuntimeSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        QueryDiagnosticsSummary queries)
        => RuntimeSnapshot(
            metadata,
            queries,
            Connections(metadata),
            Storage(metadata),
            Wal(metadata),
            Maintenance(metadata),
            Health(metadata));

    private static RuntimeDiagnosticsSnapshot RuntimeSnapshot(
        DiagnosticsSnapshotMetadata metadata,
        QueryDiagnosticsSummary queries,
        ConnectionDiagnosticsSnapshot connections,
        StorageRuntimeDiagnosticsSnapshot storage,
        WalRuntimeDiagnosticsSnapshot wal,
        MaintenanceOperationSnapshot maintenance,
        HealthDiagnosticsSnapshot health)
        => new(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(queries),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(connections),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.Available(storage),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(wal),
            DiagnosticsSection<MaintenanceOperationSnapshot>.Available(maintenance),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.Available(health));

    private static RuntimeDiagnosticsSnapshot RuntimeSnapshotWithoutValues(
        DiagnosticsSnapshotMetadata metadata,
        DiagnosticsAvailability availability)
        => new(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(availability),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(availability),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(availability),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(availability));

    private static void AssertRoundTrip<T>(T value, JsonTypeInfo<T> typeInfo)
        where T : class
    {
        string json = JsonSerializer.Serialize(value, typeInfo);
        T roundTrip = Assert.IsType<T>(JsonSerializer.Deserialize(json, typeInfo));
        Assert.Equal(value, roundTrip);
    }
}

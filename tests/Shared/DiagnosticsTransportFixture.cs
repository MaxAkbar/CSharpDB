using CSharpDB.Observability;

namespace CSharpDB.Testing;

internal sealed class DiagnosticsTransportFixture
{
    private static readonly DateTimeOffset CapturedAtUtc =
        new(2026, 8, 12, 12, 0, 0, TimeSpan.Zero);
    private static readonly QueryFingerprint Fingerprint = new(
        $"{QueryFingerprint.Algorithm}:{new string('a', 64)}");

    private DiagnosticsTransportFixture()
    {
        OperationId = Id('1');

        DiagnosticsSnapshotMetadata aggregate = Metadata(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            DiagnosticsScope.Aggregate,
            DiagnosticsSource.Client,
            "transport-fixture",
            counterEpoch: 7);
        DiagnosticsSnapshotMetadata shard = Metadata(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            DiagnosticsScope.Shard,
            DiagnosticsSource.Engine,
            "shard-a",
            counterEpoch: 11);
        DiagnosticsSnapshotMetadata aggregatePlan = FieldsTruncatedMetadata(aggregate);
        DiagnosticsSnapshotMetadata shardPlan = FieldsTruncatedMetadata(shard);

        Runtime = Topology(RuntimeSnapshot(aggregate), RuntimeSnapshot(shard));
        Storage = Topology(
            new DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>(
                aggregate,
                StorageSnapshot(aggregate)),
            new DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>(
                shard,
                StorageSnapshot(shard)));
        Wal = Topology(
            new DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>(
                aggregate,
                WalSnapshot(aggregate)),
            new DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>(
                shard,
                WalSnapshot(shard)));
        ActiveQueries = Topology(
            Collection(
                CollectionMetadata(aggregate),
                ActiveQuery(CollectionMetadata(aggregate)),
                retention: null),
            Collection(
                CollectionMetadata(shard),
                ActiveQuery(CollectionMetadata(shard)),
                retention: null));
        RecentQueries = Topology(
            Collection(
                CollectionMetadata(aggregate),
                RecentQuery(CollectionMetadata(aggregate)),
                TimeSpan.FromMinutes(15)),
            Collection(
                CollectionMetadata(shard),
                RecentQuery(CollectionMetadata(shard)),
                TimeSpan.FromMinutes(15)));
        QueryPlan = Topology(
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                aggregatePlan,
                QueryPlanSnapshot(aggregatePlan)),
            new DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>(
                shardPlan,
                QueryPlanSnapshot(shardPlan)));
        Sessions = Topology(
            Collection(
                CollectionMetadata(aggregate),
                SessionSnapshot(CollectionMetadata(aggregate)),
                retention: null),
            Collection(
                CollectionMetadata(shard),
                SessionSnapshot(CollectionMetadata(shard)),
                retention: null));
        ActiveMaintenance = Topology(
            Collection(
                CollectionMetadata(aggregate),
                ActiveMaintenanceSnapshot(CollectionMetadata(aggregate)),
                retention: null),
            Collection(
                CollectionMetadata(shard),
                ActiveMaintenanceSnapshot(CollectionMetadata(shard)),
                retention: null));
        RecentMaintenance = Topology(
            Collection(
                CollectionMetadata(aggregate),
                RecentMaintenanceSnapshot(CollectionMetadata(aggregate)),
                TimeSpan.FromHours(1)),
            Collection(
                CollectionMetadata(shard),
                RecentMaintenanceSnapshot(CollectionMetadata(shard)),
                TimeSpan.FromHours(1)));
        QueryDetail = Topology(
            new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
                aggregate,
                QueryDetailSnapshot(aggregate)),
            new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
                shard,
                QueryDetailSnapshot(shard)));
    }

    internal static DiagnosticsTransportFixture Create() => new();

    internal OpaqueDiagnosticsId OperationId { get; }

    internal DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> Runtime { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        StorageRuntimeDiagnosticsSnapshot>> Storage { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        WalRuntimeDiagnosticsSnapshot>> Wal { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        ActiveQuerySnapshot>> ActiveQueries { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        RecentQuerySnapshot>> RecentQueries { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        QueryPlanDiagnosticsSnapshot>> QueryPlan { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        SessionDiagnosticsSnapshot>> Sessions { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>> ActiveMaintenance { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>> RecentMaintenance { get; }

    internal DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        QueryDetailSnapshot>> QueryDetail { get; }

    private static DiagnosticsTopologySnapshot<T> Topology<T>(
        T aggregate,
        T shard)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            [
                new ShardDiagnosticsSection<T>(
                    "shard-a",
                    DiagnosticsAvailability.Available,
                    shard),
            ],
            shardCapacity: 2,
            droppedShardCount: 1,
            shardsTruncated: true);

    private static DiagnosticsSnapshotMetadata Metadata(
        string serverInstanceId,
        DiagnosticsScope scope,
        DiagnosticsSource source,
        string alias,
        long counterEpoch,
        bool recordsTruncated = false,
        bool fieldsTruncated = false)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            CapturedAtUtc,
            serverInstanceId,
            counterEpoch,
            scope,
            DiagnosticsAvailability.Available,
            source,
            alias,
            recordsTruncated,
            fieldsTruncated);

    private static DiagnosticsSnapshotMetadata CollectionMetadata(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            metadata.Scope,
            metadata.Availability,
            metadata.Source,
            metadata.DatabaseAlias,
            recordsTruncated: true,
            fieldsTruncated: true);

    private static DiagnosticsSnapshotMetadata FieldsTruncatedMetadata(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata.SchemaVersion,
            metadata.CapturedAtUtc,
            metadata.ServerInstanceId,
            metadata.CounterEpoch,
            metadata.Scope,
            metadata.Availability,
            metadata.Source,
            metadata.DatabaseAlias,
            metadata.RecordsTruncated,
            fieldsTruncated: true);

    private static DiagnosticsCollectionSnapshot<T> Collection<T>(
        DiagnosticsSnapshotMetadata metadata,
        T record,
        TimeSpan? retention)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            metadata,
            [record],
            capacity: 8,
            retention,
            droppedCount: 3,
            isTruncated: true);

    private static RuntimeDiagnosticsSnapshot RuntimeSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(
                new QueryDiagnosticsSummary(
                    metadata,
                    RequestCount: 3,
                    StatementExecutionCount: 4,
                    SucceededCount: 1,
                    FailedCount: 1,
                    CanceledCount: 1,
                    SlowCount: 1,
                    RowsProduced: 12,
                    RowsAffected: 2,
                    ActiveCount: 1)),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.Available(
                ConnectionSnapshot(metadata)),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.Available(
                StorageSnapshot(metadata)),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                WalSnapshot(metadata)),
            DiagnosticsSection<MaintenanceOperationSnapshot>.Available(
                ActiveMaintenanceSnapshot(metadata)),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.Available(
                new HealthDiagnosticsSnapshot(
                    metadata,
                    CSharpDbHostLifecyclePhase.Running,
                    CSharpDbHealthStatus.Healthy,
                    CSharpDbHealthStatus.Degraded,
                    CSharpDbReadinessReason.ReadOnly,
                    CapturedAtUtc.AddSeconds(-1),
                    SafeErrorProjector.Project(SafeErrorKind.DatabaseIo))));

    private static ConnectionDiagnosticsSnapshot ConnectionSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            PoolCapacity: 16,
            AvailableSlots: 9,
            WaiterCount: 2,
            ActiveLogicalSessions: 4,
            ActiveReaders: 2,
            ActiveTransactions: 1,
            RetiredPoolCount: 1,
            PoisonedPoolCount: 1,
            OldestTransactionAge: TimeSpan.FromSeconds(9))
        {
            WarmEngineIdleCount = 3,
            DisabledPoolCount = 1,
            RetiringPoolCount = 1,
            TransactionOwnerSessionId = Id('6'),
            PoolState = ConnectionPoolLifecycleState.Enabled,
            ExclusiveMaintenanceActive = true,
        };

    private static StorageRuntimeDiagnosticsSnapshot StorageSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            LogicalDatabaseBytes: 32_768,
            AllocatedDatabaseBytes: 65_536,
            PageCount: 16,
            PageReads: 10,
            PageWrites: 4,
            BytesRead: 40_960,
            BytesWritten: 16_384,
            CacheHits: 8,
            CacheMisses: 2,
            DirtyPages: 1,
            ActiveReaders: 2,
            ActiveWriters: 1,
            CommitCount: 3,
            ConflictCount: 1)
        {
            Cache = DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                new StorageCacheDiagnosticsSnapshot(
                    metadata,
                    sharedResidentPages: 6,
                    sharedCapacityPages: 32,
                    walResidentPages: 3,
                    walCapacityPages: 16)),
            PhysicalIo = DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>.Available(
                new StorageDeviceIoDiagnosticsSnapshot(
                    metadata,
                    readCount: 10,
                    bytesRead: 40_960,
                    writeCount: 4,
                    bytesWritten: 16_384,
                    flushCount: 3,
                    resizeCount: 1,
                    sequentialReadCount: 7,
                    sequentialBytesRead: 28_672,
                    memoryMappedPageExposureCount: 5,
                    memoryMappedBytesExposed: 20_480)),
        };

    private static WalRuntimeDiagnosticsSnapshot WalSnapshot(
        DiagnosticsSnapshotMetadata metadata)
    {
        var recovery = new WalRecoveryDiagnosticsSnapshot(
            metadata,
            Id('7'),
            WalRecoveryPhase.Completed,
            CapturedAtUtc.AddSeconds(-4),
            CapturedAtUtc.AddSeconds(-3),
            TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 9,
            scannedBytes: 36_864,
            recoveredFrameCount: 8,
            recoveredBytes: 32_768,
            discardedFrameCount: 1,
            discardedBytes: 4_096,
            WalRecoveryTruncationReason.IncompleteTail,
            attemptCount: 2,
            retryCount: 1,
            lastRetryError: SafeErrorProjector.Project(SafeErrorKind.DatabaseBusy),
            error: null);
        var checkpoint = new CheckpointDiagnosticsSnapshot(
            metadata,
            Id('8'),
            CheckpointPhase.Copying,
            CheckpointOrigin.BackgroundAuto,
            CapturedAtUtc.AddSeconds(-2),
            TimeSpan.FromSeconds(2),
            completedPageCount: 4,
            totalPageCount: 8,
            CheckpointRetentionReason.ActiveReaders,
            lastStartedAtUtc: CapturedAtUtc.AddSeconds(-2),
            lastSuccessfulAtUtc: CapturedAtUtc.AddMinutes(-1),
            lastFailedAtUtc: CapturedAtUtc.AddMinutes(-2),
            lastElapsed: TimeSpan.FromSeconds(2),
            activeCount: 1,
            attemptCount: 5,
            successCount: 3,
            failureCount: 1,
            canceledCount: 0,
            lastError: SafeErrorProjector.Project(SafeErrorKind.DatabaseBusy));

        return new WalRuntimeDiagnosticsSnapshot(
            metadata,
            LogicalBytes: 36_864,
            AllocatedBytes: 40_960,
            CommittedFrameBytes: 32_768,
            RetainedBytes: 4_096,
            FrameCount: 9,
            FlushCount: 3,
            BytesWritten: 36_864,
            PendingCommitCount: 2,
            CheckpointPhase.Copying,
            LastSuccessfulFlushAtUtc: CapturedAtUtc.AddSeconds(-1),
            LastSuccessfulCheckpointAtUtc: CapturedAtUtc.AddMinutes(-1),
            LastError: SafeErrorProjector.Project(SafeErrorKind.DatabaseBusy))
        {
            FlushedCommitCount = 7,
            DurableFlushCount = 5,
            LastSuccessfulDurableFlushAtUtc = CapturedAtUtc.AddSeconds(-1),
            GroupCommitBatchCount = 2,
            GroupCommitCount = 4,
            LastSuccessfulGroupCommitAtUtc = CapturedAtUtc.AddSeconds(-1),
            Recovery = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                recovery),
            Checkpoint = DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                checkpoint),
        };
    }

    private ActiveQuerySnapshot ActiveQuery(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            OperationId,
            ParentOperationId: Id('2'),
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Statement,
            QueryExecutionPhase.Streaming,
            CapturedAtUtc.AddSeconds(-3),
            TimeSpan.FromSeconds(3),
            Fingerprint,
            CSharpDbTransport.Grpc,
            new DiagnosticsTraceId("0123456789abcdef0123456789abcdef"),
            SessionId: Id('6'));

    private RecentQuerySnapshot RecentQuery(DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            OperationId,
            ParentOperationId: Id('2'),
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Statement,
            CapturedAtUtc.AddSeconds(-5),
            CapturedAtUtc.AddSeconds(-1),
            TimeSpan.FromSeconds(4),
            TimeToFirstResult: TimeSpan.FromSeconds(1),
            ResultConsumptionDuration: TimeSpan.FromSeconds(2),
            CSharpDbOperationOutcome.Failed,
            Fingerprint,
            CSharpDbTransport.Http,
            RowsProduced: 2,
            RowsAffected: 0,
            new DiagnosticsTraceId("0123456789abcdef0123456789abcdef"),
            SessionId: Id('6'),
            SafeErrorProjector.Project(SafeErrorKind.DatabaseConstraint));

    private QueryPlanDiagnosticsSnapshot QueryPlanSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            OperationId,
            Fingerprint,
            QueryAccessPathCategory.IndexSeek,
            PlanCacheHit: true,
            Reoptimized: true,
            EstimatedRows: 10,
            ActualRows: 7,
            PlanNodeCount: 4,
            PlanTruncated: true);

    private SessionDiagnosticsSnapshot SessionSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('6'),
            CapturedAtUtc.AddMinutes(-2),
            CapturedAtUtc.AddSeconds(-1),
            CurrentOperationId: OperationId,
            HasActiveReader: true,
            HasActiveTransaction: true,
            CSharpDbTransport.Grpc)
        {
            State = DiagnosticsSessionState.Transaction,
        };

    private static MaintenanceOperationSnapshot ActiveMaintenanceSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('9'),
            MaintenanceOperationKind.Backup,
            MaintenanceOperationPhase.Copying,
            CapturedAtUtc.AddSeconds(-8),
            TimeSpan.FromSeconds(8),
            CompletedUnits: 5,
            TotalUnits: 10,
            CSharpDbOperationOutcome.Unknown,
            WarningCount: 1,
            ErrorCount: 0,
            Error: null);

    private static MaintenanceOperationSnapshot RecentMaintenanceSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            Id('c'),
            MaintenanceOperationKind.Restore,
            MaintenanceOperationPhase.Completed,
            CapturedAtUtc.AddMinutes(-3),
            TimeSpan.FromMinutes(1),
            CompletedUnits: 8,
            TotalUnits: 10,
            CSharpDbOperationOutcome.Failed,
            WarningCount: 2,
            ErrorCount: 1,
            SafeErrorProjector.Project(SafeErrorKind.DatabaseIo));

    private QueryDetailSnapshot QueryDetailSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new(
            metadata,
            OperationId,
            Fingerprint,
            SqlTextCaptureMode.Normalized,
            "SELECT value FROM transport_fixture WHERE id = ?");

    private static OpaqueDiagnosticsId Id(char digit)
        => new(new string(digit, 32));
}

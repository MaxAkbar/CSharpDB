using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

public sealed record DiagnosticsSnapshotMetadata
{
    [JsonConstructor]
    public DiagnosticsSnapshotMetadata(
        string schemaVersion,
        DateTimeOffset capturedAtUtc,
        string serverInstanceId,
        long counterEpoch,
        DiagnosticsScope scope,
        DiagnosticsAvailability availability,
        DiagnosticsSource source,
        string databaseAlias,
        bool recordsTruncated,
        bool fieldsTruncated)
    {
        if (!IsSupportedSchemaVersion(schemaVersion))
        {
            throw new ArgumentException(
                "A supported bounded diagnostics schema version is required.",
                nameof(schemaVersion));
        }
        if (capturedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("The capture timestamp must be UTC.", nameof(capturedAtUtc));
        if (!CSharpDbDiagnostics.IsValidOpaqueIdentifier(serverInstanceId))
            throw new ArgumentException("A generated opaque server instance id is required.", nameof(serverInstanceId));
        if (counterEpoch < 0)
            throw new ArgumentOutOfRangeException(nameof(counterEpoch));
        if (scope == DiagnosticsScope.Unknown || !Enum.IsDefined(scope))
            throw new ArgumentOutOfRangeException(nameof(scope));
        if (availability == DiagnosticsAvailability.Unknown || !Enum.IsDefined(availability))
            throw new ArgumentOutOfRangeException(nameof(availability));
        if (source == DiagnosticsSource.Unknown || !Enum.IsDefined(source))
            throw new ArgumentOutOfRangeException(nameof(source));
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));

        SchemaVersion = schemaVersion;
        CapturedAtUtc = capturedAtUtc;
        ServerInstanceId = serverInstanceId;
        CounterEpoch = counterEpoch;
        Scope = scope;
        Availability = availability;
        Source = source;
        DatabaseAlias = databaseAlias;
        RecordsTruncated = recordsTruncated;
        FieldsTruncated = fieldsTruncated;
    }

    public string SchemaVersion { get; }
    public DateTimeOffset CapturedAtUtc { get; }
    public string ServerInstanceId { get; }
    public long CounterEpoch { get; }
    public DiagnosticsScope Scope { get; }
    public DiagnosticsAvailability Availability { get; }
    public DiagnosticsSource Source { get; }
    public string DatabaseAlias { get; }
    public bool RecordsTruncated { get; }
    public bool FieldsTruncated { get; }

    private static bool IsSupportedSchemaVersion(string? schemaVersion)
    {
        if (schemaVersion is null ||
            schemaVersion.Length is < 3 or > 16 ||
            schemaVersion[0] != '1' ||
            schemaVersion[1] != '.')
        {
            return false;
        }

        foreach (char character in schemaVersion.AsSpan(2))
        {
            if (character is not (>= '0' and <= '9'))
                return false;
        }

        return true;
    }

    public static DiagnosticsSnapshotMetadata Create(
        string serverInstanceId,
        long counterEpoch,
        DiagnosticsScope scope,
        DiagnosticsAvailability availability,
        DiagnosticsSource source,
        string databaseAlias,
        bool recordsTruncated = false,
        bool fieldsTruncated = false,
        TimeProvider? timeProvider = null)
    {
        if (!CSharpDbDiagnostics.IsValidOpaqueIdentifier(serverInstanceId))
            throw new ArgumentException("A generated opaque server instance id is required.", nameof(serverInstanceId));
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));
        if (counterEpoch < 0)
            throw new ArgumentOutOfRangeException(nameof(counterEpoch));
        if (scope == DiagnosticsScope.Unknown)
            throw new ArgumentOutOfRangeException(nameof(scope));
        if (availability == DiagnosticsAvailability.Unknown)
            throw new ArgumentOutOfRangeException(nameof(availability));
        if (source == DiagnosticsSource.Unknown)
            throw new ArgumentOutOfRangeException(nameof(source));

        return new DiagnosticsSnapshotMetadata(
            CSharpDbDiagnostics.SchemaVersion,
            (timeProvider ?? TimeProvider.System).GetUtcNow(),
            serverInstanceId,
            counterEpoch,
            scope,
            availability,
            source,
            databaseAlias,
            recordsTruncated,
            fieldsTruncated);
    }
}

public interface IRuntimeDiagnosticsSnapshot
{
    DiagnosticsSnapshotMetadata Metadata { get; }
}

public sealed record DiagnosticsSection<T>
    where T : class
{
    [JsonConstructor]
    public DiagnosticsSection(DiagnosticsAvailability availability, T? value)
    {
        if (availability == DiagnosticsAvailability.Unknown || !Enum.IsDefined(availability))
            throw new ArgumentOutOfRangeException(nameof(availability));
        if ((availability == DiagnosticsAvailability.Available) != (value is not null))
        {
            throw new ArgumentException(
                "An available diagnostics section requires a value, and an unavailable section must omit it.");
        }

        Availability = availability;
        Value = value;
    }

    public DiagnosticsAvailability Availability { get; }
    public T? Value { get; }

    public static DiagnosticsSection<T> Available(T value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new DiagnosticsSection<T>(DiagnosticsAvailability.Available, value);
    }

    public static DiagnosticsSection<T> WithoutValue(DiagnosticsAvailability availability)
    {
        if (availability is DiagnosticsAvailability.Unknown or DiagnosticsAvailability.Available)
            throw new ArgumentOutOfRangeException(nameof(availability));

        return new DiagnosticsSection<T>(availability, value: null);
    }
}

public sealed record RuntimeDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    DiagnosticsSection<QueryDiagnosticsSummary> Queries,
    DiagnosticsSection<ConnectionDiagnosticsSnapshot> Connections,
    DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> Storage,
    DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> Wal,
    DiagnosticsSection<MaintenanceOperationSnapshot> ActiveMaintenance,
    DiagnosticsSection<HealthDiagnosticsSnapshot> Health) : IRuntimeDiagnosticsSnapshot;

public sealed record QueryDiagnosticsSummary(
    DiagnosticsSnapshotMetadata Metadata,
    long RequestCount,
    long StatementExecutionCount,
    long SucceededCount,
    long FailedCount,
    long CanceledCount,
    long SlowCount,
    long RowsProduced,
    long RowsAffected,
    int ActiveCount) : IRuntimeDiagnosticsSnapshot;

public sealed record ActiveQuerySnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    OpaqueDiagnosticsId? ParentOperationId,
    CSharpDbOperationClass OperationClass,
    CSharpDbOperationRole Role,
    QueryExecutionPhase Phase,
    DateTimeOffset StartedAtUtc,
    TimeSpan Elapsed,
    QueryFingerprint? Fingerprint,
    CSharpDbTransport Transport,
    DiagnosticsTraceId? TraceId,
    OpaqueDiagnosticsId? SessionId) : IRuntimeDiagnosticsSnapshot;

public sealed record RecentQuerySnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    OpaqueDiagnosticsId? ParentOperationId,
    CSharpDbOperationClass OperationClass,
    CSharpDbOperationRole Role,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc,
    TimeSpan Duration,
    TimeSpan? TimeToFirstResult,
    TimeSpan? ResultConsumptionDuration,
    CSharpDbOperationOutcome Outcome,
    QueryFingerprint? Fingerprint,
    CSharpDbTransport Transport,
    long RowsProduced,
    long RowsAffected,
    DiagnosticsTraceId? TraceId,
    OpaqueDiagnosticsId? SessionId,
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot;

/// <summary>
/// Separately authorized query detail. This model is intentionally absent from
/// ordinary runtime, active-query, and recent-query snapshots.
/// </summary>
public sealed record QueryDetailSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    QueryFingerprint? Fingerprint,
    SqlTextCaptureMode CaptureMode,
    string? CapturedSqlText) : IRuntimeDiagnosticsSnapshot;

public sealed record QueryPlanDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    QueryFingerprint? Fingerprint,
    QueryAccessPathCategory AccessPath,
    bool? PlanCacheHit,
    bool? Reoptimized,
    long? EstimatedRows,
    long? ActualRows,
    int? PlanNodeCount,
    bool PlanTruncated) : IRuntimeDiagnosticsSnapshot;

public sealed record ConnectionDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    int? PoolCapacity,
    int? AvailableSlots,
    int? WaiterCount,
    int? ActiveLogicalSessions,
    int? ActiveReaders,
    int? ActiveTransactions,
    int? RetiredPoolCount,
    int? PoisonedPoolCount,
    TimeSpan? OldestTransactionAge) : IRuntimeDiagnosticsSnapshot;

public sealed record SessionDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId SessionId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset LastActiveAtUtc,
    OpaqueDiagnosticsId? CurrentOperationId,
    bool HasActiveReader,
    bool HasActiveTransaction,
    CSharpDbTransport Transport) : IRuntimeDiagnosticsSnapshot;

public sealed record StorageRuntimeDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    long? LogicalDatabaseBytes,
    long? AllocatedDatabaseBytes,
    long? PageCount,
    long? PageReads,
    long? PageWrites,
    long? BytesRead,
    long? BytesWritten,
    long? CacheHits,
    long? CacheMisses,
    long? DirtyPages,
    int? ActiveReaders,
    int? ActiveWriters,
    long? CommitCount,
    long? ConflictCount) : IRuntimeDiagnosticsSnapshot;

public sealed record WalRuntimeDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    long? LogicalBytes,
    long? AllocatedBytes,
    long? CommittedFrameBytes,
    long? RetainedBytes,
    long? FrameCount,
    long? FlushCount,
    long? BytesWritten,
    int? PendingCommitCount,
    CheckpointPhase CheckpointPhase,
    DateTimeOffset? LastSuccessfulFlushAtUtc,
    DateTimeOffset? LastSuccessfulCheckpointAtUtc,
    SafeErrorProjection? LastError) : IRuntimeDiagnosticsSnapshot;

public sealed record MaintenanceOperationSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    MaintenanceOperationKind Kind,
    MaintenanceOperationPhase Phase,
    DateTimeOffset StartedAtUtc,
    TimeSpan Elapsed,
    long? CompletedUnits,
    long? TotalUnits,
    CSharpDbOperationOutcome Outcome,
    int WarningCount,
    int ErrorCount,
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot;

public sealed record HealthDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    CSharpDbHostLifecyclePhase LifecyclePhase,
    CSharpDbHealthStatus Liveness,
    CSharpDbHealthStatus Readiness,
    CSharpDbReadinessReason ReadinessReason,
    DateTimeOffset ChangedAtUtc,
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot;

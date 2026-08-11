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

        TimeProvider effectiveTimeProvider = timeProvider ?? TimeProvider.System;
        DateTimeOffset capturedAtUtc;
        try
        {
            capturedAtUtc = effectiveTimeProvider.GetUtcNow().ToUniversalTime();
        }
        catch
        {
            capturedAtUtc = TimeProvider.System.GetUtcNow();
        }

        return new DiagnosticsSnapshotMetadata(
            CSharpDbDiagnostics.SchemaVersion,
            capturedAtUtc,
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
    DiagnosticsSection<HealthDiagnosticsSnapshot> Health) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
            Metadata,
            Queries,
            Connections,
            Storage,
            Wal,
            ActiveMaintenance,
            Health);
    private DiagnosticsSection<QueryDiagnosticsSummary> _queries =
        RuntimeDiagnosticsSnapshotContract.NotNull(Queries, nameof(Queries));
    private DiagnosticsSection<ConnectionDiagnosticsSnapshot> _connections =
        RuntimeDiagnosticsSnapshotContract.NotNull(Connections, nameof(Connections));
    private DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> _storage =
        RuntimeDiagnosticsSnapshotContract.NotNull(Storage, nameof(Storage));
    private DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> _wal =
        RuntimeDiagnosticsSnapshotContract.NotNull(Wal, nameof(Wal));
    private DiagnosticsSection<MaintenanceOperationSnapshot> _activeMaintenance =
        RuntimeDiagnosticsSnapshotContract.NotNull(ActiveMaintenance, nameof(ActiveMaintenance));
    private DiagnosticsSection<HealthDiagnosticsSnapshot> _health =
        RuntimeDiagnosticsSnapshotContract.NotNull(Health, nameof(Health));

    // Keep the positional constructor, init property, and Deconstruct ABI while
    // ensuring both direct construction and source-generated JSON construction
    // pass through the complete-envelope validator.
    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
            value,
            _queries,
            _connections,
            _storage,
            _wal,
            _activeMaintenance,
            _health);
    }

    public DiagnosticsSection<QueryDiagnosticsSummary> Queries
    {
        get => _queries;
        init
        {
            DiagnosticsSection<QueryDiagnosticsSummary> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(Queries));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                valid,
                _connections,
                _storage,
                _wal,
                _activeMaintenance,
                _health);
            _queries = valid;
        }
    }

    public DiagnosticsSection<ConnectionDiagnosticsSnapshot> Connections
    {
        get => _connections;
        init
        {
            DiagnosticsSection<ConnectionDiagnosticsSnapshot> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(Connections));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                _queries,
                valid,
                _storage,
                _wal,
                _activeMaintenance,
                _health);
            _connections = valid;
        }
    }

    public DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> Storage
    {
        get => _storage;
        init
        {
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(Storage));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                _queries,
                _connections,
                valid,
                _wal,
                _activeMaintenance,
                _health);
            _storage = valid;
        }
    }

    public DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> Wal
    {
        get => _wal;
        init
        {
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(Wal));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                _queries,
                _connections,
                _storage,
                valid,
                _activeMaintenance,
                _health);
            _wal = valid;
        }
    }

    public DiagnosticsSection<MaintenanceOperationSnapshot> ActiveMaintenance
    {
        get => _activeMaintenance;
        init
        {
            DiagnosticsSection<MaintenanceOperationSnapshot> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(ActiveMaintenance));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                _queries,
                _connections,
                _storage,
                _wal,
                valid,
                _health);
            _activeMaintenance = valid;
        }
    }

    public DiagnosticsSection<HealthDiagnosticsSnapshot> Health
    {
        get => _health;
        init
        {
            DiagnosticsSection<HealthDiagnosticsSnapshot> valid =
                RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(Health));
            RuntimeDiagnosticsSnapshotContract.ValidateRuntime(
                _metadata,
                _queries,
                _connections,
                _storage,
                _wal,
                _activeMaintenance,
                valid);
            _health = valid;
        }
    }
}

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
    int ActiveCount) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateQuerySummary(
            Metadata,
            RequestCount,
            StatementExecutionCount,
            SucceededCount,
            FailedCount,
            CanceledCount,
            SlowCount,
            RowsProduced,
            RowsAffected,
            ActiveCount);
    private long _requestCount = RuntimeDiagnosticsSnapshotContract.NonNegative(RequestCount, nameof(RequestCount));
    private long _statementExecutionCount = RuntimeDiagnosticsSnapshotContract.NonNegative(StatementExecutionCount, nameof(StatementExecutionCount));
    private long _succeededCount = RuntimeDiagnosticsSnapshotContract.NonNegative(SucceededCount, nameof(SucceededCount));
    private long _failedCount = RuntimeDiagnosticsSnapshotContract.NonNegative(FailedCount, nameof(FailedCount));
    private long _canceledCount = RuntimeDiagnosticsSnapshotContract.NonNegative(CanceledCount, nameof(CanceledCount));
    private long _slowCount = RuntimeDiagnosticsSnapshotContract.NonNegative(SlowCount, nameof(SlowCount));
    private long _rowsProduced = RuntimeDiagnosticsSnapshotContract.NonNegative(RowsProduced, nameof(RowsProduced));
    private long _rowsAffected = RuntimeDiagnosticsSnapshotContract.NonNegative(RowsAffected, nameof(RowsAffected));
    private int _activeCount = RuntimeDiagnosticsSnapshotContract.NonNegative(ActiveCount, nameof(ActiveCount));

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.AvailableMetadata(value);
    }

    public long RequestCount
    {
        get => _requestCount;
        init => _requestCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(RequestCount));
    }

    public long StatementExecutionCount
    {
        get => _statementExecutionCount;
        init => _statementExecutionCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(StatementExecutionCount));
    }

    public long SucceededCount
    {
        get => _succeededCount;
        init => _succeededCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(SucceededCount));
    }

    public long FailedCount
    {
        get => _failedCount;
        init => _failedCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(FailedCount));
    }

    public long CanceledCount
    {
        get => _canceledCount;
        init => _canceledCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(CanceledCount));
    }

    public long SlowCount
    {
        get => _slowCount;
        init => _slowCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(SlowCount));
    }

    public long RowsProduced
    {
        get => _rowsProduced;
        init => _rowsProduced = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(RowsProduced));
    }

    public long RowsAffected
    {
        get => _rowsAffected;
        init => _rowsAffected = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(RowsAffected));
    }

    public int ActiveCount
    {
        get => _activeCount;
        init => _activeCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(ActiveCount));
    }
}

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
    OpaqueDiagnosticsId? SessionId) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateActiveQuery(
            Metadata,
            OperationId,
            ParentOperationId,
            OperationClass,
            Role,
            Phase,
            StartedAtUtc,
            Elapsed,
            Transport);
    private OpaqueDiagnosticsId _operationId =
        RuntimeDiagnosticsSnapshotContract.NotNull(OperationId, nameof(OperationId));
    private OpaqueDiagnosticsId? _parentOperationId = ParentOperationId;
    private CSharpDbOperationClass _operationClass =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(OperationClass, nameof(OperationClass));
    private CSharpDbOperationRole _role =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Role, nameof(Role));
    private QueryExecutionPhase _phase =
        RuntimeDiagnosticsSnapshotContract.ActivePhase(Phase, nameof(Phase));
    private DateTimeOffset _startedAtUtc =
        RuntimeDiagnosticsSnapshotContract.Utc(StartedAtUtc, nameof(StartedAtUtc));
    private TimeSpan _elapsed =
        RuntimeDiagnosticsSnapshotContract.NonNegative(Elapsed, nameof(Elapsed));
    private QueryFingerprint? _fingerprint = Fingerprint;
    private CSharpDbTransport _transport =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Transport, nameof(Transport));
    private DiagnosticsTraceId? _traceId = TraceId;
    private OpaqueDiagnosticsId? _sessionId = SessionId;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateActiveQuery(
            value,
            _operationId,
            _parentOperationId,
            _operationClass,
            _role,
            _phase,
            _startedAtUtc,
            _elapsed,
            _transport);
    }

    public OpaqueDiagnosticsId OperationId
    {
        get => _operationId;
        init
        {
            OpaqueDiagnosticsId valid = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(OperationId));
            RuntimeDiagnosticsSnapshotContract.ValidateActiveQuery(
                _metadata,
                valid,
                _parentOperationId,
                _operationClass,
                _role,
                _phase,
                _startedAtUtc,
                _elapsed,
                _transport);
            _operationId = valid;
        }
    }

    public OpaqueDiagnosticsId? ParentOperationId
    {
        get => _parentOperationId;
        init
        {
            RuntimeDiagnosticsSnapshotContract.ValidateActiveQuery(
                _metadata,
                _operationId,
                value,
                _operationClass,
                _role,
                _phase,
                _startedAtUtc,
                _elapsed,
                _transport);
            _parentOperationId = value;
        }
    }

    public CSharpDbOperationClass OperationClass
    {
        get => _operationClass;
        init => _operationClass = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(OperationClass));
    }

    public CSharpDbOperationRole Role
    {
        get => _role;
        init => _role = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Role));
    }

    public QueryExecutionPhase Phase
    {
        get => _phase;
        init => _phase = RuntimeDiagnosticsSnapshotContract.ActivePhase(value, nameof(Phase));
    }

    public DateTimeOffset StartedAtUtc
    {
        get => _startedAtUtc;
        init => _startedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(StartedAtUtc));
    }

    public TimeSpan Elapsed
    {
        get => _elapsed;
        init => _elapsed = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(Elapsed));
    }

    public QueryFingerprint? Fingerprint
    {
        get => _fingerprint;
        init => _fingerprint = value;
    }

    public CSharpDbTransport Transport
    {
        get => _transport;
        init => _transport = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Transport));
    }

    public DiagnosticsTraceId? TraceId
    {
        get => _traceId;
        init => _traceId = value;
    }

    public OpaqueDiagnosticsId? SessionId
    {
        get => _sessionId;
        init => _sessionId = value;
    }
}

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
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateRecentQuery(
            Metadata,
            OperationId,
            ParentOperationId,
            OperationClass,
            Role,
            StartedAtUtc,
            CompletedAtUtc,
            Duration,
            TimeToFirstResult,
            ResultConsumptionDuration,
            Outcome,
            Transport,
            RowsProduced,
            RowsAffected,
            Error);
    private OpaqueDiagnosticsId _operationId =
        RuntimeDiagnosticsSnapshotContract.NotNull(OperationId, nameof(OperationId));
    private OpaqueDiagnosticsId? _parentOperationId = ParentOperationId;
    private CSharpDbOperationClass _operationClass =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(OperationClass, nameof(OperationClass));
    private CSharpDbOperationRole _role =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Role, nameof(Role));
    private DateTimeOffset _startedAtUtc =
        RuntimeDiagnosticsSnapshotContract.Utc(StartedAtUtc, nameof(StartedAtUtc));
    private DateTimeOffset _completedAtUtc =
        RuntimeDiagnosticsSnapshotContract.Utc(CompletedAtUtc, nameof(CompletedAtUtc));
    private TimeSpan _duration =
        RuntimeDiagnosticsSnapshotContract.NonNegative(Duration, nameof(Duration));
    private TimeSpan? _timeToFirstResult =
        RuntimeDiagnosticsSnapshotContract.OptionalDuration(TimeToFirstResult, nameof(TimeToFirstResult));
    private TimeSpan? _resultConsumptionDuration =
        RuntimeDiagnosticsSnapshotContract.OptionalDuration(ResultConsumptionDuration, nameof(ResultConsumptionDuration));
    private CSharpDbOperationOutcome _outcome =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Outcome, nameof(Outcome));
    private QueryFingerprint? _fingerprint = Fingerprint;
    private CSharpDbTransport _transport =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Transport, nameof(Transport));
    private long _rowsProduced =
        RuntimeDiagnosticsSnapshotContract.NonNegative(RowsProduced, nameof(RowsProduced));
    private long _rowsAffected =
        RuntimeDiagnosticsSnapshotContract.NonNegative(RowsAffected, nameof(RowsAffected));
    private DiagnosticsTraceId? _traceId = TraceId;
    private OpaqueDiagnosticsId? _sessionId = SessionId;
    private SafeErrorProjection? _error = Error;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateRecentQuery(
            value,
            _operationId,
            _parentOperationId,
            _operationClass,
            _role,
            _startedAtUtc,
            _completedAtUtc,
            _duration,
            _timeToFirstResult,
            _resultConsumptionDuration,
            _outcome,
            _transport,
            _rowsProduced,
            _rowsAffected,
            _error);
    }

    public OpaqueDiagnosticsId OperationId
    {
        get => _operationId;
        init
        {
            OpaqueDiagnosticsId valid = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(OperationId));
            ValidateCandidate(operationId: valid, parentOperationId: _parentOperationId);
            _operationId = valid;
        }
    }

    public OpaqueDiagnosticsId? ParentOperationId
    {
        get => _parentOperationId;
        init
        {
            ValidateCandidate(operationId: _operationId, parentOperationId: value);
            _parentOperationId = value;
        }
    }

    public CSharpDbOperationClass OperationClass
    {
        get => _operationClass;
        init => _operationClass = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(OperationClass));
    }

    public CSharpDbOperationRole Role
    {
        get => _role;
        init => _role = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Role));
    }

    public DateTimeOffset StartedAtUtc
    {
        get => _startedAtUtc;
        init => _startedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(StartedAtUtc));
    }

    public DateTimeOffset CompletedAtUtc
    {
        get => _completedAtUtc;
        init => _completedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(CompletedAtUtc));
    }

    public TimeSpan Duration
    {
        get => _duration;
        init
        {
            TimeSpan valid = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(Duration));
            ValidateTiming(valid, _timeToFirstResult, _resultConsumptionDuration);
            _duration = valid;
        }
    }

    public TimeSpan? TimeToFirstResult
    {
        get => _timeToFirstResult;
        init
        {
            TimeSpan? valid = RuntimeDiagnosticsSnapshotContract.OptionalDuration(value, nameof(TimeToFirstResult));
            ValidateTiming(_duration, valid, _resultConsumptionDuration);
            _timeToFirstResult = valid;
        }
    }

    public TimeSpan? ResultConsumptionDuration
    {
        get => _resultConsumptionDuration;
        init
        {
            TimeSpan? valid = RuntimeDiagnosticsSnapshotContract.OptionalDuration(value, nameof(ResultConsumptionDuration));
            ValidateTiming(_duration, _timeToFirstResult, valid);
            _resultConsumptionDuration = valid;
        }
    }

    public CSharpDbOperationOutcome Outcome
    {
        get => _outcome;
        init
        {
            CSharpDbOperationOutcome valid = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Outcome));
            ValidateOutcome(valid, _error);
            _outcome = valid;
        }
    }

    public QueryFingerprint? Fingerprint
    {
        get => _fingerprint;
        init => _fingerprint = value;
    }

    public CSharpDbTransport Transport
    {
        get => _transport;
        init => _transport = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Transport));
    }

    public long RowsProduced
    {
        get => _rowsProduced;
        init => _rowsProduced = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(RowsProduced));
    }

    public long RowsAffected
    {
        get => _rowsAffected;
        init => _rowsAffected = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(RowsAffected));
    }

    public DiagnosticsTraceId? TraceId
    {
        get => _traceId;
        init => _traceId = value;
    }

    public OpaqueDiagnosticsId? SessionId
    {
        get => _sessionId;
        init => _sessionId = value;
    }

    public SafeErrorProjection? Error
    {
        get => _error;
        init
        {
            ValidateOutcome(_outcome, value);
            _error = value;
        }
    }

    private void ValidateCandidate(
        OpaqueDiagnosticsId operationId,
        OpaqueDiagnosticsId? parentOperationId)
        => RuntimeDiagnosticsSnapshotContract.ValidateRecentQuery(
            _metadata,
            operationId,
            parentOperationId,
            _operationClass,
            _role,
            _startedAtUtc,
            _completedAtUtc,
            _duration,
            _timeToFirstResult,
            _resultConsumptionDuration,
            _outcome,
            _transport,
            _rowsProduced,
            _rowsAffected,
            _error);

    private void ValidateTiming(
        TimeSpan duration,
        TimeSpan? timeToFirstResult,
        TimeSpan? resultConsumptionDuration)
        => RuntimeDiagnosticsSnapshotContract.ValidateRecentQuery(
            _metadata,
            _operationId,
            _parentOperationId,
            _operationClass,
            _role,
            _startedAtUtc,
            _completedAtUtc,
            duration,
            timeToFirstResult,
            resultConsumptionDuration,
            _outcome,
            _transport,
            _rowsProduced,
            _rowsAffected,
            _error);

    private void ValidateOutcome(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
        => RuntimeDiagnosticsSnapshotContract.ValidateRecentQuery(
            _metadata,
            _operationId,
            _parentOperationId,
            _operationClass,
            _role,
            _startedAtUtc,
            _completedAtUtc,
            _duration,
            _timeToFirstResult,
            _resultConsumptionDuration,
            outcome,
            _transport,
            _rowsProduced,
            _rowsAffected,
            error);
}

/// <summary>
/// Separately authorized query detail. This model is intentionally absent from
/// ordinary runtime, active-query, and recent-query snapshots.
/// </summary>
public sealed record QueryDetailSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId OperationId,
    QueryFingerprint? Fingerprint,
    SqlTextCaptureMode CaptureMode,
    string? CapturedSqlText) : IRuntimeDiagnosticsSnapshot
{
    /// <summary>
    /// Absolute public safety bound for separately authorized captured SQL.
    /// A producer may apply a smaller configured limit before construction.
    /// </summary>
    public const int MaximumCapturedSqlTextLength = 65_536;

    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateQueryDetail(
            Metadata,
            OperationId,
            CaptureMode,
            CapturedSqlText);
    private OpaqueDiagnosticsId _operationId =
        RuntimeDiagnosticsSnapshotContract.NotNull(OperationId, nameof(OperationId));
    private QueryFingerprint? _fingerprint = Fingerprint;
    private SqlTextCaptureMode _captureMode =
        RuntimeDiagnosticsSnapshotContract.DefinedEnum(CaptureMode, nameof(CaptureMode));
    private string? _capturedSqlText =
        RuntimeDiagnosticsSnapshotContract.OptionalBoundedSqlText(
            CapturedSqlText,
            nameof(CapturedSqlText));

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateQueryDetail(
            value,
            _operationId,
            _captureMode,
            _capturedSqlText);
    }

    public OpaqueDiagnosticsId OperationId
    {
        get => _operationId;
        init
        {
            OpaqueDiagnosticsId valid = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(OperationId));
            RuntimeDiagnosticsSnapshotContract.ValidateQueryDetail(
                _metadata,
                valid,
                _captureMode,
                _capturedSqlText);
            _operationId = valid;
        }
    }

    public QueryFingerprint? Fingerprint
    {
        get => _fingerprint;
        init => _fingerprint = value;
    }

    public SqlTextCaptureMode CaptureMode
    {
        get => _captureMode;
        init
        {
            SqlTextCaptureMode valid = RuntimeDiagnosticsSnapshotContract.DefinedEnum(value, nameof(CaptureMode));
            RuntimeDiagnosticsSnapshotContract.ValidateQueryDetail(
                _metadata,
                _operationId,
                valid,
                _capturedSqlText);
            _captureMode = valid;
        }
    }

    public string? CapturedSqlText
    {
        get => _capturedSqlText;
        init
        {
            string? valid = RuntimeDiagnosticsSnapshotContract.OptionalBoundedSqlText(
                value,
                nameof(CapturedSqlText));
            RuntimeDiagnosticsSnapshotContract.ValidateQueryDetail(
                _metadata,
                _operationId,
                _captureMode,
                valid);
            _capturedSqlText = valid;
        }
    }
}

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
    bool PlanTruncated) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateQueryPlan(
            Metadata,
            OperationId,
            AccessPath,
            EstimatedRows,
            ActualRows,
            PlanNodeCount,
            PlanTruncated);
    private OpaqueDiagnosticsId _operationId =
        RuntimeDiagnosticsSnapshotContract.NotNull(OperationId, nameof(OperationId));
    private QueryFingerprint? _fingerprint = Fingerprint;
    private QueryAccessPathCategory _accessPath =
        RuntimeDiagnosticsSnapshotContract.DefinedEnum(AccessPath, nameof(AccessPath));
    private bool? _planCacheHit = PlanCacheHit;
    private bool? _reoptimized = Reoptimized;
    private long? _estimatedRows =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(EstimatedRows, nameof(EstimatedRows));
    private long? _actualRows =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActualRows, nameof(ActualRows));
    private int? _planNodeCount =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PlanNodeCount, nameof(PlanNodeCount));
    private bool _planTruncated = PlanTruncated;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateQueryPlan(
            value,
            _operationId,
            _accessPath,
            _estimatedRows,
            _actualRows,
            _planNodeCount,
            _planTruncated);
    }

    public OpaqueDiagnosticsId OperationId
    {
        get => _operationId;
        init => _operationId = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(OperationId));
    }

    public QueryFingerprint? Fingerprint
    {
        get => _fingerprint;
        init => _fingerprint = value;
    }

    public QueryAccessPathCategory AccessPath
    {
        get => _accessPath;
        init => _accessPath = RuntimeDiagnosticsSnapshotContract.DefinedEnum(value, nameof(AccessPath));
    }

    public bool? PlanCacheHit
    {
        get => _planCacheHit;
        init => _planCacheHit = value;
    }

    public bool? Reoptimized
    {
        get => _reoptimized;
        init => _reoptimized = value;
    }

    public long? EstimatedRows
    {
        get => _estimatedRows;
        init => _estimatedRows = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(EstimatedRows));
    }

    public long? ActualRows
    {
        get => _actualRows;
        init => _actualRows = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActualRows));
    }

    public int? PlanNodeCount
    {
        get => _planNodeCount;
        init => _planNodeCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PlanNodeCount));
    }

    public bool PlanTruncated
    {
        get => _planTruncated;
        init
        {
            RuntimeDiagnosticsSnapshotContract.ValidateQueryPlan(
                _metadata,
                _operationId,
                _accessPath,
                _estimatedRows,
                _actualRows,
                _planNodeCount,
                value);
            _planTruncated = value;
        }
    }

    /// <summary>
    /// Whether runtime cardinality caused an adaptive access-path
    /// reclassification. This is distinct from rebuilding/reoptimizing a plan.
    /// </summary>
    public bool? Reclassified { get; init; }

    /// <summary>
    /// Whether a cached-plan assumption stopped matching the statement and
    /// the normal planner reclassified it during this logical operation.
    /// </summary>
    public bool? CachedPlanReclassified { get; init; }

    /// <summary>
    /// Whether observed cardinality diverged enough for the adaptive runtime
    /// to reclassify the selected access path. This does not imply that a new
    /// physical plan was accepted.
    /// </summary>
    public bool? AdaptiveReclassified { get; init; }

    /// <summary>
    /// Whether adaptive reoptimization was attempted for this operation.
    /// </summary>
    public bool? AdaptiveReoptimizationAttempted { get; init; }

    /// <summary>
    /// Whether an adaptive reoptimization attempt was rejected and execution
    /// continued on its supported fallback path.
    /// </summary>
    public bool? AdaptiveReoptimizationRejected { get; init; }
}

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
    TimeSpan? OldestTransactionAge) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateConnections(
            Metadata,
            PoolCapacity,
            AvailableSlots,
            WaiterCount,
            ActiveLogicalSessions,
            ActiveReaders,
            ActiveTransactions,
            RetiredPoolCount,
            PoisonedPoolCount,
            OldestTransactionAge);
    private int? _poolCapacity =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PoolCapacity, nameof(PoolCapacity));
    private int? _availableSlots =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(AvailableSlots, nameof(AvailableSlots));
    private int? _waiterCount =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(WaiterCount, nameof(WaiterCount));
    private int? _activeLogicalSessions =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActiveLogicalSessions, nameof(ActiveLogicalSessions));
    private int? _activeReaders =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActiveReaders, nameof(ActiveReaders));
    private int? _activeTransactions =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActiveTransactions, nameof(ActiveTransactions));
    private int? _retiredPoolCount =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(RetiredPoolCount, nameof(RetiredPoolCount));
    private int? _poisonedPoolCount =
        RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PoisonedPoolCount, nameof(PoisonedPoolCount));
    private TimeSpan? _oldestTransactionAge =
        RuntimeDiagnosticsSnapshotContract.OptionalDuration(OldestTransactionAge, nameof(OldestTransactionAge));
    private int? _warmEngineIdleCount;
    private int? _disabledPoolCount;
    private int? _retiringPoolCount;
    private OpaqueDiagnosticsId? _transactionOwnerSessionId;
    private ConnectionPoolLifecycleState _poolState;
    private bool? _exclusiveMaintenanceActive;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateConnections(
            value,
            _poolCapacity,
            _availableSlots,
            _waiterCount,
            _activeLogicalSessions,
            _activeReaders,
            _activeTransactions,
            _retiredPoolCount,
            _poisonedPoolCount,
            _oldestTransactionAge);
    }

    public int? PoolCapacity
    {
        get => _poolCapacity;
        init
        {
            int? valid = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PoolCapacity));
            ValidateConnectionShape(valid, _availableSlots, _activeTransactions, _oldestTransactionAge);
            _poolCapacity = valid;
        }
    }

    public int? AvailableSlots
    {
        get => _availableSlots;
        init
        {
            int? valid = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(AvailableSlots));
            ValidateConnectionShape(_poolCapacity, valid, _activeTransactions, _oldestTransactionAge);
            _availableSlots = valid;
        }
    }

    public int? WaiterCount
    {
        get => _waiterCount;
        init => _waiterCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(WaiterCount));
    }

    public int? ActiveLogicalSessions
    {
        get => _activeLogicalSessions;
        init => _activeLogicalSessions = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActiveLogicalSessions));
    }

    public int? ActiveReaders
    {
        get => _activeReaders;
        init => _activeReaders = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActiveReaders));
    }

    public int? ActiveTransactions
    {
        get => _activeTransactions;
        init
        {
            int? valid = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActiveTransactions));
            ValidateConnectionShape(_poolCapacity, _availableSlots, valid, _oldestTransactionAge);
            if (valid == 0 && _transactionOwnerSessionId is not null)
            {
                throw new ArgumentException(
                    "A transaction owner requires at least one active transaction.",
                    nameof(ActiveTransactions));
            }
            _activeTransactions = valid;
        }
    }

    public int? RetiredPoolCount
    {
        get => _retiredPoolCount;
        init => _retiredPoolCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(RetiredPoolCount));
    }

    public int? PoisonedPoolCount
    {
        get => _poisonedPoolCount;
        init => _poisonedPoolCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PoisonedPoolCount));
    }

    public TimeSpan? OldestTransactionAge
    {
        get => _oldestTransactionAge;
        init
        {
            TimeSpan? valid = RuntimeDiagnosticsSnapshotContract.OptionalDuration(value, nameof(OldestTransactionAge));
            ValidateConnectionShape(_poolCapacity, _availableSlots, _activeTransactions, valid);
            _oldestTransactionAge = valid;
        }
    }

    public int? WarmEngineIdleCount
    {
        get => _warmEngineIdleCount;
        init => _warmEngineIdleCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(WarmEngineIdleCount));
    }

    public int? DisabledPoolCount
    {
        get => _disabledPoolCount;
        init => _disabledPoolCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(DisabledPoolCount));
    }

    public int? RetiringPoolCount
    {
        get => _retiringPoolCount;
        init => _retiringPoolCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(RetiringPoolCount));
    }
    public OpaqueDiagnosticsId? TransactionOwnerSessionId
    {
        get => _transactionOwnerSessionId;
        init
        {
            if (value is not null && _activeTransactions == 0)
            {
                throw new ArgumentException(
                    "A transaction owner requires at least one active transaction.",
                    nameof(TransactionOwnerSessionId));
            }
            _transactionOwnerSessionId = value;
        }
    }
    public ConnectionPoolLifecycleState PoolState
    {
        get => _poolState;
        init
        {
            if (!Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(nameof(value));
            _poolState = value;
        }
    }
    public bool? ExclusiveMaintenanceActive
    {
        get => _exclusiveMaintenanceActive;
        init => _exclusiveMaintenanceActive = value;
    }

    private void ValidateConnectionShape(
        int? poolCapacity,
        int? availableSlots,
        int? activeTransactions,
        TimeSpan? oldestTransactionAge)
        => RuntimeDiagnosticsSnapshotContract.ValidateConnections(
            _metadata,
            poolCapacity,
            availableSlots,
            _waiterCount,
            _activeLogicalSessions,
            _activeReaders,
            activeTransactions,
            _retiredPoolCount,
            _poisonedPoolCount,
            oldestTransactionAge);
}

public sealed record SessionDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    OpaqueDiagnosticsId SessionId,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset LastActiveAtUtc,
    OpaqueDiagnosticsId? CurrentOperationId,
    bool HasActiveReader,
    bool HasActiveTransaction,
    CSharpDbTransport Transport) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateSession(
            Metadata,
            SessionId,
            CreatedAtUtc,
            LastActiveAtUtc,
            Transport);
    private OpaqueDiagnosticsId _sessionId =
        RuntimeDiagnosticsSnapshotContract.NotNull(SessionId, nameof(SessionId));
    private DateTimeOffset _createdAtUtc =
        RuntimeDiagnosticsSnapshotContract.Utc(CreatedAtUtc, nameof(CreatedAtUtc));
    private DateTimeOffset _lastActiveAtUtc =
        RuntimeDiagnosticsSnapshotContract.Utc(LastActiveAtUtc, nameof(LastActiveAtUtc));
    private OpaqueDiagnosticsId? _currentOperationId = CurrentOperationId;
    private bool _hasActiveReader = HasActiveReader;
    private bool _hasActiveTransaction = HasActiveTransaction;
    private CSharpDbTransport _transport =
        RuntimeDiagnosticsSnapshotContract.KnownEnum(Transport, nameof(Transport));
    private DiagnosticsSessionState _state;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateSession(
            value,
            _sessionId,
            _createdAtUtc,
            _lastActiveAtUtc,
            _transport);
    }

    public OpaqueDiagnosticsId SessionId
    {
        get => _sessionId;
        init => _sessionId = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(SessionId));
    }

    public DateTimeOffset CreatedAtUtc
    {
        get => _createdAtUtc;
        init
        {
            DateTimeOffset valid = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(CreatedAtUtc));
            RuntimeDiagnosticsSnapshotContract.ValidateSession(
                _metadata,
                _sessionId,
                valid,
                _lastActiveAtUtc,
                _transport);
            _createdAtUtc = valid;
        }
    }

    public DateTimeOffset LastActiveAtUtc
    {
        get => _lastActiveAtUtc;
        init
        {
            DateTimeOffset valid = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(LastActiveAtUtc));
            RuntimeDiagnosticsSnapshotContract.ValidateSession(
                _metadata,
                _sessionId,
                _createdAtUtc,
                valid,
                _transport);
            _lastActiveAtUtc = valid;
        }
    }

    public OpaqueDiagnosticsId? CurrentOperationId
    {
        get => _currentOperationId;
        init => _currentOperationId = value;
    }

    public bool HasActiveReader
    {
        get => _hasActiveReader;
        init => _hasActiveReader = value;
    }

    public bool HasActiveTransaction
    {
        get => _hasActiveTransaction;
        init => _hasActiveTransaction = value;
    }

    public CSharpDbTransport Transport
    {
        get => _transport;
        init => _transport = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Transport));
    }

    public DiagnosticsSessionState State
    {
        get => _state;
        init
        {
            if (!Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(nameof(value));
            _state = value;
        }
    }
}

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
    long? ConflictCount) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateStorage(
            Metadata,
            LogicalDatabaseBytes,
            AllocatedDatabaseBytes,
            PageCount,
            PageReads,
            PageWrites,
            BytesRead,
            BytesWritten,
            CacheHits,
            CacheMisses,
            DirtyPages,
            ActiveReaders,
            ActiveWriters,
            CommitCount,
            ConflictCount);
    private long? _logicalDatabaseBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(LogicalDatabaseBytes, nameof(LogicalDatabaseBytes));
    private long? _allocatedDatabaseBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(AllocatedDatabaseBytes, nameof(AllocatedDatabaseBytes));
    private long? _pageCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PageCount, nameof(PageCount));
    private long? _pageReads = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PageReads, nameof(PageReads));
    private long? _pageWrites = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PageWrites, nameof(PageWrites));
    private long? _bytesRead = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(BytesRead, nameof(BytesRead));
    private long? _bytesWritten = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(BytesWritten, nameof(BytesWritten));
    private long? _cacheHits = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(CacheHits, nameof(CacheHits));
    private long? _cacheMisses = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(CacheMisses, nameof(CacheMisses));
    private long? _dirtyPages = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(DirtyPages, nameof(DirtyPages));
    private int? _activeReaders = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActiveReaders, nameof(ActiveReaders));
    private int? _activeWriters = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ActiveWriters, nameof(ActiveWriters));
    private long? _commitCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(CommitCount, nameof(CommitCount));
    private long? _conflictCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(ConflictCount, nameof(ConflictCount));

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.AvailableMetadata(value);
    }

    public long? LogicalDatabaseBytes { get => _logicalDatabaseBytes; init => _logicalDatabaseBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(LogicalDatabaseBytes)); }
    public long? AllocatedDatabaseBytes { get => _allocatedDatabaseBytes; init => _allocatedDatabaseBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(AllocatedDatabaseBytes)); }
    public long? PageCount { get => _pageCount; init => _pageCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PageCount)); }
    public long? PageReads { get => _pageReads; init => _pageReads = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PageReads)); }
    public long? PageWrites { get => _pageWrites; init => _pageWrites = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PageWrites)); }
    public long? BytesRead { get => _bytesRead; init => _bytesRead = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(BytesRead)); }
    public long? BytesWritten { get => _bytesWritten; init => _bytesWritten = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(BytesWritten)); }
    public long? CacheHits { get => _cacheHits; init => _cacheHits = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(CacheHits)); }
    public long? CacheMisses { get => _cacheMisses; init => _cacheMisses = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(CacheMisses)); }
    public long? DirtyPages { get => _dirtyPages; init => _dirtyPages = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(DirtyPages)); }
    public int? ActiveReaders { get => _activeReaders; init => _activeReaders = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActiveReaders)); }
    public int? ActiveWriters { get => _activeWriters; init => _activeWriters = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ActiveWriters)); }
    public long? CommitCount { get => _commitCount; init => _commitCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(CommitCount)); }
    public long? ConflictCount { get => _conflictCount; init => _conflictCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(ConflictCount)); }
}

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
    SafeErrorProjection? LastError) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateWal(
            Metadata,
            LogicalBytes,
            AllocatedBytes,
            CommittedFrameBytes,
            RetainedBytes,
            FrameCount,
            FlushCount,
            BytesWritten,
            PendingCommitCount,
            CheckpointPhase,
            LastSuccessfulFlushAtUtc,
            LastSuccessfulCheckpointAtUtc);
    private long? _logicalBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(LogicalBytes, nameof(LogicalBytes));
    private long? _allocatedBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(AllocatedBytes, nameof(AllocatedBytes));
    private long? _committedFrameBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(CommittedFrameBytes, nameof(CommittedFrameBytes));
    private long? _retainedBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(RetainedBytes, nameof(RetainedBytes));
    private long? _frameCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(FrameCount, nameof(FrameCount));
    private long? _flushCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(FlushCount, nameof(FlushCount));
    private long? _bytesWritten = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(BytesWritten, nameof(BytesWritten));
    private int? _pendingCommitCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(PendingCommitCount, nameof(PendingCommitCount));
    private CheckpointPhase _checkpointPhase = RuntimeDiagnosticsSnapshotContract.DefinedEnum(CheckpointPhase, nameof(CheckpointPhase));
    private DateTimeOffset? _lastSuccessfulFlushAtUtc = RuntimeDiagnosticsSnapshotContract.OptionalUtc(LastSuccessfulFlushAtUtc, nameof(LastSuccessfulFlushAtUtc));
    private DateTimeOffset? _lastSuccessfulCheckpointAtUtc = RuntimeDiagnosticsSnapshotContract.OptionalUtc(LastSuccessfulCheckpointAtUtc, nameof(LastSuccessfulCheckpointAtUtc));
    private SafeErrorProjection? _lastError = LastError;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.AvailableMetadata(value);
    }

    public long? LogicalBytes { get => _logicalBytes; init => _logicalBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(LogicalBytes)); }
    public long? AllocatedBytes { get => _allocatedBytes; init => _allocatedBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(AllocatedBytes)); }
    public long? CommittedFrameBytes { get => _committedFrameBytes; init => _committedFrameBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(CommittedFrameBytes)); }
    public long? RetainedBytes { get => _retainedBytes; init => _retainedBytes = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(RetainedBytes)); }
    public long? FrameCount { get => _frameCount; init => _frameCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(FrameCount)); }
    public long? FlushCount { get => _flushCount; init => _flushCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(FlushCount)); }
    public long? BytesWritten { get => _bytesWritten; init => _bytesWritten = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(BytesWritten)); }
    public int? PendingCommitCount { get => _pendingCommitCount; init => _pendingCommitCount = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(PendingCommitCount)); }
    public CheckpointPhase CheckpointPhase { get => _checkpointPhase; init => _checkpointPhase = RuntimeDiagnosticsSnapshotContract.DefinedEnum(value, nameof(CheckpointPhase)); }
    public DateTimeOffset? LastSuccessfulFlushAtUtc { get => _lastSuccessfulFlushAtUtc; init => _lastSuccessfulFlushAtUtc = RuntimeDiagnosticsSnapshotContract.OptionalUtc(value, nameof(LastSuccessfulFlushAtUtc)); }
    public DateTimeOffset? LastSuccessfulCheckpointAtUtc { get => _lastSuccessfulCheckpointAtUtc; init => _lastSuccessfulCheckpointAtUtc = RuntimeDiagnosticsSnapshotContract.OptionalUtc(value, nameof(LastSuccessfulCheckpointAtUtc)); }
    public SafeErrorProjection? LastError { get => _lastError; init => _lastError = value; }
}

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
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateMaintenance(
            Metadata,
            OperationId,
            Kind,
            Phase,
            StartedAtUtc,
            Elapsed,
            CompletedUnits,
            TotalUnits,
            Outcome,
            WarningCount,
            ErrorCount,
            Error);
    private OpaqueDiagnosticsId _operationId = RuntimeDiagnosticsSnapshotContract.NotNull(OperationId, nameof(OperationId));
    private MaintenanceOperationKind _kind = RuntimeDiagnosticsSnapshotContract.KnownEnum(Kind, nameof(Kind));
    private MaintenanceOperationPhase _phase = RuntimeDiagnosticsSnapshotContract.KnownEnum(Phase, nameof(Phase));
    private DateTimeOffset _startedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(StartedAtUtc, nameof(StartedAtUtc));
    private TimeSpan _elapsed = RuntimeDiagnosticsSnapshotContract.NonNegative(Elapsed, nameof(Elapsed));
    private long? _completedUnits = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(CompletedUnits, nameof(CompletedUnits));
    private long? _totalUnits = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(TotalUnits, nameof(TotalUnits));
    private CSharpDbOperationOutcome _outcome = RuntimeDiagnosticsSnapshotContract.DefinedEnum(Outcome, nameof(Outcome));
    private int _warningCount = RuntimeDiagnosticsSnapshotContract.NonNegative(WarningCount, nameof(WarningCount));
    private int _errorCount = RuntimeDiagnosticsSnapshotContract.NonNegative(ErrorCount, nameof(ErrorCount));
    private SafeErrorProjection? _error = Error;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateMaintenance(
            value,
            _operationId,
            _kind,
            _phase,
            _startedAtUtc,
            _elapsed,
            _completedUnits,
            _totalUnits,
            _outcome,
            _warningCount,
            _errorCount,
            _error);
    }

    public OpaqueDiagnosticsId OperationId { get => _operationId; init => _operationId = RuntimeDiagnosticsSnapshotContract.NotNull(value, nameof(OperationId)); }
    public MaintenanceOperationKind Kind { get => _kind; init => _kind = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Kind)); }
    public MaintenanceOperationPhase Phase
    {
        get => _phase;
        init
        {
            MaintenanceOperationPhase valid = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Phase));
            ValidateMaintenanceShape(valid, _completedUnits, _totalUnits, _outcome, _error);
            _phase = valid;
        }
    }
    public DateTimeOffset StartedAtUtc { get => _startedAtUtc; init => _startedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(StartedAtUtc)); }
    public TimeSpan Elapsed { get => _elapsed; init => _elapsed = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(Elapsed)); }
    public long? CompletedUnits
    {
        get => _completedUnits;
        init
        {
            long? valid = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(CompletedUnits));
            ValidateMaintenanceShape(_phase, valid, _totalUnits, _outcome, _error);
            _completedUnits = valid;
        }
    }
    public long? TotalUnits
    {
        get => _totalUnits;
        init
        {
            long? valid = RuntimeDiagnosticsSnapshotContract.OptionalNonNegative(value, nameof(TotalUnits));
            ValidateMaintenanceShape(_phase, _completedUnits, valid, _outcome, _error);
            _totalUnits = valid;
        }
    }
    public CSharpDbOperationOutcome Outcome
    {
        get => _outcome;
        init
        {
            CSharpDbOperationOutcome valid = RuntimeDiagnosticsSnapshotContract.DefinedEnum(value, nameof(Outcome));
            ValidateMaintenanceShape(_phase, _completedUnits, _totalUnits, valid, _error);
            _outcome = valid;
        }
    }
    public int WarningCount { get => _warningCount; init => _warningCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(WarningCount)); }
    public int ErrorCount { get => _errorCount; init => _errorCount = RuntimeDiagnosticsSnapshotContract.NonNegative(value, nameof(ErrorCount)); }

    public SafeErrorProjection? Error
    {
        get => _error;
        init
        {
            ValidateMaintenanceShape(_phase, _completedUnits, _totalUnits, _outcome, value);
            _error = value;
        }
    }

    private void ValidateMaintenanceShape(
        MaintenanceOperationPhase phase,
        long? completedUnits,
        long? totalUnits,
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
        => RuntimeDiagnosticsSnapshotContract.ValidateMaintenance(
            _metadata,
            _operationId,
            _kind,
            phase,
            _startedAtUtc,
            _elapsed,
            completedUnits,
            totalUnits,
            outcome,
            _warningCount,
            _errorCount,
            error);
}

public sealed record HealthDiagnosticsSnapshot(
    DiagnosticsSnapshotMetadata Metadata,
    CSharpDbHostLifecyclePhase LifecyclePhase,
    CSharpDbHealthStatus Liveness,
    CSharpDbHealthStatus Readiness,
    CSharpDbReadinessReason ReadinessReason,
    DateTimeOffset ChangedAtUtc,
    SafeErrorProjection? Error) : IRuntimeDiagnosticsSnapshot
{
    private DiagnosticsSnapshotMetadata _metadata =
        RuntimeDiagnosticsSnapshotContract.ValidateHealth(
            Metadata,
            LifecyclePhase,
            Liveness,
            Readiness,
            ReadinessReason,
            ChangedAtUtc,
            Error);
    private CSharpDbHostLifecyclePhase _lifecyclePhase = RuntimeDiagnosticsSnapshotContract.KnownEnum(LifecyclePhase, nameof(LifecyclePhase));
    private CSharpDbHealthStatus _liveness = RuntimeDiagnosticsSnapshotContract.KnownEnum(Liveness, nameof(Liveness));
    private CSharpDbHealthStatus _readiness = RuntimeDiagnosticsSnapshotContract.KnownEnum(Readiness, nameof(Readiness));
    private CSharpDbReadinessReason _readinessReason = RuntimeDiagnosticsSnapshotContract.KnownEnum(ReadinessReason, nameof(ReadinessReason));
    private DateTimeOffset _changedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(ChangedAtUtc, nameof(ChangedAtUtc));
    private SafeErrorProjection? _error = Error;

    public DiagnosticsSnapshotMetadata Metadata
    {
        get => _metadata;
        init => _metadata = RuntimeDiagnosticsSnapshotContract.ValidateHealth(
            value,
            _lifecyclePhase,
            _liveness,
            _readiness,
            _readinessReason,
            _changedAtUtc,
            _error);
    }

    public CSharpDbHostLifecyclePhase LifecyclePhase
    {
        get => _lifecyclePhase;
        init
        {
            CSharpDbHostLifecyclePhase valid = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(LifecyclePhase));
            RuntimeDiagnosticsSnapshotContract.ValidateHealth(
                _metadata,
                valid,
                _liveness,
                _readiness,
                _readinessReason,
                _changedAtUtc,
                _error);
            _lifecyclePhase = valid;
        }
    }
    public CSharpDbHealthStatus Liveness { get => _liveness; init => _liveness = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Liveness)); }
    public CSharpDbHealthStatus Readiness { get => _readiness; init => _readiness = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(Readiness)); }
    public CSharpDbReadinessReason ReadinessReason { get => _readinessReason; init => _readinessReason = RuntimeDiagnosticsSnapshotContract.KnownEnum(value, nameof(ReadinessReason)); }
    public DateTimeOffset ChangedAtUtc { get => _changedAtUtc; init => _changedAtUtc = RuntimeDiagnosticsSnapshotContract.Utc(value, nameof(ChangedAtUtc)); }

    public SafeErrorProjection? Error
    {
        get => _error;
        init
        {
            RuntimeDiagnosticsSnapshotContract.ValidateHealth(
                _metadata,
                _lifecyclePhase,
                _liveness,
                _readiness,
                _readinessReason,
                _changedAtUtc,
                value);
            _error = value;
        }
    }
}

file static class RuntimeDiagnosticsSnapshotContract
{
    internal static T NotNull<T>(T? value, string parameterName)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(value, parameterName);
        return value;
    }

    internal static DiagnosticsSnapshotMetadata AvailableMetadata(
        DiagnosticsSnapshotMetadata? metadata)
        => RequireAvailableMetadata(metadata);

    internal static T KnownEnum<T>(T value, string parameterName)
        where T : struct, Enum
    {
        RequireKnownEnum(value, parameterName);
        return value;
    }

    internal static T DefinedEnum<T>(T value, string parameterName)
        where T : struct, Enum
    {
        if (!Enum.IsDefined(value))
            throw new ArgumentOutOfRangeException(parameterName);
        return value;
    }

    internal static QueryExecutionPhase ActivePhase(
        QueryExecutionPhase value,
        string parameterName)
    {
        RequireKnownEnum(value, parameterName);
        if (value == QueryExecutionPhase.Completed)
        {
            throw new ArgumentException(
                "An active query cannot report the terminal Completed phase.",
                parameterName);
        }
        return value;
    }

    internal static DateTimeOffset Utc(DateTimeOffset value, string parameterName)
    {
        RequireUtc(value, parameterName);
        return value;
    }

    internal static DateTimeOffset? OptionalUtc(
        DateTimeOffset? value,
        string parameterName)
    {
        RequireOptionalUtc(value, parameterName);
        return value;
    }

    internal static TimeSpan NonNegative(TimeSpan value, string parameterName)
    {
        RequireNonNegative(value, parameterName);
        return value;
    }

    internal static TimeSpan? OptionalDuration(TimeSpan? value, string parameterName)
    {
        RequireOptionalDuration(value, parameterName);
        return value;
    }

    internal static long NonNegative(long value, string parameterName)
    {
        RequireNonNegative(value, parameterName);
        return value;
    }

    internal static int NonNegative(int value, string parameterName)
    {
        RequireNonNegative(value, parameterName);
        return value;
    }

    internal static long? OptionalNonNegative(long? value, string parameterName)
    {
        RequireOptionalNonNegative(value, parameterName);
        return value;
    }

    internal static int? OptionalNonNegative(int? value, string parameterName)
    {
        RequireOptionalNonNegative(value, parameterName);
        return value;
    }

    internal static string? OptionalBoundedSqlText(string? value, string parameterName)
    {
        if (value is not null &&
            (string.IsNullOrWhiteSpace(value) ||
             value.Length > QueryDetailSnapshot.MaximumCapturedSqlTextLength))
        {
            throw new ArgumentException(
                $"Captured SQL must be nonblank and at most {QueryDetailSnapshot.MaximumCapturedSqlTextLength} characters.",
                parameterName);
        }
        return value;
    }

    internal static DiagnosticsSnapshotMetadata ValidateRuntime(
        DiagnosticsSnapshotMetadata? metadata,
        DiagnosticsSection<QueryDiagnosticsSummary>? queries,
        DiagnosticsSection<ConnectionDiagnosticsSnapshot>? connections,
        DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>? storage,
        DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>? wal,
        DiagnosticsSection<MaintenanceOperationSnapshot>? activeMaintenance,
        DiagnosticsSection<HealthDiagnosticsSnapshot>? health)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireMetadata(metadata);
        if (validMetadata.Availability != DiagnosticsAvailability.Available &&
            (validMetadata.RecordsTruncated || validMetadata.FieldsTruncated))
        {
            throw new ArgumentException(
                "A non-available runtime snapshot cannot report truncated records or fields.",
                nameof(metadata));
        }
        ValidateRuntimeSection(validMetadata, queries, nameof(queries));
        ValidateRuntimeSection(validMetadata, connections, nameof(connections));
        ValidateRuntimeSection(validMetadata, storage, nameof(storage));
        ValidateRuntimeSection(validMetadata, wal, nameof(wal));
        ValidateRuntimeSection(validMetadata, activeMaintenance, nameof(activeMaintenance));
        ValidateRuntimeSection(validMetadata, health, nameof(health));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateQuerySummary(
        DiagnosticsSnapshotMetadata? metadata,
        long requestCount,
        long statementExecutionCount,
        long succeededCount,
        long failedCount,
        long canceledCount,
        long slowCount,
        long rowsProduced,
        long rowsAffected,
        int activeCount)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireNonNegative(requestCount, nameof(requestCount));
        RequireNonNegative(statementExecutionCount, nameof(statementExecutionCount));
        RequireNonNegative(succeededCount, nameof(succeededCount));
        RequireNonNegative(failedCount, nameof(failedCount));
        RequireNonNegative(canceledCount, nameof(canceledCount));
        RequireNonNegative(slowCount, nameof(slowCount));
        RequireNonNegative(rowsProduced, nameof(rowsProduced));
        RequireNonNegative(rowsAffected, nameof(rowsAffected));
        RequireNonNegative(activeCount, nameof(activeCount));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateActiveQuery(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? operationId,
        OpaqueDiagnosticsId? parentOperationId,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationRole role,
        QueryExecutionPhase phase,
        DateTimeOffset startedAtUtc,
        TimeSpan elapsed,
        CSharpDbTransport transport)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireOperationIdentity(operationId, parentOperationId);
        RequireKnownEnum(operationClass, nameof(operationClass));
        RequireKnownEnum(role, nameof(role));
        RequireKnownEnum(phase, nameof(phase));
        if (phase == QueryExecutionPhase.Completed)
        {
            throw new ArgumentException(
                "An active query cannot report the terminal Completed phase.",
                nameof(phase));
        }
        RequireUtc(startedAtUtc, nameof(startedAtUtc));
        RequireNonNegative(elapsed, nameof(elapsed));
        RequireKnownEnum(transport, nameof(transport));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateRecentQuery(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? operationId,
        OpaqueDiagnosticsId? parentOperationId,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationRole role,
        DateTimeOffset startedAtUtc,
        DateTimeOffset completedAtUtc,
        TimeSpan duration,
        TimeSpan? timeToFirstResult,
        TimeSpan? resultConsumptionDuration,
        CSharpDbOperationOutcome outcome,
        CSharpDbTransport transport,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection? error)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireOperationIdentity(operationId, parentOperationId);
        RequireKnownEnum(operationClass, nameof(operationClass));
        RequireKnownEnum(role, nameof(role));
        RequireUtc(startedAtUtc, nameof(startedAtUtc));
        RequireUtc(completedAtUtc, nameof(completedAtUtc));
        RequireNonNegative(duration, nameof(duration));
        RequireOptionalDuration(timeToFirstResult, nameof(timeToFirstResult));
        RequireOptionalDuration(resultConsumptionDuration, nameof(resultConsumptionDuration));
        if ((timeToFirstResult is null) != (resultConsumptionDuration is null))
        {
            throw new ArgumentException(
                "Time-to-first-result and result-consumption duration must either both be present or both be omitted.");
        }
        if (timeToFirstResult > duration || resultConsumptionDuration > duration)
        {
            throw new ArgumentException(
                "Query timing components cannot exceed the total duration.");
        }
        RequireKnownEnum(outcome, nameof(outcome));
        RequireKnownEnum(transport, nameof(transport));
        RequireNonNegative(rowsProduced, nameof(rowsProduced));
        RequireNonNegative(rowsAffected, nameof(rowsAffected));
        ValidateTerminalError(outcome, error);
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateQueryDetail(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? operationId,
        SqlTextCaptureMode captureMode,
        string? capturedSqlText)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        ArgumentNullException.ThrowIfNull(operationId);
        if (!Enum.IsDefined(captureMode))
            throw new ArgumentOutOfRangeException(nameof(captureMode));

        if (captureMode == SqlTextCaptureMode.None)
        {
            if (capturedSqlText is not null || validMetadata.FieldsTruncated)
            {
                throw new ArgumentException(
                    "Capture mode None must omit SQL text and cannot report field truncation.");
            }
        }
        else if (string.IsNullOrWhiteSpace(capturedSqlText) ||
                 capturedSqlText.Length > QueryDetailSnapshot.MaximumCapturedSqlTextLength)
        {
            throw new ArgumentException(
                $"Captured SQL must be nonblank and at most {QueryDetailSnapshot.MaximumCapturedSqlTextLength} characters.",
                nameof(capturedSqlText));
        }

        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateQueryPlan(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? operationId,
        QueryAccessPathCategory accessPath,
        long? estimatedRows,
        long? actualRows,
        int? planNodeCount,
        bool planTruncated)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        ArgumentNullException.ThrowIfNull(operationId);
        if (!Enum.IsDefined(accessPath))
            throw new ArgumentOutOfRangeException(nameof(accessPath));
        RequireOptionalNonNegative(estimatedRows, nameof(estimatedRows));
        RequireOptionalNonNegative(actualRows, nameof(actualRows));
        RequireOptionalNonNegative(planNodeCount, nameof(planNodeCount));
        if (planTruncated && !validMetadata.FieldsTruncated)
        {
            throw new ArgumentException(
                "A truncated plan must report field truncation in its capture metadata.",
                nameof(planTruncated));
        }
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateConnections(
        DiagnosticsSnapshotMetadata? metadata,
        int? poolCapacity,
        int? availableSlots,
        int? waiterCount,
        int? activeLogicalSessions,
        int? activeReaders,
        int? activeTransactions,
        int? retiredPoolCount,
        int? poisonedPoolCount,
        TimeSpan? oldestTransactionAge)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireOptionalNonNegative(poolCapacity, nameof(poolCapacity));
        RequireOptionalNonNegative(availableSlots, nameof(availableSlots));
        RequireOptionalNonNegative(waiterCount, nameof(waiterCount));
        RequireOptionalNonNegative(activeLogicalSessions, nameof(activeLogicalSessions));
        RequireOptionalNonNegative(activeReaders, nameof(activeReaders));
        RequireOptionalNonNegative(activeTransactions, nameof(activeTransactions));
        RequireOptionalNonNegative(retiredPoolCount, nameof(retiredPoolCount));
        RequireOptionalNonNegative(poisonedPoolCount, nameof(poisonedPoolCount));
        RequireOptionalDuration(oldestTransactionAge, nameof(oldestTransactionAge));
        if (poolCapacity is not null && availableSlots > poolCapacity)
        {
            throw new ArgumentException(
                "Available pool slots cannot exceed pool capacity.",
                nameof(availableSlots));
        }
        if (oldestTransactionAge is not null && activeTransactions is not null &&
            activeTransactions == 0)
        {
            throw new ArgumentException(
                "A transaction age requires at least one active transaction.",
                nameof(oldestTransactionAge));
        }
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateSession(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? sessionId,
        DateTimeOffset createdAtUtc,
        DateTimeOffset lastActiveAtUtc,
        CSharpDbTransport transport)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        ArgumentNullException.ThrowIfNull(sessionId);
        RequireUtc(createdAtUtc, nameof(createdAtUtc));
        RequireUtc(lastActiveAtUtc, nameof(lastActiveAtUtc));
        if (lastActiveAtUtc < createdAtUtc)
        {
            throw new ArgumentException(
                "A session's last-active timestamp cannot precede its creation timestamp.",
                nameof(lastActiveAtUtc));
        }
        RequireKnownEnum(transport, nameof(transport));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateStorage(
        DiagnosticsSnapshotMetadata? metadata,
        long? logicalDatabaseBytes,
        long? allocatedDatabaseBytes,
        long? pageCount,
        long? pageReads,
        long? pageWrites,
        long? bytesRead,
        long? bytesWritten,
        long? cacheHits,
        long? cacheMisses,
        long? dirtyPages,
        int? activeReaders,
        int? activeWriters,
        long? commitCount,
        long? conflictCount)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireOptionalNonNegative(logicalDatabaseBytes, nameof(logicalDatabaseBytes));
        RequireOptionalNonNegative(allocatedDatabaseBytes, nameof(allocatedDatabaseBytes));
        RequireOptionalNonNegative(pageCount, nameof(pageCount));
        RequireOptionalNonNegative(pageReads, nameof(pageReads));
        RequireOptionalNonNegative(pageWrites, nameof(pageWrites));
        RequireOptionalNonNegative(bytesRead, nameof(bytesRead));
        RequireOptionalNonNegative(bytesWritten, nameof(bytesWritten));
        RequireOptionalNonNegative(cacheHits, nameof(cacheHits));
        RequireOptionalNonNegative(cacheMisses, nameof(cacheMisses));
        RequireOptionalNonNegative(dirtyPages, nameof(dirtyPages));
        RequireOptionalNonNegative(activeReaders, nameof(activeReaders));
        RequireOptionalNonNegative(activeWriters, nameof(activeWriters));
        RequireOptionalNonNegative(commitCount, nameof(commitCount));
        RequireOptionalNonNegative(conflictCount, nameof(conflictCount));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateWal(
        DiagnosticsSnapshotMetadata? metadata,
        long? logicalBytes,
        long? allocatedBytes,
        long? committedFrameBytes,
        long? retainedBytes,
        long? frameCount,
        long? flushCount,
        long? bytesWritten,
        int? pendingCommitCount,
        CheckpointPhase checkpointPhase,
        DateTimeOffset? lastSuccessfulFlushAtUtc,
        DateTimeOffset? lastSuccessfulCheckpointAtUtc)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireOptionalNonNegative(logicalBytes, nameof(logicalBytes));
        RequireOptionalNonNegative(allocatedBytes, nameof(allocatedBytes));
        RequireOptionalNonNegative(committedFrameBytes, nameof(committedFrameBytes));
        RequireOptionalNonNegative(retainedBytes, nameof(retainedBytes));
        RequireOptionalNonNegative(frameCount, nameof(frameCount));
        RequireOptionalNonNegative(flushCount, nameof(flushCount));
        RequireOptionalNonNegative(bytesWritten, nameof(bytesWritten));
        RequireOptionalNonNegative(pendingCommitCount, nameof(pendingCommitCount));
        if (!Enum.IsDefined(checkpointPhase))
            throw new ArgumentOutOfRangeException(nameof(checkpointPhase));
        RequireOptionalUtc(lastSuccessfulFlushAtUtc, nameof(lastSuccessfulFlushAtUtc));
        RequireOptionalUtc(lastSuccessfulCheckpointAtUtc, nameof(lastSuccessfulCheckpointAtUtc));
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateMaintenance(
        DiagnosticsSnapshotMetadata? metadata,
        OpaqueDiagnosticsId? operationId,
        MaintenanceOperationKind kind,
        MaintenanceOperationPhase phase,
        DateTimeOffset startedAtUtc,
        TimeSpan elapsed,
        long? completedUnits,
        long? totalUnits,
        CSharpDbOperationOutcome outcome,
        int warningCount,
        int errorCount,
        SafeErrorProjection? error)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        ArgumentNullException.ThrowIfNull(operationId);
        RequireKnownEnum(kind, nameof(kind));
        RequireKnownEnum(phase, nameof(phase));
        RequireUtc(startedAtUtc, nameof(startedAtUtc));
        RequireNonNegative(elapsed, nameof(elapsed));
        RequireOptionalNonNegative(completedUnits, nameof(completedUnits));
        RequireOptionalNonNegative(totalUnits, nameof(totalUnits));
        if (completedUnits > totalUnits)
        {
            throw new ArgumentException(
                "Completed maintenance units cannot exceed total units.",
                nameof(completedUnits));
        }
        if (!Enum.IsDefined(outcome))
            throw new ArgumentOutOfRangeException(nameof(outcome));
        bool isCompletedPhase = phase == MaintenanceOperationPhase.Completed;
        bool hasTerminalOutcome = outcome != CSharpDbOperationOutcome.Unknown;
        if (isCompletedPhase != hasTerminalOutcome)
        {
            throw new ArgumentException(
                "Only a completed maintenance operation can report a terminal outcome, and a completed operation requires one.");
        }
        RequireNonNegative(warningCount, nameof(warningCount));
        RequireNonNegative(errorCount, nameof(errorCount));
        ValidateOptionalOutcomeError(outcome, error);
        return validMetadata;
    }

    internal static DiagnosticsSnapshotMetadata ValidateHealth(
        DiagnosticsSnapshotMetadata? metadata,
        CSharpDbHostLifecyclePhase lifecyclePhase,
        CSharpDbHealthStatus liveness,
        CSharpDbHealthStatus readiness,
        CSharpDbReadinessReason readinessReason,
        DateTimeOffset changedAtUtc,
        SafeErrorProjection? error)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireAvailableMetadata(metadata);
        RequireKnownEnum(lifecyclePhase, nameof(lifecyclePhase));
        RequireKnownEnum(liveness, nameof(liveness));
        RequireKnownEnum(readiness, nameof(readiness));
        RequireKnownEnum(readinessReason, nameof(readinessReason));
        RequireUtc(changedAtUtc, nameof(changedAtUtc));
        if (lifecyclePhase == CSharpDbHostLifecyclePhase.Failed && error is null)
        {
            throw new ArgumentException(
                "A failed health snapshot requires a safe error projection.",
                nameof(error));
        }
        return validMetadata;
    }

    private static void ValidateRuntimeSection<T>(
        DiagnosticsSnapshotMetadata metadata,
        DiagnosticsSection<T>? section,
        string parameterName)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        ArgumentNullException.ThrowIfNull(section, parameterName);
        if (section.Value is not null && section.Value.Metadata != metadata)
        {
            throw new ArgumentException(
                "An available runtime section must share the complete envelope capture metadata.",
                parameterName);
        }
        if (metadata.Availability != DiagnosticsAvailability.Available &&
            section.Availability != metadata.Availability)
        {
            throw new ArgumentException(
                "A non-available runtime snapshot requires every section to report the same availability.",
                parameterName);
        }
    }

    private static DiagnosticsSnapshotMetadata RequireAvailableMetadata(
        DiagnosticsSnapshotMetadata? metadata)
    {
        DiagnosticsSnapshotMetadata validMetadata = RequireMetadata(metadata);
        if (validMetadata.Availability != DiagnosticsAvailability.Available)
        {
            throw new ArgumentException(
                "A published diagnostics value requires Available capture metadata.",
                nameof(metadata));
        }
        return validMetadata;
    }

    private static DiagnosticsSnapshotMetadata RequireMetadata(
        DiagnosticsSnapshotMetadata? metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return metadata;
    }

    private static void RequireOperationIdentity(
        OpaqueDiagnosticsId? operationId,
        OpaqueDiagnosticsId? parentOperationId)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        if (operationId == parentOperationId)
        {
            throw new ArgumentException(
                "An operation cannot identify itself as its parent.",
                nameof(parentOperationId));
        }
    }

    private static void ValidateTerminalError(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
    {
        bool expectsError = outcome is
            CSharpDbOperationOutcome.Failed or
            CSharpDbOperationOutcome.Canceled or
            CSharpDbOperationOutcome.Rejected;
        if (expectsError != (error is not null))
        {
            throw new ArgumentException(
                "Successful query outcomes must omit errors and unsuccessful outcomes require a safe error projection.",
                nameof(error));
        }
    }

    private static void ValidateOptionalOutcomeError(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
    {
        if (outcome == CSharpDbOperationOutcome.Unknown)
        {
            if (error is not null)
            {
                throw new ArgumentException(
                    "An in-progress maintenance operation cannot report a terminal error.",
                    nameof(error));
            }
            return;
        }

        ValidateTerminalError(outcome, error);
    }

    private static void RequireKnownEnum<T>(T value, string parameterName)
        where T : struct, Enum
    {
        if (Convert.ToInt64(value) == 0 || !Enum.IsDefined(value))
            throw new ArgumentOutOfRangeException(parameterName);
    }

    private static void RequireUtc(DateTimeOffset value, string parameterName)
    {
        if (value.Offset != TimeSpan.Zero)
            throw new ArgumentException("Diagnostics timestamps must be UTC.", parameterName);
    }

    private static void RequireOptionalUtc(DateTimeOffset? value, string parameterName)
    {
        if (value is { } timestamp)
            RequireUtc(timestamp, parameterName);
    }

    private static void RequireNonNegative(TimeSpan value, string parameterName)
    {
        if (value < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameterName);
    }

    private static void RequireOptionalDuration(TimeSpan? value, string parameterName)
    {
        if (value is { } duration)
            RequireNonNegative(duration, parameterName);
    }

    private static void RequireNonNegative(long value, string parameterName)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(parameterName);
    }

    private static void RequireNonNegative(int value, string parameterName)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(parameterName);
    }

    private static void RequireOptionalNonNegative(long? value, string parameterName)
    {
        if (value is { } count)
            RequireNonNegative(count, parameterName);
    }

    private static void RequireOptionalNonNegative(int? value, string parameterName)
    {
        if (value is { } count)
            RequireNonNegative(count, parameterName);
    }
}

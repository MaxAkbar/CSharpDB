namespace CSharpDB.Observability;

public enum SqlTextCaptureMode
{
    None = 0,
    Normalized,
    Raw,
}

public enum DiagnosticsAvailability
{
    Unknown = 0,
    Available,
    Unsupported,
    Disabled,
    Denied,
    Unavailable,
    NotApplicable,
}

public enum DiagnosticsScope
{
    Unknown = 0,
    Aggregate,
    Instance,
    Shard,
}

public enum DiagnosticsSource
{
    Unknown = 0,
    Engine = 1,
    Storage = 2,
    Client = 3,
    Api = 4,
    Daemon = 5,
    Admin = 6,
}

public enum CSharpDbOperationClass
{
    Unknown = 0,
    Query,
    Script,
    Procedure,
    Transaction,
    Database,
    Recovery,
    Checkpoint,
    Backup,
    Restore,
    Reindex,
    Vacuum,
    Maintenance,
    Pipeline,
}

public enum CSharpDbOperationRole
{
    Unknown = 0,
    Root,
    Request,
    Statement,
    Internal,
}

public enum CSharpDbOperationOutcome
{
    Unknown = 0,
    Succeeded,
    Failed,
    Canceled,
    Rejected,
}

public enum CSharpDbTransport
{
    Unknown = 0,
    Embedded,
    Direct,
    Http,
    Grpc,
    Tcp,
    NamedPipe,
    Sharded,
}

public enum QueryExecutionPhase
{
    Unknown = 0,
    Queued,
    Planning,
    Executing,
    Streaming,
    Waiting,
    Disposing,
    Completed,
}

public enum QueryAccessPathCategory
{
    Unknown = 0,
    TableScan,
    PrimaryKeyLookup,
    IndexSeek,
    IndexScan,
    FullTextIndex,
    Temporary,
}

public enum CounterSemantics
{
    Unknown = 0,
    Cumulative,
    Gauge,
}

public enum MaintenanceOperationKind
{
    Unknown = 0,
    Checkpoint,
    Backup,
    RestoreValidation,
    Restore,
    Reindex,
    Vacuum,
    ForeignKeyMigration,
}

public enum MaintenanceOperationPhase
{
    Unknown = 0,
    Queued,
    AcquiringAccess,
    Checkpointing,
    Copying,
    Staging,
    Validating,
    Hashing,
    Replacing,
    RollingBack,
    Reopening,
    ReopenPending,
    Completed,
}

public enum CheckpointPhase
{
    Unknown = 0,
    Idle,
    Requested,
    Copying,
    CopyCompleteAwaitingReaders,
    Finalizing,
    Faulted,
}

public enum CSharpDbHostLifecyclePhase
{
    Unknown = 0,
    Starting = 1,
    Recovering = 2,
    Running = 3,
    Failed = 4,
    Stopping = 5,
    Stopped = 6,
}

public enum CSharpDbReadinessReason
{
    Unknown = 0,
    Starting = 1,
    None = 2,
    Recovering = 3,
    InitializationFailed = 4,
    ExclusiveMaintenance = 5,
    RestoreInProgress = 6,
    ReopenPending = 7,
    ReadOnly = 8,
    TimedOut = 9,
    Stopping = 10,
    Unavailable = 11,
}

public enum CSharpDbHealthStatus
{
    Unknown = 0,
    Healthy,
    Degraded,
    Unhealthy,
}

public enum CSharpDbHealthCheckKind
{
    Unknown = 0,
    Liveness,
    Readiness,
    Database,
    Storage,
    Wal,
}

using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

public enum CSharpDbLogEventCategory
{
    Unknown = 0,
    Host,
    Query,
    Transaction,
    Storage,
    Maintenance,
    Health,
    Api,
}

public abstract class CSharpDbLogEventDefinition
{
    private protected CSharpDbLogEventDefinition(
        int eventId,
        string name,
        CSharpDbLogEventCategory category,
        string messageTemplate)
    {
        EventId = eventId;
        Name = name;
        Category = category;
        MessageTemplate = messageTemplate;
    }

    public int EventId { get; }
    public string Name { get; }
    public CSharpDbLogEventCategory Category { get; }
    public string MessageTemplate { get; }
}

public sealed class CSharpDbLogEventDefinition<TEvent> : CSharpDbLogEventDefinition
    where TEvent : class
{
    internal CSharpDbLogEventDefinition(
        int eventId,
        string name,
        CSharpDbLogEventCategory category,
        string messageTemplate)
        : base(eventId, name, category, messageTemplate)
    {
    }
}

/// <summary>
/// Stable DiagnosticListener event names and logger message templates. The
/// templates contain only reviewed payload fields and never raw exception text.
/// </summary>
public static class CSharpDbLogEvents
{
    public static CSharpDbLogEventDefinition<CSharpDbHostStartingEvent> HostStarting { get; } =
        Define<CSharpDbHostStartingEvent>(
            CSharpDbLogEventIds.HostStarting,
            "CSharpDB.Host.Starting",
            CSharpDbLogEventCategory.Host,
            "CSharpDB host is starting for {DatabaseAlias}.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> DatabaseOpened { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.DatabaseOpened,
            "CSharpDB.Database.Opened",
            CSharpDbLogEventCategory.Host,
            "Database {DatabaseAlias} opened with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> DatabaseClosed { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.DatabaseClosed,
            "CSharpDB.Database.Closed",
            CSharpDbLogEventCategory.Host,
            "Database {DatabaseAlias} closed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbRawSqlCaptureEnabledEvent> RawSqlCaptureEnabled { get; } =
        Define<CSharpDbRawSqlCaptureEnabledEvent>(
            CSharpDbLogEventIds.RawSqlCaptureEnabled,
            "CSharpDB.Host.RawSqlCaptureEnabled",
            CSharpDbLogEventCategory.Host,
            "Raw SQL capture is enabled for {DatabaseAlias}; capture mode {SqlTextCaptureMode}.");

    public static CSharpDbLogEventDefinition<CSharpDbQueryCompletedEvent> QueryCompleted { get; } =
        Define<CSharpDbQueryCompletedEvent>(
            CSharpDbLogEventIds.QueryCompleted,
            "CSharpDB.Query.Completed",
            CSharpDbLogEventCategory.Query,
            "Query {OperationId} completed in {DurationMs} ms; rows produced {RowsProduced}, rows affected {RowsAffected}.");

    public static CSharpDbLogEventDefinition<CSharpDbSlowQueryEvent> SlowQuery { get; } =
        Define<CSharpDbSlowQueryEvent>(
            CSharpDbLogEventIds.SlowQuery,
            "CSharpDB.Query.Slow",
            CSharpDbLogEventCategory.Query,
            "Slow query {OperationId} completed with outcome {Outcome} in {DurationMs} ms; threshold {SlowQueryThresholdMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbQueryFailedEvent> QueryFailed { get; } =
        Define<CSharpDbQueryFailedEvent>(
            CSharpDbLogEventIds.QueryFailed,
            "CSharpDB.Query.Failed",
            CSharpDbLogEventCategory.Query,
            "Query {OperationId} failed with code {ErrorCode} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbQueryCanceledEvent> QueryCanceled { get; } =
        Define<CSharpDbQueryCanceledEvent>(
            CSharpDbLogEventIds.QueryCanceled,
            "CSharpDB.Query.Canceled",
            CSharpDbLogEventCategory.Query,
            "Query {OperationId} was canceled with code {ErrorCode} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> TransactionCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.TransactionCompleted,
            "CSharpDB.Transaction.Completed",
            CSharpDbLogEventCategory.Transaction,
            "Transaction {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> CheckpointCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.CheckpointCompleted,
            "CSharpDB.Checkpoint.Completed",
            CSharpDbLogEventCategory.Storage,
            "Checkpoint {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> RecoveryCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.RecoveryCompleted,
            "CSharpDB.Recovery.Completed",
            CSharpDbLogEventCategory.Storage,
            "Recovery {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> BackupCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.BackupCompleted,
            "CSharpDB.Backup.Completed",
            CSharpDbLogEventCategory.Maintenance,
            "Backup {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> RestoreCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.RestoreCompleted,
            "CSharpDB.Restore.Completed",
            CSharpDbLogEventCategory.Maintenance,
            "Restore {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> MaintenanceCompleted { get; } =
        Define<CSharpDbLifecycleCompletedEvent>(
            CSharpDbLogEventIds.MaintenanceCompleted,
            "CSharpDB.Maintenance.Completed",
            CSharpDbLogEventCategory.Maintenance,
            "Maintenance operation {OperationId} completed with outcome {Outcome} in {DurationMs} ms.");

    public static CSharpDbLogEventDefinition<CSharpDbHealthTransitionEvent> HealthTransition { get; } =
        Define<CSharpDbHealthTransitionEvent>(
            CSharpDbLogEventIds.HealthTransition,
            "CSharpDB.Health.Transition",
            CSharpDbLogEventCategory.Health,
            "Health changed to lifecycle {LifecyclePhase}, liveness {Liveness}, readiness {Readiness}.");

    public static CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> ApiRequestRejected { get; } =
        Define<CSharpDbApiErrorEvent>(
            CSharpDbLogEventIds.ApiRequestRejected,
            "CSharpDB.Api.RequestRejected",
            CSharpDbLogEventCategory.Api,
            "API request was rejected with code {ErrorCode}, type {ErrorType}; trace {TraceId}.");

    public static CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> ApiUnhandledError { get; } =
        Define<CSharpDbApiErrorEvent>(
            CSharpDbLogEventIds.ApiUnhandledError,
            "CSharpDB.Api.UnhandledError",
            CSharpDbLogEventCategory.Api,
            "API request failed with code {ErrorCode}, type {ErrorType}; trace {TraceId}.");

    public static IReadOnlyList<CSharpDbLogEventDefinition> All { get; } =
        new ReadOnlyCollection<CSharpDbLogEventDefinition>(
        [
            HostStarting,
            DatabaseOpened,
            DatabaseClosed,
            RawSqlCaptureEnabled,
            QueryCompleted,
            SlowQuery,
            QueryFailed,
            QueryCanceled,
            TransactionCompleted,
            CheckpointCompleted,
            RecoveryCompleted,
            BackupCompleted,
            RestoreCompleted,
            MaintenanceCompleted,
            HealthTransition,
            ApiRequestRejected,
            ApiUnhandledError,
        ]);

    private static CSharpDbLogEventDefinition<TEvent> Define<TEvent>(
        int eventId,
        string name,
        CSharpDbLogEventCategory category,
        string messageTemplate)
        where TEvent : class
        => new(eventId, name, category, messageTemplate);
}

public abstract record CSharpDbQueryTerminalEvent
{
    private protected CSharpDbQueryTerminalEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan totalDuration,
        TimeSpan? timeToFirstResult,
        TimeSpan queueDuration,
        TimeSpan executionAndConsumptionDuration,
        long rowsProduced,
        long rowsAffected,
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        SqlTextCaptureMode sqlTextCaptureMode,
        string? capturedSqlText)
    {
        ArgumentNullException.ThrowIfNull(context);
        ValidateUtc(completedAtUtc, nameof(completedAtUtc));
        ValidateDuration(totalDuration, nameof(totalDuration));
        ValidateOptionalDuration(timeToFirstResult, nameof(timeToFirstResult), totalDuration);
        ValidateContainedDuration(queueDuration, nameof(queueDuration), totalDuration);
        ValidateContainedDuration(
            executionAndConsumptionDuration,
            nameof(executionAndConsumptionDuration),
            totalDuration);
        ArgumentOutOfRangeException.ThrowIfNegative(rowsProduced);
        ArgumentOutOfRangeException.ThrowIfNegative(rowsAffected);
        if (outcome == CSharpDbOperationOutcome.Unknown || !Enum.IsDefined(outcome))
            throw new ArgumentOutOfRangeException(nameof(outcome));
        if (!Enum.IsDefined(sqlTextCaptureMode))
            throw new ArgumentOutOfRangeException(nameof(sqlTextCaptureMode));
        if (sqlTextCaptureMode == SqlTextCaptureMode.None && capturedSqlText is not null)
        {
            throw new ArgumentException(
                "Captured SQL text must be omitted when SQL text capture is disabled.",
                nameof(capturedSqlText));
        }
        if (capturedSqlText is not null && string.IsNullOrWhiteSpace(capturedSqlText))
            throw new ArgumentException("Captured SQL text cannot be empty.", nameof(capturedSqlText));

        Context = context;
        CompletedAtUtc = completedAtUtc;
        TotalDuration = totalDuration;
        TimeToFirstResult = timeToFirstResult;
        QueueDuration = queueDuration;
        ExecutionAndConsumptionDuration = executionAndConsumptionDuration;
        RowsProduced = rowsProduced;
        RowsAffected = rowsAffected;
        Outcome = outcome;
        Error = error;
        SqlTextCaptureMode = sqlTextCaptureMode;
        CapturedSqlText = capturedSqlText;
    }

    public CSharpDbOperationContext Context { get; }
    public DateTimeOffset CompletedAtUtc { get; }
    public TimeSpan TotalDuration { get; }
    public TimeSpan? TimeToFirstResult { get; }
    public TimeSpan QueueDuration { get; }
    public TimeSpan ExecutionAndConsumptionDuration { get; }
    public long RowsProduced { get; }
    public long RowsAffected { get; }
    public CSharpDbOperationOutcome Outcome { get; }
    public SafeErrorProjection? Error { get; }
    public SqlTextCaptureMode SqlTextCaptureMode { get; }
    public string? CapturedSqlText { get; }

    private static void ValidateUtc(DateTimeOffset value, string name)
    {
        if (value.Offset != TimeSpan.Zero)
            throw new ArgumentException("The event timestamp must be UTC.", name);
    }

    private static void ValidateDuration(TimeSpan value, string name)
    {
        if (value < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(name);
    }

    private static void ValidateOptionalDuration(TimeSpan? value, string name, TimeSpan totalDuration)
    {
        if (value is TimeSpan duration)
            ValidateContainedDuration(duration, name, totalDuration);
    }

    private static void ValidateContainedDuration(TimeSpan value, string name, TimeSpan totalDuration)
    {
        ValidateDuration(value, name);
        if (value > totalDuration)
            throw new ArgumentOutOfRangeException(name, "A phase duration cannot exceed total duration.");
    }
}

public sealed record CSharpDbQueryCompletedEvent : CSharpDbQueryTerminalEvent
{
    [JsonConstructor]
    public CSharpDbQueryCompletedEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan totalDuration,
        TimeSpan? timeToFirstResult,
        TimeSpan queueDuration,
        TimeSpan executionAndConsumptionDuration,
        long rowsProduced,
        long rowsAffected,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None,
        string? capturedSqlText = null)
        : base(
            context,
            completedAtUtc,
            totalDuration,
            timeToFirstResult,
            queueDuration,
            executionAndConsumptionDuration,
            rowsProduced,
            rowsAffected,
            CSharpDbOperationOutcome.Succeeded,
            error: null,
            sqlTextCaptureMode,
            capturedSqlText)
    {
    }
}

public sealed record CSharpDbSlowQueryEvent : CSharpDbQueryTerminalEvent
{
    [JsonConstructor]
    public CSharpDbSlowQueryEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan totalDuration,
        TimeSpan? timeToFirstResult,
        TimeSpan queueDuration,
        TimeSpan executionAndConsumptionDuration,
        long rowsProduced,
        long rowsAffected,
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error,
        TimeSpan slowQueryThreshold,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None,
        string? capturedSqlText = null)
        : base(
            context,
            completedAtUtc,
            totalDuration,
            timeToFirstResult,
            queueDuration,
            executionAndConsumptionDuration,
            rowsProduced,
            rowsAffected,
            outcome,
            error,
            sqlTextCaptureMode,
            capturedSqlText)
    {
        if (slowQueryThreshold <= TimeSpan.Zero ||
            slowQueryThreshold > CSharpDbObservabilityOptions.MaximumThreshold)
        {
            throw new ArgumentOutOfRangeException(nameof(slowQueryThreshold));
        }
        if (totalDuration < slowQueryThreshold)
            throw new ArgumentException("A slow-query event must meet its configured threshold.");
        ValidateOutcomeError(outcome, error);

        SlowQueryThreshold = slowQueryThreshold;
    }

    public TimeSpan SlowQueryThreshold { get; }

    private static void ValidateOutcomeError(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
    {
        bool requiresError = outcome is
            CSharpDbOperationOutcome.Failed or
            CSharpDbOperationOutcome.Canceled or
            CSharpDbOperationOutcome.Rejected;
        if (requiresError != (error is not null))
            throw new ArgumentException("The terminal outcome and safe error projection are inconsistent.");
    }
}

public sealed record CSharpDbQueryFailedEvent : CSharpDbQueryTerminalEvent
{
    [JsonConstructor]
    public CSharpDbQueryFailedEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan totalDuration,
        TimeSpan? timeToFirstResult,
        TimeSpan queueDuration,
        TimeSpan executionAndConsumptionDuration,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection error,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None,
        string? capturedSqlText = null)
        : base(
            context,
            completedAtUtc,
            totalDuration,
            timeToFirstResult,
            queueDuration,
            executionAndConsumptionDuration,
            rowsProduced,
            rowsAffected,
            CSharpDbOperationOutcome.Failed,
            error ?? throw new ArgumentNullException(nameof(error)),
            sqlTextCaptureMode,
            capturedSqlText)
    {
    }
}

public sealed record CSharpDbQueryCanceledEvent : CSharpDbQueryTerminalEvent
{
    [JsonConstructor]
    public CSharpDbQueryCanceledEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan totalDuration,
        TimeSpan? timeToFirstResult,
        TimeSpan queueDuration,
        TimeSpan executionAndConsumptionDuration,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection error,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None,
        string? capturedSqlText = null)
        : base(
            context,
            completedAtUtc,
            totalDuration,
            timeToFirstResult,
            queueDuration,
            executionAndConsumptionDuration,
            rowsProduced,
            rowsAffected,
            CSharpDbOperationOutcome.Canceled,
            error ?? throw new ArgumentNullException(nameof(error)),
            sqlTextCaptureMode,
            capturedSqlText)
    {
    }
}

public sealed record CSharpDbHostStartingEvent
{
    [JsonConstructor]
    public CSharpDbHostStartingEvent(CSharpDbOperationContext context)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
    }

    public CSharpDbOperationContext Context { get; }
}

public sealed record CSharpDbRawSqlCaptureEnabledEvent
{
    [JsonConstructor]
    public CSharpDbRawSqlCaptureEnabledEvent(
        string databaseAlias,
        SqlTextCaptureMode sqlTextCaptureMode)
    {
        if (!CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias))
            throw new ArgumentException("A safe database alias is required.", nameof(databaseAlias));
        if (sqlTextCaptureMode != SqlTextCaptureMode.Raw)
            throw new ArgumentOutOfRangeException(nameof(sqlTextCaptureMode));

        DatabaseAlias = databaseAlias;
        SqlTextCaptureMode = sqlTextCaptureMode;
    }

    public string DatabaseAlias { get; }
    public SqlTextCaptureMode SqlTextCaptureMode { get; }
}

public sealed record CSharpDbLifecycleCompletedEvent
{
    [JsonConstructor]
    public CSharpDbLifecycleCompletedEvent(
        CSharpDbOperationContext context,
        DateTimeOffset completedAtUtc,
        TimeSpan duration,
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        if (completedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("The event timestamp must be UTC.", nameof(completedAtUtc));
        if (duration < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(duration));
        if (outcome == CSharpDbOperationOutcome.Unknown || !Enum.IsDefined(outcome))
            throw new ArgumentOutOfRangeException(nameof(outcome));

        bool requiresError = outcome is
            CSharpDbOperationOutcome.Failed or
            CSharpDbOperationOutcome.Canceled or
            CSharpDbOperationOutcome.Rejected;
        if (requiresError != (error is not null))
            throw new ArgumentException("The lifecycle outcome and safe error projection are inconsistent.");

        CompletedAtUtc = completedAtUtc;
        Duration = duration;
        Outcome = outcome;
        Error = error;
    }

    public CSharpDbOperationContext Context { get; }
    public DateTimeOffset CompletedAtUtc { get; }
    public TimeSpan Duration { get; }
    public CSharpDbOperationOutcome Outcome { get; }
    public SafeErrorProjection? Error { get; }
}

public sealed record CSharpDbHealthTransitionEvent
{
    [JsonConstructor]
    public CSharpDbHealthTransitionEvent(CSharpDbHostStateSnapshot state)
    {
        State = state ?? throw new ArgumentNullException(nameof(state));
    }

    public CSharpDbHostStateSnapshot State { get; }
}

public sealed record CSharpDbApiErrorEvent
{
    [JsonConstructor]
    public CSharpDbApiErrorEvent(
        DateTimeOffset observedAtUtc,
        CSharpDbTransport transport,
        DiagnosticsTraceId? traceId,
        SafeErrorProjection error)
    {
        if (observedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("The event timestamp must be UTC.", nameof(observedAtUtc));
        if (transport is not (CSharpDbTransport.Http or CSharpDbTransport.Grpc))
            throw new ArgumentOutOfRangeException(nameof(transport));

        ObservedAtUtc = observedAtUtc;
        Transport = transport;
        TraceId = traceId;
        Error = error ?? throw new ArgumentNullException(nameof(error));
    }

    public DateTimeOffset ObservedAtUtc { get; }
    public CSharpDbTransport Transport { get; }
    public DiagnosticsTraceId? TraceId { get; }
    public SafeErrorProjection Error { get; }
}

/// <summary>
/// Publishes typed core events without allowing listeners, filters, or payload
/// factories to affect database execution.
/// </summary>
public sealed class CSharpDbDiagnosticEventPublisher
{
    private readonly DiagnosticListener _listener;

    public CSharpDbDiagnosticEventPublisher(DiagnosticListener listener)
    {
        _listener = listener ?? throw new ArgumentNullException(nameof(listener));
    }

    public bool IsEnabled<TEvent>(CSharpDbLogEventDefinition<TEvent> definition)
        where TEvent : class
    {
        try
        {
            if (definition is null)
                return false;

            CSharpDbDiagnosticEventBuffer? buffer =
                CSharpDbOperationScope.CurrentDiagnosticEventBuffer;
            if (buffer?.IsOwnedBy(this) != true)
                buffer = null;
            return buffer?.IsEnabled(definition.Name) ??
                _listener.IsEnabled(definition.Name);
        }
        catch
        {
            return false;
        }
    }

    public void Publish<TEvent>(
        CSharpDbLogEventDefinition<TEvent> definition,
        Func<TEvent> eventFactory)
        where TEvent : class
        => Publish(
            definition,
            eventFactory,
            static factory => factory());

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Only the closed, strongly typed built-in event definitions can be published; their payload contracts are preserved by the observability JSON source-generation context and trimming smoke tests.")]
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2091",
        Justification = "Only the closed, strongly typed built-in event definitions can be published; their public payload properties are preserved by the observability JSON source-generation context and trimming smoke tests.")]
    public void Publish<TState, TEvent>(
        CSharpDbLogEventDefinition<TEvent> definition,
        TState state,
        Func<TState, TEvent> eventFactory)
        where TEvent : class
    {
        try
        {
            if (definition is null || eventFactory is null)
                return;

            CSharpDbDiagnosticEventBuffer? buffer =
                CSharpDbOperationScope.CurrentDiagnosticEventBuffer;
            if (buffer?.IsOwnedBy(this) != true)
                buffer = null;
            bool enabled = buffer?.IsEnabled(definition.Name) ??
                _listener.IsEnabled(definition.Name);
            if (!enabled)
                return;

            TEvent payload = eventFactory(state);
            if (payload is null)
                return;

            if (buffer is null)
                _listener.Write(definition.Name, payload);
            else
                buffer.Enqueue(definition.Name, payload);
        }
        catch
        {
            // Observability is best-effort and must never change query results
            // or durability, including when a subscriber or filter fails.
        }
    }

    internal CSharpDbDiagnosticEventBuffer CreateBoundaryBuffer()
    {
        var enabledEventNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (CSharpDbLogEventDefinition definition in CSharpDbLogEvents.All)
        {
            try
            {
                if (_listener.IsEnabled(definition.Name))
                    enabledEventNames.Add(definition.Name);
            }
            catch
            {
                // A failing event filter disables only that event for this
                // boundary snapshot.
            }
        }

        return new CSharpDbDiagnosticEventBuffer(this, enabledEventNames);
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Buffered payloads are the same closed, strongly typed built-in event contracts accepted by Publish and covered by the source-generated JSON/trim smoke tests.")]
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2091",
        Justification = "Buffered payloads are the same closed, strongly typed built-in event contracts accepted by Publish and covered by the source-generated JSON/trim smoke tests.")]
    internal void WriteBuffered(string eventName, object payload)
    {
        try
        {
            _listener.Write(eventName, payload);
        }
        catch
        {
            // Buffered delivery has the same no-throw guarantee as Publish.
        }
    }
}

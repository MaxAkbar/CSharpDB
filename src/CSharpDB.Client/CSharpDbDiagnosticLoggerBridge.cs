using System.Diagnostics;
using CSharpDB.Observability;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpDB.Client;

/// <summary>
/// Optional bridge from CSharpDB's logger-independent diagnostic events to
/// normal Microsoft.Extensions.Logging providers.
/// </summary>
public sealed class CSharpDbDiagnosticLoggerBridge :
    IObserver<KeyValuePair<string, object?>>,
    IDisposable
{
    public const string OperationalLoggerCategory = "CSharpDB.Operational";
    public const string QueryLoggerCategory = "CSharpDB.Query";

    private readonly ILogger _operationalLogger;
    private readonly ILogger _queryLogger;
    private readonly bool _operationalEventsEnabled;
    private readonly bool _queriesEnabled;
    private readonly bool _slowQueriesEnabled;
    private IDisposable? _subscription;

    public CSharpDbDiagnosticLoggerBridge(
        ILoggerFactory loggerFactory,
        CSharpDbObservabilityOptions options)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _operationalLogger = CreateLoggerSafely(loggerFactory, OperationalLoggerCategory);
        _queryLogger = CreateLoggerSafely(loggerFactory, QueryLoggerCategory);
        _operationalEventsEnabled = options.Enabled && options.Logging.Enabled;
        _queriesEnabled = _operationalEventsEnabled && options.Logging.Queries;
        _slowQueriesEnabled = _operationalEventsEnabled && options.Logging.SlowQueries;

        try
        {
            // Safe API error projection preserves the host's baseline logging
            // even when optional database observability is disabled. The
            // filter keeps every query/lifecycle family opt-in.
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                IsEventEnabled);
        }
        catch
        {
            _subscription = null;
        }
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(KeyValuePair<string, object?> value)
    {
        try
        {
            switch (value.Key, value.Value)
            {
                case (var name, CSharpDbQueryCompletedEvent payload)
                    when name == CSharpDbLogEvents.QueryCompleted.Name && _queriesEnabled:
                    LogQueryCompleted(payload);
                    break;
                case (var name, CSharpDbSlowQueryEvent payload)
                    when name == CSharpDbLogEvents.SlowQuery.Name && _slowQueriesEnabled:
                    LogSlowQuery(payload);
                    break;
                case (var name, CSharpDbLongRunningQueryEvent payload)
                    when name == CSharpDbLogEvents.LongRunningQuery.Name && _slowQueriesEnabled:
                    LogLongRunningQuery(payload);
                    break;
                case (var name, CSharpDbQueryFailedEvent payload)
                    when name == CSharpDbLogEvents.QueryFailed.Name && _queriesEnabled:
                    LogQueryFailed(payload);
                    break;
                case (var name, CSharpDbQueryCanceledEvent payload)
                    when name == CSharpDbLogEvents.QueryCanceled.Name && _queriesEnabled:
                    LogQueryCanceled(payload);
                    break;
                case (var name, CSharpDbHostStartingEvent payload)
                    when name == CSharpDbLogEvents.HostStarting.Name:
                    LogHostStarting(payload);
                    break;
                case (var name, CSharpDbRawSqlCaptureEnabledEvent payload)
                    when name == CSharpDbLogEvents.RawSqlCaptureEnabled.Name:
                    LogRawSqlCaptureWarning(payload);
                    break;
                case (var name, CSharpDbLifecycleCompletedEvent payload):
                    LogLifecycle(name, payload);
                    break;
                case (var name, CSharpDbHealthTransitionEvent payload)
                    when name == CSharpDbLogEvents.HealthTransition.Name:
                    LogHealthTransition(payload);
                    break;
                case (var name, CSharpDbApiErrorEvent payload)
                    when name == CSharpDbLogEvents.ApiRequestRejected.Name ||
                         name == CSharpDbLogEvents.ApiUnhandledError.Name:
                    LogApiError(name, payload);
                    break;
            }
        }
        catch
        {
            // A logger or provider must never affect the observed operation.
        }
    }

    public void Dispose()
    {
        IDisposable? subscription = Interlocked.Exchange(ref _subscription, null);
        if (subscription is null)
            return;

        try
        {
            subscription.Dispose();
        }
        catch
        {
        }
    }

    private bool IsEventEnabled(string name, object? _, object? __)
    {
        try
        {
            if (name == CSharpDbLogEvents.QueryCompleted.Name ||
                name == CSharpDbLogEvents.QueryFailed.Name ||
                name == CSharpDbLogEvents.QueryCanceled.Name)
            {
                return _queriesEnabled;
            }

            if (name == CSharpDbLogEvents.SlowQuery.Name ||
                name == CSharpDbLogEvents.LongRunningQuery.Name)
            {
                return _slowQueriesEnabled;
            }

            if (name == CSharpDbLogEvents.ApiRequestRejected.Name ||
                name == CSharpDbLogEvents.ApiUnhandledError.Name)
            {
                return true;
            }

            return _operationalEventsEnabled && CSharpDbLogEvents.All.Any(
                definition => string.Equals(definition.Name, name, StringComparison.Ordinal));
        }
        catch
        {
            return false;
        }
    }

    private void LogQueryCompleted(CSharpDbQueryCompletedEvent payload)
    {
        using IDisposable? scope = BeginQueryScope(payload);
        LogSafely(
            _queryLogger,
            LogLevel.Information,
            CSharpDbLogEvents.QueryCompleted,
            payload.Context.OperationId.Value,
            payload.TotalDuration.TotalMilliseconds,
            payload.RowsProduced,
            payload.RowsAffected);
    }

    private void LogSlowQuery(CSharpDbSlowQueryEvent payload)
    {
        using IDisposable? scope = BeginQueryScope(payload);
        LogSafely(
            _queryLogger,
            LogLevel.Warning,
            CSharpDbLogEvents.SlowQuery,
            payload.Context.OperationId.Value,
            payload.Outcome,
            payload.TotalDuration.TotalMilliseconds,
            payload.SlowQueryThreshold.TotalMilliseconds);
    }

    private void LogLongRunningQuery(CSharpDbLongRunningQueryEvent payload)
    {
        using IDisposable? scope = BeginLongRunningQueryScope(payload);
        LogSafely(
            _queryLogger,
            LogLevel.Warning,
            CSharpDbLogEvents.LongRunningQuery,
            payload.Context.OperationId.Value,
            payload.Elapsed.TotalMilliseconds,
            payload.LongRunningQueryThreshold.TotalMilliseconds,
            payload.Phase);
    }

    private void LogQueryFailed(CSharpDbQueryFailedEvent payload)
    {
        using IDisposable? scope = BeginQueryScope(payload);
        LogSafely(
            _queryLogger,
            LogLevel.Error,
            CSharpDbLogEvents.QueryFailed,
            payload.Context.OperationId.Value,
            payload.Error?.Code,
            payload.TotalDuration.TotalMilliseconds);
    }

    private void LogQueryCanceled(CSharpDbQueryCanceledEvent payload)
    {
        using IDisposable? scope = BeginQueryScope(payload);
        LogSafely(
            _queryLogger,
            LogLevel.Information,
            CSharpDbLogEvents.QueryCanceled,
            payload.Context.OperationId.Value,
            payload.Error?.Code,
            payload.TotalDuration.TotalMilliseconds);
    }

    private void LogHostStarting(CSharpDbHostStartingEvent payload)
    {
        using IDisposable? scope = BeginOperationScope(payload.Context, error: null);
        LogSafely(
            _operationalLogger,
            LogLevel.Information,
            CSharpDbLogEvents.HostStarting,
            payload.Context.DatabaseAlias);
    }

    private void LogLifecycle(string name, CSharpDbLifecycleCompletedEvent payload)
    {
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent>? definition =
            CSharpDbLogEvents.All
                .OfType<CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent>>()
                .FirstOrDefault(candidate => string.Equals(candidate.Name, name, StringComparison.Ordinal));
        if (definition is null)
            return;

        using IDisposable? scope = BeginOperationScope(
            payload.Context,
            payload.Error,
            payload.Outcome);
        object?[] arguments = definition == CSharpDbLogEvents.DatabaseOpened ||
                              definition == CSharpDbLogEvents.DatabaseClosed
            ?
            [
                payload.Context.DatabaseAlias,
                payload.Outcome,
                payload.Duration.TotalMilliseconds,
            ]
            :
            [
                payload.Context.OperationId.Value,
                payload.Outcome,
                payload.Duration.TotalMilliseconds,
            ];

        LogSafely(
            _operationalLogger,
            payload.Outcome == CSharpDbOperationOutcome.Succeeded
                ? LogLevel.Information
                : LogLevel.Error,
            definition,
            arguments);
    }

    private void LogHealthTransition(CSharpDbHealthTransitionEvent payload)
    {
        LogSafely(
            _operationalLogger,
            !payload.State.IsLive
                ? LogLevel.Error
                : LogLevel.Information,
            CSharpDbLogEvents.HealthTransition,
            payload.State.LifecyclePhase,
            payload.State.IsLive,
            payload.State.IsReady);
    }

    private void LogApiError(string name, CSharpDbApiErrorEvent payload)
    {
        CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> definition =
            name == CSharpDbLogEvents.ApiRequestRejected.Name
                ? CSharpDbLogEvents.ApiRequestRejected
                : CSharpDbLogEvents.ApiUnhandledError;
        LogSafely(
            _operationalLogger,
            definition == CSharpDbLogEvents.ApiRequestRejected
                ? LogLevel.Warning
                : LogLevel.Error,
            definition,
            payload.Error.Code,
            payload.Error.ErrorType,
            payload.TraceId?.Value);
    }

    private IDisposable? BeginQueryScope(CSharpDbQueryTerminalEvent payload)
    {
        Dictionary<string, object?> fields = CreateOperationFields(payload.Context, payload.Error);
        fields["csharpdb.operation.outcome"] = payload.Outcome.ToString();
        fields["csharpdb.query.duration_ms"] = payload.TotalDuration.TotalMilliseconds;
        fields["csharpdb.query.time_to_first_result_ms"] = payload.TimeToFirstResult?.TotalMilliseconds;
        fields["csharpdb.query.queue_duration_ms"] = payload.QueueDuration.TotalMilliseconds;
        fields["csharpdb.query.execution_consumption_ms"] =
            payload.ExecutionAndConsumptionDuration.TotalMilliseconds;
        fields["csharpdb.query.rows_produced"] = payload.RowsProduced;
        fields["csharpdb.query.rows_affected"] = payload.RowsAffected;
        fields["csharpdb.query.sql_capture_mode"] = payload.SqlTextCaptureMode.ToString();
        if (payload.CapturedSqlText is not null)
            fields["csharpdb.query.sql"] = payload.CapturedSqlText;

        return BeginScopeSafely(_queryLogger, fields);
    }

    private IDisposable? BeginLongRunningQueryScope(CSharpDbLongRunningQueryEvent payload)
    {
        Dictionary<string, object?> fields = CreateOperationFields(payload.Context, error: null);
        fields["csharpdb.query.elapsed_ms"] = payload.Elapsed.TotalMilliseconds;
        fields["csharpdb.query.long_running_threshold_ms"] =
            payload.LongRunningQueryThreshold.TotalMilliseconds;
        fields["csharpdb.query.phase"] = payload.Phase.ToString();
        return BeginScopeSafely(_queryLogger, fields);
    }

    private IDisposable? BeginOperationScope(
        CSharpDbOperationContext context,
        SafeErrorProjection? error,
        CSharpDbOperationOutcome? outcome = null)
    {
        Dictionary<string, object?> fields = CreateOperationFields(context, error);
        fields["csharpdb.operation.outcome"] = outcome?.ToString();
        return BeginScopeSafely(_operationalLogger, fields);
    }

    private static Dictionary<string, object?> CreateOperationFields(
        CSharpDbOperationContext context,
        SafeErrorProjection? error)
    {
        var fields = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["csharpdb.operation.id"] = context.OperationId.Value,
            ["csharpdb.operation.parent_id"] = context.ParentOperationId?.Value,
            ["csharpdb.operation.class"] = context.OperationClass.ToString(),
            ["csharpdb.operation.role"] = context.Role.ToString(),
            ["csharpdb.operation.outcome"] = null,
            ["csharpdb.database.alias"] = context.DatabaseAlias,
            ["csharpdb.transport"] = context.Transport.ToString(),
            ["csharpdb.session.id"] = context.SessionId?.Value,
            ["trace.id"] = context.TraceId?.Value,
            ["csharpdb.query.fingerprint"] = context.QueryFingerprint?.Value,
            ["error.code"] = error?.Code,
            ["error.type"] = error?.ErrorType,
        };
        return fields;
    }

    private void LogRawSqlCaptureWarning(CSharpDbRawSqlCaptureEnabledEvent payload)
    {
        LogSafely(
            _operationalLogger,
            LogLevel.Warning,
            CSharpDbLogEvents.RawSqlCaptureEnabled,
            payload.DatabaseAlias,
            payload.SqlTextCaptureMode);
    }

    private static ILogger CreateLoggerSafely(ILoggerFactory factory, string category)
    {
        try
        {
            return factory.CreateLogger(category) ?? NullLogger.Instance;
        }
        catch
        {
            return NullLogger.Instance;
        }
    }

    private static IDisposable? BeginScopeSafely(
        ILogger logger,
        IReadOnlyDictionary<string, object?> fields)
    {
        try
        {
            return logger.BeginScope(fields);
        }
        catch
        {
            return null;
        }
    }

    private static void LogSafely<TEvent>(
        ILogger logger,
        LogLevel level,
        CSharpDbLogEventDefinition<TEvent> definition,
        params object?[] arguments)
        where TEvent : class
        => LogSafely(
            logger,
            level,
            new EventDefinition(definition.EventId, definition.Name, definition.MessageTemplate),
            arguments);

    private static void LogSafely(
        ILogger logger,
        LogLevel level,
        EventDefinition definition,
        params object?[] arguments)
    {
        try
        {
            logger.Log(
                level,
                new EventId(definition.EventId, definition.Name),
                definition.MessageTemplate,
                arguments);
        }
        catch
        {
        }
    }

    private sealed record EventDefinition(
        int EventId,
        string Name,
        string MessageTemplate);
}

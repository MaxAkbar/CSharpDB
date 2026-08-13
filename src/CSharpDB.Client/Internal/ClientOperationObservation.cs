using CSharpDB.Observability;
using CSharpDB.Sql;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Client.Internal;

internal interface IClientObservabilitySettingsProvider
{
    CSharpDbObservabilityOptions? ObservabilityOptions { get; }
    ObservabilityTransport ObservabilityTransport { get; }
}

/// <summary>
/// Owns the terminal event for a logical client-side composite operation. It is
/// listener driven, contains every diagnostics failure, and never publishes raw
/// errors or values unless raw SQL capture was explicitly configured.
/// </summary>
internal sealed class ClientOperationObservation
{
    private readonly CSharpDbOperationContext _context;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly string? _capturedSqlText;
    private readonly CSharpDbActivityOperation? _activityOperation;
    private int _completed;

    private ClientOperationObservation(
        CSharpDbOperationContext context,
        bool queryEventsEnabled,
        bool slowQueryEventsEnabled,
        TimeSpan slowQueryThreshold,
        SqlTextCaptureMode sqlTextCaptureMode,
        string? capturedSqlText,
        CSharpDbActivityOperation? activityOperation)
    {
        _context = context;
        _queryEventsEnabled = queryEventsEnabled;
        _slowQueryEventsEnabled = slowQueryEventsEnabled;
        _slowQueryThreshold = slowQueryThreshold;
        _sqlTextCaptureMode = sqlTextCaptureMode;
        _capturedSqlText = capturedSqlText;
        _activityOperation = activityOperation;
    }

    internal CSharpDbOperationContext Context => _context;

    internal static ClientOperationObservation? StartRequest(
        ICSharpDbClient client,
        CSharpDbOperationClass operationClass)
    {
        ArgumentNullException.ThrowIfNull(client);

        return client is IClientObservabilitySettingsProvider provider
            ? StartRequest(provider, operationClass)
            : null;
    }

    internal static ClientOperationObservation? StartRequest(
        IClientObservabilitySettingsProvider provider,
        CSharpDbOperationClass operationClass)
    {
        ArgumentNullException.ThrowIfNull(provider);

        return TryStart(
            provider.ObservabilityOptions,
            operationClass,
            (Provider: provider, OperationClass: operationClass),
            static (state, settings) =>
            {
                CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
                return parent is null
                    ? CSharpDbOperationContext.CreateRequest(
                        state.OperationClass,
                        ResolveRootTransport(state.Provider.ObservabilityTransport),
                        settings.DatabaseAlias,
                        CSharpDbOperationScope.CurrentSessionId)
                    : CSharpDbOperationContext.CreateRequest(
                        parent,
                        state.OperationClass);
            },
            sql: null);
    }

    internal static ClientOperationObservation? StartQueryCoordinator(
        CSharpDbObservabilityOptions? options,
        string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        return TryStart(
            options,
            CSharpDbOperationClass.Query,
            ObservabilityTransport.Sharded,
            static (transport, settings) =>
            {
                CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
                return parent is null
                    ? CSharpDbOperationContext.CreateRoot(
                        CSharpDbOperationClass.Query,
                        ResolveRootTransport(transport),
                        settings.DatabaseAlias,
                        CSharpDbOperationScope.CurrentSessionId,
                        queryFingerprint: settings.QueryFingerprint)
                    : settings.QueryFingerprint is null
                        ? CSharpDbOperationContext.CreateStatement(parent)
                        : CSharpDbOperationContext.CreateStatement(parent, settings.QueryFingerprint);
            },
            sql);
    }

    internal static ClientOperationObservation? StartQueryCoordinator(
        CSharpDbObservabilityOptions? options,
        string sql,
        CSharpDbOperationContext context)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        ArgumentNullException.ThrowIfNull(context);
        return TryStart(
            options,
            CSharpDbOperationClass.Query,
            context,
            static (exactContext, _) => exactContext.WithCurrentTraceId(),
            sql);
    }

    private static ObservabilityTransport ResolveRootTransport(
        ObservabilityTransport fallback)
    {
        ObservabilityTransport boundary = CSharpDbOperationScope.CurrentTransport;
        return boundary == ObservabilityTransport.Embedded
            ? fallback
            : boundary;
    }

    internal ClientOperationObservation? StartInternalAttempt(
        ObservabilityTransport transport,
        string databaseAlias)
    {
        try
        {
            string safeAlias = CSharpDbObservabilityOptions.IsValidDatabaseAlias(databaseAlias)
                ? databaseAlias
                : _context.DatabaseAlias;
            var state = (
                Parent: _context,
                Transport: transport,
                DatabaseAlias: safeAlias);
            CSharpDbActivityOperation? activityOperation = null;
            CSharpDbOperationContext context;
            if (_activityOperation is not null &&
                !_activityOperation.IsCompleted &&
                CSharpDbActivityOperation.ShouldStart(tracingEnabled: true))
            {
                try
                {
                    activityOperation = CSharpDbActivityOperation.Start(
                        CSharpDbOperationClass.Query,
                        state,
                        static current => CreateInternalAttemptContext(current),
                        out context);
                }
                catch
                {
                    // A sampler/listener failure must not abort shard fan-out.
                    // Retain the attempt event/context when its clock remains
                    // usable, otherwise omit this best-effort observation.
                    context = CreateInternalAttemptContext(state);
                }
            }
            else
            {
                context = CreateInternalAttemptContext(state);
            }

            return new ClientOperationObservation(
                context,
                _queryEventsEnabled,
                _slowQueryEventsEnabled,
                _slowQueryThreshold,
                _sqlTextCaptureMode,
                _capturedSqlText,
                activityOperation);
        }
        catch
        {
            return null;
        }
    }

    private static CSharpDbOperationContext CreateInternalAttemptContext(
        (CSharpDbOperationContext Parent,
         ObservabilityTransport Transport,
         string DatabaseAlias) state)
        => CSharpDbOperationContext.CreateInternal(
            state.Parent,
            CSharpDbOperationClass.Query,
            state.Transport,
            state.DatabaseAlias,
            state.Parent.QueryFingerprint);

    internal IDisposable EnterScope()
    {
        IDisposable operationScope = CSharpDbOperationScope.Enter(
            _context,
            _activityOperation);
        return _activityOperation?.WrapScope(operationScope) ?? operationScope;
    }

    internal void Succeed(long rowsProduced = 0, long rowsAffected = 0)
        => Complete(
            CSharpDbOperationOutcome.Succeeded,
            rowsProduced,
            rowsAffected,
            error: null);

    internal void Fail(
        Exception exception,
        long rowsProduced = 0,
        long rowsAffected = 0)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Complete(
            exception is OperationCanceledException
                ? CSharpDbOperationOutcome.Canceled
                : CSharpDbOperationOutcome.Failed,
            rowsProduced,
            rowsAffected,
            exception is OperationCanceledException
                ? SafeErrorProjector.Project(SafeErrorKind.OperationCanceled)
                : SafeErrorProjector.Project(exception));
    }

    internal void Fail(
        SafeErrorKind errorKind,
        long rowsProduced = 0,
        long rowsAffected = 0)
        => Complete(
            errorKind == SafeErrorKind.OperationCanceled
                ? CSharpDbOperationOutcome.Canceled
                : CSharpDbOperationOutcome.Failed,
            rowsProduced,
            rowsAffected,
            SafeErrorProjector.Project(errorKind));

    private static ClientOperationObservation? TryStart<TState>(
        CSharpDbObservabilityOptions? options,
        CSharpDbOperationClass operationClass,
        TState state,
        Func<TState, ObservationSettings, CSharpDbOperationContext> createContext,
        string? sql)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed ||
            options?.Enabled != true)
            return null;

        try
        {
            options.Validate();
            CSharpDbLoggingOptions logging = options.Logging;
            CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
            bool suppressDiagnosticEvents =
                CSharpDbOperationScope.AreDiagnosticEventsSuppressed;
            bool queryEventsEnabled = !suppressDiagnosticEvents &&
                logging.Enabled && logging.Queries &&
                (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
            bool slowQueryEventsEnabled = !suppressDiagnosticEvents &&
                logging.Enabled && logging.SlowQueries &&
                publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
            bool traceRequested = CSharpDbActivityOperation.ShouldStart(
                options.OpenTelemetry.Enabled);
            if (!queryEventsEnabled &&
                !slowQueryEventsEnabled &&
                !traceRequested)
                return null;

            QueryFingerprint? fingerprint = null;
            string? capturedSqlText = null;
            SqlTextCaptureMode captureMode = string.IsNullOrWhiteSpace(sql)
                ? SqlTextCaptureMode.None
                : logging.SqlText;
            if (!string.IsNullOrWhiteSpace(sql))
            {
                try
                {
                    if (captureMode == SqlTextCaptureMode.Normalized)
                    {
                        QueryFingerprintResult normalized =
                            SqlQueryFingerprintProvider.Instance.NormalizeAndFingerprint(sql);
                        fingerprint = normalized.Fingerprint;
                        capturedSqlText = normalized.NormalizedText;
                    }
                    else
                    {
                        fingerprint = SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql);
                        if (captureMode == SqlTextCaptureMode.Raw)
                            capturedSqlText = sql;
                    }
                }
                catch
                {
                    // Query classification is best-effort diagnostics work.
                    capturedSqlText = null;
                }
            }

            var settings = new ObservationSettings(
                options.DatabaseAlias,
                fingerprint,
                logging.GetSlowQueryThreshold(
                    sql is null
                        ? CSharpDbOperationClass.Pipeline
                        : CSharpDbOperationClass.Query));
            CSharpDbActivityOperation? activityOperation = null;
            CSharpDbOperationContext context;
            if (traceRequested)
            {
                activityOperation = CSharpDbActivityOperation.Start(
                    operationClass,
                    (State: state, Settings: settings, Factory: createContext),
                    static activityState => activityState.Factory(
                        activityState.State,
                        activityState.Settings),
                    out context);
            }
            else
            {
                context = createContext(state, settings);
            }

            if (!queryEventsEnabled &&
                !slowQueryEventsEnabled &&
                activityOperation is null)
            {
                return null;
            }

            return new ClientOperationObservation(
                context,
                queryEventsEnabled,
                slowQueryEventsEnabled,
                settings.SlowQueryThreshold,
                captureMode,
                capturedSqlText,
                activityOperation);
        }
        catch
        {
            return null;
        }
    }

    private void Complete(
        CSharpDbOperationOutcome outcome,
        long rowsProduced,
        long rowsAffected,
        SafeErrorProjection? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        TimeSpan totalDuration;
        DateTimeOffset completedAtUtc;
        try
        {
            totalDuration = _context.GetElapsedTime();
            completedAtUtc = _context.GetUtcNow();
        }
        catch
        {
            totalDuration = TimeSpan.Zero;
            completedAtUtc = _context.StartedAtUtc;
        }

        try
        {
            _activityOperation?.CompleteQuery(
                outcome,
                error,
                rowsProduced,
                rowsAffected,
                queueDuration: TimeSpan.Zero,
                timeToFirstResult: null,
                isSlow: totalDuration >= _slowQueryThreshold);
        }
        catch
        {
            // Tracing must not alter the client operation.
        }

        try
        {
            CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;

            if (_queryEventsEnabled)
            {
                if (outcome == CSharpDbOperationOutcome.Succeeded)
                {
                    publisher.Publish(
                        CSharpDbLogEvents.QueryCompleted,
                        () => new CSharpDbQueryCompletedEvent(
                            _context,
                            completedAtUtc,
                            totalDuration,
                            timeToFirstResult: null,
                            TimeSpan.Zero,
                            totalDuration,
                            rowsProduced,
                            rowsAffected,
                            _sqlTextCaptureMode,
                            _capturedSqlText));
                }
                else if (outcome == CSharpDbOperationOutcome.Canceled)
                {
                    publisher.Publish(
                        CSharpDbLogEvents.QueryCanceled,
                        () => new CSharpDbQueryCanceledEvent(
                            _context,
                            completedAtUtc,
                            totalDuration,
                            timeToFirstResult: null,
                            TimeSpan.Zero,
                            totalDuration,
                            rowsProduced,
                            rowsAffected,
                            error ?? SafeErrorProjector.Project(SafeErrorKind.OperationCanceled),
                            _sqlTextCaptureMode,
                            _capturedSqlText));
                }
                else
                {
                    publisher.Publish(
                        CSharpDbLogEvents.QueryFailed,
                        () => new CSharpDbQueryFailedEvent(
                            _context,
                            completedAtUtc,
                            totalDuration,
                            timeToFirstResult: null,
                            TimeSpan.Zero,
                            totalDuration,
                            rowsProduced,
                            rowsAffected,
                            error ?? SafeErrorProjector.Project(SafeErrorKind.ClientTransport),
                            _sqlTextCaptureMode,
                            _capturedSqlText));
                }
            }

            if (_slowQueryEventsEnabled && totalDuration >= _slowQueryThreshold)
            {
                publisher.Publish(
                    CSharpDbLogEvents.SlowQuery,
                    () => new CSharpDbSlowQueryEvent(
                        _context,
                        completedAtUtc,
                        totalDuration,
                        timeToFirstResult: null,
                        TimeSpan.Zero,
                        totalDuration,
                        rowsProduced,
                        rowsAffected,
                        outcome,
                        error,
                        _slowQueryThreshold,
                        _sqlTextCaptureMode,
                        _capturedSqlText));
            }
        }
        catch
        {
            // Diagnostics must not alter the client operation.
        }
    }

    private sealed record ObservationSettings(
        string DatabaseAlias,
        QueryFingerprint? QueryFingerprint,
        TimeSpan SlowQueryThreshold);
}

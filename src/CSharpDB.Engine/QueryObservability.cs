using System.Diagnostics;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Engine;

/// <summary>
/// Configuration-driven query instrumentation owned by one database instance.
/// Enabled runtimes retain bounded active/recent state even without event
/// subscribers; a disabled database creates neither this owner nor per-query
/// diagnostic state.
/// </summary>
internal sealed class QueryObservability : IDisposable
{
    private readonly string _databaseAlias;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly TimeProvider _timeProvider;
    private readonly QueryRuntimeDiagnostics _runtimeDiagnostics;
    private readonly QueryPlanRuntimeDiagnosticsAdapter _planRuntimeObserver;
    private readonly CSharpDbRuntimeDiagnosticsState? _ownedRuntimeState;

    internal QueryObservability(
        CSharpDbObservabilityOptions options,
        TimeProvider? timeProvider = null,
        bool startLongRunningSweepTimer = true)
        : this(
            new CSharpDbRuntimeDiagnosticsState(options, timeProvider),
            startLongRunningSweepTimer,
            ownsRuntimeState: true)
    {
    }

    internal QueryObservability(CSharpDbRuntimeDiagnosticsState runtimeState)
        : this(
            runtimeState,
            startLongRunningSweepTimer: true,
            ownsRuntimeState: false)
    {
    }

    internal QueryObservability(
        CSharpDbRuntimeDiagnosticsState runtimeState,
        bool startLongRunningSweepTimer)
        : this(
            runtimeState,
            startLongRunningSweepTimer,
            ownsRuntimeState: false)
    {
    }

    private QueryObservability(
        CSharpDbRuntimeDiagnosticsState runtimeState,
        bool startLongRunningSweepTimer,
        bool ownsRuntimeState)
    {
        ArgumentNullException.ThrowIfNull(runtimeState);
        CSharpDbObservabilityOptions options = runtimeState.CreateOptionsSnapshot();

        _timeProvider = runtimeState.TimeProvider;
        _databaseAlias = options.DatabaseAlias;
        _queryEventsEnabled = options.Logging.Enabled && options.Logging.Queries;
        _slowQueryEventsEnabled = options.Logging.Enabled && options.Logging.SlowQueries;
        _sqlTextCaptureMode = options.Logging.SqlText;
        _slowQueryThreshold = options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query);
        _runtimeDiagnostics = QueryRuntimeDiagnostics.GetOrCreate(
            runtimeState,
            startLongRunningSweepTimer);
        _planRuntimeObserver = new QueryPlanRuntimeDiagnosticsAdapter(
            _runtimeDiagnostics);
        _ownedRuntimeState = ownsRuntimeState ? runtimeState : null;
    }

    internal QueryOperation? Start(
        string? sql,
        QueryFingerprint? suppliedFingerprint = null)
        => (QueryOperation?)StartCore(
            sql,
            suppliedFingerprint,
            allowDirectRuntimeObservation: false);

    internal IQueryExecutionObservation? StartExecution(
        string? sql,
        QueryFingerprint? suppliedFingerprint = null,
        bool allowLeanRuntime = false)
        => StartCore(
            sql,
            suppliedFingerprint,
            allowDirectRuntimeObservation: true,
            allowLeanRuntime);

    private IQueryExecutionObservation? StartCore(
        string? sql,
        QueryFingerprint? suppliedFingerprint,
        bool allowDirectRuntimeObservation,
        bool allowLeanRuntime = false)
    {
        CSharpDbQueryScopeSnapshot ambientScope =
            CSharpDbOperationScope.CaptureQueryScope();
        if (ambientScope.IsDiagnosticsSuppressed)
            return null;

        bool suppressDiagnosticEvents =
            ambientScope.AreDiagnosticEventsSuppressed;

        QueryFingerprint? fingerprint = suppliedFingerprint;
        string? capturedSqlText = null;
        if (!string.IsNullOrWhiteSpace(sql))
        {
            try
            {
                if (_sqlTextCaptureMode == SqlTextCaptureMode.Normalized)
                {
                    QueryFingerprintResult normalized =
                        SqlQueryFingerprintProvider.Instance.NormalizeAndFingerprint(sql);
                    fingerprint ??= normalized.Fingerprint;
                    capturedSqlText = normalized.NormalizedText;
                }
                else
                {
                    fingerprint ??= SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql);
                    if (_sqlTextCaptureMode == SqlTextCaptureMode.Raw)
                        capturedSqlText = sql;
                }
            }
            catch
            {
                // Fingerprinting and optional capture are diagnostic work and
                // must never make an otherwise valid database operation fail.
                capturedSqlText = null;
            }
        }

        bool directInterestCaptured = false;
        bool capturedPublishQueryEvents = false;
        bool capturedPublishSlowQueryEvents = false;
        bool capturedPublishLongRunningQueryEvents = false;
        if (allowLeanRuntime &&
            allowDirectRuntimeObservation &&
            _sqlTextCaptureMode == SqlTextCaptureMode.None &&
            !suppressDiagnosticEvents &&
            ambientScope.Operation is null &&
            ambientScope.QueryRuntimeOperation is null &&
            ambientScope.QueryEventInterest is null &&
            ambientScope.QueryEventBoundary is null &&
            ambientScope.SessionId is null &&
            ambientScope.Transport == CSharpDbTransport.Embedded &&
            Activity.Current is null)
        {
            CSharpDbDiagnosticEventPublisher publisher =
                CSharpDbDiagnostics.EventPublisher;
            capturedPublishQueryEvents = _queryEventsEnabled &&
                (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
            capturedPublishSlowQueryEvents = _slowQueryEventsEnabled &&
                publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
            capturedPublishLongRunningQueryEvents = _slowQueryEventsEnabled &&
                publisher.IsEnabled(CSharpDbLogEvents.LongRunningQuery);
            directInterestCaptured = true;
            if (!capturedPublishQueryEvents &&
                !capturedPublishSlowQueryEvents &&
                !capturedPublishLongRunningQueryEvents &&
                _runtimeDiagnostics.TryStartLean(
                    fingerprint,
                    ambientScope.Transport) is IQueryExecutionObservation lean)
            {
                return lean;
            }
        }

        CSharpDbOperationContext context;
        try
        {
            context = CreateContext(fingerprint, ambientScope);
        }
        catch
        {
            // A custom diagnostics clock must never make query work fail.
            return null;
        }
        TimeSpan queueDuration = context.OperationClass == CSharpDbOperationClass.Query &&
                                 ReferenceEquals(context, ambientScope.Operation)
            ? ambientScope.QueryQueueDuration
            : TimeSpan.Zero;
        QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation = null;
        CSharpDbOperationContext? ambientContext = ambientScope.Operation;
        bool isExactAmbientOperation = ReferenceEquals(context, ambientContext);
        object? ambientRuntimeOperation = isExactAmbientOperation
            ? ambientScope.QueryRuntimeOperation
            : null;
        CSharpDbQueryEventInterestSnapshot? ambientEventInterest =
            isExactAmbientOperation
                ? ambientScope.QueryEventInterest
                : null;
        CSharpDbDeferredDiagnosticBoundary? ambientEventBoundary =
            isExactAmbientOperation
                ? ambientScope.QueryEventBoundary
                : null;
        bool publishQueryEvents;
        bool publishSlowQueryEvents;
        bool publishLongRunningQueryEvents;
        if (suppressDiagnosticEvents)
        {
            publishQueryEvents = false;
            publishSlowQueryEvents = false;
            publishLongRunningQueryEvents = false;
        }
        else if (ambientEventInterest is CSharpDbQueryEventInterestSnapshot interest)
        {
            // The serialized adapter made this decision at operation start,
            // immediately before admission. Do not re-check listeners after
            // the queue wait; late subscribers begin with the next operation.
            publishQueryEvents = interest.QueryEventsEnabled;
            publishSlowQueryEvents = interest.SlowQueryEventsEnabled;
            publishLongRunningQueryEvents =
                interest.LongRunningQueryEventsEnabled;
        }
        else if (directInterestCaptured)
        {
            publishQueryEvents = capturedPublishQueryEvents;
            publishSlowQueryEvents = capturedPublishSlowQueryEvents;
            publishLongRunningQueryEvents =
                capturedPublishLongRunningQueryEvents;
        }
        else
        {
            // Direct Engine calls have no outer adapter and therefore snapshot
            // listener interest at their own Start boundary.
            CSharpDbDiagnosticEventPublisher publisher =
                CSharpDbDiagnostics.EventPublisher;
            publishQueryEvents = _queryEventsEnabled &&
                (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
            publishSlowQueryEvents = _slowQueryEventsEnabled &&
                publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
            publishLongRunningQueryEvents = _slowQueryEventsEnabled &&
                publisher.IsEnabled(CSharpDbLogEvents.LongRunningQuery);
        }

        if (ambientRuntimeOperation is not null)
        {
            // A present lease is authoritative for this exact operation
            // frame. Owner/context mismatches indicate a boundary wiring
            // error; starting again in another registry would create two
            // histories for one operation identity.
            if (ambientRuntimeOperation is not
                    QueryRuntimeDiagnostics.QueryRuntimeOperation ambientOperation ||
                !ambientOperation.TryAdopt(_runtimeDiagnostics, context))
            {
                return null;
            }

            ambientOperation.SetPhase(QueryExecutionPhase.Planning);
            ambientOperation.TryRetainQueryDetail(
                _sqlTextCaptureMode,
                capturedSqlText);
            runtimeOperation = ambientOperation;
        }
        else
        {
            // The ownership table was already claimed by a valid ambient
            // lease, so a failed/repeated adoption cannot create a second
            // terminal owner when this fallback is attempted.
            runtimeOperation = _runtimeDiagnostics.TryStart(
                context,
                QueryExecutionPhase.Planning,
                _sqlTextCaptureMode,
                capturedSqlText,
                suppressDiagnosticEvents,
                publishLongRunningQueryEvents,
                out bool operationAlreadyClaimed);
            if (operationAlreadyClaimed)
                return null;
        }
        if (runtimeOperation is null && !publishQueryEvents && !publishSlowQueryEvents)
            return null;

        IDisposable? eventBoundaryLifetime =
            ambientEventBoundary?.TryAcquireLifetime();
        if (ambientEventBoundary is not null && eventBoundaryLifetime is null)
        {
            // The exact adapter boundary already flushed. Publishing outside
            // it would violate the start-time subscriber snapshot.
            runtimeOperation?.Abandon();
            return null;
        }

        if (allowDirectRuntimeObservation &&
            runtimeOperation is not null &&
            !publishQueryEvents &&
            !publishSlowQueryEvents &&
            !publishLongRunningQueryEvents &&
            ambientEventBoundary is null &&
            eventBoundaryLifetime is null)
        {
            return runtimeOperation;
        }

        return new QueryOperation(
            context,
            publishQueryEvents,
            publishSlowQueryEvents,
            _slowQueryThreshold,
            _sqlTextCaptureMode,
            capturedSqlText,
            queueDuration,
            runtimeOperation,
            ambientEventBoundary,
            eventBoundaryLifetime);
    }

    internal BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> GetActiveSnapshot(
        int maximumRecords)
        => _runtimeDiagnostics.GetActiveSnapshot(maximumRecords);

    internal DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        GetActiveCollectionSnapshot(int maximumRecords)
        => _runtimeDiagnostics.GetActiveCollectionSnapshot(maximumRecords);

    internal BoundedDiagnosticsSnapshot<RecentQuerySnapshot> GetRecentSnapshot(
        int maximumRecords)
        => _runtimeDiagnostics.GetRecentSnapshot(maximumRecords);

    internal DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        GetRecentCollectionSnapshot(int maximumRecords)
        => _runtimeDiagnostics.GetRecentCollectionSnapshot(maximumRecords);

    internal QueryDiagnosticsSummary GetSummary()
        => _runtimeDiagnostics.GetSummary();

    internal QueryPlanDiagnosticsSnapshot? GetPlanSnapshot(
        OpaqueDiagnosticsId operationId)
        => _runtimeDiagnostics.GetPlanSnapshot(operationId);

    internal QueryDetailSnapshot? GetQueryDetailSnapshot(
        OpaqueDiagnosticsId operationId)
        => _runtimeDiagnostics.GetQueryDetailSnapshot(operationId);

    internal IQueryPlanRuntimeObserver PlanRuntimeObserver =>
        _planRuntimeObserver;

    internal int SweepLongRunningQueries()
        => _runtimeDiagnostics.SweepLongRunningQueries();

    internal IDisposable? EnterWaiting()
        => _runtimeDiagnostics.EnterCurrentWaiting();

    public void Dispose()
        => _ownedRuntimeState?.Dispose();

    private CSharpDbOperationContext CreateContext(
        QueryFingerprint? fingerprint,
        CSharpDbQueryScopeSnapshot ambientScope)
    {
        CSharpDbOperationContext? ambient = ambientScope.Operation;
        if (ambient is not null)
        {
            if (ambient.OperationClass == CSharpDbOperationClass.Query &&
                ambient.Role is CSharpDbOperationRole.Root or CSharpDbOperationRole.Statement)
            {
                return ambient;
            }

            return fingerprint is null
                ? CSharpDbOperationContext.CreateStatement(ambient)
                : CSharpDbOperationContext.CreateStatement(ambient, fingerprint);
        }

        return CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            ambientScope.Transport,
            _databaseAlias,
            sessionId: ambientScope.SessionId,
            queryFingerprint: fingerprint,
            timeProvider: _timeProvider);
    }
}

internal interface IQueryExecutionObservation : IQueryResultObserver
{
    IQueryPlanRuntimeObserver? ExplicitPlanObserver { get; }

    IDisposable EnterScope();

    void MarkExecuting();

    QueryResult Observe(QueryResult result);

    void Fail(Exception exception);
}

internal sealed class QueryOperation : IQueryExecutionObservation
{
    private readonly CSharpDbOperationContext _context;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly string? _capturedSqlText;
    private readonly TimeSpan _queueDuration;
    private readonly QueryRuntimeDiagnostics.QueryRuntimeOperation? _runtimeOperation;
    private readonly CSharpDbDeferredDiagnosticBoundary? _eventBoundary;
    private IDisposable? _eventBoundaryLifetime;
    private TimeSpan? _timeToFirstResult;
    private int _completed;

    internal QueryOperation(
        CSharpDbOperationContext context,
        bool queryEventsEnabled,
        bool slowQueryEventsEnabled,
        TimeSpan slowQueryThreshold,
        SqlTextCaptureMode sqlTextCaptureMode,
        string? capturedSqlText,
        TimeSpan queueDuration,
        QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation = null,
        CSharpDbDeferredDiagnosticBoundary? eventBoundary = null,
        IDisposable? eventBoundaryLifetime = null)
    {
        _context = context;
        _queryEventsEnabled = queryEventsEnabled;
        _slowQueryEventsEnabled = slowQueryEventsEnabled;
        _slowQueryThreshold = slowQueryThreshold;
        _sqlTextCaptureMode = sqlTextCaptureMode;
        _capturedSqlText = capturedSqlText;
        _queueDuration = queueDuration < TimeSpan.Zero
            ? TimeSpan.Zero
            : queueDuration;
        _runtimeOperation = runtimeOperation;
        _eventBoundary = eventBoundary;
        _eventBoundaryLifetime = eventBoundaryLifetime;
    }

    public IDisposable EnterScope()
        => _runtimeOperation is null
            ? CSharpDbOperationScope.Enter(_context)
            : CSharpDbOperationScope.Enter(_context, _runtimeOperation);

    public IQueryPlanRuntimeObserver? ExplicitPlanObserver => null;

    public void MarkExecuting()
        => _runtimeOperation?.SetPhase(QueryExecutionPhase.Executing);

    public QueryResult Observe(QueryResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.IsQuery)
        {
            _runtimeOperation?.SetPhase(QueryExecutionPhase.Streaming);
            result.SetObserver(this);
            if (result.RequiresRuntimeExecutionScope)
            {
                // Adaptive execution can outlive the ExecuteAsync call that
                // established the original operation scope. Re-enter it
                // outside any storage binding so its runtime plan callbacks
                // retain exact attribution throughout Open/MoveNext/Dispose.
                result.PrependExecutionScopeFactory(EnterScope);
            }
        }
        else
        {
            CompleteSucceeded(rowsProduced: 0, result.RowsAffected);
        }

        return result;
    }

    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Complete(
            exception is OperationCanceledException
                ? CSharpDbOperationOutcome.Canceled
                : CSharpDbOperationOutcome.Failed,
            rowsProduced: 0,
            rowsAffected: 0,
            ProjectError(exception));
    }

    public void OnFirstRowProduced()
    {
        _runtimeOperation?.SetPhase(QueryExecutionPhase.Streaming);
        _timeToFirstResult ??= _context.GetElapsedTime();
    }

    public void OnRowProduced()
    {
    }

    public void OnDisposing()
        => _runtimeOperation?.SetPhase(QueryExecutionPhase.Disposing);

    public void OnCompleted(QueryResultCompletion completion)
    {
        switch (completion.Reason)
        {
            case QueryResultCompletionReason.Exhausted:
            case QueryResultCompletionReason.Disposed:
                CompleteSucceeded(completion.RowsProduced, rowsAffected: 0);
                break;
            case QueryResultCompletionReason.Canceled:
                Complete(
                    CSharpDbOperationOutcome.Canceled,
                    completion.RowsProduced,
                    rowsAffected: 0,
                    ProjectError(completion.Error ?? new OperationCanceledException()));
                break;
            default:
                Complete(
                    CSharpDbOperationOutcome.Failed,
                    completion.RowsProduced,
                    rowsAffected: 0,
                    ProjectError(completion.Error ?? new InvalidOperationException()));
                break;
        }
    }

    private void CompleteSucceeded(long rowsProduced, long rowsAffected)
        => Complete(
            CSharpDbOperationOutcome.Succeeded,
            rowsProduced,
            rowsAffected,
            error: null);

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
            _runtimeOperation?.Complete(
                outcome,
                completedAtUtc,
                totalDuration,
                _timeToFirstResult,
                rowsProduced,
                rowsAffected,
                error,
                isSlow: totalDuration >= _slowQueryThreshold);
        }
        catch
        {
            // Registry/history failures cannot affect query completion.
        }

        IDisposable? eventBoundaryScope = null;
        try
        {
            if (_eventBoundary is not null &&
                (_queryEventsEnabled || _slowQueryEventsEnabled))
            {
                try
                {
                    eventBoundaryScope = _eventBoundary.Enter();
                }
                catch
                {
                    // Retained start-time delivery is best effort and must
                    // never alter query completion.
                }
            }

            TimeSpan queueDuration = _queueDuration <= totalDuration
                ? _queueDuration
                : totalDuration;
            TimeSpan executionAndConsumptionDuration = totalDuration - queueDuration;
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
                            _timeToFirstResult,
                            queueDuration,
                            executionAndConsumptionDuration,
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
                            _timeToFirstResult,
                            queueDuration,
                            executionAndConsumptionDuration,
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
                            _timeToFirstResult,
                            queueDuration,
                            executionAndConsumptionDuration,
                            rowsProduced,
                            rowsAffected,
                            error ?? SafeErrorProjector.Project(SafeErrorKind.DatabaseOperation),
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
                        _timeToFirstResult,
                        queueDuration,
                        executionAndConsumptionDuration,
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
            // Query completion must remain independent from every diagnostic
            // payload, listener, and clock failure.
        }
        finally
        {
            try
            {
                eventBoundaryScope?.Dispose();
            }
            catch
            {
                // Deferred delivery scope teardown is best effort.
            }

            ReleaseEventBoundary();
        }
    }

    private void ReleaseEventBoundary()
    {
        try
        {
            Interlocked.Exchange(ref _eventBoundaryLifetime, null)?.Dispose();
        }
        catch
        {
            // A deferred diagnostic lifetime cannot affect query completion.
        }

        try
        {
            _eventBoundary?.Dispose();
        }
        catch
        {
            // Deferred diagnostic delivery is best effort.
        }
    }

    internal static SafeErrorProjection ProjectError(Exception exception)
    {
        if (exception is OperationCanceledException)
            return SafeErrorProjector.Project(SafeErrorKind.OperationCanceled);

        if (exception is not CSharpDbException databaseException)
            return SafeErrorProjector.Project(exception);

        SafeErrorKind kind = databaseException.Code switch
        {
            ErrorCode.TableNotFound or
            ErrorCode.ColumnNotFound or
            ErrorCode.TriggerNotFound => SafeErrorKind.DatabaseNotFound,

            ErrorCode.TableAlreadyExists or
            ErrorCode.TriggerAlreadyExists => SafeErrorKind.DatabaseAlreadyExists,

            ErrorCode.ConstraintViolation or
            ErrorCode.DuplicateKey => SafeErrorKind.DatabaseConstraint,

            ErrorCode.TypeMismatch => SafeErrorKind.DatabaseTypeMismatch,
            ErrorCode.SyntaxError => SafeErrorKind.DatabaseSyntax,
            ErrorCode.Busy => SafeErrorKind.DatabaseBusy,
            ErrorCode.TransactionConflict => SafeErrorKind.DatabaseConflict,
            ErrorCode.ResourceLimitExceeded => SafeErrorKind.DatabaseResourceLimit,
            ErrorCode.CorruptDatabase => SafeErrorKind.DatabaseCorrupt,
            ErrorCode.IoError or
            ErrorCode.JournalError or
            ErrorCode.WalError => SafeErrorKind.DatabaseIo,
            _ => SafeErrorKind.DatabaseOperation,
        };

        return SafeErrorProjector.Project(kind);
    }
}

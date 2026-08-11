using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data;

/// <summary>
/// Owns the part of an embedded ADO.NET query that precedes engine dispatch.
/// The engine takes over terminal publication as soon as a Database execution
/// method is invoked.
/// </summary>
internal sealed class AdoCommandObservation : IDisposable
{
    internal static Action? QueueWaitStartingForTest { get; set; }

    internal static DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>
        CaptureActiveQueriesForTest(
            object runtimeDiagnosticsState,
            int maximumRecords = 32)
        => QueryRuntimeDiagnostics
            .GetOrCreate(GetRuntimeDiagnosticsStateForTest(runtimeDiagnosticsState))
            .GetActiveCollectionSnapshot(maximumRecords);

    internal static DiagnosticsCollectionSnapshot<RecentQuerySnapshot>
        CaptureRecentQueriesForTest(
            object runtimeDiagnosticsState,
            int maximumRecords = 32)
        => QueryRuntimeDiagnostics
            .GetOrCreate(GetRuntimeDiagnosticsStateForTest(runtimeDiagnosticsState))
            .GetRecentCollectionSnapshot(maximumRecords);

    internal static QueryDetailSnapshot? CaptureQueryDetailForTest(
        object runtimeDiagnosticsState,
        OpaqueDiagnosticsId operationId)
        => QueryRuntimeDiagnostics
            .GetOrCreate(GetRuntimeDiagnosticsStateForTest(runtimeDiagnosticsState))
            .GetQueryDetailSnapshot(operationId);

    internal static object? GetRuntimeDiagnosticsStateForTest(
        ICSharpDbSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        return session.RuntimeDiagnosticsState;
    }

    internal static IDisposable CreateRuntimeDiagnosticsStateForTest(
        CSharpDbObservabilityOptions options,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new CSharpDbRuntimeDiagnosticsState(options, timeProvider);
    }

    internal static AdoCommandObservation? TryStartForTest(
        CSharpDbObservabilityOptions options,
        object runtimeDiagnosticsState,
        string sql,
        OpaqueDiagnosticsId sessionId)
        => TryStart(
            options,
            GetRuntimeDiagnosticsStateForTest(runtimeDiagnosticsState),
            sql,
            sessionId);

    internal static AdoRuntimeDiagnosticsStateInfoForTest
        GetRuntimeDiagnosticsStateInfoForTest(object runtimeDiagnosticsState)
    {
        CSharpDbRuntimeDiagnosticsState state =
            GetRuntimeDiagnosticsStateForTest(runtimeDiagnosticsState);
        return new AdoRuntimeDiagnosticsStateInfoForTest(
            state.ServerInstanceId,
            state.DatabaseAlias,
            state.ActiveQueryCapacity);
    }

    private static CSharpDbRuntimeDiagnosticsState GetRuntimeDiagnosticsStateForTest(
        object runtimeDiagnosticsState)
        => runtimeDiagnosticsState as CSharpDbRuntimeDiagnosticsState
           ?? throw new ArgumentException(
               "The value is not a runtime diagnostics state.",
               nameof(runtimeDiagnosticsState));

    private readonly CSharpDbOperationContext? _context;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly string? _capturedSqlText;
    private readonly CSharpDbRuntimeDiagnosticsState? _runtimeDiagnosticsState;
    private readonly QueryRuntimeDiagnostics.QueryRuntimeOperation? _runtimeOperation;
    private readonly IDisposable? _operationScope;
    private readonly IDisposable _boundaryScope;
    private readonly CSharpDbDeferredDiagnosticBoundary? _deferredEventBoundary;

    // 0 = ADO pre-dispatch, 1 = handed to Engine, 2 = completed by ADO.
    private int _ownershipState;
    private int _disposed;
    private long _queueDurationTicks;

    private AdoCommandObservation(
        CSharpDbOperationContext context,
        bool queryEventsEnabled,
        bool slowQueryEventsEnabled,
        TimeSpan slowQueryThreshold,
        SqlTextCaptureMode sqlTextCaptureMode,
        string? capturedSqlText,
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState,
        QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation,
        IDisposable operationScope,
        IDisposable boundaryScope,
        CSharpDbDeferredDiagnosticBoundary? deferredEventBoundary)
    {
        _context = context;
        _queryEventsEnabled = queryEventsEnabled;
        _slowQueryEventsEnabled = slowQueryEventsEnabled;
        _slowQueryThreshold = slowQueryThreshold;
        _sqlTextCaptureMode = sqlTextCaptureMode;
        _capturedSqlText = capturedSqlText;
        _runtimeDiagnosticsState = runtimeDiagnosticsState;
        _runtimeOperation = runtimeOperation;
        _operationScope = operationScope;
        _boundaryScope = boundaryScope;
        _deferredEventBoundary = deferredEventBoundary;
    }

    internal static AdoCommandObservation? TryStart(
        CSharpDbObservabilityOptions? options,
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState,
        string sql,
        OpaqueDiagnosticsId sessionId)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed ||
            options?.Enabled != true)
        {
            return null;
        }

        bool runtimeHistoryEnabled = runtimeDiagnosticsState?.IsEnabled == true;
        bool eventBoundaryRequired =
            QueryObservabilitySource.IsBoundaryRequired(options);
        if (!runtimeHistoryEnabled && !eventBoundaryRequired)
            return null;

        IDisposable? boundaryScope = null;
        IDisposable? operationScope = null;
        CSharpDbDeferredDiagnosticBoundary? deferredEventBoundary = null;
        QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation = null;
        try
        {
            if (eventBoundaryRequired)
            {
                deferredEventBoundary =
                    CSharpDbOperationScope.CreateDeferredBoundary(
                        CSharpDbTransport.Direct,
                        sessionId);
                boundaryScope = deferredEventBoundary.Enter();
            }
            else
            {
                boundaryScope = CSharpDbOperationScope.EnterTransport(
                    CSharpDbTransport.Direct,
                    sessionId);
            }

            CSharpDbLoggingOptions logging = options.Logging;
            CSharpDbDiagnosticEventPublisher publisher =
                CSharpDbDiagnostics.EventPublisher;
            bool publishQueryEvents = logging.Enabled && logging.Queries &&
                (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
            bool publishSlowQueryEvents = logging.Enabled &&
                logging.SlowQueries &&
                publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
            bool publishLongRunningQueryEvents = logging.Enabled &&
                logging.SlowQueries &&
                publisher.IsEnabled(CSharpDbLogEvents.LongRunningQuery);
            QueryFingerprint? fingerprint = null;
            string? capturedSqlText = null;

            if (!string.IsNullOrWhiteSpace(sql))
            {
                try
                {
                    if (logging.SqlText == SqlTextCaptureMode.Normalized)
                    {
                        QueryFingerprintResult normalized =
                            SqlQueryFingerprintProvider.Instance.NormalizeAndFingerprint(sql);
                        fingerprint = normalized.Fingerprint;
                        if (runtimeHistoryEnabled ||
                            publishQueryEvents ||
                            publishSlowQueryEvents)
                        {
                            capturedSqlText = normalized.NormalizedText;
                        }
                    }
                    else
                    {
                        fingerprint = SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql);
                        if (logging.SqlText == SqlTextCaptureMode.Raw &&
                            (runtimeHistoryEnabled ||
                             publishQueryEvents ||
                             publishSlowQueryEvents))
                        {
                            capturedSqlText = sql;
                        }
                    }
                }
                catch
                {
                    // Fingerprinting is diagnostic work. Classification or
                    // binding failures still need a terminal event even when
                    // the source cannot be normalized.
                    fingerprint = null;
                    capturedSqlText = null;
                }
            }

            CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
                CSharpDbOperationClass.Query,
                CSharpDbOperationScope.CurrentTransport,
                runtimeDiagnosticsState?.DatabaseAlias ?? options.DatabaseAlias,
                CSharpDbOperationScope.CurrentSessionId,
                fingerprint,
                runtimeDiagnosticsState?.TimeProvider);
            if (runtimeHistoryEnabled)
            {
                runtimeOperation = QueryRuntimeDiagnostics
                    .GetOrCreate(runtimeDiagnosticsState!)
                    .TryStart(
                        context,
                        QueryExecutionPhase.Queued,
                        logging.SqlText,
                        capturedSqlText,
                        suppressDiagnosticEvents: false,
                        publishLongRunningQueryEvents,
                        out _);
            }
            operationScope = CSharpDbOperationScope.Enter(
                context,
                runtimeOperation,
                new CSharpDbQueryEventInterestSnapshot(
                    publishQueryEvents,
                    publishSlowQueryEvents,
                    publishLongRunningQueryEvents),
                deferredEventBoundary);

            var observation = new AdoCommandObservation(
                context,
                publishQueryEvents,
                publishSlowQueryEvents,
                logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query),
                logging.SqlText,
                capturedSqlText,
                runtimeDiagnosticsState,
                runtimeOperation,
                operationScope,
                boundaryScope,
                deferredEventBoundary);
            operationScope = null;
            boundaryScope = null;
            deferredEventBoundary = null;
            return observation;
        }
        catch
        {
            runtimeOperation?.Abandon();
            operationScope?.Dispose();
            boundaryScope?.Dispose();
            deferredEventBoundary?.Dispose();
            return null;
        }
    }

    /// <summary>
    /// Measures only an admission-gate wait. Binding, classification, and
    /// other pre-dispatch work are intentionally excluded from queue time.
    /// </summary>
    internal IDisposable? MeasureQueueWait()
    {
        try
        {
            QueueWaitStartingForTest?.Invoke();
        }
        catch
        {
            // Test diagnostics cannot change admission behavior.
        }

        if (_context is null)
            return null;

        try
        {
            return new QueueWaitMeasurement(this, _context.GetElapsedTime());
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Exposes the measured admission-gate wait to the engine while it creates
    /// its query observer. Direct execution and paths that reached no gate use
    /// a zero duration.
    /// </summary>
    internal IDisposable? EnterQueueDurationScope()
    {
        if (_context is null)
            return null;

        try
        {
            return CSharpDbOperationScope.EnterQueryQueueDuration(
                GetQueueDuration());
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Transfers terminal ownership to Engine immediately before Database is
    /// called. A failure after this point is observed by Engine, never here.
    /// </summary>
    internal void MarkDispatchHandoff(Database database)
    {
        ArgumentNullException.ThrowIfNull(database);
        if (_runtimeOperation is not null &&
            !ReferenceEquals(
                _runtimeDiagnosticsState,
                database.RuntimeDiagnosticsState))
        {
            if (Interlocked.CompareExchange(ref _ownershipState, 2, 0) == 0)
            {
                try
                {
                    _runtimeOperation.Abandon();
                }
                catch
                {
                    // A racing family retirement cannot affect dispatch.
                }
            }
            return;
        }

        Interlocked.CompareExchange(ref _ownershipState, 1, 0);
    }

    internal void FailBeforeDispatch(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (Interlocked.CompareExchange(ref _ownershipState, 2, 0) != 0)
            return;

        if (_context is null)
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

        TimeSpan queueDuration = GetQueueDuration(totalDuration);
        TimeSpan executionAndConsumptionDuration = totalDuration - queueDuration;
        SafeErrorProjection error = ProjectError(exception);
        CSharpDbOperationOutcome outcome = exception is OperationCanceledException
            ? CSharpDbOperationOutcome.Canceled
            : CSharpDbOperationOutcome.Failed;
        try
        {
            _runtimeOperation?.Complete(
                outcome,
                completedAtUtc,
                totalDuration,
                timeToFirstResult: null,
                rowsProduced: 0,
                rowsAffected: 0,
                error,
                isSlow: totalDuration >= _slowQueryThreshold);
        }
        catch
        {
            // Runtime history is best effort and must not replace the
            // original binding, classification, or admission failure.
        }

        try
        {
            CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;

            if (_queryEventsEnabled)
            {
                if (outcome == CSharpDbOperationOutcome.Canceled)
                {
                    publisher.Publish(
                        CSharpDbLogEvents.QueryCanceled,
                        () => new CSharpDbQueryCanceledEvent(
                            _context,
                            completedAtUtc,
                            totalDuration,
                            timeToFirstResult: null,
                            queueDuration,
                            executionAndConsumptionDuration,
                            rowsProduced: 0,
                            rowsAffected: 0,
                            error,
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
                            queueDuration,
                            executionAndConsumptionDuration,
                            rowsProduced: 0,
                            rowsAffected: 0,
                            error,
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
                        queueDuration,
                        executionAndConsumptionDuration,
                        rowsProduced: 0,
                        rowsAffected: 0,
                        outcome,
                        error,
                        _slowQueryThreshold,
                        _sqlTextCaptureMode,
                        _capturedSqlText));
            }
        }
        catch
        {
            // Diagnostics must not replace the original ADO.NET exception.
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (Interlocked.CompareExchange(ref _ownershipState, 2, 0) == 0)
        {
            try
            {
                _runtimeOperation?.Abandon();
            }
            catch
            {
                // Abandonment is diagnostic cleanup only.
            }
        }

        try
        {
            _operationScope?.Dispose();
        }
        finally
        {
            try
            {
                _boundaryScope.Dispose();
            }
            finally
            {
                _deferredEventBoundary?.Dispose();
            }
        }
    }

    private TimeSpan GetQueueDuration()
        => TimeSpan.FromTicks(Math.Max(0, Volatile.Read(ref _queueDurationTicks)));

    private TimeSpan GetQueueDuration(TimeSpan totalDuration)
    {
        long queueTicks = Math.Clamp(
            Volatile.Read(ref _queueDurationTicks),
            0,
            totalDuration.Ticks);
        return TimeSpan.FromTicks(queueTicks);
    }

    private void CompleteQueueWait(TimeSpan startedAt)
    {
        if (_context is null)
            return;

        try
        {
            TimeSpan duration = _context.GetElapsedTime() - startedAt;
            Interlocked.Exchange(
                ref _queueDurationTicks,
                Math.Max(0, duration.Ticks));
        }
        catch
        {
            // Timing diagnostics must never affect admission or execution.
        }
    }

    private sealed class QueueWaitMeasurement : IDisposable
    {
        private AdoCommandObservation? _owner;
        private readonly TimeSpan _startedAt;

        internal QueueWaitMeasurement(
            AdoCommandObservation owner,
            TimeSpan startedAt)
        {
            _owner = owner;
            _startedAt = startedAt;
        }

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.CompleteQueueWait(_startedAt);
    }

    private static SafeErrorProjection ProjectError(Exception exception)
    {
        if (exception is OperationCanceledException)
            return SafeErrorProjector.Project(SafeErrorKind.OperationCanceled);

        if (exception is not CSharpDbException databaseException)
        {
            return exception is InvalidOperationException
                ? SafeErrorProjector.Project(SafeErrorKind.InvalidArgument)
                : SafeErrorProjector.Project(exception);
        }

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

internal readonly record struct AdoRuntimeDiagnosticsStateInfoForTest(
    string ServerInstanceId,
    string DatabaseAlias,
    int ActiveQueryCapacity);

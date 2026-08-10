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

    private readonly CSharpDbOperationContext? _context;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly string? _capturedSqlText;
    private readonly IDisposable? _operationScope;
    private readonly IDisposable _boundaryScope;

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
        IDisposable operationScope,
        IDisposable boundaryScope)
    {
        _context = context;
        _queryEventsEnabled = queryEventsEnabled;
        _slowQueryEventsEnabled = slowQueryEventsEnabled;
        _slowQueryThreshold = slowQueryThreshold;
        _sqlTextCaptureMode = sqlTextCaptureMode;
        _capturedSqlText = capturedSqlText;
        _operationScope = operationScope;
        _boundaryScope = boundaryScope;
    }

    private AdoCommandObservation(IDisposable boundaryScope)
    {
        _context = null;
        _queryEventsEnabled = false;
        _slowQueryEventsEnabled = false;
        _slowQueryThreshold = TimeSpan.Zero;
        _sqlTextCaptureMode = SqlTextCaptureMode.None;
        _capturedSqlText = null;
        _operationScope = null;
        _boundaryScope = boundaryScope;
    }

    internal static AdoCommandObservation? TryStart(
        CSharpDbObservabilityOptions? options,
        string sql,
        OpaqueDiagnosticsId sessionId)
    {
        if (!QueryObservabilitySource.IsBoundaryRequired(options))
            return null;

        IDisposable? boundaryScope = null;
        IDisposable? operationScope = null;
        try
        {
            boundaryScope = CSharpDbOperationScope.EnterBoundary(
                CSharpDbTransport.Direct,
                sessionId);
            if (!QueryObservabilitySource.IsObservationRequested(options))
            {
                var boundaryOnlyObservation = new AdoCommandObservation(
                    boundaryScope);
                boundaryScope = null;
                return boundaryOnlyObservation;
            }

            CSharpDbLoggingOptions logging = options!.Logging;
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
                        capturedSqlText = normalized.NormalizedText;
                    }
                    else
                    {
                        fingerprint = SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql);
                        if (logging.SqlText == SqlTextCaptureMode.Raw)
                            capturedSqlText = sql;
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
                options.DatabaseAlias,
                CSharpDbOperationScope.CurrentSessionId,
                fingerprint);
            operationScope = CSharpDbOperationScope.Enter(context);

            var observation = new AdoCommandObservation(
                context,
                logging.Enabled && logging.Queries,
                logging.Enabled && logging.SlowQueries,
                logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query),
                logging.SqlText,
                capturedSqlText,
                operationScope,
                boundaryScope);
            operationScope = null;
            boundaryScope = null;
            return observation;
        }
        catch
        {
            operationScope?.Dispose();
            boundaryScope?.Dispose();
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
    internal void MarkDispatchHandoff()
        => Interlocked.CompareExchange(ref _ownershipState, 1, 0);

    internal void FailBeforeDispatch(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (Interlocked.CompareExchange(ref _ownershipState, 2, 0) != 0)
            return;

        if (_context is null)
            return;

        try
        {
            TimeSpan totalDuration = _context.GetElapsedTime();
            TimeSpan queueDuration = GetQueueDuration(totalDuration);
            TimeSpan executionAndConsumptionDuration = totalDuration - queueDuration;
            DateTimeOffset completedAtUtc = _context.GetUtcNow();
            SafeErrorProjection error = ProjectError(exception);
            CSharpDbOperationOutcome outcome = exception is OperationCanceledException
                ? CSharpDbOperationOutcome.Canceled
                : CSharpDbOperationOutcome.Failed;
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

        _operationScope?.Dispose();
        _boundaryScope.Dispose();
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

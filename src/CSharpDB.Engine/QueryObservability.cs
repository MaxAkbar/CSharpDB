using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Engine;

/// <summary>
/// Listener-driven query instrumentation owned by one database instance. No
/// per-query state is created until a configured event has a subscriber.
/// </summary>
internal sealed class QueryObservability
{
    private readonly string _databaseAlias;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly TimeSpan _slowQueryThreshold;

    internal QueryObservability(CSharpDbObservabilityOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _databaseAlias = options.DatabaseAlias;
        _queryEventsEnabled = options.Logging.Enabled && options.Logging.Queries;
        _slowQueryEventsEnabled = options.Logging.Enabled && options.Logging.SlowQueries;
        _sqlTextCaptureMode = options.Logging.SqlText;
        _slowQueryThreshold = options.Logging.GetSlowQueryThreshold(CSharpDbOperationClass.Query);
    }

    internal QueryOperation? Start(string? sql, QueryFingerprint? suppliedFingerprint = null)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed)
            return null;

        CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
        bool terminalListenerEnabled = _queryEventsEnabled &&
            (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
             publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
             publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
        bool slowListenerEnabled = _slowQueryEventsEnabled &&
            publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
        if (!terminalListenerEnabled && !slowListenerEnabled)
            return null;

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

        CSharpDbOperationContext context = CreateContext(fingerprint);
        TimeSpan queueDuration = context.OperationClass == CSharpDbOperationClass.Query &&
                                 ReferenceEquals(context, CSharpDbOperationScope.Current)
            ? CSharpDbOperationScope.CurrentQueryQueueDuration
            : TimeSpan.Zero;
        return new QueryOperation(
            context,
            _queryEventsEnabled,
            _slowQueryEventsEnabled,
            _slowQueryThreshold,
            _sqlTextCaptureMode,
            capturedSqlText,
            queueDuration);
    }

    private CSharpDbOperationContext CreateContext(QueryFingerprint? fingerprint)
    {
        CSharpDbOperationContext? ambient = CSharpDbOperationScope.Current;
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
            CSharpDbOperationScope.CurrentTransport,
            _databaseAlias,
            sessionId: CSharpDbOperationScope.CurrentSessionId,
            queryFingerprint: fingerprint);
    }
}

internal sealed class QueryOperation : IQueryResultObserver
{
    private readonly CSharpDbOperationContext _context;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly TimeSpan _slowQueryThreshold;
    private readonly SqlTextCaptureMode _sqlTextCaptureMode;
    private readonly string? _capturedSqlText;
    private readonly TimeSpan _queueDuration;
    private TimeSpan? _timeToFirstResult;
    private int _completed;

    internal QueryOperation(
        CSharpDbOperationContext context,
        bool queryEventsEnabled,
        bool slowQueryEventsEnabled,
        TimeSpan slowQueryThreshold,
        SqlTextCaptureMode sqlTextCaptureMode,
        string? capturedSqlText,
        TimeSpan queueDuration)
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
    }

    internal IDisposable EnterScope()
        => CSharpDbOperationScope.Enter(_context);

    internal QueryResult Observe(QueryResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.IsQuery)
        {
            result.SetObserver(this);
        }
        else
        {
            CompleteSucceeded(rowsProduced: 0, result.RowsAffected);
        }

        return result;
    }

    internal void Fail(Exception exception)
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
        => _timeToFirstResult ??= _context.GetElapsedTime();

    public void OnRowProduced()
    {
    }

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

        try
        {
            TimeSpan totalDuration = _context.GetElapsedTime();
            TimeSpan queueDuration = _queueDuration <= totalDuration
                ? _queueDuration
                : totalDuration;
            TimeSpan executionAndConsumptionDuration = totalDuration - queueDuration;
            DateTimeOffset completedAtUtc = _context.GetUtcNow();
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
    }

    private static SafeErrorProjection ProjectError(Exception exception)
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

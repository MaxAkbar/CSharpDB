using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private CompositeQueryOperation? StartCompositeQueryOperation(
        CSharpDbOperationClass operationClass,
        string? sql = null)
    {
        if (!IsQueryObservationRequested())
            return null;

        try
        {
            CSharpDbLoggingOptions? logging = _directDatabaseOptions.ObservabilityOptions?.Logging;
            SqlTextCaptureMode captureMode = operationClass == CSharpDbOperationClass.Query
                ? logging?.SqlText ?? SqlTextCaptureMode.None
                : SqlTextCaptureMode.None;
            QueryFingerprint? fingerprint = null;
            string? capturedSqlText = null;
            if (operationClass == CSharpDbOperationClass.Query &&
                !string.IsNullOrWhiteSpace(sql))
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
                    capturedSqlText = null;
                }
            }

            CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
            CSharpDbOperationContext context = parent switch
            {
                null when operationClass == CSharpDbOperationClass.Query =>
                    CSharpDbOperationContext.CreateRoot(
                        operationClass,
                        CSharpDbOperationScope.CurrentTransport,
                        _observabilityDatabaseAlias,
                        sessionId: CSharpDbOperationScope.CurrentSessionId,
                        queryFingerprint: fingerprint,
                        timeProvider: _observabilityTimeProvider),
                null => CSharpDbOperationContext.CreateRequest(
                    operationClass,
                    CSharpDbOperationScope.CurrentTransport,
                    _observabilityDatabaseAlias,
                    sessionId: CSharpDbOperationScope.CurrentSessionId,
                    timeProvider: _observabilityTimeProvider),
                _ when operationClass == CSharpDbOperationClass.Query && fingerprint is not null =>
                    CSharpDbOperationContext.CreateStatement(parent, fingerprint),
                _ when operationClass == CSharpDbOperationClass.Query =>
                    CSharpDbOperationContext.CreateStatement(parent),
                _ => CSharpDbOperationContext.CreateRequest(parent, operationClass),
            };
            TimeSpan slowQueryThreshold = operationClass switch
            {
                CSharpDbOperationClass.Query => GetConfiguredSlowQueryThreshold(
                    logging,
                    CSharpDbOperationClass.Query),
                CSharpDbOperationClass.Script => _scriptSlowQueryThreshold,
                CSharpDbOperationClass.Procedure => _procedureSlowQueryThreshold,
                _ => throw new ArgumentOutOfRangeException(nameof(operationClass)),
            };

            return new CompositeQueryOperation(
                context,
                _queryEventsEnabled,
                _slowQueryEventsEnabled,
                slowQueryThreshold,
                captureMode,
                capturedSqlText);
        }
        catch
        {
            return null;
        }
    }

    private sealed class CompositeQueryOperation
    {
        private readonly CSharpDbOperationContext _context;
        private readonly bool _queryEventsEnabled;
        private readonly bool _slowQueryEventsEnabled;
        private readonly TimeSpan _slowQueryThreshold;
        private readonly SqlTextCaptureMode _sqlTextCaptureMode;
        private readonly string? _capturedSqlText;
        private TimeSpan _queueDuration;
        private int _dequeued;
        private int _completed;

        internal CompositeQueryOperation(
            CSharpDbOperationContext context,
            bool queryEventsEnabled,
            bool slowQueryEventsEnabled,
            TimeSpan slowQueryThreshold,
            SqlTextCaptureMode sqlTextCaptureMode,
            string? capturedSqlText)
        {
            _context = context;
            _queryEventsEnabled = queryEventsEnabled;
            _slowQueryEventsEnabled = slowQueryEventsEnabled;
            _slowQueryThreshold = slowQueryThreshold;
            _sqlTextCaptureMode = sqlTextCaptureMode;
            _capturedSqlText = capturedSqlText;
        }

        internal IDisposable EnterScope()
            => CSharpDbOperationScope.Enter(_context);

        internal TimeSpan Elapsed => _context.GetElapsedTime();

        internal void MarkDequeued()
            => MarkDequeued(_context.GetElapsedTime());

        internal void MarkDequeued(TimeSpan queueDuration)
        {
            if (Interlocked.Exchange(ref _dequeued, 1) == 0)
            {
                _queueDuration = queueDuration < TimeSpan.Zero
                    ? TimeSpan.Zero
                    : queueDuration;
            }
        }

        internal IDisposable EnterQueueDurationScope()
            => CSharpDbOperationScope.EnterQueryQueueDuration(_queueDuration);

        internal void Succeed(long rowsProduced, long rowsAffected)
            => Complete(
                CSharpDbOperationOutcome.Succeeded,
                rowsProduced,
                rowsAffected,
                error: null);

        internal void Fail(
            ErrorCode? errorCode,
            long rowsProduced = 0,
            long rowsAffected = 0)
            => Complete(
                CSharpDbOperationOutcome.Failed,
                rowsProduced,
                rowsAffected,
                ProjectError(errorCode));

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
                    : exception is CSharpDbException databaseException
                        ? ProjectError(databaseException.Code)
                        : SafeErrorProjector.Project(exception));
        }

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
                                timeToFirstResult: null,
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
                                timeToFirstResult: null,
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
                                timeToFirstResult: null,
                                queueDuration,
                                executionAndConsumptionDuration,
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
                // Diagnostics must not alter the client operation.
            }
        }

        private static SafeErrorProjection ProjectError(ErrorCode? errorCode)
        {
            SafeErrorKind kind = errorCode switch
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
                _ => SafeErrorKind.ClientTransport,
            };
            return SafeErrorProjector.Project(kind);
        }
    }

    private static long AddDiagnosticCount(long current, long addition)
    {
        if (addition <= 0)
            return current;
        if (current >= long.MaxValue - addition)
            return long.MaxValue;

        return current + addition;
    }
}

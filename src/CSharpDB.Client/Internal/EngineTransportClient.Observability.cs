using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private CompositeQueryOperation? StartCompositeQueryOperation(
        CSharpDbOperationClass operationClass,
        string? sql = null,
        OpaqueDiagnosticsId? diagnosticsSessionId = null)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed)
            return null;

        try
        {
            bool suppressDiagnosticEvents =
                CSharpDbOperationScope.AreDiagnosticEventsSuppressed;
            RuntimeDatabaseFamily runtimeFamily =
                Volatile.Read(ref _runtimeDatabaseFamily);
            CSharpDbRuntimeDiagnosticsState? runtimeState =
                runtimeFamily.RuntimeDiagnosticsState;
            CaptureQueryObservationInterest(
                out bool queryEventsObservedAtStart,
                out bool slowQueryEventsObservedAtStart,
                out bool longRunningQueryEventsObservedAtStart);
            if (suppressDiagnosticEvents)
            {
                queryEventsObservedAtStart = false;
                slowQueryEventsObservedAtStart = false;
                longRunningQueryEventsObservedAtStart = false;
            }
            bool listenerObservationRequested =
                queryEventsObservedAtStart ||
                slowQueryEventsObservedAtStart ||
                longRunningQueryEventsObservedAtStart;
            if (runtimeState?.IsEnabled == false)
            {
                queryEventsObservedAtStart = false;
                slowQueryEventsObservedAtStart = false;
                longRunningQueryEventsObservedAtStart = false;
                listenerObservationRequested = false;
            }

            if (runtimeState?.IsEnabled != true && !listenerObservationRequested)
                return null;

            CSharpDbObservabilityOptions? observability =
                runtimeFamily.DatabaseOptions.ObservabilityOptions;
            CSharpDbLoggingOptions? logging = observability?.Logging;
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
            var transport = CSharpDbOperationScope.CurrentTransport;
            OpaqueDiagnosticsId? sessionId = CSharpDbOperationScope.CurrentSessionId;
            if (transport == CSharpDB.Observability.CSharpDbTransport.Embedded)
            {
                transport = CSharpDB.Observability.CSharpDbTransport.Direct;
                sessionId = diagnosticsSessionId ?? GetOrCreateDiagnosticsSessionId();
            }
            TimeProvider timeProvider = runtimeState?.TimeProvider ??
                _observabilityTimeProvider;
            string databaseAlias = runtimeState?.DatabaseAlias ??
                (CSharpDbObservabilityOptions.IsValidDatabaseAlias(
                    observability?.DatabaseAlias)
                        ? observability!.DatabaseAlias
                        : _observabilityDatabaseAlias);
            CSharpDbOperationContext context = parent switch
            {
                null when operationClass == CSharpDbOperationClass.Query =>
                    CSharpDbOperationContext.CreateRoot(
                        operationClass,
                        transport,
                        databaseAlias,
                        sessionId,
                        queryFingerprint: fingerprint,
                        timeProvider: timeProvider),
                null => CSharpDbOperationContext.CreateRequest(
                    operationClass,
                    transport,
                    databaseAlias,
                    sessionId,
                    timeProvider: timeProvider),
                _ when operationClass == CSharpDbOperationClass.Query =>
                    CSharpDbOperationContext.CreateStatement(
                        parent,
                        fingerprint,
                        timeProvider),
                _ => CSharpDbOperationContext.CreateRequest(
                    parent,
                    operationClass,
                    timeProvider),
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
            QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation = null;
            if (runtimeState?.IsEnabled == true)
            {
                try
                {
                    runtimeOperation = QueryRuntimeDiagnostics
                        .GetOrCreate(runtimeState)
                        .TryStart(
                            context,
                            QueryExecutionPhase.Queued,
                            SqlTextCaptureMode.None,
                            capturedSqlText: null,
                            suppressDiagnosticEvents,
                            longRunningQueryEventsObservedAtStart,
                            out _);
                }
                catch
                {
                    // Runtime history is best-effort. A configured listener
                    // can still observe the operation if the state is being
                    // retired concurrently.
                }
            }

            if (runtimeOperation is null && !listenerObservationRequested)
                return null;

            return new CompositeQueryOperation(
                this,
                context,
                runtimeState,
                runtimeOperation,
                queryEventsObservedAtStart,
                slowQueryEventsObservedAtStart,
                longRunningQueryEventsObservedAtStart,
                slowQueryThreshold,
                captureMode,
                capturedSqlText,
                suppressDiagnosticEvents);
        }
        catch
        {
            return null;
        }
    }

    private sealed class CompositeQueryOperation
    {
        private readonly EngineTransportClient _owner;
        private readonly CSharpDbOperationContext _context;
        private readonly object _runtimeBindingGate = new();
        private readonly QueryRuntimeDiagnostics.QueryRuntimeOperation? _scopeRuntimeOperation;
        private readonly bool _queryEventsEnabled;
        private readonly bool _slowQueryEventsEnabled;
        private readonly CSharpDbQueryEventInterestSnapshot _queryEventInterest;
        private readonly TimeSpan _slowQueryThreshold;
        private readonly SqlTextCaptureMode _sqlTextCaptureMode;
        private readonly string? _capturedSqlText;
        private readonly bool _suppressDiagnosticEvents;
        private TimeSpan _queueDuration;
        private CSharpDbRuntimeDiagnosticsState? _explicitRuntimeState;
        private CSharpDbRuntimeDiagnosticsState? _runtimeState;
        private QueryRuntimeDiagnostics.QueryRuntimeOperation? _runtimeOperation;
        private bool _hasExplicitRuntimeState;
        private int _runtimeBindingVersion;
        private int _dequeued;
        private int _completed;

        internal CompositeQueryOperation(
            EngineTransportClient owner,
            CSharpDbOperationContext context,
            CSharpDbRuntimeDiagnosticsState? runtimeState,
            QueryRuntimeDiagnostics.QueryRuntimeOperation? runtimeOperation,
            bool queryEventsEnabled,
            bool slowQueryEventsEnabled,
            bool longRunningQueryEventsEnabled,
            TimeSpan slowQueryThreshold,
            SqlTextCaptureMode sqlTextCaptureMode,
            string? capturedSqlText,
            bool suppressDiagnosticEvents)
        {
            _owner = owner;
            _context = context;
            _runtimeState = runtimeState;
            _runtimeOperation = runtimeOperation;
            _scopeRuntimeOperation = runtimeOperation;
            _queryEventsEnabled = queryEventsEnabled;
            _slowQueryEventsEnabled = slowQueryEventsEnabled;
            _queryEventInterest = new CSharpDbQueryEventInterestSnapshot(
                queryEventsEnabled,
                slowQueryEventsEnabled,
                longRunningQueryEventsEnabled);
            _slowQueryThreshold = slowQueryThreshold;
            _sqlTextCaptureMode = sqlTextCaptureMode;
            _capturedSqlText = capturedSqlText;
            _suppressDiagnosticEvents = suppressDiagnosticEvents;
        }

        internal IDisposable EnterScope()
            => CSharpDbOperationScope.Enter(
                _context,
                _scopeRuntimeOperation,
                _queryEventInterest);

        internal TimeSpan Elapsed => _context.GetElapsedTime();
        internal OpaqueDiagnosticsId OperationId => _context.OperationId;

        internal void BindRuntimeDiagnosticsState(
            CSharpDbRuntimeDiagnosticsState? runtimeState)
        {
            QueryRuntimeDiagnostics.QueryRuntimeOperation? operation;
            lock (_runtimeBindingGate)
            {
                if (Volatile.Read(ref _completed) != 0)
                    return;

                _explicitRuntimeState = runtimeState;
                _hasExplicitRuntimeState = true;
                operation = ReconcileRuntimeOperationLocked(
                    QueryExecutionPhase.Queued);
            }

            operation?.SetPhase(QueryExecutionPhase.Queued);
        }

        internal void MarkDequeued()
            => MarkDequeued(_context.GetElapsedTime());

        internal void MarkDequeued(TimeSpan queueDuration)
        {
            if (Interlocked.Exchange(ref _dequeued, 1) == 0)
            {
                _queueDuration = queueDuration < TimeSpan.Zero
                    ? TimeSpan.Zero
                    : queueDuration;
                ReconcileRuntimeOperation(
                        QueryExecutionPhase.Planning,
                        allowAfterCompositeCompletion: false)
                    ?.SetPhase(QueryExecutionPhase.Planning);
            }
        }

        internal IDisposable EnterQueueDurationScope()
        {
            IDisposable? reboundOperationScope = null;
            if (Volatile.Read(ref _runtimeBindingVersion) != 0)
            {
                QueryRuntimeDiagnostics.QueryRuntimeOperation? operation =
                    Volatile.Read(ref _runtimeOperation);
                reboundOperationScope = operation is null
                    ? CSharpDbOperationScope.Enter(
                        _context,
                        queryRuntimeOperation: null,
                        _queryEventInterest)
                    : CSharpDbOperationScope.Enter(
                        _context,
                        operation,
                        _queryEventInterest);
            }

            IDisposable queueDurationScope =
                CSharpDbOperationScope.EnterQueryQueueDuration(_queueDuration);
            return reboundOperationScope is null
                ? queueDurationScope
                : new CompositeScope(reboundOperationScope, queueDurationScope);
        }

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
                QueryExecutionPhase terminalPhase = Volatile.Read(ref _dequeued) == 0
                    ? QueryExecutionPhase.Queued
                    : QueryExecutionPhase.Planning;
                ReconcileRuntimeOperation(
                        terminalPhase,
                        allowAfterCompositeCompletion: true)
                    ?.Complete(
                    outcome,
                    completedAtUtc,
                    totalDuration,
                    timeToFirstResult: null,
                    rowsProduced,
                    rowsAffected,
                    error,
                    isSlow: totalDuration >= _slowQueryThreshold);
            }
            catch
            {
                // Runtime history must not alter the client operation.
            }

            try
            {
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

        private QueryRuntimeDiagnostics.QueryRuntimeOperation? ReconcileRuntimeOperation(
            QueryExecutionPhase initialPhase,
            bool allowAfterCompositeCompletion)
        {
            lock (_runtimeBindingGate)
            {
                if (!allowAfterCompositeCompletion &&
                    Volatile.Read(ref _completed) != 0)
                {
                    return _runtimeOperation;
                }

                return ReconcileRuntimeOperationLocked(initialPhase);
            }
        }

        private QueryRuntimeDiagnostics.QueryRuntimeOperation?
            ReconcileRuntimeOperationLocked(QueryExecutionPhase initialPhase)
        {
            while (true)
            {
                CSharpDbRuntimeDiagnosticsState? targetState =
                    _hasExplicitRuntimeState
                        ? _explicitRuntimeState
                        : _owner.CurrentRuntimeDiagnosticsState;
                CSharpDbRuntimeDiagnosticsState? boundState = _runtimeState;
                QueryRuntimeDiagnostics.QueryRuntimeOperation? boundOperation =
                    _runtimeOperation;
                if (ReferenceEquals(targetState, boundState))
                    return boundOperation;

                QueryRuntimeDiagnostics.QueryRuntimeOperation? candidate = null;
                if (targetState?.IsEnabled == true)
                {
                    try
                    {
                        QueryRuntimeDiagnostics targetRegistry =
                            QueryRuntimeDiagnostics.GetOrCreate(targetState);
                        candidate = boundOperation is null
                            ? targetRegistry.TryStart(
                                _context,
                                initialPhase,
                                SqlTextCaptureMode.None,
                                capturedSqlText: null,
                                _suppressDiagnosticEvents,
                                _queryEventInterest.LongRunningQueryEventsEnabled,
                                out _)
                            : boundOperation.RebindTo(
                                targetRegistry,
                                initialPhase);
                    }
                    catch
                    {
                        // If the replacement cannot accept the operation,
                        // retain the previous owner so a pre-handoff terminal
                        // still clears its active entry once.
                        return boundOperation;
                    }

                }
                else
                {
                    boundOperation?.Abandon();
                }

                _runtimeState = targetState;
                Volatile.Write(ref _runtimeOperation, candidate);
                Interlocked.Increment(ref _runtimeBindingVersion);

                CSharpDbRuntimeDiagnosticsState? latestTarget =
                    _hasExplicitRuntimeState
                        ? _explicitRuntimeState
                        : _owner.CurrentRuntimeDiagnosticsState;
                if (!ReferenceEquals(targetState, latestTarget))
                    continue;

                return candidate;
            }
        }

        private sealed class CompositeScope(
            IDisposable operationScope,
            IDisposable queueDurationScope) : IDisposable
        {
            private IDisposable? _operationScope = operationScope;
            private IDisposable? _queueDurationScope = queueDurationScope;

            public void Dispose()
            {
                Interlocked.Exchange(ref _queueDurationScope, null)?.Dispose();
                Interlocked.Exchange(ref _operationScope, null)?.Dispose();
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

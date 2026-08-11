using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Engine;

/// <summary>
/// Creates typed operational lifecycle events only when logging is enabled and
/// the specific event has a listener. All diagnostic work is isolated from the
/// database operation it observes.
/// </summary>
internal static class LifecycleObservability
{
    internal static LifecycleOperation? Start(
        CSharpDbObservabilityOptions? options,
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbOperationClass operationClass)
        => StartCore(options, definition, operationClass, exactContext: null);

    /// <summary>
    /// Starts a lifecycle event for an already-created operation context. This
    /// preserves the exact operation id shared with runtime diagnostics rather
    /// than deriving a child from the ambient scope.
    /// </summary>
    internal static LifecycleOperation? StartExact(
        CSharpDbObservabilityOptions? options,
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.OperationClass != operationClass)
            throw new ArgumentException(
                "The lifecycle operation class must match the supplied context.",
                nameof(context));

        return StartCore(options, definition, operationClass, context);
    }

    private static LifecycleOperation? StartCore(
        CSharpDbObservabilityOptions? options,
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbOperationClass operationClass,
        CSharpDbOperationContext? exactContext)
    {
        if (options is null ||
            !options.Logging.Enabled ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed)
        {
            return null;
        }

        CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
        if (!publisher.IsEnabled(definition))
            return null;

        try
        {
            CSharpDbOperationContext context = exactContext ??
                CreateContext(options, operationClass);

            return new LifecycleOperation(context, definition, publisher);
        }
        catch
        {
            // Context and clock creation are diagnostic work and must not alter
            // the observed operation.
            return null;
        }
    }

    private static CSharpDbOperationContext CreateContext(
        CSharpDbObservabilityOptions options,
        CSharpDbOperationClass operationClass)
    {
        CSharpDbOperationContext? ambient = CSharpDbOperationScope.Current;
        return ambient switch
        {
            null => CSharpDbOperationContext.CreateRoot(
                operationClass,
                CSharpDbOperationScope.CurrentTransport,
                options.DatabaseAlias,
                sessionId: CSharpDbOperationScope.CurrentSessionId),
            _ => CSharpDbOperationContext.CreateRequest(ambient, operationClass),
        };
    }
}

internal sealed class LifecycleOperation
{
    private readonly CSharpDbOperationContext _context;
    private readonly CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> _definition;
    private readonly CSharpDbDiagnosticEventPublisher _publisher;
    private int _completed;

    internal LifecycleOperation(
        CSharpDbOperationContext context,
        CSharpDbLogEventDefinition<CSharpDbLifecycleCompletedEvent> definition,
        CSharpDbDiagnosticEventPublisher publisher)
    {
        _context = context;
        _definition = definition;
        _publisher = publisher;
    }

    internal CSharpDbOperationContext Context => _context;

    internal void Succeed()
        => Complete(CSharpDbOperationOutcome.Succeeded, error: null);

    internal void Reject(SafeErrorKind errorKind)
        => Complete(
            CSharpDbOperationOutcome.Rejected,
            SafeErrorProjector.Project(errorKind));

    internal void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        try
        {
            Complete(
                exception is OperationCanceledException
                    ? CSharpDbOperationOutcome.Canceled
                    : CSharpDbOperationOutcome.Failed,
                ProjectError(exception));
        }
        catch
        {
            // Error projection is diagnostic work and must never replace the
            // original operational result.
        }
    }

    private void Complete(
        CSharpDbOperationOutcome outcome,
        SafeErrorProjection? error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        try
        {
            TimeSpan duration = _context.GetElapsedTime();
            DateTimeOffset completedAtUtc = _context.GetUtcNow();
            _publisher.Publish(
                _definition,
                (_context, completedAtUtc, duration, outcome, error),
                static state => new CSharpDbLifecycleCompletedEvent(
                    state._context,
                    state.completedAtUtc,
                    state.duration,
                    state.outcome,
                    state.error));
        }
        catch
        {
            // Clocks, payload construction, filters, and listeners are all
            // isolated from durability-sensitive work.
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

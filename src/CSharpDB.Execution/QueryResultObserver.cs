namespace CSharpDB.Execution;

/// <summary>
/// Internal query-result lifecycle seam. Callbacks are best-effort: QueryResult
/// swallows observer exceptions so diagnostics cannot change query behavior.
/// </summary>
internal interface IQueryResultObserver
{
    void OnFirstRowProduced();

    void OnDisposing()
    {
    }

    void OnCompleted(QueryResultCompletion completion);
}

/// <summary>
/// Optional per-row callback for observers that need more than the exact row
/// count supplied with the terminal completion. Runtime history only needs the
/// first-row timestamp and terminal count, so it deliberately avoids one
/// virtual callback for every streamed row.
/// </summary>
internal interface IQueryResultRowObserver : IQueryResultObserver
{
    void OnRowProduced();
}

/// <summary>
/// Internal registration owned by QueryResult after observation is attached.
/// Implementations own the exact row-count and terminal handshake. QueryResult
/// invokes these methods only from its lifecycle paths.
/// </summary>
internal interface IQueryResultLifecycleRegistration
{
    bool HasLifecycleStarted { get; }

    bool TryStartDisposal();

    void OnRowProduced();

    void Complete(QueryResultCompletionReason reason, Exception? error);
}

/// <summary>
/// Marker for a trusted, allocation-free lifecycle registration that can be
/// installed directly as a QueryResult execution feature. Implementations are
/// diagnostics-only and must never allow an exception to escape a callback.
/// </summary>
internal interface IQueryResultDirectLifecycleRegistration :
    IQueryResultLifecycleRegistration
{
    /// <summary>
    /// True only after the direct attachment has passed QueryResult's
    /// post-install lifecycle checks. Runtime-scope arbitration waits while
    /// an attached registration is still installing so a losing attachment
    /// can roll back without causing a transient false rejection.
    /// </summary>
    bool IsDirectLifecycleCommitted => true;

    /// <summary>
    /// Completes a pre-materialized synchronous result without routing
    /// through QueryResult's asynchronous resource-disposal path. The default
    /// preserves the ordinary callback contract; trusted runtime-history
    /// registrations may fuse the no-resource terminal handshake.
    /// </summary>
    void CompleteSynchronousResult(
        QueryResultCompletionReason reason,
        long rowsProduced)
    {
        if (reason == QueryResultCompletionReason.Disposed &&
            !TryStartDisposal())
        {
            return;
        }

        Complete(reason, error: null);
    }
}

internal enum QueryResultDirectLifecycleInstallResult
{
    Installed,
    NeedsPromotion,
    TooLateOrConflicting,
}

internal enum QueryResultCompletionReason
{
    Exhausted,
    Disposed,
    Canceled,
    Failed,
}

internal readonly record struct QueryResultCompletion(
    QueryResultCompletionReason Reason,
    long RowsProduced,
    Exception? Error = null);

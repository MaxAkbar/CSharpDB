namespace CSharpDB.Execution;

/// <summary>
/// Internal query-result lifecycle seam. Callbacks are best-effort: QueryResult
/// swallows observer exceptions so diagnostics cannot change query behavior.
/// </summary>
internal interface IQueryResultObserver
{
    void OnFirstRowProduced();

    void OnRowProduced();

    void OnCompleted(QueryResultCompletion completion);
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

using System.Runtime.CompilerServices;

namespace CSharpDB.Execution;

/// <summary>
/// Coarse access-path categories emitted by the execution planner. The engine
/// maps these values to its public diagnostics contracts so the execution
/// assembly remains independent of CSharpDB.Observability.
/// </summary>
internal enum QueryPlanAccessPathCategory
{
    Unknown = 0,
    TableScan,
    PrimaryKeyLookup,
    IndexSeek,
    IndexScan,
    FullTextIndex,
    Temporary,
}

internal enum QueryPlanChangeKind
{
    Unknown = 0,
    CachedPlanReclassified = 1,
    AdaptiveCardinalityReclassified = 2,
    AdaptiveReoptimizationAttempted = 3,
    AdaptiveReoptimized = 4,
    AdaptiveReoptimizationRejected = 5,
}

internal readonly record struct QueryPlanRuntimeSelection(
    QueryPlanAccessPathCategory AccessPath,
    long? EstimatedRows);

/// <summary>
/// Receives planner decisions that have already been made for normal query
/// execution. Callbacks never cause a query replay or an implicit EXPLAIN.
/// </summary>
internal interface IQueryPlanRuntimeObserver
{
    void OnPlanCacheLookup(bool hit);

    void OnAccessPathSelected(in QueryPlanRuntimeSelection selection);

    void OnPlanChanged(QueryPlanChangeKind change);
}

/// <summary>
/// Keeps diagnostics callbacks outside the correctness boundary. Observer
/// implementations are allowed to fail without changing planner or operator
/// behavior.
/// </summary>
internal static class QueryPlanRuntimeObserver
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void PlanCacheLookup(IQueryPlanRuntimeObserver? observer, bool hit)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnPlanCacheLookup(hit);
        }
        catch
        {
            // Diagnostics are best-effort and must never affect query execution.
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void AccessPathSelected(
        IQueryPlanRuntimeObserver? observer,
        in QueryPlanRuntimeSelection selection)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnAccessPathSelected(in selection);
        }
        catch
        {
            // Diagnostics are best-effort and must never affect query execution.
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void PlanChanged(
        IQueryPlanRuntimeObserver? observer,
        QueryPlanChangeKind change)
    {
        if (observer is null)
            return;

        try
        {
            observer.OnPlanChanged(change);
        }
        catch
        {
            // Diagnostics are best-effort and must never affect query execution.
        }
    }
}

using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Engine;

/// <summary>
/// Immutable database-lifetime bridge from planner callbacks to the exact
/// active query lease carried by the nearest operation scope. The bridge is
/// shared by root, transaction, and reader-session planners and is never
/// replaced for an individual query.
/// </summary>
internal sealed class QueryPlanRuntimeDiagnosticsAdapter(
    QueryRuntimeDiagnostics runtimeDiagnostics) : IQueryPlanRuntimeObserver
{
    private readonly QueryRuntimeDiagnostics _runtimeDiagnostics =
        runtimeDiagnostics ?? throw new ArgumentNullException(nameof(runtimeDiagnostics));

    public void OnPlanCacheLookup(bool hit)
        => ResolveCurrentOperation()?.RecordPlanCacheLookup(hit);

    public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection)
        => ResolveCurrentOperation()?.RecordAccessPath(
            selection.AccessPath,
            selection.EstimatedRows);

    public void OnPlanChanged(QueryPlanChangeKind change)
        => ResolveCurrentOperation()?.RecordPlanChange(change);

    private QueryRuntimeDiagnostics.QueryRuntimeOperation? ResolveCurrentOperation()
    {
        // The first operation frame is authoritative. Do not walk through a
        // distinct nested operation and accidentally attribute its physical
        // planner work to a parent query.
        CSharpDbQueryRuntimeBinding binding =
            CSharpDbOperationScope.CaptureQueryRuntimeBinding();
        CSharpDbOperationContext? context = binding.Operation;
        if (context is null || context.OperationClass != CSharpDbOperationClass.Query)
            return null;

        return binding.RuntimeOperation is
                   QueryRuntimeDiagnostics.QueryRuntimeOperation operation &&
               operation.Matches(_runtimeDiagnostics, context)
            ? operation
            : null;
    }
}

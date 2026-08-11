using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal enum AdaptiveQueryReoptimizationFallbackReason
{
    None = 0,
    MaxBufferedRows,
    ReoptimizationLimit,
    Unsupported,
}

internal sealed class AdaptiveQueryExecutionLease
{
    private int _remainingReoptimizations;
    private int _requiresRuntimeExecutionScope;

    public AdaptiveQueryExecutionLease(AdaptiveQueryReoptimizationOptions options)
    {
        Options = options;
        _remainingReoptimizations = options.MaxReoptimizationsPerQuery;
    }

    public AdaptiveQueryReoptimizationOptions Options { get; }

    internal bool RequiresRuntimeExecutionScope
        => Volatile.Read(ref _requiresRuntimeExecutionScope) != 0;

    internal void RequireRuntimeExecutionScope()
        => Volatile.Write(ref _requiresRuntimeExecutionScope, 1);

    public bool TryConsumeReoptimization()
    {
        while (true)
        {
            int current = Volatile.Read(ref _remainingReoptimizations);
            if (current <= 0)
                return false;

            if (Interlocked.CompareExchange(ref _remainingReoptimizations, current - 1, current) == current)
                return true;
        }
    }
}

internal sealed class AdaptiveQueryReoptimizationRuntimeDiagnostics
{
    private readonly Action _recordAttempt;
    private readonly Action _recordSuccessfulSwitch;
    private readonly Action<AdaptiveQueryReoptimizationFallbackReason> _recordRejectedSwitch;
    private readonly Action _recordDivergence;
    private readonly Action<long> _recordBufferedRows;
    private IQueryPlanRuntimeObserver? _queryPlanRuntimeObserver;

    public AdaptiveQueryReoptimizationRuntimeDiagnostics(
        Action recordAttempt,
        Action recordSuccessfulSwitch,
        Action<AdaptiveQueryReoptimizationFallbackReason> recordRejectedSwitch,
        Action recordDivergence,
        Action<long> recordBufferedRows)
    {
        _recordAttempt = recordAttempt;
        _recordSuccessfulSwitch = recordSuccessfulSwitch;
        _recordRejectedSwitch = recordRejectedSwitch;
        _recordDivergence = recordDivergence;
        _recordBufferedRows = recordBufferedRows;
    }

    public IQueryPlanRuntimeObserver? RuntimeObserver
    {
        get => Volatile.Read(ref _queryPlanRuntimeObserver);
        set => Volatile.Write(ref _queryPlanRuntimeObserver, value);
    }

    public void RecordAttempt()
    {
        _recordAttempt();
        QueryPlanRuntimeObserver.PlanChanged(
            RuntimeObserver,
            QueryPlanChangeKind.AdaptiveReoptimizationAttempted);
    }

    public void RecordSuccessfulSwitch()
    {
        _recordSuccessfulSwitch();
        QueryPlanRuntimeObserver.PlanChanged(
            RuntimeObserver,
            QueryPlanChangeKind.AdaptiveReoptimized);
    }

    public void RecordRejectedSwitch(AdaptiveQueryReoptimizationFallbackReason reason)
    {
        _recordRejectedSwitch(reason);
        QueryPlanRuntimeObserver.PlanChanged(
            RuntimeObserver,
            QueryPlanChangeKind.AdaptiveReoptimizationRejected);
    }

    public void RecordDivergence()
    {
        _recordDivergence();
        QueryPlanRuntimeObserver.PlanChanged(
            RuntimeObserver,
            QueryPlanChangeKind.AdaptiveCardinalityReclassified);
    }

    public void RecordBufferedRows(long count) => _recordBufferedRows(count);
}

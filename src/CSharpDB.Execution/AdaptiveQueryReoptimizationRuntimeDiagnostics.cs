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

    public AdaptiveQueryExecutionLease(AdaptiveQueryReoptimizationOptions options)
    {
        Options = options;
        _remainingReoptimizations = options.MaxReoptimizationsPerQuery;
    }

    public AdaptiveQueryReoptimizationOptions Options { get; }

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

    public void RecordAttempt() => _recordAttempt();
    public void RecordSuccessfulSwitch() => _recordSuccessfulSwitch();
    public void RecordRejectedSwitch(AdaptiveQueryReoptimizationFallbackReason reason) => _recordRejectedSwitch(reason);
    public void RecordDivergence() => _recordDivergence();
    public void RecordBufferedRows(long count) => _recordBufferedRows(count);
}

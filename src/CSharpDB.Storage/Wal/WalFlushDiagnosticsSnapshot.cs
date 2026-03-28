namespace CSharpDB.Storage.Wal;

/// <summary>
/// Cumulative WAL flush diagnostics used by benchmarks and tests.
/// </summary>
public readonly record struct WalFlushDiagnosticsSnapshot(
    long FlushCount,
    long FlushedCommitCount,
    long FlushedByteCount,
    long BatchWindowWaitCount,
    long BatchWindowThresholdBypassCount,
    long PreallocationCount,
    long PreallocatedByteCount)
{
    public static WalFlushDiagnosticsSnapshot Empty { get; } = new();
}

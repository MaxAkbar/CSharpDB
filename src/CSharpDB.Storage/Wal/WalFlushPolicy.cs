using CSharpDB.Storage.StorageEngine;
using Microsoft.Win32.SafeHandles;

namespace CSharpDB.Storage.Wal;

internal interface IWalFlushPolicy
{
    bool AllowsWriteConcurrencyDuringCommitFlush { get; }
    ValueTask FlushBufferedWritesAsync(SafeFileHandle handle, CancellationToken cancellationToken);
    ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken);
}

internal static class WalFlushPolicy
{
    public static IWalFlushPolicy Create(DurabilityMode mode)
    {
        return mode switch
        {
            DurabilityMode.Buffered => BufferedWalFlushPolicy.Instance,
            DurabilityMode.Durable => DurableWalFlushPolicy.Instance,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unsupported durability mode."),
        };
    }
}

internal sealed class BufferedWalFlushPolicy : IWalFlushPolicy
{
    public static BufferedWalFlushPolicy Instance { get; } = new();
    public bool AllowsWriteConcurrencyDuringCommitFlush => false;

    private BufferedWalFlushPolicy()
    {
    }

    public ValueTask FlushBufferedWritesAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }
}

internal sealed class DurableWalFlushPolicy : IWalFlushPolicy
{
    public static DurableWalFlushPolicy Instance { get; } = new();
    public bool AllowsWriteConcurrencyDuringCommitFlush => true;

    private DurableWalFlushPolicy()
    {
    }

    public ValueTask FlushBufferedWritesAsync(SafeFileHandle handle, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    public ValueTask FlushCommitAsync(SafeFileHandle handle, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RandomAccess.FlushToDisk(handle);
        return ValueTask.CompletedTask;
    }
}

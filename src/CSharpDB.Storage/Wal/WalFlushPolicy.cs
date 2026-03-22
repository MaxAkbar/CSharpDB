using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Storage.Wal;

internal interface IWalFlushPolicy
{
    bool AllowsWriteConcurrencyDuringCommitFlush { get; }
    ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken);
    ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken);
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

    public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
        => new(stream.FlushAsync(cancellationToken));
}

internal sealed class DurableWalFlushPolicy : IWalFlushPolicy
{
    public static DurableWalFlushPolicy Instance { get; } = new();
    public bool AllowsWriteConcurrencyDuringCommitFlush => true;

    private DurableWalFlushPolicy()
    {
    }

    public ValueTask FlushBufferedWritesAsync(FileStream stream, CancellationToken cancellationToken)
        => new(stream.FlushAsync(cancellationToken));

    public ValueTask FlushCommitAsync(FileStream stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        RandomAccess.FlushToDisk(stream.SafeFileHandle);
        return ValueTask.CompletedTask;
    }
}

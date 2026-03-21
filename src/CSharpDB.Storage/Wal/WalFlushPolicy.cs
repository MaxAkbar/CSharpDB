using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Storage.Wal;

internal interface IWalFlushPolicy
{
    ValueTask FlushAsync(FileStream stream, CancellationToken cancellationToken);
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

    private BufferedWalFlushPolicy()
    {
    }

    public ValueTask FlushAsync(FileStream stream, CancellationToken cancellationToken)
        => new(stream.FlushAsync(cancellationToken));
}

internal sealed class DurableWalFlushPolicy : IWalFlushPolicy
{
    public static DurableWalFlushPolicy Instance { get; } = new();

    private DurableWalFlushPolicy()
    {
    }

    public ValueTask FlushAsync(FileStream stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        stream.Flush(flushToDisk: true);
        return ValueTask.CompletedTask;
    }
}

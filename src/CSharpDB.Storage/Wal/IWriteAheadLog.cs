namespace CSharpDB.Storage.Wal;

/// <summary>
/// Abstraction over write-ahead logging operations used by Pager.
/// </summary>
public interface IWriteAheadLog : IAsyncDisposable
{
    ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default);
    void BeginTransaction();
    ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken cancellationToken = default);
    ValueTask CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default);
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);
    ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default);
    ValueTask ReadPageIntoAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken = default);
    ValueTask CheckpointAsync(IStorageDevice device, uint pageCount, CancellationToken cancellationToken = default);
    ValueTask CloseAndDeleteAsync();
}

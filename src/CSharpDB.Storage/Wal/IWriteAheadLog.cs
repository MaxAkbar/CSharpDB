namespace CSharpDB.Storage.Wal;

/// <summary>
/// Abstraction over write-ahead logging operations used by Pager.
/// </summary>
public interface IWriteAheadLog : IAsyncDisposable
{
    ValueTask OpenAsync(uint currentDbPageCount, CancellationToken ct = default);
    void BeginTransaction();
    ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken ct = default);
    ValueTask CommitAsync(uint newDbPageCount, CancellationToken ct = default);
    ValueTask RollbackAsync(CancellationToken ct = default);
    ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken ct = default);
    ValueTask CheckpointAsync(IStorageDevice device, uint pageCount, CancellationToken ct = default);
    ValueTask CloseAndDeleteAsync();
}

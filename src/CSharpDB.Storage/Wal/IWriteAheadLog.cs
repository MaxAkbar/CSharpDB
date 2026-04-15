namespace CSharpDB.Storage.Wal;

/// <summary>
/// Abstraction over write-ahead logging operations used by Pager.
/// </summary>
public interface IWriteAheadLog : IAsyncDisposable
{
    bool HasPendingCheckpoint { get; }
    bool IsCheckpointCopyComplete { get; }
    bool HasPendingCommitWork { get; }
    bool IsOpen { get; }
    bool TryGetCheckpointRetainedWalStartOffset(out long walOffset);
    ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default);
    void BeginTransaction();
    ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken cancellationToken = default);
    ValueTask AppendFramesAsync(ReadOnlyMemory<WalFrameWrite> frames, CancellationToken cancellationToken = default);
    ValueTask<WalCommitResult> AppendFramesAndCommitAsync(ReadOnlyMemory<WalFrameWrite> frames, uint newDbPageCount, CancellationToken cancellationToken = default);
    ValueTask<WalCommitResult> CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default);
    ValueTask RollbackAsync(CancellationToken cancellationToken = default);
    ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default);
    ValueTask ReadPageIntoAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken = default);
    ValueTask<bool> CheckpointStepAsync(
        IStorageDevice device,
        uint pageCount,
        int maxPages,
        CancellationToken cancellationToken = default,
        bool allowFinalize = true);
    ValueTask CheckpointAsync(
        IStorageDevice device,
        uint pageCount,
        CancellationToken cancellationToken = default,
        bool allowFinalize = true);
    ValueTask CloseAndDeleteAsync();
}

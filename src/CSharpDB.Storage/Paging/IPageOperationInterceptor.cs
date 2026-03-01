namespace CSharpDB.Storage.Paging;

/// <summary>
/// Intercepts page/transaction lifecycle events for diagnostics, testing, and fault injection.
/// </summary>
public interface IPageOperationInterceptor
{
    ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default);
    ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default);
    ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default);
    ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default);
    ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default);
    ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default);
    ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default);
    ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default);
    ValueTask OnRecoveryStartAsync(CancellationToken ct = default);
    ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default);
}

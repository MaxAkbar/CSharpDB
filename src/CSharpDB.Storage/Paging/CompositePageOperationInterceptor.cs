namespace CSharpDB.Storage.Paging;

/// <summary>
/// Dispatches lifecycle notifications to multiple interceptors in registration order.
/// </summary>
internal sealed class CompositePageOperationInterceptor : IPageOperationInterceptor
{
    private readonly IReadOnlyList<IPageOperationInterceptor> _interceptors;

    public CompositePageOperationInterceptor(IReadOnlyList<IPageOperationInterceptor> interceptors)
    {
        _interceptors = interceptors;
    }

    public async ValueTask OnBeforeReadAsync(uint pageId, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnBeforeReadAsync(pageId, ct);
    }

    public async ValueTask OnAfterReadAsync(uint pageId, PageReadSource source, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnAfterReadAsync(pageId, source, ct);
    }

    public async ValueTask OnBeforeWriteAsync(uint pageId, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnBeforeWriteAsync(pageId, ct);
    }

    public async ValueTask OnAfterWriteAsync(uint pageId, bool succeeded, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnAfterWriteAsync(pageId, succeeded, ct);
    }

    public async ValueTask OnCommitStartAsync(int dirtyPageCount, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnCommitStartAsync(dirtyPageCount, ct);
    }

    public async ValueTask OnCommitEndAsync(int dirtyPageCount, bool succeeded, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnCommitEndAsync(dirtyPageCount, succeeded, ct);
    }

    public async ValueTask OnCheckpointStartAsync(int committedFrameCount, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnCheckpointStartAsync(committedFrameCount, ct);
    }

    public async ValueTask OnCheckpointEndAsync(int committedFrameCount, bool succeeded, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnCheckpointEndAsync(committedFrameCount, succeeded, ct);
    }

    public async ValueTask OnRecoveryStartAsync(CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnRecoveryStartAsync(ct);
    }

    public async ValueTask OnRecoveryEndAsync(bool succeeded, CancellationToken ct = default)
    {
        for (int i = 0; i < _interceptors.Count; i++)
            await _interceptors[i].OnRecoveryEndAsync(succeeded, ct);
    }
}

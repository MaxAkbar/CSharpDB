namespace CSharpDB.Storage.Paging;

/// <summary>
/// Handles page allocation and freelist reuse.
/// </summary>
internal sealed class PageAllocator : IPageAllocator
{
    private readonly PageBufferManager _buffers;
    private readonly Func<uint> _getFreelistHead;
    private readonly Action<uint> _setFreelistHead;
    private readonly Func<uint> _getPageCount;
    private readonly Action<uint> _setPageCount;
    private readonly Func<uint, CancellationToken, ValueTask<byte[]>> _getPageAsync;
    private readonly Func<uint, CancellationToken, ValueTask> _markDirtyAsync;
    private readonly Func<bool> _isSnapshotReader;

    public PageAllocator(
        PageBufferManager buffers,
        Func<uint> getFreelistHead,
        Action<uint> setFreelistHead,
        Func<uint> getPageCount,
        Action<uint> setPageCount,
        Func<uint, CancellationToken, ValueTask<byte[]>> getPageAsync,
        Func<uint, CancellationToken, ValueTask> markDirtyAsync,
        Func<bool> isSnapshotReader)
    {
        _buffers = buffers;
        _getFreelistHead = getFreelistHead;
        _setFreelistHead = setFreelistHead;
        _getPageCount = getPageCount;
        _setPageCount = setPageCount;
        _getPageAsync = getPageAsync;
        _markDirtyAsync = markDirtyAsync;
        _isSnapshotReader = isSnapshotReader;
    }

    public async ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader())
            throw new InvalidOperationException("Cannot allocate pages on a read-only snapshot pager.");

        uint freelistHead = _getFreelistHead();
        if (freelistHead != PageConstants.NullPageId)
        {
            uint pageId = freelistHead;
            var freePage = await _getPageAsync(pageId, ct);
            _setFreelistHead(BitConverter.ToUInt32(freePage, 0));
            Array.Clear(freePage);
            await _markDirtyAsync(pageId, ct);
            return pageId;
        }

        uint newPageId = _getPageCount();
        _setPageCount(newPageId + 1);
        var newPage = new byte[PageConstants.PageSize];
        _buffers.SetCached(newPageId, newPage);
        await _markDirtyAsync(newPageId, ct);
        return newPageId;
    }

    public async ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
    {
        if (_isSnapshotReader())
            throw new InvalidOperationException("Cannot free pages on a read-only snapshot pager.");

        var page = await _getPageAsync(pageId, ct);
        await _markDirtyAsync(pageId, ct);
        Array.Clear(page);
        BitConverter.TryWriteBytes(page.AsSpan(), _getFreelistHead());
        page[PageConstants.ContentOffset(pageId)] = PageConstants.PageTypeFreelist;
        _setFreelistHead(pageId);
    }
}

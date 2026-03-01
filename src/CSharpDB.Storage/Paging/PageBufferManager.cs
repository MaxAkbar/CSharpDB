using CSharpDB.Core;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Manages page cache, WAL/file read routing, and dirty-page tracking.
/// </summary>
internal sealed class PageBufferManager
{
    private readonly IPageCache _cache;
    private readonly IWriteAheadLog _wal;
    private readonly WalIndex _walIndex;
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;
    private readonly IPageOperationInterceptor _interceptor;
    private readonly HashSet<uint> _dirtyPages = new();

    public PageBufferManager(
        IPageCache cache,
        IWriteAheadLog wal,
        WalIndex walIndex,
        WalSnapshot? readerSnapshot,
        bool isSnapshotReader,
        IPageOperationInterceptor interceptor)
    {
        _cache = cache;
        _wal = wal;
        _walIndex = walIndex;
        _readerSnapshot = readerSnapshot;
        _isSnapshotReader = isSnapshotReader;
        _interceptor = interceptor;
    }

    public IReadOnlyCollection<uint> DirtyPages => _dirtyPages;

    public byte[]? TryGetCachedPage(uint pageId)
    {
        _cache.TryGet(pageId, out var page);
        return page;
    }

    public bool TryGetDirtyPage(uint pageId, out byte[] page) => _cache.TryGet(pageId, out page);

    public async ValueTask<byte[]> GetPageAsync(IStorageDevice device, uint pageId, CancellationToken ct = default)
    {
        await _interceptor.OnBeforeReadAsync(pageId, ct);

        if (_cache.TryGet(pageId, out var cached))
        {
            await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return cached;
        }

        if (_isSnapshotReader && _readerSnapshot != null)
        {
            if (_readerSnapshot.TryGet(pageId, out long walOffset))
            {
                var walPage = await _wal.ReadPageAsync(walOffset, ct);
                _cache.Set(pageId, walPage);
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
                return walPage;
            }
        }
        else if (_walIndex.TryGetLatest(pageId, out long latestOffset))
        {
            var walPage = await _wal.ReadPageAsync(latestOffset, ct);
            _cache.Set(pageId, walPage);
            await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalLatest, ct);
            return walPage;
        }

        var buffer = new byte[PageConstants.PageSize];
        await device.ReadAsync((long)pageId * PageConstants.PageSize, buffer, ct);
        _cache.Set(pageId, buffer);
        await _interceptor.OnAfterReadAsync(pageId, PageReadSource.StorageDevice, ct);
        return buffer;
    }

    public ValueTask MarkDirtyAsync(
        uint pageId,
        bool inTransaction,
        Func<uint, CancellationToken, ValueTask<byte[]>> getPageAsync,
        CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot modify pages on a read-only snapshot pager.");
        if (!inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Cannot mark pages dirty outside a transaction.");

        _dirtyPages.Add(pageId);
        if (!_cache.Contains(pageId))
            return EnsurePageInCacheAsync(pageId, getPageAsync, ct);

        return ValueTask.CompletedTask;
    }

    public void AddDirty(uint pageId) => _dirtyPages.Add(pageId);

    public void SetCached(uint pageId, byte[] page) => _cache.Set(pageId, page);

    public void ClearDirty() => _dirtyPages.Clear();

    public void ClearAll()
    {
        _dirtyPages.Clear();
        _cache.Clear();
    }

    public void ClearCache() => _cache.Clear();

    private async ValueTask EnsurePageInCacheAsync(
        uint pageId,
        Func<uint, CancellationToken, ValueTask<byte[]>> getPageAsync,
        CancellationToken ct)
    {
        _dirtyPages.Add(pageId);
        if (!_cache.Contains(pageId))
            await getPageAsync(pageId, ct);
    }
}

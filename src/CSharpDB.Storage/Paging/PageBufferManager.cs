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
    private readonly bool _hasInterceptor;
    private readonly HashSet<uint> _dirtyPages = new();
    private readonly Dictionary<uint, byte[]> _dirtyBuffers = new();

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
        _hasInterceptor = interceptor is not NoOpPageOperationInterceptor;
    }

    internal bool HasInterceptor => _hasInterceptor;

    public IReadOnlyCollection<uint> DirtyPages => _dirtyPages;

    public byte[]? TryGetCachedPage(uint pageId)
    {
        _cache.TryGet(pageId, out var page);
        return page;
    }

    public bool TryGetDirtyPage(uint pageId, out byte[] page)
    {
        if (_dirtyBuffers.TryGetValue(pageId, out page!))
            return true;
        return _cache.TryGet(pageId, out page);
    }

    public ValueTask<byte[]> GetPageAsync(IStorageDevice device, uint pageId, CancellationToken ct = default)
    {
        // Fast path: no interceptor + cache hit = zero async overhead
        if (!_hasInterceptor && _cache.TryGet(pageId, out var fastCached))
            return new ValueTask<byte[]>(fastCached);

        return GetPageCoreAsync(device, pageId, ct);
    }

    private async ValueTask<byte[]> GetPageCoreAsync(IStorageDevice device, uint pageId, CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        if (_cache.TryGet(pageId, out var cached))
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return cached;
        }

        if (_isSnapshotReader && _readerSnapshot != null)
        {
            if (_readerSnapshot.TryGet(pageId, out long walOffset))
            {
                var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
                await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
                _cache.Set(pageId, walPage);
                if (_hasInterceptor)
                    await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
                return walPage;
            }
        }
        else if (_walIndex.TryGetLatest(pageId, out long latestOffset))
        {
            var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(latestOffset, walPage, ct);
            _cache.Set(pageId, walPage);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalLatest, ct);
            return walPage;
        }

        var buffer = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        await device.ReadAsync((long)pageId * PageConstants.PageSize, buffer, ct);
        _cache.Set(pageId, buffer);
        if (_hasInterceptor)
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

        // Pin the page buffer so it survives LRU cache eviction before commit.
        if (_cache.TryGet(pageId, out var buffer))
        {
            _dirtyBuffers.TryAdd(pageId, buffer);
            return ValueTask.CompletedTask;
        }

        return EnsurePageInCacheAndPinAsync(pageId, getPageAsync, ct);
    }

    public void AddDirty(uint pageId)
    {
        _dirtyPages.Add(pageId);
        if (_cache.TryGet(pageId, out var buffer))
            _dirtyBuffers.TryAdd(pageId, buffer);
    }

    public void SetCached(uint pageId, byte[] page) => _cache.Set(pageId, page);

    public void ClearDirty()
    {
        _dirtyPages.Clear();
        _dirtyBuffers.Clear();
    }

    public void ClearAll()
    {
        _dirtyPages.Clear();
        _dirtyBuffers.Clear();
        _cache.Clear();
    }

    public void ClearCache() => _cache.Clear();

    private async ValueTask EnsurePageInCacheAndPinAsync(
        uint pageId,
        Func<uint, CancellationToken, ValueTask<byte[]>> getPageAsync,
        CancellationToken ct)
    {
        var page = await getPageAsync(pageId, ct);
        _dirtyBuffers.TryAdd(pageId, page);
    }
}

using CSharpDB.Primitives;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Manages page cache, WAL/file read routing, and dirty-page tracking.
/// </summary>
internal sealed class PageBufferManager
{
    private readonly IPageCache _cache;
    private readonly bool _useEvictionDrivenDirtyBufferTracking;
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
        _useEvictionDrivenDirtyBufferTracking = cache is IPageCacheEvictionEvents;

        if (cache is IPageCacheEvictionEvents evictionEvents)
            evictionEvents.PageEvicted += OnCachePageEvicted;
    }

    internal bool HasInterceptor => _hasInterceptor;

    public IReadOnlyCollection<uint> DirtyPages => _dirtyPages;

    public byte[]? TryGetCachedPage(uint pageId)
    {
        if (_cache.TryGet(pageId, out var page))
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            return page;
        }

        // Dirty pages can outlive bounded-cache eviction until commit.
        if (_dirtyBuffers.Count != 0 && _dirtyBuffers.Remove(pageId, out page!))
        {
            _cache.Set(pageId, page);
            return page;
        }

        return null;
    }

    public bool TryGetDirtyPage(uint pageId, out byte[] page)
    {
        // Prefer the cache if present; it may contain a newer buffer than an older pinned/evicted entry.
        if (_cache.TryGet(pageId, out page))
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            return true;
        }

        if (_dirtyBuffers.TryGetValue(pageId, out page!))
            return true;

        return false;
    }

    public ValueTask<byte[]> GetPageAsync(IStorageDevice device, uint pageId, CancellationToken ct = default)
    {
        // Fast path: no interceptor + cache hit = zero async overhead
        if (!_hasInterceptor && _cache.TryGet(pageId, out var fastCached))
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            return new ValueTask<byte[]>(fastCached);
        }

        return GetPageCoreAsync(device, pageId, ct);
    }

    private async ValueTask<byte[]> GetPageCoreAsync(IStorageDevice device, uint pageId, CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        if (_cache.TryGet(pageId, out var cached))
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return cached;
        }

        if (_dirtyBuffers.Count != 0 && _dirtyBuffers.Remove(pageId, out var dirtyBuffer))
        {
            _cache.Set(pageId, dirtyBuffer);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return dirtyBuffer;
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

        if (_cache.TryGet(pageId, out var buffer))
        {
            if (_useEvictionDrivenDirtyBufferTracking)
                _dirtyBuffers.Remove(pageId);
            else
                PinDirtyBuffer(pageId, buffer);
            return ValueTask.CompletedTask;
        }

        if (_dirtyBuffers.TryGetValue(pageId, out _))
            return ValueTask.CompletedTask;

        return EnsurePageInCacheAndPinAsync(pageId, getPageAsync, ct);
    }

    public void AddDirty(uint pageId)
    {
        _dirtyPages.Add(pageId);
        if (_useEvictionDrivenDirtyBufferTracking)
            return;

        if (_cache.TryGet(pageId, out var buffer))
            PinDirtyBuffer(pageId, buffer);
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
        if (!_useEvictionDrivenDirtyBufferTracking)
            PinDirtyBuffer(pageId, page);
    }

    private void PinDirtyBuffer(uint pageId, byte[] buffer)
    {
        if (_dirtyBuffers.TryGetValue(pageId, out var existing) && ReferenceEquals(existing, buffer))
            return;

        _dirtyBuffers[pageId] = buffer;
    }

    private void OnCachePageEvicted(uint pageId, byte[] buffer)
    {
        if (!_useEvictionDrivenDirtyBufferTracking || !_dirtyPages.Contains(pageId))
            return;

        _dirtyBuffers[pageId] = buffer;
    }
}

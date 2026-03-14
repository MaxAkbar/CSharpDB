using CSharpDB.Primitives;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Manages page cache, WAL/file read routing, and dirty-page tracking.
/// </summary>
internal sealed class PageBufferManager
{
    private static readonly byte[] ReadOnlyCacheSentinel = new byte[0];

    private enum CachedPageKind
    {
        None,
        Owned,
        ReadOnly,
    }

    private readonly IPageCache _cache;
    private readonly IPageReadProvider _pageReads;
    private readonly IPageReadProvider _speculativePageReads;
    private readonly WalReadCache? _walReadCache;
    private readonly bool _useEvictionDrivenDirtyBufferTracking;
    private readonly IWriteAheadLog _wal;
    private readonly WalIndex _walIndex;
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;
    private readonly IPageOperationInterceptor _interceptor;
    private readonly bool _hasInterceptor;
    private readonly HashSet<uint> _dirtyPages = new();
    private readonly Dictionary<uint, byte[]> _dirtyBuffers = new();
    private readonly Dictionary<uint, PageReadBuffer> _readOnlyPages = new();

    public PageBufferManager(
        IPageCache cache,
        IPageReadProvider pageReads,
        IPageReadProvider speculativePageReads,
        int maxCachedWalReadPages,
        IWriteAheadLog wal,
        WalIndex walIndex,
        WalSnapshot? readerSnapshot,
        bool isSnapshotReader,
        IPageOperationInterceptor interceptor)
    {
        _cache = cache;
        _pageReads = pageReads;
        _speculativePageReads = speculativePageReads;
        _walReadCache = maxCachedWalReadPages > 0 ? new WalReadCache(maxCachedWalReadPages) : null;
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
        var cachedKind = TryGetCachedEntry(pageId, out var page, out var readOnlyPage);
        if (cachedKind == CachedPageKind.Owned)
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            return page;
        }

        if (cachedKind == CachedPageKind.ReadOnly)
        {
            _readOnlyPages.Remove(pageId);
            page = readOnlyPage.MaterializeOwnedBuffer();
            _cache.Set(pageId, page);
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

    public bool TryGetCachedPageReadBuffer(uint pageId, out PageReadBuffer page)
    {
        var cachedKind = TryGetCachedEntry(pageId, out var cached, out var readOnlyPage);
        if (cachedKind == CachedPageKind.Owned)
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);

            page = PageReadBuffer.FromOwnedBuffer(cached);
            return true;
        }

        if (cachedKind == CachedPageKind.ReadOnly)
        {
            page = readOnlyPage;
            return true;
        }

        if (_dirtyBuffers.TryGetValue(pageId, out var dirty))
        {
            page = PageReadBuffer.FromOwnedBuffer(dirty);
            return true;
        }

        if (TryGetCachedWalPage(pageId, out page))
            return true;

        page = default;
        return false;
    }

    public bool TryGetSnapshotCachedPageReadBuffer(uint pageId, WalSnapshot snapshot, out PageReadBuffer page)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        if (snapshot.TryGet(pageId, out long walOffset))
        {
            if (_walReadCache is not null && _walReadCache.TryGet(walOffset, out page))
                return true;

            page = default;
            return false;
        }

        if (!CanUseSnapshotSharedMainFileCache(snapshot, pageId))
        {
            page = default;
            return false;
        }

        var cachedKind = TryGetCachedEntry(pageId, out var cached, out var readOnlyPage);
        if (cachedKind == CachedPageKind.Owned)
        {
            page = PageReadBuffer.FromOwnedBuffer(cached);
            return true;
        }

        if (cachedKind == CachedPageKind.ReadOnly)
        {
            page = readOnlyPage;
            return true;
        }

        page = default;
        return false;
    }

    public bool TryGetDirtyPage(uint pageId, out byte[] page)
    {
        // Prefer the cache if present; it may contain a newer buffer than an older pinned/evicted entry.
        if (TryGetCachedEntry(pageId, out page, out _) == CachedPageKind.Owned)
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            return true;
        }

        if (_dirtyBuffers.TryGetValue(pageId, out page!))
            return true;

        return false;
    }

    public ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        // Fast path: no interceptor + cache hit = zero async overhead
        if (!_hasInterceptor)
        {
            var cachedKind = TryGetCachedEntry(pageId, out var fastCached, out var fastReadOnly);
            if (cachedKind == CachedPageKind.Owned)
            {
                if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                    _dirtyBuffers.Remove(pageId);
                return new ValueTask<byte[]>(fastCached);
            }

            if (cachedKind == CachedPageKind.ReadOnly)
            {
                _readOnlyPages.Remove(pageId);
                var materialized = fastReadOnly.MaterializeOwnedBuffer();
                _cache.Set(pageId, materialized);
                return new ValueTask<byte[]>(materialized);
            }
        }

        if (!_hasInterceptor && _dirtyBuffers.Count != 0 && _dirtyBuffers.Remove(pageId, out var fastDirty))
        {
            _cache.Set(pageId, fastDirty);
            return new ValueTask<byte[]>(fastDirty);
        }

        return GetPageCoreAsync(pageId, ct);
    }

    public ValueTask<PageReadBuffer> GetPageReadAsync(uint pageId, CancellationToken ct = default)
    {
        if (!_hasInterceptor && TryGetCachedPageReadBuffer(pageId, out var fastCached))
            return new ValueTask<PageReadBuffer>(fastCached);

        return GetPageReadCoreAsync(pageId, ct);
    }

    public ValueTask<PageReadBuffer> GetSnapshotPageReadAsync(
        uint pageId,
        WalSnapshot snapshot,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        if (!_hasInterceptor && TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out var fastCached))
            return new ValueTask<PageReadBuffer>(fastCached);

        return GetSnapshotPageReadCoreAsync(pageId, snapshot, ct);
    }

    public async ValueTask<PageReadBuffer> ReadPageUncachedAsync(uint pageId, CancellationToken ct = default)
    {
        if (TryResolveWalOffset(pageId, out long walOffset, out _))
        {
            byte[] walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
            return PageReadBuffer.FromOwnedBuffer(walPage);
        }

        return await _speculativePageReads.ReadPageAsync(pageId, ct);
    }

    private async ValueTask<byte[]> GetPageCoreAsync(uint pageId, CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        var cachedKind = TryGetCachedEntry(pageId, out var cached, out var readOnlyPage);
        if (cachedKind == CachedPageKind.Owned)
        {
            if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                _dirtyBuffers.Remove(pageId);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return cached;
        }

        if (cachedKind == CachedPageKind.ReadOnly)
        {
            _readOnlyPages.Remove(pageId);
            var materialized = readOnlyPage.MaterializeOwnedBuffer();
            _cache.Set(pageId, materialized);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return materialized;
        }

        if (_dirtyBuffers.Count != 0 && _dirtyBuffers.Remove(pageId, out var dirtyBuffer))
        {
            _cache.Set(pageId, dirtyBuffer);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            return dirtyBuffer;
        }

        if (TryResolveWalOffset(pageId, out long walOffset, out PageReadSource walSource))
            return await ReadMutableWalPageAsync(pageId, walOffset, walSource, ct);

        var buffer = await _pageReads.ReadOwnedPageAsync(pageId, ct);
        _cache.Set(pageId, buffer);
        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, PageReadSource.StorageDevice, ct);
        return buffer;
    }

    private async ValueTask<PageReadBuffer> GetPageReadCoreAsync(uint pageId, CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        if (TryGetCachedPageReadBuffer(pageId, out var cached))
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(
                    pageId,
                    cached.TryGetOwnedBuffer(out _) ? PageReadSource.Cache : GetCachedReadSource(pageId),
                    ct);
            return cached;
        }

        if (TryResolveWalOffset(pageId, out long walOffset, out PageReadSource walSource))
            return await ReadWalPageAsync(pageId, walOffset, walSource, ct);

        PageReadBuffer page = await _pageReads.ReadPageAsync(pageId, ct);
        if (page.TryGetOwnedBuffer(out var ownedPage) && ownedPage is not null)
            _cache.Set(pageId, ownedPage);
        else
        {
            _readOnlyPages[pageId] = page;
            _cache.Set(pageId, ReadOnlyCacheSentinel);
        }

        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, GetReadSource(page), ct);

        return page;
    }

    private async ValueTask<PageReadBuffer> GetSnapshotPageReadCoreAsync(
        uint pageId,
        WalSnapshot snapshot,
        CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        if (snapshot.TryGet(pageId, out long walOffset))
        {
            if (_walReadCache is not null && _walReadCache.TryGet(walOffset, out var cachedWalPage))
            {
                if (_hasInterceptor)
                    await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
                return cachedWalPage;
            }

            byte[] walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);

            if (_walReadCache is null)
            {
                if (_hasInterceptor)
                    await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
                return PageReadBuffer.FromOwnedBuffer(walPage);
            }

            var snapshotWalPage = PageReadBuffer.FromReadOnlyMemory(walPage);
            _walReadCache.Set(walOffset, snapshotWalPage);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
            return snapshotWalPage;
        }

        if (TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out var cached))
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, GetSnapshotCachedReadSource(pageId, snapshot), ct);
            return cached;
        }

        var page = await _pageReads.ReadPageAsync(pageId, ct);
        if (CanUseSnapshotSharedMainFileCache(snapshot, pageId))
        {
            if (page.TryGetOwnedBuffer(out var ownedPage) && ownedPage is not null)
                _cache.Set(pageId, ownedPage);
            else
            {
                _readOnlyPages[pageId] = page;
                _cache.Set(pageId, ReadOnlyCacheSentinel);
            }
        }

        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, GetReadSource(page), ct);

        return page;
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

        var cachedKind = TryGetCachedEntry(pageId, out var buffer, out var readOnlyPage);
        if (cachedKind == CachedPageKind.Owned)
        {
            if (_useEvictionDrivenDirtyBufferTracking)
                _dirtyBuffers.Remove(pageId);
            else
                PinDirtyBuffer(pageId, buffer);
            return ValueTask.CompletedTask;
        }

        if (cachedKind == CachedPageKind.ReadOnly)
        {
            _readOnlyPages.Remove(pageId);
            byte[] materialized = readOnlyPage.MaterializeOwnedBuffer();
            _cache.Set(pageId, materialized);
            if (!_useEvictionDrivenDirtyBufferTracking)
                PinDirtyBuffer(pageId, materialized);
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

    public void SetCached(uint pageId, byte[] page)
    {
        _readOnlyPages.Remove(pageId);
        _cache.Set(pageId, page);
    }

    public void ClearDirty()
    {
        _dirtyPages.Clear();
        _dirtyBuffers.Clear();
    }

    public void ClearAll()
    {
        _dirtyPages.Clear();
        _dirtyBuffers.Clear();
        _readOnlyPages.Clear();
        _walReadCache?.Clear();
        _cache.Clear();
    }

    public void ClearCache()
    {
        _readOnlyPages.Clear();
        _walReadCache?.Clear();
        _cache.Clear();
    }

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
        if (ReferenceEquals(buffer, ReadOnlyCacheSentinel))
        {
            _readOnlyPages.Remove(pageId);
            return;
        }

        if (!_useEvictionDrivenDirtyBufferTracking || !_dirtyPages.Contains(pageId))
            return;

        _dirtyBuffers[pageId] = buffer;
    }

    private CachedPageKind TryGetCachedEntry(uint pageId, out byte[] ownedPage, out PageReadBuffer readOnlyPage)
    {
        ownedPage = null!;
        readOnlyPage = default;

        if (!_cache.TryGet(pageId, out var cached))
            return CachedPageKind.None;

        if (!ReferenceEquals(cached, ReadOnlyCacheSentinel))
        {
            ownedPage = cached;
            return CachedPageKind.Owned;
        }

        return _readOnlyPages.TryGetValue(pageId, out readOnlyPage)
            ? CachedPageKind.ReadOnly
            : CachedPageKind.None;
    }

    private bool TryResolveWalOffset(uint pageId, out long walOffset, out PageReadSource source)
    {
        if (_isSnapshotReader && _readerSnapshot != null && _readerSnapshot.TryGet(pageId, out walOffset))
        {
            source = PageReadSource.WalSnapshot;
            return true;
        }

        if (!_isSnapshotReader && _walIndex.TryGetLatest(pageId, out walOffset))
        {
            source = PageReadSource.WalLatest;
            return true;
        }

        walOffset = 0;
        source = default;
        return false;
    }

    private bool TryGetCachedWalPage(uint pageId, out PageReadBuffer page)
    {
        page = default;
        if (_walReadCache is null)
            return false;

        return TryResolveWalOffset(pageId, out long walOffset, out _)
            && _walReadCache.TryGet(walOffset, out page);
    }

    private async ValueTask<PageReadBuffer> ReadWalPageAsync(
        uint pageId,
        long walOffset,
        PageReadSource source,
        CancellationToken ct)
    {
        if (_walReadCache != null && _walReadCache.TryGet(walOffset, out var cachedPage))
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
            return cachedPage;
        }

        var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
        if (_walReadCache is null)
        {
            _cache.Set(pageId, walPage);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, source, ct);
            return PageReadBuffer.FromOwnedBuffer(walPage);
        }

        var readOnlyPage = PageReadBuffer.FromReadOnlyMemory(walPage);
        _walReadCache.Set(walOffset, readOnlyPage);
        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, source, ct);
        return readOnlyPage;
    }

    private async ValueTask<byte[]> ReadMutableWalPageAsync(
        uint pageId,
        long walOffset,
        PageReadSource source,
        CancellationToken ct)
    {
        if (_walReadCache != null && _walReadCache.TryGet(walOffset, out var cachedWalPage))
        {
            byte[] materialized = cachedWalPage.MaterializeOwnedBuffer();
            _cache.Set(pageId, materialized);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
            return materialized;
        }

        if (_walReadCache is null)
        {
            var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
            _cache.Set(pageId, walPage);
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, source, ct);
            return walPage;
        }

        var walReadOnlyPage = await ReadWalPageAsync(pageId, walOffset, source, ct);
        byte[] materializedPage = walReadOnlyPage.MaterializeOwnedBuffer();
        _cache.Set(pageId, materializedPage);
        return materializedPage;
    }

    private PageReadSource GetCachedReadSource(uint pageId)
        => TryResolveWalOffset(pageId, out long walOffset, out _)
           && _walReadCache is not null
           && _walReadCache.TryGet(walOffset, out _)
            ? PageReadSource.WalCache
            : PageReadSource.Cache;

    private PageReadSource GetSnapshotCachedReadSource(uint pageId, WalSnapshot snapshot)
        => snapshot.TryGet(pageId, out long walOffset)
           && _walReadCache is not null
           && _walReadCache.TryGet(walOffset, out _)
            ? PageReadSource.WalCache
            : PageReadSource.Cache;

    private bool CanUseSnapshotSharedMainFileCache(WalSnapshot snapshot, uint pageId)
        => snapshot.CommitCounter == _walIndex.CommitCounter &&
           !_dirtyPages.Contains(pageId);

    private static PageReadSource GetReadSource(PageReadBuffer page)
        => page.TryGetOwnedBuffer(out _)
            ? PageReadSource.StorageDevice
            : PageReadSource.MemoryMappedMainFile;
}

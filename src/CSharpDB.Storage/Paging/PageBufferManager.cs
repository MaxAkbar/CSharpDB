using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics;
using System.Runtime.CompilerServices;

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
    private readonly LogicalPageReadRuntimeCounters? _logicalReadDiagnostics;
    private readonly StorageCacheRuntimeDiagnostics.Lease? _cacheDiagnosticsLease;
    private readonly object _stateGate = new();
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
        IPageOperationInterceptor interceptor,
        StorageIoRuntimeDiagnostics? runtimeDiagnostics)
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
        _logicalReadDiagnostics = runtimeDiagnostics?.LogicalReads;
        _useEvictionDrivenDirtyBufferTracking = cache is IPageCacheEvictionEvents;

        _cacheDiagnosticsLease = runtimeDiagnostics?.Cache.TryRegister(
            cache,
            _walReadCache?.Count ?? 0,
            _walReadCache?.Capacity ?? 0);

        if (cache is IPageCacheEvictionEvents evictionEvents)
            evictionEvents.PageEvicted += OnCachePageEvicted;
    }

    internal bool HasInterceptor => _hasInterceptor;

    internal int DirtyPageCount
    {
        get
        {
            lock (_stateGate)
            {
                return _dirtyPages.Count;
            }
        }
    }

    public IReadOnlyCollection<uint> DirtyPages
    {
        get
        {
            lock (_stateGate)
            {
                return _dirtyPages.Count == 0 ? Array.Empty<uint>() : _dirtyPages.ToArray();
            }
        }
    }

    public byte[]? TryGetCachedPage(uint pageId)
    {
        lock (_stateGate)
        {
            var cachedKind = TryGetCachedEntryLocked(pageId, out var page, out var readOnlyPage);
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
                SetSharedCacheLocked(pageId, page);
                return page;
            }

            // Dirty pages can outlive bounded-cache eviction until commit.
            if (_dirtyBuffers.Count != 0 && _dirtyBuffers.Remove(pageId, out page!))
            {
                SetSharedCacheLocked(pageId, page);
                return page;
            }

            return null;
        }
    }

    public byte[]? TryGetCachedPageAndRecordRead(uint pageId)
    {
        byte[]? page = TryGetCachedPage(pageId);
        if (page is null)
            return null;

        if (_hasInterceptor)
            _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, CancellationToken.None).GetAwaiter().GetResult();

        RecordCacheHit();

        return page;
    }

    public bool TryGetCachedPageReadBuffer(uint pageId, out PageReadBuffer page)
    {
        lock (_stateGate)
        {
            var cachedKind = TryGetCachedEntryLocked(pageId, out var cached, out var readOnlyPage);
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

            if (TryGetCachedWalPageLocked(pageId, out page))
                return true;

            page = default;
            return false;
        }
    }

    public bool TryGetCachedPageReadBufferAndRecordRead(uint pageId, out PageReadBuffer page)
    {
        if (!TryGetCachedPageReadBuffer(pageId, out page))
            return false;

        if (_hasInterceptor)
        {
            PageReadSource source = page.TryGetOwnedBuffer(out _)
                ? PageReadSource.Cache
                : GetCachedReadSource(pageId);
            _interceptor.OnAfterReadAsync(pageId, source, CancellationToken.None).GetAwaiter().GetResult();
        }

        RecordCacheHit();

        return true;
    }

    public bool TryGetSnapshotCachedPageReadBufferAndRecordRead(
        uint pageId,
        WalSnapshot snapshot,
        out PageReadBuffer page)
    {
        if (!TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out page))
            return false;

        if (_hasInterceptor)
        {
            _interceptor.OnAfterReadAsync(
                    pageId,
                    GetSnapshotCachedReadSource(pageId, snapshot),
                    CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }

        RecordCacheHit();
        return true;
    }

    public bool TryGetSnapshotCachedPageReadBuffer(uint pageId, WalSnapshot snapshot, out PageReadBuffer page)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        lock (_stateGate)
        {
            if (snapshot.TryGet(pageId, out long walOffset))
            {
                if (_walReadCache is not null && _walReadCache.TryGet(walOffset, out page))
                    return true;

                page = default;
                return false;
            }

            if (!CanUseSnapshotSharedMainFileCacheLocked(snapshot, pageId))
            {
                page = default;
                return false;
            }

            var cachedKind = TryGetCachedEntryLocked(pageId, out var cached, out var readOnlyPage);
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
    }

    public bool TryGetDirtyPage(uint pageId, out byte[] page)
    {
        lock (_stateGate)
        {
            // Prefer the cache if present; it may contain a newer buffer than an older pinned/evicted entry.
            if (TryGetCachedEntryLocked(pageId, out page, out _) == CachedPageKind.Owned)
            {
                if (_useEvictionDrivenDirtyBufferTracking && _dirtyBuffers.Count != 0)
                    _dirtyBuffers.Remove(pageId);
                return true;
            }

            if (_dirtyBuffers.TryGetValue(pageId, out page!))
                return true;

            return false;
        }
    }

    public ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        // Fast path: no interceptor + cache hit = zero async overhead
        if (!_hasInterceptor)
        {
            byte[]? cached = TryGetCachedPage(pageId);
            if (cached is not null)
            {
                RecordCacheHit();
                return new ValueTask<byte[]>(cached);
            }
        }

        return GetPageCoreAsync(pageId, ct);
    }

    public ValueTask<PageReadBuffer> GetPageReadAsync(uint pageId, CancellationToken ct = default)
    {
        if (!_hasInterceptor && TryGetCachedPageReadBuffer(pageId, out var fastCached))
        {
            RecordCacheHit();
            return new ValueTask<PageReadBuffer>(fastCached);
        }

        return GetPageReadCoreAsync(pageId, ct);
    }

    public ValueTask<PageReadBuffer> GetSnapshotPageReadAsync(
        uint pageId,
        WalSnapshot snapshot,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        if (!_hasInterceptor && TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out var fastCached))
        {
            RecordCacheHit();
            return new ValueTask<PageReadBuffer>(fastCached);
        }

        return GetSnapshotPageReadCoreAsync(pageId, snapshot, ct);
    }

    public async ValueTask<PageReadBuffer> ReadPageUncachedAsync(uint pageId, CancellationToken ct = default)
    {
        if (TryResolveWalOffset(pageId, out long walOffset, out _))
        {
            byte[] walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
            RecordCacheMiss();
            return PageReadBuffer.FromOwnedBuffer(walPage);
        }

        PageReadBuffer page = await _speculativePageReads.ReadPageAsync(pageId, ct);
        RecordCacheMiss();
        return page;
    }

    private async ValueTask<byte[]> GetPageCoreAsync(uint pageId, CancellationToken ct)
    {
        if (_hasInterceptor)
            await _interceptor.OnBeforeReadAsync(pageId, ct);

        byte[]? cachedPage = TryGetCachedPage(pageId);
        if (cachedPage is not null)
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.Cache, ct);
            RecordCacheHit();
            return cachedPage;
        }

        if (TryResolveWalOffset(pageId, out long walOffset, out PageReadSource walSource))
            return await ReadMutableWalPageAsync(pageId, walOffset, walSource, ct);

        var buffer = await _pageReads.ReadOwnedPageAsync(pageId, ct);
        lock (_stateGate)
        {
            _readOnlyPages.Remove(pageId);
            SetSharedCacheLocked(pageId, buffer);
        }
        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, PageReadSource.StorageDevice, ct);
        RecordCacheMiss();
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
            RecordCacheHit();
            return cached;
        }

        if (TryResolveWalOffset(pageId, out long walOffset, out PageReadSource walSource))
            return await ReadWalPageAsync(pageId, walOffset, walSource, ct);

        PageReadBuffer page = await _pageReads.ReadPageAsync(pageId, ct);
        lock (_stateGate)
        {
            if (page.TryGetOwnedBuffer(out var ownedPage) && ownedPage is not null)
            {
                _readOnlyPages.Remove(pageId);
                SetSharedCacheLocked(pageId, ownedPage);
            }
            else
            {
                _readOnlyPages[pageId] = page;
                SetSharedCacheLocked(pageId, ReadOnlyCacheSentinel);
            }
        }

        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, GetReadSource(page), ct);

        RecordCacheMiss();

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
            PageReadBuffer cachedWalPage = default;
            bool hasCachedWalPage;
            lock (_stateGate)
            {
                hasCachedWalPage = _walReadCache is not null && _walReadCache.TryGet(walOffset, out cachedWalPage);
            }

            if (hasCachedWalPage)
            {
                if (_hasInterceptor)
                    await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
                RecordCacheHit();
                return cachedWalPage;
            }

            byte[] walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);

            if (_walReadCache is null)
            {
                if (_hasInterceptor)
                    await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
                RecordCacheMiss();
                return PageReadBuffer.FromOwnedBuffer(walPage);
            }

            var snapshotWalPage = PageReadBuffer.FromReadOnlyMemory(walPage);
            lock (_stateGate)
            {
                SetWalReadCacheLocked(walOffset, snapshotWalPage);
            }
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalSnapshot, ct);
            RecordCacheMiss();
            return snapshotWalPage;
        }

        if (TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out var cached))
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, GetSnapshotCachedReadSource(pageId, snapshot), ct);
            RecordCacheHit();
            return cached;
        }

        var page = await _pageReads.ReadPageAsync(pageId, ct);
        if (CanUseSnapshotSharedMainFileCache(snapshot, pageId))
        {
            lock (_stateGate)
            {
                if (CanUseSnapshotSharedMainFileCacheLocked(snapshot, pageId))
                {
                    if (page.TryGetOwnedBuffer(out var ownedPage) && ownedPage is not null)
                    {
                        _readOnlyPages.Remove(pageId);
                        SetSharedCacheLocked(pageId, ownedPage);
                    }
                    else
                    {
                        _readOnlyPages[pageId] = page;
                        SetSharedCacheLocked(pageId, ReadOnlyCacheSentinel);
                    }
                }
            }
        }

        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, GetReadSource(page), ct);

        RecordCacheMiss();

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

        lock (_stateGate)
        {
            _dirtyPages.Add(pageId);

            var cachedKind = TryGetCachedEntryLocked(pageId, out var buffer, out var readOnlyPage);
            if (cachedKind == CachedPageKind.Owned)
            {
                if (_useEvictionDrivenDirtyBufferTracking)
                    _dirtyBuffers.Remove(pageId);
                else
                    PinDirtyBufferLocked(pageId, buffer);
                return ValueTask.CompletedTask;
            }

            if (cachedKind == CachedPageKind.ReadOnly)
            {
                _readOnlyPages.Remove(pageId);
                byte[] materialized = readOnlyPage.MaterializeOwnedBuffer();
                SetSharedCacheLocked(pageId, materialized);
                if (!_useEvictionDrivenDirtyBufferTracking)
                    PinDirtyBufferLocked(pageId, materialized);
                return ValueTask.CompletedTask;
            }

            if (_dirtyBuffers.TryGetValue(pageId, out _))
                return ValueTask.CompletedTask;
        }

        return EnsurePageInCacheAndPinAsync(pageId, getPageAsync, ct);
    }

    public void AddDirty(uint pageId)
    {
        lock (_stateGate)
        {
            _dirtyPages.Add(pageId);
            if (_useEvictionDrivenDirtyBufferTracking)
                return;

            if (_cache.TryGet(pageId, out var buffer))
                PinDirtyBufferLocked(pageId, buffer);
        }
    }

    public void SetCached(uint pageId, byte[] page)
    {
        lock (_stateGate)
        {
            _readOnlyPages.Remove(pageId);
            SetSharedCacheLocked(pageId, page);
        }
    }

    public void ClearDirty()
    {
        lock (_stateGate)
        {
            _dirtyPages.Clear();
            _dirtyBuffers.Clear();
        }
    }

    public void ClearAll()
    {
        lock (_stateGate)
        {
            _dirtyPages.Clear();
            _dirtyBuffers.Clear();
            _readOnlyPages.Clear();
            ClearWalReadCacheLocked();
            ClearSharedCacheLocked();
        }
    }

    public void ClearCache()
    {
        lock (_stateGate)
        {
            _readOnlyPages.Clear();
            ClearWalReadCacheLocked();
            ClearSharedCacheLocked();
        }
    }

    public void InvalidateCheckpointTransientReads(bool preserveOwnedPages)
    {
        lock (_stateGate)
        {
            ClearWalReadCacheLocked();

            if (!preserveOwnedPages)
            {
                _readOnlyPages.Clear();
                ClearSharedCacheLocked();
                return;
            }

            if (_readOnlyPages.Count == 0)
                return;

            uint[] readOnlyPageIds = _readOnlyPages.Keys.ToArray();
            _readOnlyPages.Clear();
            foreach (uint pageId in readOnlyPageIds)
                RemoveSharedCacheLocked(pageId);
        }
    }

    private async ValueTask EnsurePageInCacheAndPinAsync(
        uint pageId,
        Func<uint, CancellationToken, ValueTask<byte[]>> getPageAsync,
        CancellationToken ct)
    {
        var page = await getPageAsync(pageId, ct);
        if (!_useEvictionDrivenDirtyBufferTracking)
        {
            lock (_stateGate)
            {
                PinDirtyBufferLocked(pageId, page);
            }
        }
    }

    private void PinDirtyBufferLocked(uint pageId, byte[] buffer)
    {
        if (_dirtyBuffers.TryGetValue(pageId, out var existing) && ReferenceEquals(existing, buffer))
            return;

        _dirtyBuffers[pageId] = buffer;
    }

    private void OnCachePageEvicted(uint pageId, byte[] buffer)
    {
        lock (_stateGate)
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
    }

    private CachedPageKind TryGetCachedEntry(uint pageId, out byte[] ownedPage, out PageReadBuffer readOnlyPage)
    {
        lock (_stateGate)
        {
            return TryGetCachedEntryLocked(pageId, out ownedPage, out readOnlyPage);
        }
    }

    private CachedPageKind TryGetCachedEntryLocked(uint pageId, out byte[] ownedPage, out PageReadBuffer readOnlyPage)
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
        lock (_stateGate)
        {
            return TryGetCachedWalPageLocked(pageId, out page);
        }
    }

    private bool TryGetCachedWalPageLocked(uint pageId, out PageReadBuffer page)
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
        PageReadBuffer cachedPage = default;
        bool hasCachedPage;
        lock (_stateGate)
        {
            hasCachedPage = _walReadCache != null && _walReadCache.TryGet(walOffset, out cachedPage);
        }

        if (hasCachedPage)
        {
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
            RecordCacheHit();
            return cachedPage;
        }

        var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
        if (_walReadCache is null)
        {
            lock (_stateGate)
            {
                _readOnlyPages.Remove(pageId);
                SetSharedCacheLocked(pageId, walPage);
            }
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, source, ct);
            RecordCacheMiss();
            return PageReadBuffer.FromOwnedBuffer(walPage);
        }

        var readOnlyPage = PageReadBuffer.FromReadOnlyMemory(walPage);
        lock (_stateGate)
        {
            SetWalReadCacheLocked(walOffset, readOnlyPage);
        }
        if (_hasInterceptor)
            await _interceptor.OnAfterReadAsync(pageId, source, ct);
        RecordCacheMiss();
        return readOnlyPage;
    }

    private async ValueTask<byte[]> ReadMutableWalPageAsync(
        uint pageId,
        long walOffset,
        PageReadSource source,
        CancellationToken ct)
    {
        PageReadBuffer cachedWalPage = default;
        bool hasCachedWalPage;
        lock (_stateGate)
        {
            hasCachedWalPage = _walReadCache != null && _walReadCache.TryGet(walOffset, out cachedWalPage);
        }

        if (hasCachedWalPage)
        {
            byte[] materialized = cachedWalPage.MaterializeOwnedBuffer();
            lock (_stateGate)
            {
                _readOnlyPages.Remove(pageId);
                SetSharedCacheLocked(pageId, materialized);
            }
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, PageReadSource.WalCache, ct);
            RecordCacheHit();
            return materialized;
        }

        if (_walReadCache is null)
        {
            var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
            lock (_stateGate)
            {
                _readOnlyPages.Remove(pageId);
                SetSharedCacheLocked(pageId, walPage);
            }
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, source, ct);
            RecordCacheMiss();
            return walPage;
        }

        PageReadBuffer walReadOnlyPage = default;
        bool racedToWalCache;
        lock (_stateGate)
        {
            racedToWalCache = _walReadCache.TryGet(
                walOffset,
                out walReadOnlyPage);
        }

        if (racedToWalCache)
        {
            if (_hasInterceptor)
            {
                await _interceptor.OnAfterReadAsync(
                    pageId,
                    PageReadSource.WalCache,
                    ct);
            }
        }
        else
        {
            var walPage = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
            await _wal.ReadPageIntoAsync(walOffset, walPage, ct);
            walReadOnlyPage = PageReadBuffer.FromReadOnlyMemory(walPage);
            lock (_stateGate)
            {
                SetWalReadCacheLocked(walOffset, walReadOnlyPage);
            }
            if (_hasInterceptor)
                await _interceptor.OnAfterReadAsync(pageId, source, ct);
        }

        byte[] materializedPage = walReadOnlyPage.MaterializeOwnedBuffer();
        lock (_stateGate)
        {
            _readOnlyPages.Remove(pageId);
            SetSharedCacheLocked(pageId, materializedPage);
        }
        if (racedToWalCache)
            RecordCacheHit();
        else
            RecordCacheMiss();
        return materializedPage;
    }

    private PageReadSource GetCachedReadSource(uint pageId)
    {
        lock (_stateGate)
        {
            return TryResolveWalOffset(pageId, out long walOffset, out _)
               && _walReadCache is not null
               && _walReadCache.TryGet(walOffset, out _)
                ? PageReadSource.WalCache
                : PageReadSource.Cache;
        }
    }

    private PageReadSource GetSnapshotCachedReadSource(uint pageId, WalSnapshot snapshot)
    {
        lock (_stateGate)
        {
            return snapshot.TryGet(pageId, out long walOffset)
               && _walReadCache is not null
               && _walReadCache.TryGet(walOffset, out _)
                ? PageReadSource.WalCache
                : PageReadSource.Cache;
        }
    }

    private bool CanUseSnapshotSharedMainFileCache(WalSnapshot snapshot, uint pageId)
    {
        lock (_stateGate)
        {
            return CanUseSnapshotSharedMainFileCacheLocked(snapshot, pageId);
        }
    }

    private bool CanUseSnapshotSharedMainFileCacheLocked(WalSnapshot snapshot, uint pageId)
        => snapshot.CommitCounter == _walIndex.CommitCounter &&
           !_dirtyPages.Contains(pageId);

    internal void DisposeRuntimeDiagnostics()
    {
        lock (_stateGate)
        {
            PublishCacheGaugeLocked();
            _cacheDiagnosticsLease?.Dispose();
        }
    }

    private void SetSharedCacheLocked(uint pageId, byte[] page)
    {
        if (_cacheDiagnosticsLease is null)
        {
            _cache.Set(pageId, page);
            return;
        }

        try
        {
            _cache.Set(pageId, page);
        }
        finally
        {
            PublishCacheGaugeLocked();
        }
    }

    private bool RemoveSharedCacheLocked(uint pageId)
    {
        if (_cacheDiagnosticsLease is null)
            return _cache.Remove(pageId);

        try
        {
            return _cache.Remove(pageId);
        }
        finally
        {
            PublishCacheGaugeLocked();
        }
    }

    private void ClearSharedCacheLocked()
    {
        if (_cacheDiagnosticsLease is null)
        {
            _cache.Clear();
            return;
        }

        try
        {
            _cache.Clear();
        }
        finally
        {
            PublishCacheGaugeLocked();
        }
    }

    private void SetWalReadCacheLocked(long walOffset, PageReadBuffer page)
    {
        if (_cacheDiagnosticsLease is null)
        {
            _walReadCache!.Set(walOffset, page);
            return;
        }

        try
        {
            _walReadCache!.Set(walOffset, page);
        }
        finally
        {
            PublishCacheGaugeLocked();
        }
    }

    private void ClearWalReadCacheLocked()
    {
        if (_walReadCache is null)
            return;

        if (_cacheDiagnosticsLease is null)
        {
            _walReadCache.Clear();
            return;
        }

        try
        {
            _walReadCache.Clear();
        }
        finally
        {
            PublishCacheGaugeLocked();
        }
    }

    private void PublishCacheGaugeLocked()
    {
        StorageCacheRuntimeDiagnostics.Lease? lease = _cacheDiagnosticsLease;
        if (lease is null)
            return;

        try
        {
            if (_cache is not IPageCacheRuntimeDiagnosticsProvider provider)
                return;

            lease.TryPublish(
                provider.RuntimeResidentPageCount,
                provider.RuntimeCapacityPageCount,
                _walReadCache?.Count ?? 0,
                _walReadCache?.Capacity ?? 0);
        }
        catch
        {
            try
            {
                lease.TryMarkUnavailable();
            }
            catch
            {
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordCacheHit()
    {
        if (_logicalReadDiagnostics is { } diagnostics)
            diagnostics.RecordCacheHit();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordCacheMiss()
    {
        if (_logicalReadDiagnostics is { } diagnostics)
            diagnostics.RecordCacheMiss();
    }

    private static PageReadSource GetReadSource(PageReadBuffer page)
        => page.TryGetOwnedBuffer(out _)
            ? PageReadSource.StorageDevice
            : PageReadSource.MemoryMappedMainFile;
}

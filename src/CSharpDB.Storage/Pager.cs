using CSharpDB.Core;

namespace CSharpDB.Storage;

/// <summary>
/// The Pager owns all page I/O and dirty tracking. It mediates between the B+tree layer and the storage device.
/// Uses a Write-Ahead Log (WAL) for crash recovery and concurrent reader support.
/// </summary>
public sealed class Pager : IAsyncDisposable, IDisposable
{
    private readonly IStorageDevice _device;
    private readonly WriteAheadLog _wal;
    private readonly WalIndex _walIndex;
    private readonly Dictionary<uint, byte[]> _cache = new();
    private readonly HashSet<uint> _dirtyPages = new();
    private bool _inTransaction;

    // Concurrency primitives
    private readonly SemaphoreSlim? _writerLock;
    private readonly SemaphoreSlim? _checkpointLock; // async-safe checkpoint mutex
    private int _activeReaderCount; // tracks snapshot readers to prevent checkpoint during reads

    // Non-null for read-only snapshot pager instances
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;

    // File header state (cached in memory)
    public uint PageCount { get; internal set; }
    public uint SchemaRootPage { get; set; }
    public uint FreelistHead { get; set; }
    public uint ChangeCounter { get; private set; }

    // Auto-checkpoint threshold
    public int CheckpointThreshold { get; set; } = PageConstants.DefaultCheckpointThreshold;

    /// <summary>Primary constructor for writer pager.</summary>
    private Pager(IStorageDevice device, WriteAheadLog wal, WalIndex walIndex)
    {
        _device = device;
        _wal = wal;
        _walIndex = walIndex;
        _writerLock = new SemaphoreSlim(1, 1);
        _checkpointLock = new SemaphoreSlim(1, 1);
    }

    /// <summary>Constructor for read-only snapshot pager.</summary>
    private Pager(IStorageDevice device, WriteAheadLog wal, WalIndex walIndex, WalSnapshot snapshot)
    {
        _device = device;
        _wal = wal;
        _walIndex = walIndex;
        _readerSnapshot = snapshot;
        _isSnapshotReader = true;
        // No writer lock or checkpoint lock needed for readers
    }

    public static async ValueTask<Pager> CreateAsync(IStorageDevice device, WriteAheadLog wal,
        WalIndex walIndex, CancellationToken ct = default)
    {
        var pager = new Pager(device, wal, walIndex);
        if (device.Length >= PageConstants.PageSize)
            await pager.ReadFileHeaderAsync(ct);
        return pager;
    }

    /// <summary>
    /// Create a read-only Pager that uses a WAL snapshot for all reads.
    /// Used for concurrent reader sessions.
    /// </summary>
    public Pager CreateSnapshotReader(WalSnapshot snapshot)
    {
        var reader = new Pager(_device, _wal, _walIndex, snapshot);
        // Copy header state so schema catalog can be loaded
        reader.PageCount = PageCount;
        reader.SchemaRootPage = SchemaRootPage;
        reader.FreelistHead = FreelistHead;
        reader.ChangeCounter = ChangeCounter;
        return reader;
    }

    /// <summary>
    /// Initialize a brand new database file.
    /// </summary>
    public async ValueTask InitializeNewDatabaseAsync(CancellationToken ct = default)
    {
        PageCount = 1; // page 0 exists
        SchemaRootPage = 0;
        FreelistHead = 0;
        ChangeCounter = 0;

        // Ensure page 0 exists
        var page0 = new byte[PageConstants.PageSize];
        WriteFileHeaderTo(page0);
        await _device.SetLengthAsync(PageConstants.PageSize, ct);
        await _device.WriteAsync(0, page0, ct);
        await _device.FlushAsync(ct);
        _cache[0] = page0;
    }

    private async ValueTask ReadFileHeaderAsync(CancellationToken ct = default)
    {
        var header = new byte[PageConstants.FileHeaderSize];
        await _device.ReadAsync(0, header, ct);

        // Validate magic
        if (header[0] != PageConstants.MagicBytes[0] ||
            header[1] != PageConstants.MagicBytes[1] ||
            header[2] != PageConstants.MagicBytes[2] ||
            header[3] != PageConstants.MagicBytes[3])
        {
            throw new CSharpDbException(ErrorCode.CorruptDatabase, "Invalid database file (bad magic bytes).");
        }

        int version = BitConverter.ToInt32(header, PageConstants.VersionOffset);
        if (version != PageConstants.FormatVersion)
            throw new CSharpDbException(ErrorCode.CorruptDatabase, $"Unsupported format version {version}.");

        PageCount = BitConverter.ToUInt32(header, PageConstants.PageCountOffset);
        SchemaRootPage = BitConverter.ToUInt32(header, PageConstants.SchemaRootPageOffset);
        FreelistHead = BitConverter.ToUInt32(header, PageConstants.FreelistHeadOffset);
        ChangeCounter = BitConverter.ToUInt32(header, PageConstants.ChangeCounterOffset);
    }

    private void WriteFileHeaderTo(Span<byte> page0)
    {
        PageConstants.MagicBytes.AsSpan().CopyTo(page0[PageConstants.MagicOffset..]);
        BitConverter.TryWriteBytes(page0[PageConstants.VersionOffset..], PageConstants.FormatVersion);
        BitConverter.TryWriteBytes(page0[PageConstants.PageSizeOffset..], PageConstants.PageSize);
        BitConverter.TryWriteBytes(page0[PageConstants.PageCountOffset..], PageCount);
        BitConverter.TryWriteBytes(page0[PageConstants.SchemaRootPageOffset..], SchemaRootPage);
        BitConverter.TryWriteBytes(page0[PageConstants.FreelistHeadOffset..], FreelistHead);
        BitConverter.TryWriteBytes(page0[PageConstants.ChangeCounterOffset..], ChangeCounter);
    }

    /// <summary>
    /// Synchronous cache-only page lookup. Returns null if the page is not in cache.
    /// Used by the sync fast path for point lookups to avoid async overhead.
    /// </summary>
    public byte[]? TryGetCachedPage(uint pageId)
    {
        _cache.TryGetValue(pageId, out var page);
        return page;
    }

    public async ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        if (_cache.TryGetValue(pageId, out var cached))
            return cached;

        // Check WAL for the page
        if (_isSnapshotReader && _readerSnapshot != null)
        {
            // Snapshot reader: use frozen snapshot
            if (_readerSnapshot.TryGet(pageId, out long walOffset))
            {
                var walPage = await _wal.ReadPageAsync(walOffset, ct);
                _cache[pageId] = walPage;
                return walPage;
            }
        }
        else if (_walIndex.TryGetLatest(pageId, out long latestOffset))
        {
            // Writer or main pager: use latest WAL index
            var walPage = await _wal.ReadPageAsync(latestOffset, ct);
            _cache[pageId] = walPage;
            return walPage;
        }

        // Fall through: read from DB file
        var buffer = new byte[PageConstants.PageSize];
        await _device.ReadAsync((long)pageId * PageConstants.PageSize, buffer, ct);
        _cache[pageId] = buffer;
        return buffer;
    }

    public ValueTask MarkDirtyAsync(uint pageId, CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot modify pages on a read-only snapshot pager.");
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Cannot mark pages dirty outside a transaction.");

        // In WAL mode, just track the page as dirty.
        // No need to save original (WAL is redo, not undo).
        _dirtyPages.Add(pageId);

        // Ensure the page is in cache
        if (!_cache.ContainsKey(pageId))
        {
            // Need to read it synchronously into cache — but since callers
            // always call GetPageAsync before modifying, it should be cached already.
            // Add to cache via async read if needed.
            return EnsurePageInCacheAsync(pageId, ct);
        }

        return ValueTask.CompletedTask;
    }

    private async ValueTask EnsurePageInCacheAsync(uint pageId, CancellationToken ct)
    {
        _dirtyPages.Add(pageId);
        if (!_cache.ContainsKey(pageId))
        {
            await GetPageAsync(pageId, ct);
        }
    }

    /// <summary>
    /// Allocate a new page. Tries the freelist first, then extends the page count.
    /// In WAL mode, the DB file is NOT extended here — that happens during checkpoint.
    /// </summary>
    public async ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot allocate pages on a read-only snapshot pager.");

        if (FreelistHead != PageConstants.NullPageId)
        {
            uint pageId = FreelistHead;
            var freePage = await GetPageAsync(pageId, ct);
            // First 4 bytes of a freelist page = next free page ID
            FreelistHead = BitConverter.ToUInt32(freePage, 0);
            Array.Clear(freePage);
            await MarkDirtyAsync(pageId, ct);
            return pageId;
        }

        // Extend — allocate a new page ID but don't extend the DB file.
        // The page will live in the WAL until checkpoint.
        uint newPageId = PageCount;
        PageCount++;
        var newPage = new byte[PageConstants.PageSize];
        _cache[newPageId] = newPage;
        await MarkDirtyAsync(newPageId, ct);
        return newPageId;
    }

    /// <summary>
    /// Free a page by adding it to the freelist.
    /// </summary>
    public async ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot free pages on a read-only snapshot pager.");

        var page = await GetPageAsync(pageId, ct);
        await MarkDirtyAsync(pageId, ct);
        Array.Clear(page);
        // Write next-free pointer
        BitConverter.TryWriteBytes(page.AsSpan(), FreelistHead);
        page[PageConstants.ContentOffset(pageId)] = PageConstants.PageTypeFreelist;
        FreelistHead = pageId;
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot begin transactions on a read-only snapshot pager.");
        if (_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Nested transactions are not supported.");

        // Acquire writer lock
        if (!await _writerLock!.WaitAsync(TimeSpan.FromSeconds(5), ct))
            throw new CSharpDbException(ErrorCode.Busy, "Could not acquire write lock (database is busy).");

        _wal.BeginTransaction();
        _inTransaction = true;
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction to commit.");

        ChangeCounter++;

        // Update file header in page 0
        var page0 = await GetPageAsync(0, ct);
        WriteFileHeaderTo(page0);
        _dirtyPages.Add(0);

        // Write all dirty pages to WAL
        foreach (var pageId in _dirtyPages)
        {
            if (_cache.TryGetValue(pageId, out var data))
                await _wal.AppendFrameAsync(pageId, data, ct);
        }

        // Commit the WAL (makes frames durable and visible to new readers)
        await _wal.CommitAsync(PageCount, ct);

        _dirtyPages.Clear();
        _inTransaction = false;

        // Release writer lock
        _writerLock!.Release();

        // Auto-checkpoint if threshold reached
        if (_walIndex.FrameCount >= CheckpointThreshold)
        {
            await CheckpointAsync(ct);
        }
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (!_inTransaction)
            return;

        // Truncate uncommitted frames from WAL
        await _wal.RollbackAsync(ct);

        _dirtyPages.Clear();
        _cache.Clear();
        _inTransaction = false;

        // Release writer lock
        try { _writerLock!.Release(); } catch (SemaphoreFullException) { }

        // Re-read header from DB file (WAL may have committed data, so check WAL too)
        if (_device.Length >= PageConstants.PageSize)
            await ReadFileHeaderAsync(ct);

        // If WAL has committed page 0, re-read header from WAL version
        if (_walIndex.TryGetLatest(0, out long walOffset))
        {
            var walPage0 = await _wal.ReadPageAsync(walOffset, ct);
            ReadFileHeaderFrom(walPage0);
        }
    }

    private void ReadFileHeaderFrom(byte[] page0)
    {
        PageCount = BitConverter.ToUInt32(page0, PageConstants.PageCountOffset);
        SchemaRootPage = BitConverter.ToUInt32(page0, PageConstants.SchemaRootPageOffset);
        FreelistHead = BitConverter.ToUInt32(page0, PageConstants.FreelistHeadOffset);
        ChangeCounter = BitConverter.ToUInt32(page0, PageConstants.ChangeCounterOffset);
    }

    /// <summary>
    /// Recover from a crash by opening/scanning the WAL.
    /// </summary>
    public async ValueTask RecoverAsync(CancellationToken ct = default)
    {
        // WAL recovery: open/scan the WAL file, rebuild index
        await _wal.OpenAsync(PageCount, ct);

        // If WAL has committed data, checkpoint to bring DB file up to date
        if (_walIndex.FrameCount > 0)
        {
            // Read the latest header from WAL if page 0 was committed
            if (_walIndex.TryGetLatest(0, out long walOffset))
            {
                var walPage0 = await _wal.ReadPageAsync(walOffset, ct);
                ReadFileHeaderFrom(walPage0);
            }

            await CheckpointAsync(ct);
        }
    }

    /// <summary>
    /// Checkpoint: copy committed WAL pages to the DB file, then reset the WAL.
    /// </summary>
    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot checkpoint on a read-only snapshot pager.");

        // Nothing to checkpoint if WAL has no committed frames
        if (_walIndex.FrameCount == 0)
            return;

        // Skip checkpoint if readers are active (their snapshots reference WAL data)
        if (Volatile.Read(ref _activeReaderCount) > 0)
            return;

        if (_checkpointLock != null)
            await _checkpointLock.WaitAsync(ct);
        try
        {
            // Double-check after acquiring lock
            if (_walIndex.FrameCount == 0)
                return;

            // Ensure DB file is large enough for all pages
            long requiredLength = (long)PageCount * PageConstants.PageSize;
            if (_device.Length < requiredLength)
            {
                await _device.SetLengthAsync(requiredLength, ct);
            }

            await _wal.CheckpointAsync(_device, PageCount, ct);

            // Clear cache so next reads come from the now-updated DB file
            _cache.Clear();
            if (_device.Length >= PageConstants.PageSize)
                await ReadFileHeaderAsync(ct);
        }
        finally
        {
            _checkpointLock?.Release();
        }
    }

    /// <summary>
    /// Acquire a reader snapshot for concurrent reads.
    /// Increments active reader count to prevent checkpoint during reads.
    /// </summary>
    public WalSnapshot AcquireReaderSnapshot()
    {
        Interlocked.Increment(ref _activeReaderCount);
        return _walIndex.TakeSnapshot();
    }

    /// <summary>
    /// Release a reader snapshot.
    /// Decrements active reader count, allowing checkpoint to proceed.
    /// </summary>
    public void ReleaseReaderSnapshot()
    {
        Interlocked.Decrement(ref _activeReaderCount);
    }

    public async ValueTask DisposeAsync()
    {
        if (_isSnapshotReader)
            return; // Snapshot readers don't own resources

        if (_inTransaction)
            await RollbackAsync();

        // Final checkpoint before close
        if (_walIndex.FrameCount > 0)
        {
            try { await CheckpointAsync(); } catch { }
        }

        // Delete WAL file on clean close
        await _wal.CloseAndDeleteAsync();

        if (_device is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync();
        else
            _device.Dispose();

        _checkpointLock?.Dispose();
        _writerLock?.Dispose();
    }

    public void Dispose()
    {
        if (_isSnapshotReader)
            return;

        if (_inTransaction)
            RollbackAsync().AsTask().GetAwaiter().GetResult();
        _device.Dispose();
        _checkpointLock?.Dispose();
        _writerLock?.Dispose();
    }
}

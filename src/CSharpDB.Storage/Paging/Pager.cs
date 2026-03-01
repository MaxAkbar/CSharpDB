using CSharpDB.Core;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// The Pager owns all page I/O and dirty tracking. It mediates between the B+tree layer and the storage device.
/// Uses a Write-Ahead Log (WAL) for crash recovery and concurrent reader support.
/// </summary>
public sealed class Pager : IAsyncDisposable, IDisposable
{
    private readonly IStorageDevice _device;
    private readonly IWriteAheadLog _wal;
    private readonly WalIndex _walIndex;
    private readonly PageBufferManager _buffers;
    private readonly IPageAllocator _allocator;
    private readonly TransactionCoordinator? _transactions;
    private readonly CheckpointCoordinator? _checkpoints;

    // Non-null for read-only snapshot pager instances
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;

    // File header state (cached in memory)
    public uint PageCount { get; internal set; }
    public uint SchemaRootPage { get; set; }
    public uint FreelistHead { get; set; }
    public uint ChangeCounter { get; private set; }

    // Configurable behavior
    private readonly PagerOptions _options;

    /// <summary>
    /// Legacy threshold property preserved for compatibility.
    /// Prefer configuring <see cref="CheckpointPolicy"/> via <see cref="PagerOptions"/>.
    /// </summary>
    public int CheckpointThreshold { get; set; } = PageConstants.DefaultCheckpointThreshold;

    /// <summary>
    /// Auto-checkpoint policy used after commits.
    /// </summary>
    public ICheckpointPolicy CheckpointPolicy { get; set; }

    /// <summary>Primary constructor for writer pager.</summary>
    private Pager(IStorageDevice device, IWriteAheadLog wal, WalIndex walIndex, PagerOptions? options = null)
    {
        _device = device;
        _wal = wal;
        _walIndex = walIndex;
        _options = options ?? new PagerOptions();
        _buffers = new PageBufferManager(
            _options.CreatePageCache(),
            _wal,
            _walIndex,
            readerSnapshot: null,
            isSnapshotReader: false);
        _transactions = new TransactionCoordinator();
        _checkpoints = new CheckpointCoordinator();
        _allocator = new PageAllocator(
            _buffers,
            () => FreelistHead,
            value => FreelistHead = value,
            () => PageCount,
            value => PageCount = value,
            GetPageAsync,
            MarkDirtyAsync,
            () => _isSnapshotReader);
        CheckpointPolicy = _options.CheckpointPolicy;

        if (_options.CheckpointPolicy is FrameCountCheckpointPolicy framePolicy)
            CheckpointThreshold = framePolicy.Threshold;
    }

    /// <summary>Constructor for read-only snapshot pager.</summary>
    private Pager(IStorageDevice device, IWriteAheadLog wal, WalIndex walIndex, WalSnapshot snapshot, PagerOptions? options = null)
    {
        _device = device;
        _wal = wal;
        _walIndex = walIndex;
        _options = options ?? new PagerOptions();
        _readerSnapshot = snapshot;
        _isSnapshotReader = true;
        _buffers = new PageBufferManager(
            _options.CreatePageCache(),
            _wal,
            _walIndex,
            _readerSnapshot,
            _isSnapshotReader);
        _allocator = new PageAllocator(
            _buffers,
            () => FreelistHead,
            value => FreelistHead = value,
            () => PageCount,
            value => PageCount = value,
            GetPageAsync,
            MarkDirtyAsync,
            () => _isSnapshotReader);
        CheckpointPolicy = _options.CheckpointPolicy;

        if (_options.CheckpointPolicy is FrameCountCheckpointPolicy framePolicy)
            CheckpointThreshold = framePolicy.Threshold;
        // No writer lock or checkpoint lock needed for readers
    }

    public static async ValueTask<Pager> CreateAsync(IStorageDevice device, IWriteAheadLog wal,
        WalIndex walIndex, PagerOptions? options = null, CancellationToken ct = default)
    {
        var pager = new Pager(device, wal, walIndex, options);
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
        var reader = new Pager(_device, _wal, _walIndex, snapshot, _options);
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
        _buffers.SetCached(0, page0);
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
        return _buffers.TryGetCachedPage(pageId);
    }

    public async ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        return await _buffers.GetPageAsync(_device, pageId, ct);
    }

    public ValueTask MarkDirtyAsync(uint pageId, CancellationToken ct = default)
    {
        return _buffers.MarkDirtyAsync(
            pageId,
            _transactions?.InTransaction == true,
            GetPageAsync,
            ct);
    }

    /// <summary>
    /// Allocate a new page. Tries the freelist first, then extends the page count.
    /// In WAL mode, the DB file is NOT extended here — that happens during checkpoint.
    /// </summary>
    public async ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
    {
        return await _allocator.AllocatePageAsync(ct);
    }

    /// <summary>
    /// Free a page by adding it to the freelist.
    /// </summary>
    public async ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
    {
        await _allocator.FreePageAsync(pageId, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot begin transactions on a read-only snapshot pager.");
        await _transactions!.BeginAsync(_wal, _options.WriterLockTimeout, ct);
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_transactions is null || !_transactions.InTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction to commit.");

        ChangeCounter++;

        // Update file header in page 0
        var page0 = await GetPageAsync(0, ct);
        WriteFileHeaderTo(page0);
        _buffers.AddDirty(0);

        // Write all dirty pages to WAL
        foreach (var pageId in _buffers.DirtyPages)
        {
            if (_buffers.TryGetDirtyPage(pageId, out var data))
                await _wal.AppendFrameAsync(pageId, data, ct);
        }

        // Commit the WAL (makes frames durable and visible to new readers)
        await _wal.CommitAsync(PageCount, ct);

        _buffers.ClearDirty();
        _transactions.CompleteCommit();

        // Auto-checkpoint according to policy
        bool shouldCheckpoint = _checkpoints!.ShouldCheckpoint(
            CheckpointPolicy,
            _walIndex.FrameCount,
            CheckpointThreshold);
        if (shouldCheckpoint)
        {
            await CheckpointAsync(ct);
        }
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_transactions is null || !_transactions.TryBeginRollback())
            return;

        // Truncate uncommitted frames from WAL
        await _wal.RollbackAsync(ct);

        _buffers.ClearAll();
        _transactions.CompleteRollback();

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
        await _checkpoints!.RunCheckpointAsync(
            _walIndex.FrameCount,
            async checkpointCt =>
            {
                if (_walIndex.FrameCount == 0)
                    return;

                long requiredLength = (long)PageCount * PageConstants.PageSize;
                if (_device.Length < requiredLength)
                    await _device.SetLengthAsync(requiredLength, checkpointCt);

                await _wal.CheckpointAsync(_device, PageCount, checkpointCt);

                _buffers.ClearCache();
                if (_device.Length >= PageConstants.PageSize)
                    await ReadFileHeaderAsync(checkpointCt);
            },
            ct);
    }

    /// <summary>
    /// Acquire a reader snapshot for concurrent reads.
    /// Increments active reader count to prevent checkpoint during reads.
    /// </summary>
    public WalSnapshot AcquireReaderSnapshot()
    {
        if (_checkpoints == null)
            return _walIndex.TakeSnapshot();

        return _checkpoints.AcquireReaderSnapshot(_walIndex);
    }

    /// <summary>
    /// Release a reader snapshot.
    /// Decrements active reader count, allowing checkpoint to proceed.
    /// </summary>
    public void ReleaseReaderSnapshot()
    {
        _checkpoints?.ReleaseReaderSnapshot();
    }

    public async ValueTask DisposeAsync()
    {
        if (_isSnapshotReader)
            return; // Snapshot readers don't own resources

        if (_transactions?.InTransaction == true)
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

        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }

    public void Dispose()
    {
        if (_isSnapshotReader)
            return;

        if (_transactions?.InTransaction == true)
            RollbackAsync().AsTask().GetAwaiter().GetResult();
        _device.Dispose();
        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }
}

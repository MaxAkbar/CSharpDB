using CSharpDB.Core;
using CSharpDB.Storage.Caching;
using System.Buffers;

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
    private readonly IPageOperationInterceptor _interceptor;
    private readonly bool _hasInterceptor;
    private readonly PageBufferManager _buffers;
    private readonly IPageAllocator _allocator;
    private readonly TransactionCoordinator? _transactions;
    private readonly CheckpointCoordinator? _checkpoints;
    private readonly Func<CancellationToken, ValueTask>? _checkpointAction;

    // Non-null for read-only snapshot pager instances
    private readonly WalSnapshot? _readerSnapshot;
    private readonly bool _isSnapshotReader;

    // File header state (cached in memory)
    public uint PageCount { get; internal set; }
    public uint SchemaRootPage { get; set; }
    public uint FreelistHead { get; set; }
    public uint ChangeCounter { get; private set; }
    public int ActiveReaderCount => _checkpoints?.ActiveReaderCount ?? 0;

    // Configurable behavior
    private readonly PagerOptions _options;
    private readonly byte[] _fileHeaderBuffer = new byte[PageConstants.FileHeaderSize];
    private readonly byte[] _walHeaderPageBuffer = new byte[PageConstants.PageSize];

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
        ValidateOptions(_options);
        _interceptor = _options.CreateInterceptor();
        _hasInterceptor = _interceptor is not NoOpPageOperationInterceptor;
        var cache = _options.CreatePageCache();
        if (_options.OnCachePageEvicted != null && cache is IPageCacheEvictionEvents evictingCache)
            evictingCache.PageEvicted += _options.OnCachePageEvicted;
        _buffers = new PageBufferManager(
            cache,
            _wal,
            _walIndex,
            readerSnapshot: null,
            isSnapshotReader: false,
            _interceptor);
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
        _checkpointAction = RunCheckpointCoreAsync;
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
        ValidateOptions(_options);
        _interceptor = _options.CreateInterceptor();
        _hasInterceptor = _interceptor is not NoOpPageOperationInterceptor;
        _readerSnapshot = snapshot;
        _isSnapshotReader = true;
        var cache = _options.CreatePageCache();
        if (_options.OnCachePageEvicted != null && cache is IPageCacheEvictionEvents evictingCache)
            evictingCache.PageEvicted += _options.OnCachePageEvicted;
        _buffers = new PageBufferManager(
            cache,
            _wal,
            _walIndex,
            _readerSnapshot,
            _isSnapshotReader,
            _interceptor);
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
        // No writer lock, checkpoint lock, or checkpoint action needed for readers
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
        var header = _fileHeaderBuffer;
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

    public ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        return _buffers.GetPageAsync(_device, pageId, ct);
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
    public ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
    {
        return _allocator.AllocatePageAsync(ct);
    }

    /// <summary>
    /// Free a page by adding it to the freelist.
    /// </summary>
    public ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
    {
        return _allocator.FreePageAsync(pageId, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot begin transactions on a read-only snapshot pager.");
        await WaitForBackgroundCheckpointAsync(ct);
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
        int dirtyCount = _buffers.DirtyPages.Count;

        if (_hasInterceptor)
        {
            bool commitSucceeded = false;
            await _interceptor.OnCommitStartAsync(dirtyCount, ct);
            uint[]? orderedDirtyPageIds = null;
            int orderedDirtyCount = 0;
            try
            {
                EnforceReaderWalBackpressure(dirtyCount);
                orderedDirtyPageIds = RentSortedPageIds(_buffers.DirtyPages, out orderedDirtyCount);

                // Write all dirty pages to WAL with per-page interceptor hooks
                for (int i = 0; i < orderedDirtyCount; i++)
                {
                    uint pageId = orderedDirtyPageIds[i];
                    if (!_buffers.TryGetDirtyPage(pageId, out var data))
                    {
                        throw new CSharpDbException(
                            ErrorCode.Unknown,
                            $"Dirty page {pageId} could not be materialized during commit.");
                    }

                    bool writeSucceeded = false;
                    await _interceptor.OnBeforeWriteAsync(pageId, ct);
                    try
                    {
                        await _wal.AppendFrameAsync(pageId, data, ct);
                        writeSucceeded = true;
                    }
                    finally
                    {
                        await _interceptor.OnAfterWriteAsync(pageId, writeSucceeded, ct);
                    }
                }

                await CommitWalAndFinalizeAsync(ct);
                commitSucceeded = true;
            }
            finally
            {
                if (orderedDirtyPageIds != null)
                    ArrayPool<uint>.Shared.Return(orderedDirtyPageIds, clearArray: false);
                await _interceptor.OnCommitEndAsync(dirtyCount, commitSucceeded, ct);
            }
        }
        else
        {
            EnforceReaderWalBackpressure(dirtyCount);

            // Fast path: no interceptor — batch WAL appends to reduce per-frame write overhead.
            uint[]? orderedDirtyPageIds = null;
            int orderedDirtyCount = 0;
            WalFrameWrite[]? frameBatch = null;
            int frameCount = 0;
            try
            {
                orderedDirtyPageIds = RentSortedPageIds(_buffers.DirtyPages, out orderedDirtyCount);
                frameBatch = ArrayPool<WalFrameWrite>.Shared.Rent(dirtyCount);
                for (int i = 0; i < orderedDirtyCount; i++)
                {
                    uint pageId = orderedDirtyPageIds[i];
                    if (!_buffers.TryGetDirtyPage(pageId, out var data))
                    {
                        throw new CSharpDbException(
                            ErrorCode.Unknown,
                            $"Dirty page {pageId} could not be materialized during commit.");
                    }

                    frameBatch[frameCount++] = new WalFrameWrite(pageId, data);
                }

                if (frameCount > 0)
                {
                    await _wal.AppendFramesAndCommitAsync(frameBatch.AsMemory(0, frameCount), PageCount, ct);
                }
                else
                {
                    await _wal.CommitAsync(PageCount, ct);
                }
            }
            finally
            {
                if (frameBatch != null)
                {
                    frameBatch.AsSpan(0, frameCount).Clear();
                    ArrayPool<WalFrameWrite>.Shared.Return(frameBatch, clearArray: false);
                }
                if (orderedDirtyPageIds != null)
                    ArrayPool<uint>.Shared.Return(orderedDirtyPageIds, clearArray: false);
            }

            await FinalizeCommitAndCheckpointAsync(ct);
        }
    }

    private async ValueTask CommitWalAndFinalizeAsync(CancellationToken ct)
    {
        // Commit the WAL (makes frames durable and visible to new readers)
        await _wal.CommitAsync(PageCount, ct);
        await FinalizeCommitAndCheckpointAsync(ct);
    }

    private async ValueTask FinalizeCommitAndCheckpointAsync(CancellationToken ct)
    {
        _buffers.ClearDirty();
        _transactions!.CompleteCommit();

        // Auto-checkpoint according to policy
        bool shouldCheckpoint = _checkpoints!.ShouldCheckpoint(
            CheckpointPolicy,
            _walIndex.FrameCount,
            CheckpointThreshold,
            EstimateCommittedWalBytes(_walIndex.FrameCount));
        if (shouldCheckpoint)
            _checkpoints.RequestDeferredCheckpoint();

        if (_options.AutoCheckpointExecutionMode == AutoCheckpointExecutionMode.Background)
        {
            ScheduleBackgroundCheckpointIfNeeded();
            return;
        }

        if (_checkpoints.HasPendingCheckpointRequest)
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
            await _wal.ReadPageIntoAsync(walOffset, _walHeaderPageBuffer, ct);
            ReadFileHeaderFrom(_walHeaderPageBuffer);
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
        if (_hasInterceptor)
        {
            bool recoverySucceeded = false;
            await _interceptor.OnRecoveryStartAsync(ct);
            try
            {
                await RecoverCoreAsync(ct);
                recoverySucceeded = true;
            }
            finally
            {
                await _interceptor.OnRecoveryEndAsync(recoverySucceeded, ct);
            }
        }
        else
        {
            await RecoverCoreAsync(ct);
        }
    }

    private async ValueTask RecoverCoreAsync(CancellationToken ct)
    {
        // WAL recovery: open/scan the WAL file, rebuild index
        await _wal.OpenAsync(PageCount, ct);

        // If WAL has committed data, checkpoint to bring DB file up to date
        if (_walIndex.FrameCount > 0)
        {
            // Read the latest header from WAL if page 0 was committed
            if (_walIndex.TryGetLatest(0, out long walOffset))
            {
                await _wal.ReadPageIntoAsync(walOffset, _walHeaderPageBuffer, ct);
                ReadFileHeaderFrom(_walHeaderPageBuffer);
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

        await WaitForBackgroundCheckpointAsync(ct);

        await RunCheckpointWithInterceptorsAsync(ct);
        if (!HasCheckpointWorkPending())
            _checkpoints?.ClearDeferredCheckpointRequest();
    }

    private async ValueTask<bool> RunCheckpointWithInterceptorsAsync(CancellationToken ct)
    {
        if (_checkpoints is null || _checkpointAction is null)
            return false;

        int frameCount = _walIndex.FrameCount;
        bool checkpointRan = false;

        if (_hasInterceptor)
        {
            bool checkpointSucceeded = false;
            await _interceptor.OnCheckpointStartAsync(frameCount, ct);
            try
            {
                await _checkpoints.RunCheckpointAsync(
                    frameCount,
                    async innerCt =>
                    {
                        checkpointRan = true;
                        await _checkpointAction(innerCt);
                    },
                    ct);
                checkpointSucceeded = checkpointRan;
            }
            finally
            {
                await _interceptor.OnCheckpointEndAsync(frameCount, checkpointSucceeded, ct);
            }
        }
        else
        {
            await _checkpoints.RunCheckpointAsync(
                frameCount,
                async innerCt =>
                {
                    checkpointRan = true;
                    await _checkpointAction(innerCt);
                },
                ct);
        }

        return checkpointRan;
    }

    private async ValueTask RunBackgroundCheckpointStepWithInterceptorsAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return;

        while (true)
        {
            if (!HasCheckpointWorkPending())
            {
                _checkpoints.ClearDeferredCheckpointRequest();
                return;
            }

            bool checkpointRan = await RunBackgroundCheckpointStepAsync(ct);
            if (!checkpointRan)
                return;

            if (!HasCheckpointWorkPending())
            {
                _checkpoints.ClearDeferredCheckpointRequest();
                return;
            }

            await Task.Yield();
        }
    }

    private async ValueTask<bool> RunBackgroundCheckpointStepAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return false;

        int frameCount = _walIndex.FrameCount;
        bool checkpointRan = false;

        if (_hasInterceptor)
        {
            bool checkpointSucceeded = false;
            await _interceptor.OnCheckpointStartAsync(frameCount, ct);
            try
            {
                await _checkpoints.RunCheckpointAsync(
                    frameCount,
                    async innerCt =>
                    {
                        checkpointRan = true;
                        await RunCheckpointStepCoreAsync(innerCt);
                    },
                    ct);
                checkpointSucceeded = checkpointRan;
            }
            finally
            {
                await _interceptor.OnCheckpointEndAsync(frameCount, checkpointSucceeded, ct);
            }
        }
        else
        {
            await _checkpoints.RunCheckpointAsync(
                frameCount,
                async innerCt =>
                {
                    checkpointRan = true;
                    await RunCheckpointStepCoreAsync(innerCt);
                },
                ct);
        }

        return checkpointRan;
    }

    private async ValueTask RunCheckpointCoreAsync(CancellationToken ct)
    {
        if (_walIndex.FrameCount == 0 && !_wal.HasPendingCheckpoint)
            return;

        await _wal.CheckpointAsync(_device, PageCount, ct);
        await RefreshStateAfterCheckpointCompletionAsync(ct);
    }

    private async ValueTask RunCheckpointStepCoreAsync(CancellationToken ct)
    {
        if (_walIndex.FrameCount == 0 && !_wal.HasPendingCheckpoint)
            return;

        bool completed = await _wal.CheckpointStepAsync(
            _device,
            PageCount,
            _options.AutoCheckpointMaxPagesPerStep,
            ct);

        if (completed)
            await RefreshStateAfterCheckpointCompletionAsync(ct);
    }

    private async ValueTask RefreshStateAfterCheckpointCompletionAsync(CancellationToken ct)
    {
        _buffers.ClearCache();
        if (_device.Length >= PageConstants.PageSize)
            await ReadFileHeaderAsync(ct);
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
        bool drained = _checkpoints?.ReleaseReaderSnapshot() == true;
        if (drained)
            ScheduleBackgroundCheckpointIfNeeded();
    }

    public async ValueTask DisposeAsync()
    {
        if (_isSnapshotReader)
            return; // Snapshot readers don't own resources

        if (_transactions?.InTransaction == true)
            await RollbackAsync();

        await WaitForBackgroundCheckpointAsync();

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

    public async ValueTask SaveToFileAsync(string filePath, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        if (_isSnapshotReader)
            throw new InvalidOperationException("Cannot save from a read-only snapshot pager.");
        if (_transactions?.InTransaction == true)
            throw new InvalidOperationException("Cannot save while a transaction is active.");
        if (ActiveReaderCount > 0)
            throw new InvalidOperationException("Cannot save while reader snapshots are active.");

        await WaitForBackgroundCheckpointAsync(ct);

        if (_walIndex.FrameCount > 0)
            await CheckpointAsync(ct);

        long logicalLength = Math.Max((long)PageCount * PageConstants.PageSize, PageConstants.PageSize);
        string destinationPath = Path.GetFullPath(filePath);
        string? directory = Path.GetDirectoryName(destinationPath);
        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        string tempPath = destinationPath + $".tmp.{Guid.NewGuid():N}";
        var buffer = new byte[64 * 1024];

        try
        {
            await using (var stream = new FileStream(
                             tempPath,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.None,
                             bufferSize: buffer.Length,
                             useAsync: true))
            {
                long offset = 0;
                while (offset < logicalLength)
                {
                    int chunkLength = (int)Math.Min(buffer.Length, logicalLength - offset);
                    int bytesRead = await _device.ReadAsync(offset, buffer.AsMemory(0, chunkLength), ct);
                    if (bytesRead != chunkLength)
                    {
                        throw new InvalidOperationException(
                            $"Short database read while saving snapshot (expected {chunkLength} bytes, read {bytesRead}).");
                    }

                    await stream.WriteAsync(buffer.AsMemory(0, chunkLength), ct);
                    offset += chunkLength;
                }

                await stream.FlushAsync(ct);
            }

            File.Move(tempPath, destinationPath, overwrite: true);
        }
        catch
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch
            {
                // Best-effort cleanup for temporary snapshot files.
            }

            throw;
        }
    }

    public void Dispose()
    {
        if (_isSnapshotReader)
            return;

        if (_transactions?.InTransaction == true)
            RollbackAsync().AsTask().GetAwaiter().GetResult();
        WaitForBackgroundCheckpointAsync().AsTask().GetAwaiter().GetResult();
        _device.Dispose();
        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }

    private void ScheduleBackgroundCheckpointIfNeeded()
    {
        if (_isSnapshotReader ||
            _checkpoints is null ||
            _options.AutoCheckpointExecutionMode != AutoCheckpointExecutionMode.Background)
        {
            return;
        }

        _checkpoints.TryStartBackgroundCheckpoint(RunBackgroundCheckpointStepWithInterceptorsAsync);
    }

    private ValueTask WaitForBackgroundCheckpointAsync(CancellationToken ct = default)
    {
        if (_isSnapshotReader ||
            _checkpoints is null ||
            _options.AutoCheckpointExecutionMode != AutoCheckpointExecutionMode.Background)
        {
            return ValueTask.CompletedTask;
        }

        return _checkpoints.WaitForBackgroundCheckpointAsync(ct);
    }

    private static long EstimateCommittedWalBytes(int frameCount)
    {
        if (frameCount <= 0)
            return PageConstants.WalHeaderSize;
        return PageConstants.WalHeaderSize + (long)frameCount * PageConstants.WalFrameSize;
    }

    private bool HasCheckpointWorkPending()
        => _walIndex.FrameCount > 0 || _wal.HasPendingCheckpoint;

    private static uint[] RentSortedPageIds(IReadOnlyCollection<uint> pageIds, out int count)
    {
        count = pageIds.Count;
        int capacity = count > 0 ? count : 1;
        uint[] orderedPageIds = ArrayPool<uint>.Shared.Rent(capacity);

        int index = 0;
        foreach (uint pageId in pageIds)
        {
            orderedPageIds[index++] = pageId;
        }

        if (index > 1)
            Array.Sort(orderedPageIds, 0, index);

        count = index;
        return orderedPageIds;
    }

    private static void ValidateOptions(PagerOptions options)
    {
        if (options.MaxWalBytesWhenReadersActive is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.MaxWalBytesWhenReadersActive),
                options.MaxWalBytesWhenReadersActive,
                "MaxWalBytesWhenReadersActive must be greater than zero when configured.");
        }

        if (options.AutoCheckpointMaxPagesPerStep <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.AutoCheckpointMaxPagesPerStep),
                options.AutoCheckpointMaxPagesPerStep,
                "AutoCheckpointMaxPagesPerStep must be greater than zero.");
        }
    }

    private void EnforceReaderWalBackpressure(int dirtyPageCount)
    {
        if (dirtyPageCount <= 0)
            return;

        long? limitBytes = _options.MaxWalBytesWhenReadersActive;
        if (!limitBytes.HasValue)
            return;

        int activeReaders = _checkpoints?.ActiveReaderCount ?? 0;
        if (activeReaders <= 0)
            return;

        long projectedBytes = EstimateCommittedWalBytes(_walIndex.FrameCount + dirtyPageCount);
        if (projectedBytes <= limitBytes.Value)
            return;

        throw new CSharpDbException(
            ErrorCode.Busy,
            $"WAL growth limit exceeded while snapshot readers are active (activeReaders={activeReaders}, projectedWalBytes={projectedBytes}, limitBytes={limitBytes.Value}).");
    }
}

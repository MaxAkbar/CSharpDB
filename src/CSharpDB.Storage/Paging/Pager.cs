using CSharpDB.Primitives;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Internal;
using System.Buffers;
using System.Diagnostics;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// The Pager owns all page I/O and dirty tracking. It mediates between the B+tree layer and the storage device.
/// Uses a Write-Ahead Log (WAL) for crash recovery and concurrent reader support.
/// </summary>
public sealed class Pager : IAsyncDisposable, IDisposable
{
    private readonly IStorageDevice _device;
    private readonly IPageReadProvider _pageReads;
    private readonly IPageReadProvider _speculativePageReads;
    private readonly bool _ownsPageReadProviders;
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
    private long _walAppendCount;
    private long _walAppendTicks;
    private long _finalizeCommitCount;
    private long _finalizeCommitTicks;
    private long _checkpointDecisionCount;
    private long _checkpointDecisionTicks;
    private long _backgroundCheckpointStartCount;

    // File header state (cached in memory)
    public uint PageCount { get; internal set; }
    public uint SchemaRootPage { get; set; }
    public uint FreelistHead { get; set; }
    public uint ChangeCounter { get; private set; }
    public int ActiveReaderCount => _checkpoints?.ActiveReaderCount ?? 0;

    internal WalFlushDiagnosticsSnapshot GetWalFlushDiagnosticsSnapshot()
    {
        return _wal is IWalRuntimeDiagnosticsProvider diagnosticsProvider
            ? diagnosticsProvider.GetWalFlushDiagnosticsSnapshot()
            : WalFlushDiagnosticsSnapshot.Empty;
    }

    internal void ResetWalFlushDiagnostics()
    {
        if (_wal is IWalRuntimeDiagnosticsProvider diagnosticsProvider)
            diagnosticsProvider.ResetWalFlushDiagnostics();
    }

    internal CommitPathDiagnosticsSnapshot GetCommitPathDiagnosticsSnapshot()
    {
        CommitPathDiagnosticsSnapshot walDiagnostics = _wal is ICommitPathDiagnosticsProvider diagnosticsProvider
            ? diagnosticsProvider.GetCommitPathDiagnosticsSnapshot()
            : CommitPathDiagnosticsSnapshot.Empty;

        return new CommitPathDiagnosticsSnapshot(
            WalAppendCount: Interlocked.Read(ref _walAppendCount),
            WalAppendTicks: Interlocked.Read(ref _walAppendTicks),
            BufferedFlushCount: walDiagnostics.BufferedFlushCount,
            BufferedFlushTicks: walDiagnostics.BufferedFlushTicks,
            DurableFlushCount: walDiagnostics.DurableFlushCount,
            DurableFlushTicks: walDiagnostics.DurableFlushTicks,
            PublishBatchCount: walDiagnostics.PublishBatchCount,
            PublishBatchTicks: walDiagnostics.PublishBatchTicks,
            FinalizeCommitCount: Interlocked.Read(ref _finalizeCommitCount),
            FinalizeCommitTicks: Interlocked.Read(ref _finalizeCommitTicks),
            CheckpointDecisionCount: Interlocked.Read(ref _checkpointDecisionCount),
            CheckpointDecisionTicks: Interlocked.Read(ref _checkpointDecisionTicks),
            BackgroundCheckpointStartCount: Interlocked.Read(ref _backgroundCheckpointStartCount),
            MaxPendingCommitCount: walDiagnostics.MaxPendingCommitCount,
            MaxPendingCommitBytes: walDiagnostics.MaxPendingCommitBytes);
    }

    internal void ResetCommitPathDiagnostics()
    {
        if (_wal is ICommitPathDiagnosticsProvider diagnosticsProvider)
            diagnosticsProvider.ResetCommitPathDiagnostics();

        Interlocked.Exchange(ref _walAppendCount, 0);
        Interlocked.Exchange(ref _walAppendTicks, 0);
        Interlocked.Exchange(ref _finalizeCommitCount, 0);
        Interlocked.Exchange(ref _finalizeCommitTicks, 0);
        Interlocked.Exchange(ref _checkpointDecisionCount, 0);
        Interlocked.Exchange(ref _checkpointDecisionTicks, 0);
        Interlocked.Exchange(ref _backgroundCheckpointStartCount, 0);
    }

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
        _options = options ?? new PagerOptions();
        ValidateOptions(_options);
        _pageReads = CreatePageReadProvider(device, _options);
        _speculativePageReads = CreateSpeculativePageReadProvider(device, _options, _pageReads);
        _ownsPageReadProviders = true;
        _wal = wal;
        _walIndex = walIndex;
        _interceptor = _options.CreateInterceptor();
        _hasInterceptor = _interceptor is not NoOpPageOperationInterceptor;
        var cache = _options.CreatePageCache();
        if (_options.OnCachePageEvicted != null && cache is IPageCacheEvictionEvents evictingCache)
            evictingCache.PageEvicted += _options.OnCachePageEvicted;
        _buffers = new PageBufferManager(
            cache,
            _pageReads,
            _speculativePageReads,
            _options.MaxCachedWalReadPages,
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
    private Pager(
        IStorageDevice device,
        IPageReadProvider pageReads,
        IPageReadProvider speculativePageReads,
        IWriteAheadLog wal,
        WalIndex walIndex,
        WalSnapshot snapshot,
        bool ownsPageReadProviders,
        PagerOptions? options = null)
    {
        _device = device;
        _pageReads = pageReads;
        _speculativePageReads = speculativePageReads;
        _ownsPageReadProviders = ownsPageReadProviders;
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
            _pageReads,
            _speculativePageReads,
            _options.MaxCachedWalReadPages,
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
        bool needsDedicatedProviders =
            _pageReads is MemoryMappedPageReadProvider ||
            _speculativePageReads is MemoryMappedPageReadProvider;

        var pageReads = needsDedicatedProviders
            ? CreatePageReadProvider(_device, _options)
            : _pageReads;

        var speculativePageReads = needsDedicatedProviders
            ? CreateSpeculativePageReadProvider(_device, _options, pageReads)
            : _speculativePageReads;

        var reader = new Pager(
            _device,
            pageReads,
            speculativePageReads,
            _wal,
            _walIndex,
            snapshot,
            needsDedicatedProviders,
            _options);
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
        RefreshPageReadProviderMapping();
        if (!_wal.IsOpen)
            await _wal.OpenAsync(PageCount, ct);
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

    internal byte[]? TryGetCachedPageAndRecordRead(uint pageId)
    {
        return _buffers.TryGetCachedPageAndRecordRead(pageId);
    }

    internal bool TryGetCachedPageReadBuffer(uint pageId, out PageReadBuffer page)
    {
        return _buffers.TryGetCachedPageReadBuffer(pageId, out page);
    }

    internal bool TryGetCachedPageReadBufferAndRecordRead(uint pageId, out PageReadBuffer page)
    {
        return _buffers.TryGetCachedPageReadBufferAndRecordRead(pageId, out page);
    }

    public ValueTask<byte[]> GetPageAsync(uint pageId, CancellationToken ct = default)
    {
        return _buffers.GetPageAsync(pageId, ct);
    }

    internal ValueTask<PageReadBuffer> GetPageReadAsync(uint pageId, CancellationToken ct = default)
    {
        return _buffers.GetPageReadAsync(pageId, ct);
    }

    internal ValueTask<PageReadBuffer> GetSnapshotPageReadAsync(
        uint pageId,
        WalSnapshot snapshot,
        CancellationToken ct = default)
    {
        return _buffers.GetSnapshotPageReadAsync(pageId, snapshot, ct);
    }

    internal ValueTask<PageReadBuffer> ReadPageUncachedAsync(uint pageId, CancellationToken ct = default)
    {
        return _buffers.ReadPageUncachedAsync(pageId, ct);
    }

    internal bool TryGetSnapshotCachedPageReadBuffer(
        uint pageId,
        WalSnapshot snapshot,
        out PageReadBuffer page)
    {
        return _buffers.TryGetSnapshotCachedPageReadBuffer(pageId, snapshot, out page);
    }

    internal bool CanSpeculativePageReads =>
        _options.EnableSequentialLeafReadAhead &&
        !_buffers.HasInterceptor &&
        (_transactions?.InTransaction != true);

    internal bool UsesReadOnlyPageViews =>
        _options.UseMemoryMappedReads ||
        _options.MaxCachedWalReadPages > 0;

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
        if (!_wal.IsOpen)
            await _wal.OpenAsync(PageCount, ct);
        await _transactions!.BeginAsync(_wal, _options.WriterLockTimeout, ct);
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        PagerCommitResult commit = await BeginCommitAsync(ct);
        await commit.WaitAsync(ct);
    }

    /// <summary>
    /// Starts committing the active transaction and returns a handle that completes
    /// after durable WAL flush and pager post-commit finalization finish.
    /// </summary>
    public async ValueTask<PagerCommitResult> BeginCommitAsync(CancellationToken ct = default)
    {
        if (_transactions is null || !_transactions.InTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "No active transaction to commit.");
        try
        {
            ChangeCounter++;

            // Update file header in page 0
            var page0 = await GetPageAsync(0, ct);
            WriteFileHeaderTo(page0);
            _buffers.AddDirty(0);
            int dirtyCount = _buffers.DirtyPages.Count;

            return _hasInterceptor
                ? await BeginCommitWithInterceptorAsync(dirtyCount, ct)
                : await BeginCommitFastAsync(dirtyCount, ct);
        }
        catch
        {
            await RollbackAsync(ct);
            await ResetPagerStateFromCommittedStorageAsync(ct);
            throw;
        }
    }

    private async ValueTask<PagerCommitResult> BeginCommitWithInterceptorAsync(int dirtyCount, CancellationToken ct)
    {
        await _interceptor.OnCommitStartAsync(dirtyCount, ct);

        uint[]? orderedDirtyPageIds = null;
        int orderedDirtyCount = 0;
        try
        {
            EnforceReaderWalBackpressure(dirtyCount);
            orderedDirtyPageIds = RentSortedPageIds(_buffers.DirtyPages, out orderedDirtyCount);
            long walAppendStartTicks = Stopwatch.GetTimestamp();

            // Write all dirty pages to WAL with per-page interceptor hooks.
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

            WalCommitResult commitResult = await PrepareWalCommitAsync(ct);
            RecordWalAppendDiagnostics(walAppendStartTicks);
            _buffers.ClearDirty();
            long transactionId = _transactions!.ReleaseWriterAfterCommitAppend();
            return new PagerCommitResult(CompleteCommitWithInterceptorAsync(commitResult, transactionId, dirtyCount));
        }
        catch
        {
            await _interceptor.OnCommitEndAsync(dirtyCount, succeeded: false, ct);
            throw;
        }
        finally
        {
            if (orderedDirtyPageIds != null)
                ArrayPool<uint>.Shared.Return(orderedDirtyPageIds, clearArray: false);
        }
    }

    private async ValueTask<PagerCommitResult> BeginCommitFastAsync(int dirtyCount, CancellationToken ct)
    {
        EnforceReaderWalBackpressure(dirtyCount);

        // Fast path: no interceptor — batch WAL appends to reduce per-frame write overhead.
        uint[]? orderedDirtyPageIds = null;
        int orderedDirtyCount = 0;
        WalFrameWrite[]? frameBatch = null;
        int frameCount = 0;
        try
        {
            long walAppendStartTicks = Stopwatch.GetTimestamp();
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

            WalCommitResult commitResult = frameCount > 0
                ? await _wal.AppendFramesAndCommitAsync(frameBatch.AsMemory(0, frameCount), PageCount, ct)
                : await _wal.CommitAsync(PageCount, ct);

            RecordWalAppendDiagnostics(walAppendStartTicks);
            _buffers.ClearDirty();
            long transactionId = _transactions!.ReleaseWriterAfterCommitAppend();
            return new PagerCommitResult(CompleteCommitAsync(commitResult, transactionId));
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
    }

    private async Task CompleteCommitAsync(WalCommitResult commitResult, long transactionId)
    {
        try
        {
            await commitResult.WaitAsync();
            await FinalizeCommitAndCheckpointAsync(transactionId, CancellationToken.None);
        }
        catch
        {
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
    }

    private async Task CompleteCommitWithInterceptorAsync(WalCommitResult commitResult, long transactionId, int dirtyCount)
    {
        bool commitSucceeded = false;
        try
        {
            await commitResult.WaitAsync();
            await FinalizeCommitAndCheckpointAsync(transactionId, CancellationToken.None);
            commitSucceeded = true;
        }
        catch
        {
            await ResetPagerStateFromCommittedStorageAsync(CancellationToken.None);
            throw;
        }
        finally
        {
            await _interceptor.OnCommitEndAsync(dirtyCount, commitSucceeded, CancellationToken.None);
        }
    }

    private ValueTask<WalCommitResult> PrepareWalCommitAsync(CancellationToken ct)
    {
        // Commit the WAL (makes frames durable and visible to new readers)
        return _wal.CommitAsync(PageCount, ct);
    }

    private async ValueTask FinalizeCommitAndCheckpointAsync(long transactionId, CancellationToken ct)
    {
        long finalizeStartTicks = Stopwatch.GetTimestamp();
        _transactions!.CompleteCommit(transactionId);

        // Auto-checkpoint according to policy
        long checkpointDecisionStartTicks = Stopwatch.GetTimestamp();
        bool shouldCheckpoint = _checkpoints!.ShouldCheckpoint(
            CheckpointPolicy,
            _walIndex.FrameCount,
            CheckpointThreshold,
            EstimateCommittedWalBytes(_walIndex.FrameCount));
        if (shouldCheckpoint)
            _checkpoints.RequestDeferredCheckpoint();
        RecordCheckpointDecisionDiagnostics(checkpointDecisionStartTicks);

        bool backgroundMode = _options.AutoCheckpointExecutionMode == AutoCheckpointExecutionMode.Background;
        bool shouldRunForegroundCheckpoint =
            !backgroundMode &&
            _checkpoints.HasPendingCheckpointRequest;
        RecordFinalizeCommitDiagnostics(finalizeStartTicks);

        if (backgroundMode)
        {
            ScheduleBackgroundCheckpointIfNeeded();
            return;
        }

        if (shouldRunForegroundCheckpoint)
            await CheckpointAsync(ct);
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_transactions is null || !_transactions.TryBeginRollback())
            return;

        // Truncate uncommitted frames from WAL
        await _wal.RollbackAsync(ct);

        _transactions.CompleteRollback();
        await ResetPagerStateFromCommittedStorageAsync(ct);
    }

    private void ReadFileHeaderFrom(byte[] page0)
    {
        PageCount = BitConverter.ToUInt32(page0, PageConstants.PageCountOffset);
        SchemaRootPage = BitConverter.ToUInt32(page0, PageConstants.SchemaRootPageOffset);
        FreelistHead = BitConverter.ToUInt32(page0, PageConstants.FreelistHeadOffset);
        ChangeCounter = BitConverter.ToUInt32(page0, PageConstants.ChangeCounterOffset);
    }

    private async ValueTask ResetPagerStateFromCommittedStorageAsync(CancellationToken ct)
    {
        _buffers.ClearAll();

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

        IDisposable? checkpointBarrier = _transactions is not null
            ? await _transactions.AcquireCheckpointBarrierAsync(ct)
            : null;

        try
        {
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
        finally
        {
            checkpointBarrier?.Dispose();
        }
    }

    private async ValueTask RunBackgroundCheckpointStepWithInterceptorsAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return;

        IDisposable? checkpointBarrier = _transactions is not null
            ? await _transactions.AcquireCheckpointBarrierAsync(ct)
            : null;

        try
        {
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
        finally
        {
            checkpointBarrier?.Dispose();
        }
    }

    private async ValueTask<bool> RunBackgroundCheckpointStepAsync(CancellationToken ct)
    {
        if (_checkpoints is null)
            return false;
        if (_transactions?.InTransaction == true || _wal.HasPendingCommitWork)
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
        ProcessCrashInjector.TripIfRequested(
            "checkpoint-after-wal-finalize",
            "checkpoint-after-wal-finalize");
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
        _buffers.InvalidateCheckpointTransientReads(_options.PreserveOwnedPagesOnCheckpoint);
        RefreshPageReadProviderMapping();
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
        {
            if (_ownsPageReadProviders)
                await DisposePageReadProviderAsync();
            return; // Snapshot readers don't own resources
        }

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

        if (_ownsPageReadProviders)
            await DisposePageReadProviderAsync();

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
        {
            if (_ownsPageReadProviders)
                DisposePageReadProvider();
            return;
        }

        if (_transactions?.InTransaction == true)
            RollbackAsync().AsTask().GetAwaiter().GetResult();
        WaitForBackgroundCheckpointAsync().AsTask().GetAwaiter().GetResult();
        if (_ownsPageReadProviders)
            DisposePageReadProvider();
        _device.Dispose();
        _checkpoints?.Dispose();
        _transactions?.Dispose();
    }

    private void ScheduleBackgroundCheckpointIfNeeded()
    {
        if (_isSnapshotReader ||
            _checkpoints is null ||
            _options.AutoCheckpointExecutionMode != AutoCheckpointExecutionMode.Background ||
            _transactions?.InTransaction == true ||
            _wal.HasPendingCommitWork)
        {
            return;
        }

        if (_checkpoints.TryStartBackgroundCheckpoint(RunBackgroundCheckpointStepWithInterceptorsAsync))
            Interlocked.Increment(ref _backgroundCheckpointStartCount);
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

    private void RecordWalAppendDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _walAppendCount);
        Interlocked.Add(ref _walAppendTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordFinalizeCommitDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _finalizeCommitCount);
        Interlocked.Add(ref _finalizeCommitTicks, Stopwatch.GetTimestamp() - startTicks);
    }

    private void RecordCheckpointDecisionDiagnostics(long startTicks)
    {
        if (startTicks == 0)
            return;

        Interlocked.Increment(ref _checkpointDecisionCount);
        Interlocked.Add(ref _checkpointDecisionTicks, Stopwatch.GetTimestamp() - startTicks);
    }

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

        if (options.MaxCachedWalReadPages < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options.MaxCachedWalReadPages),
                options.MaxCachedWalReadPages,
                "MaxCachedWalReadPages must be greater than or equal to zero.");
        }
    }

    private static IPageReadProvider CreatePageReadProvider(IStorageDevice device, PagerOptions? options)
    {
        if (options?.UseMemoryMappedReads == true && device is FileStorageDevice fileDevice)
            return new MemoryMappedPageReadProvider(fileDevice);

        return new StorageDevicePageReadProvider(device);
    }

    private static IPageReadProvider CreateSpeculativePageReadProvider(
        IStorageDevice device,
        PagerOptions? options,
        IPageReadProvider primaryPageReads)
    {
        if (options?.UseMemoryMappedReads == true && device is FileStorageDevice)
            return primaryPageReads;

        if (device is FileStorageDevice fileDevice)
            return new StorageDevicePageReadProvider(fileDevice, useSequentialAccessHint: true);

        return primaryPageReads;
    }

    private void RefreshPageReadProviderMapping()
    {
        if (_pageReads is MemoryMappedPageReadProvider mappedReads)
            mappedReads.RefreshMapping();
    }

    private async ValueTask DisposePageReadProviderAsync()
    {
        if (ReferenceEquals(_pageReads, _device))
            return;

        await DisposePageReadProviderInstanceAsync(_speculativePageReads);
        if (!ReferenceEquals(_speculativePageReads, _pageReads))
            await DisposePageReadProviderInstanceAsync(_pageReads);
    }

    private void DisposePageReadProvider()
    {
        if (ReferenceEquals(_pageReads, _device))
            return;

        DisposePageReadProviderInstance(_speculativePageReads);
        if (!ReferenceEquals(_speculativePageReads, _pageReads))
            DisposePageReadProviderInstance(_pageReads);
    }

    private async ValueTask DisposePageReadProviderInstanceAsync(IPageReadProvider pageReads)
    {
        if (ReferenceEquals(pageReads, _device))
            return;

        if (pageReads is IAsyncDisposable asyncPageReads)
            await asyncPageReads.DisposeAsync();
        else if (pageReads is IDisposable disposablePageReads)
            disposablePageReads.Dispose();
    }

    private void DisposePageReadProviderInstance(IPageReadProvider pageReads)
    {
        if (ReferenceEquals(pageReads, _device))
            return;

        if (pageReads is IDisposable disposablePageReads)
            disposablePageReads.Dispose();
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

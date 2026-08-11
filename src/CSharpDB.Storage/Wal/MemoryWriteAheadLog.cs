using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Paging;
using System.Buffers;
using System.Buffers.Binary;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory WAL implementation that preserves the same frame/header format as the file-backed WAL.
/// This allows load-from-disk recovery to run entirely in memory.
/// </summary>
public sealed class MemoryWriteAheadLog : IWriteAheadLog, IWalRuntimeDiagnosticsProvider, ICommitPathDiagnosticsProvider,
    ILiveWalRuntimeSnapshotProvider, IWalRecoveryRuntimeSnapshotProvider,
    IWalCheckpointRuntimeSnapshotProvider
{
    private const int RuntimeDiagnosticsSnapshotMaxAttempts = 3;
    private const int AppendFrameChunkSize = 16;
    private const int CheckpointWriteChunkPages = 16;
    private static readonly IComparer<KeyValuePair<uint, long>> PageIdComparer =
        Comparer<KeyValuePair<uint, long>>.Create(static (left, right) => left.Key.CompareTo(right.Key));

    private readonly record struct RecoveryScanResult(
        long ScannedFrameCount,
        long ScannedBytes,
        long RecoveredFrameCount,
        long RecoveredBytes,
        long DiscardedFrameCount,
        long DiscardedBytes,
        StorageRecoveryTruncationReasonRaw TruncationReason)
    {
        internal static RecoveryScanResult Empty { get; } = new(
            0,
            0,
            0,
            0,
            0,
            0,
            StorageRecoveryTruncationReasonRaw.None);
    }

    private readonly MemoryStorageDevice _storage;
    private readonly WalIndex _index;
    private readonly IPageChecksumProvider _checksumProvider;
    private readonly bool _useAdditiveHeaderChecksum;
    private readonly byte[] _seedBytes;

    private bool _isOpen;
    private bool _seedConsumed;
    private uint _salt1;
    private uint _salt2;
    private readonly List<(uint PageId, long WalOffset)> _uncommittedFrames = new(capacity: 256);
    private readonly List<BufferedUncommittedFrame> _bufferedUncommittedFrames = new(capacity: 256);
    private uint _lastUncommittedDataChecksum;
    private readonly List<(uint PageId, long WalOffset)> _recoverUncommittedBatch = new();
    private long _uncommittedStartOffset;
    private readonly byte[] _walHeaderBuffer = new byte[PageConstants.WalHeaderSize];
    private readonly byte[] _appendFrameHeader = new byte[PageConstants.WalFrameHeaderSize];
    private readonly byte[] _appendFrameBuffer = new byte[PageConstants.WalFrameSize];
    private readonly byte[] _appendFrameChunkBuffer = new byte[PageConstants.WalFrameSize * AppendFrameChunkSize];
    private readonly byte[] _recoveryFrameHeaderBuffer = new byte[PageConstants.WalFrameHeaderSize];
    private readonly byte[] _recoveryPageBuffer = new byte[PageConstants.PageSize];
    private byte[]? _checkpointReadBuffer;
    private byte[]? _checkpointWriteBuffer;
    private long[]? _checkpointBatchWalOffsets;
    private KeyValuePair<uint, long>[] _checkpointCommittedPages = Array.Empty<KeyValuePair<uint, long>>();
    private IncrementalCheckpointState? _incrementalCheckpoint;
    private long _runtimeRetainedWalStartOffset = -1;
    private readonly bool _recoveryRuntimeDiagnosticsEnabled;
    private StorageRecoveryRuntimeRawSnapshot _recoveryRuntimeSnapshot;
    private int _recoveryRuntimeSnapshotVersion;
    private int _hasRecoveryRuntimeSnapshot;
    private long _recoveryAttemptCount;
    private long _runtimeCheckpointCompletedPageCount;
    private long _runtimeCheckpointTotalPageCount;
    private int _runtimeCheckpointProgressVersion;

    // Internal deterministic interleaving seam; production never assigns it.
    internal Action? RuntimeDiagnosticsBetweenSnapshotSamplesForTests { get; set; }

    public MemoryWriteAheadLog(
        WalIndex index,
        IPageChecksumProvider? checksumProvider = null,
        ReadOnlyMemory<byte> initialBytes = default)
        : this(
            index,
            checksumProvider,
            initialBytes,
            runtimeDiagnosticsObserver: null)
    {
    }

    internal MemoryWriteAheadLog(
        WalIndex index,
        IPageChecksumProvider? checksumProvider,
        ReadOnlyMemory<byte> initialBytes,
        IStorageRuntimeDiagnosticsObserver? runtimeDiagnosticsObserver)
    {
        _storage = new MemoryStorageDevice(initialBytes);
        _seedBytes = initialBytes.IsEmpty ? Array.Empty<byte>() : initialBytes.ToArray();
        _index = index;
        _checksumProvider = checksumProvider ?? new AdditiveChecksumProvider();
        _useAdditiveHeaderChecksum = _checksumProvider is AdditiveChecksumProvider;
        _recoveryRuntimeDiagnosticsEnabled = runtimeDiagnosticsObserver is not null;
    }

    public async ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _isOpen))
            return;

        if (!_seedConsumed && _seedBytes.Length > 0)
        {
            _seedConsumed = true;
            long attemptCount = BeginRecoveryRuntimeAttempt();
            RecoveryScanResult result = await RecoverAsync(attemptCount, cancellationToken);
            PublishRecoveryRuntimeScan(result, attemptCount);
            return;
        }

        _seedConsumed = true;
        long createAttemptCount = BeginRecoveryRuntimeAttempt();
        try
        {
            await CreateNewAsync(currentDbPageCount, cancellationToken);
            PublishRecoveryRuntimeScan(RecoveryScanResult.Empty, createAttemptCount);
        }
        catch (Exception ex)
        {
            PublishRecoveryRuntimeFailure(ex, RecoveryScanResult.Empty, createAttemptCount);
            throw;
        }
    }

    public bool HasPendingCheckpoint => _incrementalCheckpoint is not null;
    public bool IsCheckpointCopyComplete =>
        _incrementalCheckpoint is not null &&
        _incrementalCheckpoint.NextPageIndex >= _incrementalCheckpoint.CommittedPageCount;
    public bool TryGetCheckpointRetainedWalStartOffset(out long walOffset)
    {
        if (_incrementalCheckpoint is { } checkpoint)
        {
            walOffset = checkpoint.RetainedWalStartOffset;
            return true;
        }

        walOffset = 0;
        return false;
    }
    public bool HasPendingCommitWork => false;
    public bool IsOpen => Volatile.Read(ref _isOpen);

    bool ILiveWalRuntimeSnapshotProvider.TryGetLiveRuntimeDiagnosticsSnapshot(
        out WalRuntimeRawSnapshot snapshot)
    {
        for (int attempt = 0; attempt < RuntimeDiagnosticsSnapshotMaxAttempts; attempt++)
        {
            if (!Volatile.Read(ref _isOpen))
                break;

            WalRuntimeRawCaptureState first = CaptureLiveRuntimeDiagnosticsState();
            RuntimeDiagnosticsBetweenSnapshotSamplesForTests?.Invoke();
            WalRuntimeRawCaptureState second = CaptureLiveRuntimeDiagnosticsState();
            if (first != second ||
                !Volatile.Read(ref _isOpen) ||
                !second.TryCreateSnapshot(out snapshot))
            {
                continue;
            }

            return true;
        }

        snapshot = default;
        return false;
    }

    bool IWalRecoveryRuntimeSnapshotProvider.TryGetRecoveryRuntimeSnapshot(
        out StorageRecoveryRuntimeRawSnapshot snapshot)
    {
        if (!_recoveryRuntimeDiagnosticsEnabled ||
            Volatile.Read(ref _hasRecoveryRuntimeSnapshot) == 0)
        {
            snapshot = default;
            return false;
        }

        for (int attempt = 0; attempt < RuntimeDiagnosticsSnapshotMaxAttempts; attempt++)
        {
            int firstVersion = Volatile.Read(ref _recoveryRuntimeSnapshotVersion);
            if ((firstVersion & 1) != 0)
                continue;

            StorageRecoveryRuntimeRawSnapshot candidate = _recoveryRuntimeSnapshot;
            int secondVersion = Volatile.Read(ref _recoveryRuntimeSnapshotVersion);
            if (firstVersion == secondVersion && (secondVersion & 1) == 0)
            {
                snapshot = candidate;
                return true;
            }
        }

        snapshot = default;
        return false;
    }

    bool IWalCheckpointRuntimeSnapshotProvider.TryGetCheckpointProgressSnapshot(
        out WalCheckpointProgressRawSnapshot snapshot)
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
        {
            snapshot = default;
            return false;
        }

        for (int attempt = 0; attempt < RuntimeDiagnosticsSnapshotMaxAttempts; attempt++)
        {
            int firstVersion = Volatile.Read(ref _runtimeCheckpointProgressVersion);
            if ((firstVersion & 1) != 0)
                continue;

            long retainedWalStartOffset = Volatile.Read(ref _runtimeRetainedWalStartOffset);
            long completedPageCount = Interlocked.Read(ref _runtimeCheckpointCompletedPageCount);
            long totalPageCount = Interlocked.Read(ref _runtimeCheckpointTotalPageCount);
            long logicalBytes = _storage.Length;
            int secondVersion = Volatile.Read(ref _runtimeCheckpointProgressVersion);
            if (firstVersion != secondVersion || (secondVersion & 1) != 0)
                continue;

            if (retainedWalStartOffset < 0 ||
                completedPageCount < 0 ||
                totalPageCount < completedPageCount)
            {
                snapshot = default;
                return false;
            }

            snapshot = new WalCheckpointProgressRawSnapshot(
                completedPageCount,
                totalPageCount,
                logicalBytes > retainedWalStartOffset);
            return true;
        }

        snapshot = default;
        return false;
    }

    private WalRuntimeRawCaptureState CaptureLiveRuntimeDiagnosticsState()
    {
        var indexState = _index.GetRuntimeStateSnapshot();
        return new WalRuntimeRawCaptureState(
            LogicalBytes: _storage.Length,
            AllocatedBytes: null,
            RetainedWalStartOffset: Volatile.Read(
                ref _runtimeRetainedWalStartOffset),
            PendingCommitCount: 0,
            FrameCount: indexState.FrameCount,
            LogicalCommitCount: indexState.LogicalCommitCount,
            LogicalPageWriteCount: indexState.LogicalPageWriteCount,
            CommitFlushBatchCount: null,
            CommittedFrameBytesWritten: null);
    }

    WalFlushDiagnosticsSnapshot IWalRuntimeDiagnosticsProvider.GetWalFlushDiagnosticsSnapshot() =>
        WalFlushDiagnosticsSnapshot.Empty;

    void IWalRuntimeDiagnosticsProvider.ResetWalFlushDiagnostics()
    {
    }

    CommitPathDiagnosticsSnapshot ICommitPathDiagnosticsProvider.GetCommitPathDiagnosticsSnapshot() => new(
        WalAppendCount: 0,
        WalAppendTicks: 0,
        ExplicitCommitLockWaitCount: 0,
        ExplicitCommitLockWaitTicks: 0,
        ExplicitCommitLockHoldCount: 0,
        ExplicitCommitLockHoldTicks: 0,
        ExplicitConflictResolutionCount: 0,
        ExplicitConflictResolutionTicks: 0,
        ExplicitLeafRebaseAttemptCount: 0,
        ExplicitLeafRebaseSuccessCount: 0,
        ExplicitLeafRebaseStructuralRejectCount: 0,
        ExplicitLeafRebaseCapacityRejectCount: 0,
        ExplicitPendingLeafRebaseAttemptCount: 0,
        ExplicitPendingLeafRebaseSuccessCount: 0,
        ExplicitPendingLeafRebaseRejectCount: 0,
        ExplicitLeafRebaseRejectNonInsertOnlyCount: 0,
        ExplicitLeafRebaseRejectDuplicateKeyCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackPreconditionCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackMissingTraversalCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackDirtyAncestorCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackParentBoundaryCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackTargetPageDirtyCount: 0,
        ExplicitLeafRebaseRejectDirtyParentMissingParentPageCount: 0,
        ExplicitLeafRebaseRejectDirtyParentTransactionLeafNotSplitCount: 0,
        ExplicitLeafRebaseRejectDirtyParentBaseBoundaryMissingCount: 0,
        ExplicitLeafRebaseRejectDirtyParentInsertionShapeCount: 0,
        ExplicitLeafRebaseRejectDirtyParentInsertionMismatchCount: 0,
        ExplicitLeafRebaseRejectDirtyParentMissingLocalRightPageCount: 0,
        ExplicitLeafRebaseRejectDirtyParentLocalSplitShapeCount: 0,
        ExplicitLeafRebaseRejectDirtyParentRebaseFailureCount: 0,
        ExplicitLeafRebaseRejectDirtyParentDescribedInsertionMatchCount: 0,
        ExplicitLeafRebaseRejectSplitFallbackShapeCount: 0,
        ExplicitLeafRebaseRejectOtherCount: 0,
        ExplicitInteriorRebaseAttemptCount: 0,
        ExplicitInteriorRebaseSuccessCount: 0,
        ExplicitInteriorRebaseStructuralRejectCount: 0,
        ExplicitInteriorRebaseCapacityRejectCount: 0,
        ExplicitPendingCommitWaitCount: 0,
        ExplicitPendingCommitWaitTicks: 0,
        ExplicitHeaderPreparationCount: 0,
        ExplicitHeaderPreparationTicks: 0,
        ExplicitPendingCommitReservationCount: 0,
        ExplicitPendingCommitReservationTicks: 0,
        DurableBatchWindowWaitCount: 0,
        DurableBatchWindowWaitTicks: 0,
        PendingCommitWriteCount: 0,
        PendingCommitWriteTicks: 0,
        PendingCommitDrainCount: 0,
        PendingCommitDrainTicks: 0,
        BufferedFlushCount: 0,
        BufferedFlushTicks: 0,
        DurableFlushCount: 0,
        DurableFlushTicks: 0,
        PublishBatchCount: 0,
        PublishBatchTicks: 0,
        FinalizeCommitCount: 0,
        FinalizeCommitTicks: 0,
        CheckpointDecisionCount: 0,
        CheckpointDecisionTicks: 0,
        BackgroundCheckpointStartCount: 0,
        BTreeLeafSplitCount: 0,
        BTreeRightEdgeLeafSplitCount: 0,
        BTreeInteriorInsertCount: 0,
        BTreeRightEdgeInteriorInsertCount: 0,
        BTreeInteriorSplitCount: 0,
        BTreeRightEdgeInteriorSplitCount: 0,
        BTreeRootSplitCount: 0,
        HashedIndexAppendContextHitCount: 0,
        HashedIndexAppendContextMissCount: 0,
        HashedIndexAppendExternalMetadataReadCount: 0,
        HashedIndexAppendPromotionCount: 0,
        HashedIndexAppendNotApplicableCount: 0,
        HashedIndexDeferredAppendCount: 0,
        HashedIndexDeferredFlushCount: 0,
        MaxPendingCommitCount: 0,
        MaxPendingCommitBytes: 0,
        BTreeResourceDiagnostics: []);

    void ICommitPathDiagnosticsProvider.ResetCommitPathDiagnostics()
    {
    }

    public void BeginTransaction()
    {
        EnsureOpen();
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _uncommittedStartOffset = _storage.Length;
    }

    public ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        if (_uncommittedFrames.Count > 0)
            return AppendFrameDirectAsync(pageId, pageData, cancellationToken);

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            _bufferedUncommittedFrames.Add(CreateBufferedUncommittedFrame(pageId, pageData));
            return ValueTask.CompletedTask;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to buffer in-memory WAL frame for pageId={pageId}.",
                ex);
        }
    }

    public async ValueTask AppendFramesAsync(ReadOnlyMemory<WalFrameWrite> frames, CancellationToken cancellationToken = default)
    {
        if (_bufferedUncommittedFrames.Count > 0)
        {
            await AppendBufferedFramesCoreAsync(
                commitOnLastFrame: false,
                newDbPageCount: 0u,
                trackUncommittedFrames: true,
                cancellationToken);
            ClearBufferedUncommittedFrames();
        }

        await AppendFramesCoreAsync(
            frames,
            commitOnLastFrame: false,
            newDbPageCount: 0u,
            trackUncommittedFrames: true,
            cancellationToken);
    }

    public async ValueTask<WalCommitResult> AppendFramesAndCommitAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        uint newDbPageCount,
        CancellationToken cancellationToken = default)
    {
        if (frames.IsEmpty)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");
        if (_uncommittedFrames.Count != 0 || _bufferedUncommittedFrames.Count != 0)
            throw new CSharpDbException(ErrorCode.WalError, "AppendFramesAndCommitAsync cannot be used with existing uncommitted frames.");

        EnsureOpen();

        long firstFrameOffset = _storage.Length;
        await AppendFramesCoreAsync(
            frames,
            commitOnLastFrame: true,
            newDbPageCount,
            trackUncommittedFrames: false,
            cancellationToken);

        try
        {
            await _storage.FlushAsync(cancellationToken);
            PublishCommittedFramesFromBatch(frames, firstFrameOffset);
            return WalCommitResult.Completed;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append+commit {frames.Length} in-memory WAL frame(s), newDbPageCount={newDbPageCount}.",
                ex);
        }
    }

    public async ValueTask<WalCommitResult> CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        if (_uncommittedFrames.Count == 0 && _bufferedUncommittedFrames.Count == 0)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");

        if (_bufferedUncommittedFrames.Count > 0)
        {
            int bufferedFrameCount = _bufferedUncommittedFrames.Count;
            try
            {
                await AppendBufferedFramesCoreAsync(
                    commitOnLastFrame: true,
                    newDbPageCount,
                    trackUncommittedFrames: true,
                    cancellationToken);
                ClearBufferedUncommittedFrames();
                await _storage.FlushAsync(cancellationToken);
                PublishCommittedFrames();
                return WalCommitResult.Completed;
            }
            catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Failed to commit buffered in-memory WAL transaction with {bufferedFrameCount} frame(s), newDbPageCount={newDbPageCount}.",
                    ex);
            }
        }

        var (lastPageId, lastOffset) = _uncommittedFrames[^1];
        uint lastDataChecksum = _lastUncommittedDataChecksum;
        var frameHeader = _appendFrameHeader;
        var frameHeaderSpan = frameHeader.AsSpan(0, PageConstants.WalFrameHeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(0, 4), lastPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(4, 4), newDbPageCount);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(8, 4), _salt1);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(12, 4), _salt2);

        uint headerChecksum = ComputeHeaderChecksum(
            lastPageId,
            newDbPageCount,
            _salt1,
            _salt2,
            frameHeaderSpan.Slice(0, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(16, 4), headerChecksum);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(20, 4), lastDataChecksum);

        try
        {
            await _storage.WriteAsync(lastOffset, frameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);
            await _storage.FlushAsync(cancellationToken);
            PublishCommittedFrames();
            return WalCommitResult.Completed;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to commit in-memory WAL transaction with {_uncommittedFrames.Count} frame(s), newDbPageCount={newDbPageCount}, commitFrameOffset={lastOffset}.",
                ex);
        }
    }

    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (!Volatile.Read(ref _isOpen))
            return;

        if (_uncommittedFrames.Count == 0)
        {
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            return;
        }

        try
        {
            await _storage.SetLengthAsync(_uncommittedStartOffset, cancellationToken);
            await _storage.FlushAsync(cancellationToken);
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to rollback in-memory WAL transaction to offset {_uncommittedStartOffset}.",
                ex);
        }
    }

    public async ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default)
    {
        var page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        await ReadPageIntoAsync(walFrameOffset, page, cancellationToken);
        return page;
    }

    public async ValueTask ReadPageIntoAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

        long dataOffset = walFrameOffset + PageConstants.WalFrameHeaderSize;
        int bytesRead = await _storage.ReadAsync(dataOffset, destination, cancellationToken);
        if (bytesRead != destination.Length)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Short in-memory WAL read at walFrameOffset={walFrameOffset} (expected {destination.Length} bytes, read {bytesRead}).");
        }
    }

    public async ValueTask CheckpointAsync(
        IStorageDevice device,
        uint pageCount,
        CancellationToken cancellationToken = default,
        bool allowFinalize = true)
    {
        while (!await CheckpointStepAsync(device, pageCount, int.MaxValue, cancellationToken, allowFinalize))
        {
            if (!allowFinalize && IsCheckpointCopyComplete)
                return;
        }
    }

    public async ValueTask<bool> CheckpointStepAsync(
        IStorageDevice device,
        uint pageCount,
        int maxPages,
        CancellationToken cancellationToken = default,
        bool allowFinalize = true)
    {
        if (!Volatile.Read(ref _isOpen))
            return true;

        if (maxPages <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPages), "Value must be greater than zero.");

        long requiredLength = 0;
        try
        {
            var checkpoint = EnsureIncrementalCheckpointState();
            if (checkpoint is null)
                return true;

            requiredLength = (long)pageCount * PageConstants.PageSize;
            if (device.Length < requiredLength)
                await device.SetLengthAsync(requiredLength, cancellationToken);

            int remainingPageCount = checkpoint.CommittedPageCount - checkpoint.NextPageIndex;
            if (remainingPageCount > 0)
            {
                int pagesToProcess = Math.Min(maxPages, remainingPageCount);
                await FlushCheckpointSliceAsync(device, checkpoint, pagesToProcess, cancellationToken);
                checkpoint.NextPageIndex += pagesToProcess;
                PublishCheckpointProgressMirror(checkpoint);

                if (checkpoint.NextPageIndex < checkpoint.CommittedPageCount)
                    return false;
            }

            await device.FlushAsync(cancellationToken);

            if (!allowFinalize)
                return false;

            await FinalizeIncrementalCheckpointAsync(pageCount, cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            int committedPageCount = _incrementalCheckpoint?.CommittedPageCount ?? _index.FrameCount;
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to checkpoint in-memory WAL with {committedPageCount} committed page(s), pageCount={pageCount}, requiredLength={requiredLength}, deviceLength={device.Length}.",
                ex);
        }
    }

    public ValueTask CloseAndDeleteAsync()
    {
        Volatile.Write(ref _isOpen, false);
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        ClearIncrementalCheckpointState();
        _index.Reset();
        _storage.SetLengthAsync(0).GetAwaiter().GetResult();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        Volatile.Write(ref _isOpen, false);
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        ClearIncrementalCheckpointState();
        return ValueTask.CompletedTask;
    }

    private async ValueTask CreateNewAsync(uint dbPageCount, CancellationToken cancellationToken)
    {
        ClearIncrementalCheckpointState();
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _index.Reset();
        Volatile.Write(ref _isOpen, true);
        _salt1 = (uint)Random.Shared.Next();
        _salt2 = (uint)Random.Shared.Next();
        WriteWalHeader(_walHeaderBuffer, dbPageCount);
        await _storage.SetLengthAsync(0, cancellationToken);
        await _storage.WriteAsync(0, _walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
        await _storage.FlushAsync(cancellationToken);
        _uncommittedStartOffset = PageConstants.WalHeaderSize;
    }

    private async ValueTask<RecoveryScanResult> RecoverAsync(
        long attemptCount,
        CancellationToken cancellationToken)
    {
        long originalLength = _storage.Length;
        long scannedBytes = 0;
        bool frameRegionScanned = false;
        StorageRecoveryTruncationReasonRaw truncationReason =
            StorageRecoveryTruncationReasonRaw.Unknown;

        try
        {
        ClearIncrementalCheckpointState();
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _index.Reset();
        Volatile.Write(ref _isOpen, true);

        var header = _walHeaderBuffer;
        if (await _storage.ReadAsync(0, header, cancellationToken) != PageConstants.WalHeaderSize)
            throw new CSharpDbException(ErrorCode.WalError, "Invalid in-memory WAL: header too short.");

        if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
            throw new CSharpDbException(ErrorCode.WalError, "Invalid in-memory WAL: bad magic.");

        _salt1 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        _salt2 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(20, 4));
        frameRegionScanned = true;

        var uncommittedBatch = _recoverUncommittedBatch;
        uncommittedBatch.Clear();
        var frameHeaderBuffer = _recoveryFrameHeaderBuffer;
        var pageDataBuffer = _recoveryPageBuffer;
        long offset = PageConstants.WalHeaderSize;
        long frameRegionBytes = originalLength - PageConstants.WalHeaderSize;
        long completeFrameBytes =
            frameRegionBytes / PageConstants.WalFrameSize * PageConstants.WalFrameSize;
        long completeFrameEnd = PageConstants.WalHeaderSize + completeFrameBytes;
        long incompleteTailBytes = frameRegionBytes - completeFrameBytes;
        long truncateAt = originalLength;
        bool scanStoppedEarly = false;
        truncationReason = StorageRecoveryTruncationReasonRaw.None;
        if (completeFrameEnd < originalLength)
        {
            truncateAt = completeFrameEnd;
            truncationReason = StorageRecoveryTruncationReasonRaw.IncompleteTail;
        }

        while (offset + PageConstants.WalFrameSize <= completeFrameEnd)
        {
            long frameOffset = offset;
            int headerRead = await _storage.ReadAsync(offset, frameHeaderBuffer, cancellationToken);
            scannedBytes = AddScannedBytes(
                scannedBytes,
                headerRead,
                frameRegionBytes);
            int dataRead = await _storage.ReadAsync(offset + PageConstants.WalFrameHeaderSize, pageDataBuffer, cancellationToken);
            scannedBytes = AddScannedBytes(
                scannedBytes,
                dataRead,
                frameRegionBytes);
            offset += PageConstants.WalFrameSize;

            if (headerRead != PageConstants.WalFrameHeaderSize || dataRead != PageConstants.PageSize)
            {
                scanStoppedEarly = true;
                ConsiderRecoveryTruncation(
                    frameOffset,
                    StorageRecoveryTruncationReasonRaw.IncompleteTail,
                    ref truncateAt,
                    ref truncationReason);
                break;
            }

            uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
            uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
            if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
            {
                scanStoppedEarly = true;
                ConsiderRecoveryTruncation(
                    frameOffset,
                    StorageRecoveryTruncationReasonRaw.SaltMismatch,
                    ref truncateAt,
                    ref truncationReason);
                break;
            }

            uint expectedHeaderChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(16, 4));
            uint expectedDataChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(20, 4));
            uint actualHeaderChecksum = ComputeHeaderChecksum(
                BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4)),
                BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4)),
                BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4)),
                BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4)),
                frameHeaderBuffer.AsSpan(0, 16));
            uint actualDataChecksum = _checksumProvider.Compute(pageDataBuffer);

            if (expectedHeaderChecksum != actualHeaderChecksum || expectedDataChecksum != actualDataChecksum)
            {
                scanStoppedEarly = true;
                ConsiderRecoveryTruncation(
                    frameOffset,
                    StorageRecoveryTruncationReasonRaw.ChecksumMismatch,
                    ref truncateAt,
                    ref truncationReason);
                break;
            }

            uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
            uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4));
            uncommittedBatch.Add((pageId, frameOffset));

            if (dbPageCount != 0)
            {
                foreach (var (committedPageId, committedOffset) in uncommittedBatch)
                    _index.AddCommittedFrame(committedPageId, committedOffset);

                _index.AdvanceRecoveredCommit();
                uncommittedBatch.Clear();
            }
        }

        if (!scanStoppedEarly && incompleteTailBytes > 0)
        {
            scannedBytes = AddScannedBytes(
                scannedBytes,
                incompleteTailBytes,
                frameRegionBytes);
        }

        if (uncommittedBatch.Count > 0)
        {
            ConsiderRecoveryTruncation(
                uncommittedBatch[0].WalOffset,
                StorageRecoveryTruncationReasonRaw.UncommittedTail,
                ref truncateAt,
                ref truncationReason);
        }

        uncommittedBatch.Clear();
        if (truncateAt < originalLength)
            await _storage.SetLengthAsync(truncateAt, cancellationToken);

        _uncommittedStartOffset = _storage.Length;
        return CreateRecoveryScanResult(
            originalLength,
            _storage.Length,
            scannedBytes,
            _index.FrameCount,
            truncationReason);
        }
        catch (Exception ex)
        {
            PublishRecoveryRuntimeFailure(
                ex,
                CreateFailedRecoveryScanResult(
                    frameRegionScanned,
                    originalLength,
                    scannedBytes,
                    truncationReason),
                attemptCount);
            throw;
        }
    }

    private long BeginRecoveryRuntimeAttempt()
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
            return 0;

        SaturatingIncrementRecoveryCounter(ref _recoveryAttemptCount);
        long attemptCount = Interlocked.Read(ref _recoveryAttemptCount);
        PublishRecoveryRuntimeSnapshot(new StorageRecoveryRuntimeRawSnapshot(
            StorageRecoveryPhaseRaw.Scanning,
            ScannedFrameCount: 0,
            ScannedBytes: 0,
            RecoveredFrameCount: 0,
            RecoveredBytes: 0,
            DiscardedFrameCount: 0,
            DiscardedBytes: 0,
            StorageRecoveryTruncationReasonRaw.Unknown,
            attemptCount,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            StorageRuntimeOperationOutcomeRaw.Running,
            StorageRuntimeFailureKindRaw.None));
        return attemptCount;
    }

    private void PublishRecoveryRuntimeScan(
        RecoveryScanResult result,
        long attemptCount)
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
            return;

        PublishRecoveryRuntimeSnapshot(new StorageRecoveryRuntimeRawSnapshot(
            StorageRecoveryPhaseRaw.Scanning,
            result.ScannedFrameCount,
            result.ScannedBytes,
            result.RecoveredFrameCount,
            result.RecoveredBytes,
            result.DiscardedFrameCount,
            result.DiscardedBytes,
            result.TruncationReason,
            attemptCount,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            StorageRuntimeOperationOutcomeRaw.Running,
            StorageRuntimeFailureKindRaw.None));
    }

    private void PublishRecoveryRuntimeFailure(
        Exception exception,
        RecoveryScanResult result,
        long attemptCount)
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
            return;

        PublishRecoveryRuntimeSnapshot(new StorageRecoveryRuntimeRawSnapshot(
            StorageRecoveryPhaseRaw.Completed,
            result.ScannedFrameCount,
            result.ScannedBytes,
            result.RecoveredFrameCount,
            result.RecoveredBytes,
            result.DiscardedFrameCount,
            result.DiscardedBytes,
            result.TruncationReason,
            attemptCount,
            RetryCount: 0,
            LastRetryFailureKind: StorageRuntimeFailureKindRaw.None,
            exception is OperationCanceledException
                ? StorageRuntimeOperationOutcomeRaw.Canceled
                : StorageRuntimeOperationOutcomeRaw.Failed,
            StorageRuntimeDiagnosticsObserverExtensions.ClassifyRuntimeFailure(exception)));
    }

    private void PublishRecoveryRuntimeSnapshot(
        StorageRecoveryRuntimeRawSnapshot snapshot)
    {
        Interlocked.Increment(ref _recoveryRuntimeSnapshotVersion);
        _recoveryRuntimeSnapshot = snapshot;
        Volatile.Write(ref _hasRecoveryRuntimeSnapshot, 1);
        Interlocked.Increment(ref _recoveryRuntimeSnapshotVersion);
    }

    private RecoveryScanResult CreateFailedRecoveryScanResult(
        bool frameRegionScanned,
        long originalLength,
        long scannedBytes,
        StorageRecoveryTruncationReasonRaw truncationReason)
    {
        if (!frameRegionScanned)
        {
            return new RecoveryScanResult(
                0,
                0,
                0,
                0,
                0,
                0,
                StorageRecoveryTruncationReasonRaw.Unknown);
        }

        return CreateRecoveryScanResult(
            originalLength,
            _storage.Length,
            scannedBytes,
            _index.FrameCount,
            truncationReason);
    }

    private static RecoveryScanResult CreateRecoveryScanResult(
        long originalLength,
        long finalLength,
        long scannedBytes,
        long recoveredFrameCount,
        StorageRecoveryTruncationReasonRaw truncationReason)
    {
        long frameRegionBytes = Math.Max(
            0,
            originalLength - PageConstants.WalHeaderSize);
        scannedBytes = Math.Clamp(scannedBytes, 0, frameRegionBytes);
        long scannedFrameCount = DivideRoundUp(scannedBytes, PageConstants.WalFrameSize);
        long discardedBytes = Math.Min(
            frameRegionBytes,
            Math.Max(0, originalLength - finalLength));
        long discardedFrameCount = DivideRoundUp(discardedBytes, PageConstants.WalFrameSize);
        long normalizedRecoveredFrameCount = Math.Max(0, recoveredFrameCount);
        StorageRecoveryTruncationReasonRaw effectiveTruncationReason =
            discardedBytes == 0 &&
            truncationReason != StorageRecoveryTruncationReasonRaw.Unknown
                ? StorageRecoveryTruncationReasonRaw.None
                : truncationReason;

        return new RecoveryScanResult(
            scannedFrameCount,
            scannedBytes,
            normalizedRecoveredFrameCount,
            normalizedRecoveredFrameCount * PageConstants.WalFrameSize,
            discardedFrameCount,
            discardedBytes,
            effectiveTruncationReason);
    }

    private static long AddScannedBytes(
        long scannedBytes,
        long candidateBytes,
        long frameRegionBytes)
    {
        long maximum = Math.Max(0, frameRegionBytes);
        long current = Math.Clamp(scannedBytes, 0, maximum);
        long addition = Math.Max(0, candidateBytes);
        return addition >= maximum - current
            ? maximum
            : current + addition;
    }

    private static long DivideRoundUp(long value, int divisor) =>
        value == 0 ? 0 : 1 + ((value - 1) / divisor);

    private static void ConsiderRecoveryTruncation(
        long candidateOffset,
        StorageRecoveryTruncationReasonRaw candidateReason,
        ref long truncateAt,
        ref StorageRecoveryTruncationReasonRaw truncationReason)
    {
        if (candidateOffset >= truncateAt)
            return;

        truncateAt = candidateOffset;
        truncationReason = candidateReason;
    }

    private static void SaturatingIncrementRecoveryCounter(ref long target)
    {
        while (true)
        {
            long current = Interlocked.Read(ref target);
            if (current == long.MaxValue)
                return;
            if (Interlocked.CompareExchange(ref target, current + 1, current) == current)
                return;
        }
    }

    private async ValueTask AppendFramesCoreAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        bool commitOnLastFrame,
        uint newDbPageCount,
        bool trackUncommittedFrames,
        CancellationToken cancellationToken)
    {
        EnsureOpen();
        if (frames.IsEmpty)
            return;

        int frameIndex = 0;
        int totalFrameCount = frames.Length;
        int lastFrameIndex = totalFrameCount - 1;
        if (trackUncommittedFrames)
            _uncommittedFrames.EnsureCapacity(_uncommittedFrames.Count + totalFrameCount);

        try
        {
            while (frameIndex < totalFrameCount)
            {
                int framesInChunk = Math.Min(AppendFrameChunkSize, totalFrameCount - frameIndex);
                long chunkStartOffset = _storage.Length;

                for (int i = 0; i < framesInChunk; i++)
                {
                    int currentFrameIndex = frameIndex + i;
                    WalFrameWrite frame = frames.Span[currentFrameIndex];
                    int destinationOffset = i * PageConstants.WalFrameSize;
                    uint dbPageCount = commitOnLastFrame && currentFrameIndex == lastFrameIndex
                        ? newDbPageCount
                        : 0u;

                    uint dataChecksum = WriteWalFrame(
                        _appendFrameChunkBuffer.AsSpan(destinationOffset, PageConstants.WalFrameSize),
                        frame.PageId,
                        frame.PageData.Span,
                        dbPageCount);

                    if (trackUncommittedFrames)
                    {
                        long frameOffset = chunkStartOffset + (long)i * PageConstants.WalFrameSize;
                        _uncommittedFrames.Add((frame.PageId, frameOffset));
                        _lastUncommittedDataChecksum = dataChecksum;
                    }
                }

                int bytesToWrite = framesInChunk * PageConstants.WalFrameSize;
                await _storage.WriteAsync(chunkStartOffset, _appendFrameChunkBuffer.AsMemory(0, bytesToWrite), cancellationToken);
                frameIndex += framesInChunk;
            }
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append {frames.Length} in-memory WAL frame(s).",
                ex);
        }
    }

    private void PublishCommittedFramesFromBatch(ReadOnlyMemory<WalFrameWrite> frames, long firstFrameOffset)
    {
        _index.EnsurePageCapacity(frames.Length);

        for (int i = 0; i < frames.Length; i++)
        {
            long frameOffset = firstFrameOffset + (long)i * PageConstants.WalFrameSize;
            _index.AddCommittedFrame(frames.Span[i].PageId, frameOffset);
        }

        _index.AdvanceCommit(frames.Length);
        _lastUncommittedDataChecksum = 0;
    }

    private void PublishCommittedFrames()
    {
        _index.EnsurePageCapacity(_uncommittedFrames.Count);

        foreach (var (pageId, walOffset) in _uncommittedFrames)
            _index.AddCommittedFrame(pageId, walOffset);

        _index.AdvanceCommit(_uncommittedFrames.Count);
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
    }

    private async ValueTask AppendFrameDirectAsync(
        uint pageId,
        ReadOnlyMemory<byte> pageData,
        CancellationToken cancellationToken)
    {
        long frameOffset = _storage.Length;

        try
        {
            uint dataChecksum = WriteWalFrame(
                _appendFrameBuffer.AsSpan(0, PageConstants.WalFrameSize),
                pageId,
                pageData.Span,
                dbPageCount: 0u);
            await _storage.WriteAsync(frameOffset, _appendFrameBuffer.AsMemory(0, PageConstants.WalFrameSize), cancellationToken);
            _uncommittedFrames.Add((pageId, frameOffset));
            _lastUncommittedDataChecksum = dataChecksum;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append in-memory WAL frame for pageId={pageId} at walOffset={frameOffset}.",
                ex);
        }
    }

    private async ValueTask AppendBufferedFramesCoreAsync(
        bool commitOnLastFrame,
        uint newDbPageCount,
        bool trackUncommittedFrames,
        CancellationToken cancellationToken)
    {
        if (_bufferedUncommittedFrames.Count == 0)
            return;

        WalFrameWrite[] rentedFrames = ArrayPool<WalFrameWrite>.Shared.Rent(_bufferedUncommittedFrames.Count);
        try
        {
            for (int i = 0; i < _bufferedUncommittedFrames.Count; i++)
            {
                var frame = _bufferedUncommittedFrames[i];
                rentedFrames[i] = new WalFrameWrite(frame.PageId, frame.Buffer.AsMemory(0, PageConstants.PageSize));
            }

            await AppendFramesCoreAsync(
                rentedFrames.AsMemory(0, _bufferedUncommittedFrames.Count),
                commitOnLastFrame,
                newDbPageCount,
                trackUncommittedFrames,
                cancellationToken);
        }
        finally
        {
            rentedFrames.AsSpan(0, _bufferedUncommittedFrames.Count).Clear();
            ArrayPool<WalFrameWrite>.Shared.Return(rentedFrames, clearArray: false);
        }
    }

    private BufferedUncommittedFrame CreateBufferedUncommittedFrame(uint pageId, ReadOnlyMemory<byte> pageData)
    {
        if (pageData.Length != PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Invalid in-memory WAL page payload size for pageId={pageId}. Expected {PageConstants.PageSize}, got {pageData.Length}.");
        }

        byte[] pageBuffer = ArrayPool<byte>.Shared.Rent(PageConstants.PageSize);
        pageData.Span.CopyTo(pageBuffer.AsSpan(0, PageConstants.PageSize));
        return new BufferedUncommittedFrame(pageId, pageBuffer);
    }

    private void ClearBufferedUncommittedFrames()
    {
        for (int i = 0; i < _bufferedUncommittedFrames.Count; i++)
            ArrayPool<byte>.Shared.Return(_bufferedUncommittedFrames[i].Buffer, clearArray: false);

        _bufferedUncommittedFrames.Clear();
    }

    private uint WriteWalFrame(Span<byte> frameDestination, uint pageId, ReadOnlySpan<byte> pageData, uint dbPageCount)
    {
        if (pageData.Length != PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Invalid in-memory WAL page payload size for pageId={pageId}. Expected {PageConstants.PageSize}, got {pageData.Length}.");
        }

        var frameHeader = frameDestination[..PageConstants.WalFrameHeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(0, 4), pageId);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(4, 4), dbPageCount);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(8, 4), _salt1);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(12, 4), _salt2);

        uint headerChecksum = ComputeHeaderChecksum(pageId, dbPageCount, _salt1, _salt2, frameHeader.Slice(0, 16));
        uint dataChecksum = _checksumProvider.Compute(pageData);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(16, 4), headerChecksum);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeader.Slice(20, 4), dataChecksum);

        pageData.CopyTo(frameDestination.Slice(PageConstants.WalFrameHeaderSize, PageConstants.PageSize));
        return dataChecksum;
    }

    private uint ComputeHeaderChecksum(uint pageId, uint dbPageCount, uint salt1, uint salt2, ReadOnlySpan<byte> headerPrefix)
    {
        if (_useAdditiveHeaderChecksum)
            return unchecked(pageId + dbPageCount + salt1 + salt2);

        return _checksumProvider.Compute(headerPrefix);
    }

    private void WriteWalHeader(Span<byte> header, uint dbPageCount)
    {
        header.Clear();
        PageConstants.WalMagic.AsSpan().CopyTo(header);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(4, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(8, 4), PageConstants.PageSize);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(12, 4), dbPageCount);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(16, 4), _salt1);
        BinaryPrimitives.WriteUInt32LittleEndian(header.Slice(20, 4), _salt2);
    }

    private void EnsureCheckpointBuffers()
    {
        int readBufferSize = PageConstants.WalFrameSize * CheckpointWriteChunkPages;
        if (_checkpointReadBuffer is null || _checkpointReadBuffer.Length < readBufferSize)
            _checkpointReadBuffer = new byte[readBufferSize];

        int writeBufferSize = PageConstants.PageSize * CheckpointWriteChunkPages;
        if (_checkpointWriteBuffer is null || _checkpointWriteBuffer.Length < writeBufferSize)
            _checkpointWriteBuffer = new byte[writeBufferSize];

        if (_checkpointBatchWalOffsets is null || _checkpointBatchWalOffsets.Length < CheckpointWriteChunkPages)
            _checkpointBatchWalOffsets = new long[CheckpointWriteChunkPages];
    }

    private void EnsureCheckpointCommittedPageCapacity(int requiredCount)
    {
        if (_checkpointCommittedPages.Length >= requiredCount)
            return;

        int newLength = _checkpointCommittedPages.Length == 0 ? requiredCount : _checkpointCommittedPages.Length;
        while (newLength < requiredCount)
            newLength *= 2;

        _checkpointCommittedPages = new KeyValuePair<uint, long>[newLength];
    }

    private ValueTask FlushCheckpointBatchAsync(
        IStorageDevice device,
        uint startPageId,
        int pageCount,
        long startWalOffset,
        bool hasContiguousWalOffsets,
        CancellationToken cancellationToken)
    {
        return FlushCheckpointBatchCoreAsync(
            device,
            startPageId,
            pageCount,
            startWalOffset,
            hasContiguousWalOffsets,
            cancellationToken);
    }

    private async ValueTask FlushCheckpointBatchCoreAsync(
        IStorageDevice device,
        uint startPageId,
        int pageCount,
        long startWalOffset,
        bool hasContiguousWalOffsets,
        CancellationToken cancellationToken)
    {
        byte[] checkpointReadBuffer = _checkpointReadBuffer
            ?? throw new CSharpDbException(ErrorCode.WalError, "Checkpoint read buffer was not initialized.");
        byte[] checkpointWriteBuffer = _checkpointWriteBuffer
            ?? throw new CSharpDbException(ErrorCode.WalError, "Checkpoint write buffer was not initialized.");
        long[] checkpointBatchWalOffsets = _checkpointBatchWalOffsets
            ?? throw new CSharpDbException(ErrorCode.WalError, "Checkpoint WAL offset buffer was not initialized.");

        if (hasContiguousWalOffsets)
        {
            int readByteCount = pageCount * PageConstants.WalFrameSize;
            int bytesRead = await _storage.ReadAsync(startWalOffset, checkpointReadBuffer.AsMemory(0, readByteCount), cancellationToken);
            if (bytesRead != readByteCount)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Short in-memory WAL range read at walOffset={startWalOffset} (expected {readByteCount} bytes, read {bytesRead}).");
            }

            var sourceFrames = checkpointReadBuffer.AsSpan(0, readByteCount);
            var destinationPages = checkpointWriteBuffer.AsSpan(0, pageCount * PageConstants.PageSize);
            for (int i = 0; i < pageCount; i++)
            {
                sourceFrames
                    .Slice(i * PageConstants.WalFrameSize + PageConstants.WalFrameHeaderSize, PageConstants.PageSize)
                    .CopyTo(destinationPages.Slice(i * PageConstants.PageSize, PageConstants.PageSize));
            }
        }
        else
        {
            for (int i = 0; i < pageCount; i++)
            {
                await ReadPageIntoAsync(
                    checkpointBatchWalOffsets[i],
                    checkpointWriteBuffer.AsMemory(i * PageConstants.PageSize, PageConstants.PageSize),
                    cancellationToken);
            }
        }

        long dbOffset = (long)startPageId * PageConstants.PageSize;
        int writeByteCount = pageCount * PageConstants.PageSize;
        await device.WriteAsync(dbOffset, checkpointWriteBuffer.AsMemory(0, writeByteCount), cancellationToken);
    }

    private void SetIncrementalCheckpointState(IncrementalCheckpointState checkpoint)
    {
        _incrementalCheckpoint = checkpoint;
        PublishCheckpointProgressMirror(checkpoint);
    }

    private void ClearIncrementalCheckpointState()
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
        {
            Volatile.Write(ref _runtimeRetainedWalStartOffset, -1);
            _incrementalCheckpoint = null;
            return;
        }

        Interlocked.Increment(ref _runtimeCheckpointProgressVersion);
        Interlocked.Exchange(ref _runtimeCheckpointCompletedPageCount, 0);
        Interlocked.Exchange(ref _runtimeCheckpointTotalPageCount, 0);
        Volatile.Write(ref _runtimeRetainedWalStartOffset, -1);
        _incrementalCheckpoint = null;
        Interlocked.Increment(ref _runtimeCheckpointProgressVersion);
    }

    private void PublishCheckpointProgressMirror(IncrementalCheckpointState checkpoint)
    {
        if (!_recoveryRuntimeDiagnosticsEnabled)
        {
            Volatile.Write(
                ref _runtimeRetainedWalStartOffset,
                checkpoint.RetainedWalStartOffset);
            return;
        }

        Interlocked.Increment(ref _runtimeCheckpointProgressVersion);
        Interlocked.Exchange(
            ref _runtimeCheckpointCompletedPageCount,
            checkpoint.NextPageIndex);
        Interlocked.Exchange(
            ref _runtimeCheckpointTotalPageCount,
            checkpoint.CommittedPageCount);
        Volatile.Write(
            ref _runtimeRetainedWalStartOffset,
            checkpoint.RetainedWalStartOffset);
        Interlocked.Increment(ref _runtimeCheckpointProgressVersion);
    }

    private IncrementalCheckpointState? EnsureIncrementalCheckpointState()
    {
        if (_incrementalCheckpoint is not null)
            return _incrementalCheckpoint;

        var committedPages = _index.GetCommittedPages();
        int committedPageCount = committedPages.Count;
        if (committedPageCount == 0)
            return null;

        EnsureCheckpointCommittedPageCapacity(committedPageCount);
        var sortedCommittedPages = _checkpointCommittedPages;
        int sortedCount = 0;
        bool isPageIdSortedAscending = true;
        uint previousPageId = 0;
        foreach (var committedPage in committedPages)
        {
            if (sortedCount > 0 && committedPage.Key < previousPageId)
                isPageIdSortedAscending = false;

            sortedCommittedPages[sortedCount++] = committedPage;
            previousPageId = committedPage.Key;
        }

        if (!isPageIdSortedAscending && committedPageCount > 1)
            Array.Sort(sortedCommittedPages, 0, committedPageCount, PageIdComparer);

        var snapshot = new KeyValuePair<uint, long>[committedPageCount];
        Array.Copy(sortedCommittedPages, 0, snapshot, 0, committedPageCount);
        var checkpoint = new IncrementalCheckpointState(snapshot, committedPageCount, _storage.Length);
        SetIncrementalCheckpointState(checkpoint);
        return checkpoint;
    }

    private async ValueTask FlushCheckpointSliceAsync(
        IStorageDevice device,
        IncrementalCheckpointState checkpoint,
        int pagesToProcess,
        CancellationToken cancellationToken)
    {
        if (pagesToProcess <= 0)
            return;

        EnsureCheckpointBuffers();
        var checkpointBatchWalOffsets = _checkpointBatchWalOffsets!;

        int endIndexExclusive = checkpoint.NextPageIndex + pagesToProcess;
        uint batchStartPageId = 0;
        int batchPageCount = 0;
        long batchStartWalOffset = 0;
        bool batchHasContiguousWalOffsets = true;

        for (int i = checkpoint.NextPageIndex; i < endIndexExclusive; i++)
        {
            var committedPage = checkpoint.CommittedPages[i];
            uint pageId = committedPage.Key;
            long walOffset = committedPage.Value;
            bool startsNewBatch = batchPageCount == 0 ||
                (ulong)pageId != (ulong)batchStartPageId + (uint)batchPageCount ||
                batchPageCount == CheckpointWriteChunkPages;

            if (startsNewBatch && batchPageCount > 0)
            {
                await FlushCheckpointBatchAsync(
                    device,
                    batchStartPageId,
                    batchPageCount,
                    batchStartWalOffset,
                    batchHasContiguousWalOffsets,
                    cancellationToken);
                batchPageCount = 0;
            }

            if (batchPageCount == 0)
            {
                batchStartPageId = pageId;
                batchStartWalOffset = walOffset;
                batchHasContiguousWalOffsets = true;
            }
            else if (batchHasContiguousWalOffsets)
            {
                long expectedWalOffset = batchStartWalOffset + (long)batchPageCount * PageConstants.WalFrameSize;
                if (walOffset != expectedWalOffset)
                    batchHasContiguousWalOffsets = false;
            }

            checkpointBatchWalOffsets[batchPageCount] = walOffset;
            batchPageCount++;
        }

        if (batchPageCount > 0)
        {
            await FlushCheckpointBatchAsync(
                device,
                batchStartPageId,
                batchPageCount,
                batchStartWalOffset,
                batchHasContiguousWalOffsets,
                cancellationToken);
        }
    }

    private async ValueTask FinalizeIncrementalCheckpointAsync(uint pageCount, CancellationToken cancellationToken)
    {
        var checkpoint = _incrementalCheckpoint;
        if (checkpoint is null)
            return;

        long retainedByteCount = _storage.Length - checkpoint.RetainedWalStartOffset;
        if (retainedByteCount <= 0)
        {
            await ResetWalAsync(pageCount, generateNewSalts: true, cancellationToken);
            ClearIncrementalCheckpointState();
            return;
        }

        await CompactRetainedFramesAsync(checkpoint.RetainedWalStartOffset, retainedByteCount, pageCount, cancellationToken);
        ClearIncrementalCheckpointState();
    }

    private async ValueTask CompactRetainedFramesAsync(
        long retainedWalStartOffset,
        long retainedByteCount,
        uint pageCount,
        CancellationToken cancellationToken)
    {
        EnsureCheckpointBuffers();
        byte[] moveBuffer = _checkpointReadBuffer
            ?? throw new CSharpDbException(ErrorCode.WalError, "Checkpoint read buffer was not initialized.");
        var retainedLatestPages = new Dictionary<uint, long>();
        int retainedFrameCount = 0;
        int retainedCommitCount = 0;

        long sourceOffset = retainedWalStartOffset;
        long destinationOffset = PageConstants.WalHeaderSize;
        while (sourceOffset < retainedWalStartOffset + retainedByteCount)
        {
            int chunkLength = (int)Math.Min(moveBuffer.Length, retainedWalStartOffset + retainedByteCount - sourceOffset);
            int bytesRead = await _storage.ReadAsync(sourceOffset, moveBuffer.AsMemory(0, chunkLength), cancellationToken);
            if (bytesRead != chunkLength)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Short in-memory WAL range read at walOffset={sourceOffset} (expected {chunkLength} bytes, read {bytesRead}).");
            }

            await _storage.WriteAsync(destinationOffset, moveBuffer.AsMemory(0, chunkLength), cancellationToken);
            CaptureRetainedFrameMetadata(
                moveBuffer.AsSpan(0, chunkLength),
                destinationOffset,
                retainedLatestPages,
                ref retainedFrameCount,
                ref retainedCommitCount);
            sourceOffset += chunkLength;
            destinationOffset += chunkLength;
        }

        await _storage.SetLengthAsync(PageConstants.WalHeaderSize + retainedByteCount, cancellationToken);
        await RewriteWalHeaderAsync(pageCount, cancellationToken);
        await _storage.FlushAsync(cancellationToken);

        _index.ReplaceCommittedState(retainedLatestPages, retainedFrameCount, retainedCommitCount);
        _uncommittedStartOffset = _storage.Length;
    }

    private async ValueTask ResetWalAsync(uint pageCount, bool generateNewSalts, CancellationToken cancellationToken)
    {
        _index.Reset();
        if (generateNewSalts)
        {
            _salt1 = (uint)Random.Shared.Next();
            _salt2 = (uint)Random.Shared.Next();
        }

        WriteWalHeader(_walHeaderBuffer, pageCount);
        await _storage.WriteAsync(0, _walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
        await _storage.SetLengthAsync(PageConstants.WalHeaderSize, cancellationToken);
        await _storage.FlushAsync(cancellationToken);
        _uncommittedStartOffset = PageConstants.WalHeaderSize;
    }

    private ValueTask RewriteWalHeaderAsync(uint pageCount, CancellationToken cancellationToken)
    {
        WriteWalHeader(_walHeaderBuffer, pageCount);
        return _storage.WriteAsync(0, _walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
    }

    private static void CaptureRetainedFrameMetadata(
        ReadOnlySpan<byte> retainedBytes,
        long relocatedChunkOffset,
        Dictionary<uint, long> retainedLatestPages,
        ref int retainedFrameCount,
        ref int retainedCommitCount)
    {
        if (retainedBytes.Length == 0)
            return;
        if (retainedBytes.Length % PageConstants.WalFrameSize != 0)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Retained in-memory WAL suffix length {retainedBytes.Length} was not frame-aligned during compaction.");
        }

        for (int offset = 0; offset < retainedBytes.Length; offset += PageConstants.WalFrameSize)
        {
            ReadOnlySpan<byte> frameHeader = retainedBytes.Slice(offset, PageConstants.WalFrameHeaderSize);
            uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.Slice(0, 4));
            uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.Slice(4, 4));
            long relocatedFrameOffset = relocatedChunkOffset + offset;

            retainedLatestPages[pageId] = relocatedFrameOffset;
            retainedFrameCount++;
            if (dbPageCount != 0)
                retainedCommitCount++;
        }
    }

    private void EnsureOpen()
    {
        if (!Volatile.Read(ref _isOpen))
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
    }

    private sealed class IncrementalCheckpointState
    {
        public IncrementalCheckpointState(
            KeyValuePair<uint, long>[] committedPages,
            int committedPageCount,
            long retainedWalStartOffset)
        {
            CommittedPages = committedPages;
            CommittedPageCount = committedPageCount;
            RetainedWalStartOffset = retainedWalStartOffset;
        }

        public KeyValuePair<uint, long>[] CommittedPages { get; }
        public int CommittedPageCount { get; }
        public int NextPageIndex { get; set; }
        public long RetainedWalStartOffset { get; }
    }

    private readonly record struct BufferedUncommittedFrame(uint PageId, byte[] Buffer);
}

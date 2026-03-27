using CSharpDB.Primitives;
using CSharpDB.Storage.Internal;
using CSharpDB.Storage.StorageEngine;
using Microsoft.Win32.SafeHandles;
using System.Buffers;
using System.Buffers.Binary;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// Write-Ahead Log for redo-style crash recovery and concurrent readers.
/// The WAL file sits alongside the database file with a ".wal" extension.
///
/// File layout:
///   [WalHeader: 32 bytes]
///   [Frame 0: 24-byte header + 4096-byte page data]
///   [Frame 1: 24-byte header + 4096-byte page data]
///   ...
///
/// A commit is marked by a frame whose dbPageCount field is nonzero.
/// All frames from the previous commit marker (or start) up to and including
/// the commit frame belong to that transaction.
/// </summary>
public sealed class WriteAheadLog : IWriteAheadLog, IWalRuntimeDiagnosticsProvider
{
    private const int WalStreamBufferSize = 64 * 1024;
    private const int AppendFrameChunkSize = 16;
    private const int CheckpointWriteChunkPages = 16;
    internal const int DurableCommitBatchBypassPendingCommitThreshold = 4;
    internal const long DurableCommitBatchBypassPendingByteThreshold =
        (long)AppendFrameChunkSize * PageConstants.WalFrameSize;
    private static readonly IComparer<KeyValuePair<uint, long>> PageIdComparer =
        Comparer<KeyValuePair<uint, long>>.Create(static (left, right) => left.Key.CompareTo(right.Key));

    private readonly string _walPath;
    private FileStream? _stream;
    private SafeFileHandle? _readHandle; // separate read handle for concurrent reads
    private readonly WalIndex _index;
    private readonly IPageChecksumProvider _checksumProvider;
    private readonly bool _useAdditiveHeaderChecksum;
    private readonly IWalFlushPolicy _flushPolicy;
    private readonly TimeSpan _durableCommitBatchWindow;
    private readonly long _walPreallocationChunkBytes;

    // WAL header fields
    private uint _salt1;
    private uint _salt2;

    // Uncommitted frame tracking for current transaction
    private readonly List<(uint PageId, long WalOffset)> _uncommittedFrames = new(capacity: 256);
    private readonly List<BufferedUncommittedFrame> _bufferedUncommittedFrames = new(capacity: 256);
    private uint _lastUncommittedDataChecksum;
    private readonly List<(uint PageId, long WalOffset)> _recoverUncommittedBatch = new();
    private long _uncommittedStartOffset;
    private long _writePosition;
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
    private readonly SemaphoreSlim _streamMutex = new(1, 1);
    private readonly object _pendingCommitSync = new();
    private readonly List<PendingCommitBatch> _pendingCommitBatches = new();
    private TaskCompletionSource? _pendingCommitBatchWindowSignal;
    private long _nextCommitSequence;
    private long _pendingCommitByteCount;
    private bool _flushInProgress;
    private CSharpDbException? _writeFault;
    private long _flushCount;
    private long _flushedCommitCount;
    private long _flushedByteCount;
    private long _batchWindowWaitCount;
    private long _batchWindowThresholdBypassCount;
    private long _preallocationCount;
    private long _preallocatedByteCount;

    public WriteAheadLog(
        string databasePath,
        WalIndex index,
        IPageChecksumProvider? checksumProvider = null,
        DurabilityMode durabilityMode = DurabilityMode.Durable,
        TimeSpan? durableCommitBatchWindow = null,
        long walPreallocationChunkBytes = 0)
        : this(
            databasePath,
            index,
            checksumProvider,
            WalFlushPolicy.Create(durabilityMode),
            durableCommitBatchWindow,
            walPreallocationChunkBytes)
    {
    }

    internal WriteAheadLog(
        string databasePath,
        WalIndex index,
        IPageChecksumProvider? checksumProvider,
        IWalFlushPolicy flushPolicy,
        TimeSpan? durableCommitBatchWindow = null,
        long walPreallocationChunkBytes = 0)
    {
        _walPath = databasePath + ".wal";
        _index = index;
        _checksumProvider = checksumProvider ?? new AdditiveChecksumProvider();
        _useAdditiveHeaderChecksum = _checksumProvider is AdditiveChecksumProvider;
        _flushPolicy = flushPolicy;
        _durableCommitBatchWindow = flushPolicy.AllowsWriteConcurrencyDuringCommitFlush
            ? durableCommitBatchWindow.GetValueOrDefault()
            : TimeSpan.Zero;
        _walPreallocationChunkBytes = walPreallocationChunkBytes;
    }

    public WalIndex Index => _index;
    public bool IsOpen => _stream != null;
    public bool HasPendingCheckpoint => _incrementalCheckpoint is not null;
    public bool HasPendingCommitWork
    {
        get
        {
            lock (_pendingCommitSync)
            {
                return _flushInProgress || _pendingCommitBatches.Count > 0;
            }
        }
    }
    internal IWalFlushPolicy FlushPolicy => _flushPolicy;

    WalFlushDiagnosticsSnapshot IWalRuntimeDiagnosticsProvider.GetWalFlushDiagnosticsSnapshot()
    {
        return new WalFlushDiagnosticsSnapshot(
            Interlocked.Read(ref _flushCount),
            Interlocked.Read(ref _flushedCommitCount),
            Interlocked.Read(ref _flushedByteCount),
            Interlocked.Read(ref _batchWindowWaitCount),
            Interlocked.Read(ref _batchWindowThresholdBypassCount),
            Interlocked.Read(ref _preallocationCount),
            Interlocked.Read(ref _preallocatedByteCount));
    }

    void IWalRuntimeDiagnosticsProvider.ResetWalFlushDiagnostics()
    {
        Interlocked.Exchange(ref _flushCount, 0);
        Interlocked.Exchange(ref _flushedCommitCount, 0);
        Interlocked.Exchange(ref _flushedByteCount, 0);
        Interlocked.Exchange(ref _batchWindowWaitCount, 0);
        Interlocked.Exchange(ref _batchWindowThresholdBypassCount, 0);
        Interlocked.Exchange(ref _preallocationCount, 0);
        Interlocked.Exchange(ref _preallocatedByteCount, 0);
    }

    // ============ Open / Create ============

    /// <summary>
    /// Open an existing WAL file or create a new one.
    /// If a WAL file exists, scan it to rebuild the index (recovery).
    /// </summary>
    public async ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default)
    {
        if (_stream != null)
            return;

        if (File.Exists(_walPath))
        {
            await RecoverAsync(cancellationToken);
        }
        else
        {
            await CreateNewAsync(currentDbPageCount, cancellationToken);
        }
    }

    private async ValueTask CreateNewAsync(uint dbPageCount, CancellationToken cancellationToken)
    {
        try
        {
            _writeFault = null;
            lock (_pendingCommitSync)
            {
                _pendingCommitBatches.Clear();
                _flushInProgress = false;
            }
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            _recoverUncommittedBatch.Clear();
            _index.Reset();

            _stream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite,
                FileShare.ReadWrite, bufferSize: WalStreamBufferSize, useAsync: true);

            _salt1 = (uint)Random.Shared.Next();
            _salt2 = (uint)Random.Shared.Next();

            WriteWalHeader(_walHeaderBuffer, dbPageCount);
            await _stream.WriteAsync(_walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
            await FlushAsync(cancellationToken);

            _writePosition = PageConstants.WalHeaderSize;
            _uncommittedStartOffset = _writePosition;
            OpenReadHandle();
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to create WAL file '{_walPath}' for dbPageCount={dbPageCount}.",
                ex);
        }
    }

    // ============ Transaction operations ============

    /// <summary>
    /// Begin a new write transaction in the WAL.
    /// Records the current end-of-WAL position so rollback can truncate.
    /// </summary>
    public void BeginTransaction()
    {
        ThrowIfWriteFaulted();
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _uncommittedStartOffset = _writePosition;
    }

    /// <summary>
    /// Append a dirty page to the WAL as an uncommitted frame.
    /// </summary>
    public ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData,
        CancellationToken cancellationToken = default)
    {
        ThrowIfWriteFaulted();
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        if (_uncommittedFrames.Count > 0)
            return AppendFrameDirectAsync(pageId, pageData, cancellationToken);

        try
        {
            _bufferedUncommittedFrames.Add(CreateBufferedUncommittedFrame(pageId, pageData));
            return ValueTask.CompletedTask;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to buffer WAL frame for pageId={pageId}.",
                ex);
        }
    }

    public async ValueTask AppendFramesAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        CancellationToken cancellationToken = default)
    {
        await _streamMutex.WaitAsync(cancellationToken);
        try
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
        finally
        {
            _streamMutex.Release();
        }
    }

    public async ValueTask<WalCommitResult> AppendFramesAndCommitAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        uint newDbPageCount,
        CancellationToken cancellationToken = default)
    {
        ThrowIfWriteFaulted();
        if (frames.IsEmpty)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");
        if (_uncommittedFrames.Count != 0 || _bufferedUncommittedFrames.Count != 0)
            throw new CSharpDbException(ErrorCode.WalError, "AppendFramesAndCommitAsync cannot be used with existing uncommitted frames.");
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        long firstFrameOffset;
        await _streamMutex.WaitAsync(cancellationToken);
        try
        {
            firstFrameOffset = _writePosition;
            await AppendFramesCoreAsync(
                frames,
                commitOnLastFrame: true,
                newDbPageCount,
                trackUncommittedFrames: false,
                cancellationToken);
        }
        finally
        {
            _streamMutex.Release();
        }

        try
        {
            return QueuePendingCommit(CreatePendingBatch(frames, firstFrameOffset));
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append+commit {frames.Length} WAL frame(s), newDbPageCount={newDbPageCount}.",
                ex);
        }
    }

    /// <summary>
    /// Commit the current transaction. Marks the last frame as a commit frame,
    /// flushes to disk, then updates the in-memory WAL index.
    /// </summary>
    public async ValueTask<WalCommitResult> CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default)
    {
        ThrowIfWriteFaulted();
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        if (_uncommittedFrames.Count == 0 && _bufferedUncommittedFrames.Count == 0)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");

        if (_bufferedUncommittedFrames.Count > 0)
        {
            int bufferedFrameCount = _bufferedUncommittedFrames.Count;

            await _streamMutex.WaitAsync(cancellationToken);
            try
            {
                await AppendBufferedFramesCoreAsync(
                    commitOnLastFrame: true,
                    newDbPageCount,
                    trackUncommittedFrames: true,
                    cancellationToken);
                ClearBufferedUncommittedFrames();
                return QueuePendingCommit(CreatePendingBatch(_uncommittedFrames, clearSource: true));
            }
            catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Failed to commit buffered WAL transaction with {bufferedFrameCount} frame(s), newDbPageCount={newDbPageCount}.",
                    ex);
            }
            finally
            {
                _streamMutex.Release();
            }
        }

        // Rewrite the last frame's dbPageCount to mark it as a commit frame
        var (lastPageId, lastOffset) = _uncommittedFrames[^1];
        uint lastDataChecksum = _lastUncommittedDataChecksum;

        // Rebuild the commit-frame header directly (avoid read-back I/O on the hot commit path)
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

        await _streamMutex.WaitAsync(cancellationToken);
        try
        {
            _stream.Position = lastOffset;
            await _stream.WriteAsync(frameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);

            // Seek to end for next writes
            _stream.Position = lastOffset + PageConstants.WalFrameSize;
            _writePosition = lastOffset + PageConstants.WalFrameSize;
            return QueuePendingCommit(CreatePendingBatch(_uncommittedFrames, clearSource: true));
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to commit WAL transaction with {_uncommittedFrames.Count} frame(s), newDbPageCount={newDbPageCount}, commitFrameOffset={lastOffset}.",
                ex);
        }
        finally
        {
            _streamMutex.Release();
        }
    }

    /// <summary>
    /// Rollback: truncate the WAL file back to where the transaction started.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_stream == null) return;
        if (_uncommittedFrames.Count == 0)
        {
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            return;
        }

        await _streamMutex.WaitAsync(cancellationToken);
        try
        {
            _stream.SetLength(_uncommittedStartOffset);
            _stream.Position = _uncommittedStartOffset;
            _writePosition = _uncommittedStartOffset;
            await _stream.FlushAsync(cancellationToken);
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to rollback WAL transaction to offset {_uncommittedStartOffset}.",
                ex);
        }
        finally
        {
            _streamMutex.Release();
        }
    }

    // ============ Read operations ============

    /// <summary>
    /// Read a page from the WAL at the given frame offset.
    /// Uses a separate read handle for thread safety with concurrent readers.
    /// </summary>
    public async ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default)
    {
        var page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        await ReadPageIntoAsync(walFrameOffset, page, cancellationToken);

        return page;
    }

    // ============ Recovery ============

    /// <summary>
    /// Scan an existing WAL file, validate frames, and rebuild the index
    /// with only committed frames.
    /// </summary>
    private async ValueTask RecoverAsync(CancellationToken cancellationToken)
    {
        try
        {
            _writeFault = null;
            lock (_pendingCommitSync)
            {
                _pendingCommitBatches.Clear();
                _flushInProgress = false;
            }
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            _recoverUncommittedBatch.Clear();
            _index.Reset();

            _stream = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite,
                FileShare.ReadWrite, bufferSize: WalStreamBufferSize, useAsync: true);

            var header = _walHeaderBuffer;
            if (await _stream.ReadAsync(header.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken) != PageConstants.WalHeaderSize)
                throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: header too short.");

            if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
                throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: bad magic.");

            _salt1 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
            _salt2 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(20, 4));

            var uncommittedBatch = _recoverUncommittedBatch;
            uncommittedBatch.Clear();
            var frameHeaderBuffer = _recoveryFrameHeaderBuffer;
            var pageDataBuffer = _recoveryPageBuffer;

            while (_stream.Position + PageConstants.WalFrameSize <= _stream.Length)
            {
                long frameOffset = _stream.Position;

                int headerRead = await _stream.ReadAsync(frameHeaderBuffer, cancellationToken);
                int dataRead = await _stream.ReadAsync(pageDataBuffer, cancellationToken);

                if (headerRead != PageConstants.WalFrameHeaderSize ||
                    dataRead != PageConstants.PageSize)
                {
                    _stream.SetLength(frameOffset);
                    break;
                }

                uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
                uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
                if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
                {
                    _stream.SetLength(frameOffset);
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

                if (expectedHeaderChecksum != actualHeaderChecksum ||
                    expectedDataChecksum != actualDataChecksum)
                {
                    _stream.SetLength(frameOffset);
                    break;
                }

                uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
                uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4));

                uncommittedBatch.Add((pageId, frameOffset));

                if (dbPageCount != 0)
                {
                    foreach (var (committedPageId, committedOffset) in uncommittedBatch)
                    {
                        _index.AddCommittedFrame(committedPageId, committedOffset);
                    }
                    _index.AdvanceCommit();
                    uncommittedBatch.Clear();
                }
            }

            if (uncommittedBatch.Count > 0)
            {
                long truncateAt = uncommittedBatch[0].WalOffset;
                _stream.SetLength(truncateAt);
            }
            uncommittedBatch.Clear();

            _writePosition = _stream.Length;
            _stream.Position = _writePosition;
            _uncommittedStartOffset = _writePosition;
            OpenReadHandle();
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to recover WAL file '{_walPath}'.",
                ex);
        }
    }

    // ============ Checkpoint ============

    /// <summary>
    /// Checkpoint: copy all committed WAL pages to the database file, then
    /// truncate (reset) the WAL.
    /// </summary>
    public async ValueTask CheckpointAsync(IStorageDevice device, uint pageCount, CancellationToken cancellationToken = default)
    {
        while (!await CheckpointStepAsync(device, pageCount, int.MaxValue, cancellationToken))
        {
        }
    }

    public async ValueTask<bool> CheckpointStepAsync(
        IStorageDevice device,
        uint pageCount,
        int maxPages,
        CancellationToken cancellationToken = default)
    {
        if (_stream == null)
            return true;

        if (maxPages <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxPages), "Value must be greater than zero.");

        try
        {
            var checkpoint = await EnsureIncrementalCheckpointStateAsync(cancellationToken);
            if (checkpoint is null)
                return true;

            long requiredLength = (long)pageCount * PageConstants.PageSize;
            if (device.Length < requiredLength)
                await device.SetLengthAsync(requiredLength, cancellationToken);

            int remainingPageCount = checkpoint.CommittedPageCount - checkpoint.NextPageIndex;
            if (remainingPageCount > 0)
            {
                int pagesToProcess = Math.Min(maxPages, remainingPageCount);
                await FlushCheckpointSliceAsync(device, checkpoint, pagesToProcess, cancellationToken);
                checkpoint.NextPageIndex += pagesToProcess;

                if (checkpoint.NextPageIndex < checkpoint.CommittedPageCount)
                    return false;
                }

                await device.FlushAsync(cancellationToken);
                ProcessCrashInjector.TripIfRequested(
                    "checkpoint-after-device-flush",
                    "checkpoint-after-device-flush");
                await FinalizeIncrementalCheckpointAsync(pageCount, cancellationToken);
                return true;
            }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            int committedPageCount = _incrementalCheckpoint?.CommittedPageCount ?? _index.FrameCount;
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to checkpoint WAL '{_walPath}' with {committedPageCount} committed page(s).",
                ex);
        }
    }

    /// <summary>
    /// Delete the WAL file entirely. Called when closing the database after a final checkpoint.
    /// </summary>
    public async ValueTask CloseAndDeleteAsync()
    {
        try
        {
            FailPendingCommits(new ObjectDisposedException(nameof(WriteAheadLog), "WAL was closed while commits were pending."));
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            _recoverUncommittedBatch.Clear();
            _index.Reset();
            CloseReadHandle();
            if (_stream != null)
            {
                await _stream.DisposeAsync();
                _stream = null;
            }
            if (File.Exists(_walPath))
                File.Delete(_walPath);
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to close/delete WAL file '{_walPath}'.",
                ex);
        }
    }

    // ============ Dispose ============

    public async ValueTask DisposeAsync()
    {
        FailPendingCommits(new ObjectDisposedException(nameof(WriteAheadLog), "WAL was disposed while commits were pending."));
        _incrementalCheckpoint = null;
        _uncommittedFrames.Clear();
        ClearBufferedUncommittedFrames();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        CloseReadHandle();
        if (_stream != null)
        {
            await _stream.DisposeAsync();
            _stream = null;
        }
    }

    // ============ Read handle management ============

    private void OpenReadHandle()
    {
        try
        {
            CloseReadHandle();
            if (File.Exists(_walPath))
            {
                _readHandle = File.OpenHandle(
                    _walPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite,
                    FileOptions.Asynchronous | FileOptions.RandomAccess);
            }
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to open WAL read handle for '{_walPath}'.",
                ex);
        }
    }

    private void CloseReadHandle()
    {
        if (_readHandle != null)
        {
            _readHandle.Dispose();
            _readHandle = null;
        }
    }

    public async ValueTask ReadPageIntoAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        long dataOffset = walFrameOffset + PageConstants.WalFrameHeaderSize;

        if (_readHandle != null)
        {
            try
            {
                int bytesRead = 0;
                while (bytesRead < destination.Length)
                {
                    int read = await RandomAccess.ReadAsync(
                        _readHandle,
                        destination.Slice(bytesRead),
                        dataOffset + bytesRead,
                        cancellationToken);
                    if (read == 0) break;
                    bytesRead += read;
                }

                if (bytesRead != destination.Length)
                {
                    throw new CSharpDbException(
                        ErrorCode.WalError,
                        $"Short WAL read at walFrameOffset={walFrameOffset} (expected {destination.Length} bytes, read {bytesRead}).");
                }
                return;
            }
            catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Failed to read WAL page via random-access handle at walFrameOffset={walFrameOffset}.",
                    ex);
            }
        }

        if (_stream != null)
        {
            try
            {
                _stream.Position = dataOffset;
                int bytesRead = 0;
                while (bytesRead < destination.Length)
                {
                    int read = await _stream.ReadAsync(destination.Slice(bytesRead), cancellationToken);
                    if (read == 0) break;
                    bytesRead += read;
                }

                if (bytesRead != destination.Length)
                {
                    throw new CSharpDbException(
                        ErrorCode.WalError,
                        $"Short WAL read at walFrameOffset={walFrameOffset} (expected {destination.Length} bytes, read {bytesRead}).");
                }
                return;
            }
            catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Failed to read WAL page via stream at walFrameOffset={walFrameOffset}.",
                    ex);
            }
        }

        throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
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

        await _streamMutex.WaitAsync(cancellationToken);
        try
        {
            if (_stream == null)
                throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

            if (hasContiguousWalOffsets)
            {
                int readByteCount = pageCount * PageConstants.WalFrameSize;
                await ReadWalRangeFromStreamAsync(
                    startWalOffset,
                    checkpointReadBuffer.AsMemory(0, readByteCount),
                    cancellationToken);

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
                    await ReadPageFromStreamIntoAsync(
                        checkpointBatchWalOffsets[i],
                        checkpointWriteBuffer.AsMemory(i * PageConstants.PageSize, PageConstants.PageSize),
                        cancellationToken);
                }
            }
        }
        finally
        {
            _streamMutex.Release();
        }

        long dbOffset = (long)startPageId * PageConstants.PageSize;
        int writeByteCount = pageCount * PageConstants.PageSize;
        await device.WriteAsync(dbOffset, checkpointWriteBuffer.AsMemory(0, writeByteCount), cancellationToken);
    }

    private async ValueTask ReadPageFromStreamIntoAsync(
        long walFrameOffset,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        await ReadWalRangeFromStreamAsync(
            walFrameOffset + PageConstants.WalFrameHeaderSize,
            destination,
            cancellationToken);
    }

    private async ValueTask ReadWalRangeFromStreamAsync(
        long walOffset,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        _stream.Position = walOffset;
        int bytesRead = 0;
        while (bytesRead < destination.Length)
        {
            int read = await _stream.ReadAsync(destination.Slice(bytesRead), cancellationToken);
            if (read == 0) break;
            bytesRead += read;
        }

        if (bytesRead != destination.Length)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Short WAL stream read at walOffset={walOffset} (expected {destination.Length} bytes, read {bytesRead}).");
        }
    }

    private async ValueTask ReadWalRangeIntoAsync(long walOffset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (_readHandle != null)
        {
            int bytesRead = 0;
            while (bytesRead < destination.Length)
            {
                int read = await RandomAccess.ReadAsync(
                    _readHandle,
                    destination.Slice(bytesRead),
                    walOffset + bytesRead,
                    cancellationToken);
                if (read == 0) break;
                bytesRead += read;
            }

            if (bytesRead != destination.Length)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Short WAL range read at walOffset={walOffset} (expected {destination.Length} bytes, read {bytesRead}).");
            }

            return;
        }

        if (_stream != null)
        {
            _stream.Position = walOffset;
            int bytesRead = 0;
            while (bytesRead < destination.Length)
            {
                int read = await _stream.ReadAsync(destination.Slice(bytesRead), cancellationToken);
                if (read == 0) break;
                bytesRead += read;
            }

            if (bytesRead != destination.Length)
            {
                throw new CSharpDbException(
                    ErrorCode.WalError,
                    $"Short WAL range read at walOffset={walOffset} (expected {destination.Length} bytes, read {bytesRead}).");
            }

            return;
        }

        throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
    }

    private async ValueTask AppendFramesCoreAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        bool commitOnLastFrame,
        uint newDbPageCount,
        bool trackUncommittedFrames,
        CancellationToken cancellationToken)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        if (frames.IsEmpty)
            return;

        int frameIndex = 0;
        int totalFrameCount = frames.Length;
        int lastFrameIndex = totalFrameCount - 1;
        if (trackUncommittedFrames)
            _uncommittedFrames.EnsureCapacity(_uncommittedFrames.Count + totalFrameCount);

        try
        {
            EnsureAppendCapacity_NoLock((long)totalFrameCount * PageConstants.WalFrameSize);
            _stream.Position = _writePosition;
            while (frameIndex < totalFrameCount)
            {
                int framesInChunk = Math.Min(AppendFrameChunkSize, totalFrameCount - frameIndex);
                long chunkStartOffset = _writePosition;

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
                    }

                    if (trackUncommittedFrames)
                        _lastUncommittedDataChecksum = dataChecksum;
                }

                int bytesToWrite = framesInChunk * PageConstants.WalFrameSize;
                await _stream.WriteAsync(_appendFrameChunkBuffer.AsMemory(0, bytesToWrite), cancellationToken);
                _writePosition += bytesToWrite;
                frameIndex += framesInChunk;
            }
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append {frames.Length} WAL frame(s).",
                ex);
        }
    }

    private WalCommitResult QueuePendingCommit(PendingCommitBatch batch)
    {
        bool startLeader = false;
        TaskCompletionSource? batchWindowSignal = null;
        lock (_pendingCommitSync)
        {
            if (_writeFault is not null)
            {
                batch.Completion.TrySetException(_writeFault);
                return new WalCommitResult(batch.Completion.Task);
            }

            batch.Sequence = ++_nextCommitSequence;
            _pendingCommitBatches.Add(batch);
            _pendingCommitByteCount += batch.ByteCount;
            if (_pendingCommitBatchWindowSignal is not null && ShouldBypassBatchWindow_NoLock())
            {
                batchWindowSignal = _pendingCommitBatchWindowSignal;
                _pendingCommitBatchWindowSignal = null;
            }

            if (!_flushInProgress)
            {
                _flushInProgress = true;
                startLeader = true;
            }
        }

        batchWindowSignal?.TrySetResult();

        if (startLeader)
            _ = ProcessPendingCommitsAsync();

        return new WalCommitResult(batch.Completion.Task);
    }

    private void EnsureAppendCapacity_NoLock(long appendByteCount)
    {
        if (_stream is null ||
            _walPreallocationChunkBytes <= 0 ||
            appendByteCount <= 0)
        {
            return;
        }

        long requiredLength = checked(_writePosition + appendByteCount);
        long currentLength = _stream.Length;
        if (requiredLength <= currentLength)
            return;

        long growthChunk = _walPreallocationChunkBytes;
        long newLength = checked(((requiredLength + growthChunk - 1) / growthChunk) * growthChunk);
        if (newLength <= currentLength)
            return;

        _stream.SetLength(newLength);
        _stream.Position = _writePosition;
        Interlocked.Increment(ref _preallocationCount);
        Interlocked.Add(ref _preallocatedByteCount, newLength - currentLength);
    }

    private async Task ProcessPendingCommitsAsync()
    {
        while (true)
        {
            long flushThroughSequence;
            lock (_pendingCommitSync)
            {
                if (_pendingCommitBatches.Count == 0)
                {
                    _flushInProgress = false;
                    return;
                }
            }

            try
            {
                await WaitForDurableCommitBatchWindowAsync().ConfigureAwait(false);

                lock (_pendingCommitSync)
                {
                    if (_pendingCommitBatches.Count == 0)
                    {
                        _flushInProgress = false;
                        return;
                    }

                    flushThroughSequence = _pendingCommitBatches[^1].Sequence;
                }

                if (_flushPolicy.AllowsWriteConcurrencyDuringCommitFlush)
                {
                    await _streamMutex.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        await _flushPolicy.FlushBufferedWritesAsync(
                            _stream ?? throw new CSharpDbException(ErrorCode.WalError, "WAL not open."),
                            CancellationToken.None).ConfigureAwait(false);
                    }
                    finally
                    {
                        _streamMutex.Release();
                    }

                    await _flushPolicy.FlushCommitAsync(
                        _stream ?? throw new CSharpDbException(ErrorCode.WalError, "WAL not open."),
                        CancellationToken.None).ConfigureAwait(false);
                }
                else
                {
                    await _streamMutex.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        await FlushAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    finally
                    {
                        _streamMutex.Release();
                    }
                }
            }
            catch (Exception ex)
            {
                await RewindPendingCommitBytesAsync().ConfigureAwait(false);
                FailPendingCommits(ex);
                return;
            }

            List<PendingCommitBatch> committedBatches;
            lock (_pendingCommitSync)
            {
                committedBatches = DrainCommittedBatches(flushThroughSequence);
            }

            long flushedByteCount = 0;
            foreach (var batch in committedBatches)
            {
                flushedByteCount += batch.ByteCount;
                PublishCommittedBatch(batch);
                batch.Completion.TrySetResult();
            }

            if (committedBatches.Count > 0)
            {
                Interlocked.Increment(ref _flushCount);
                Interlocked.Add(ref _flushedCommitCount, committedBatches.Count);
                Interlocked.Add(ref _flushedByteCount, flushedByteCount);
            }

            bool morePending;
            lock (_pendingCommitSync)
            {
                // Keep commit work marked in-flight until flushed batches are visible in the WAL index.
                morePending = _pendingCommitBatches.Count > 0;
                if (!morePending)
                    _flushInProgress = false;
            }

            if (!morePending)
                return;
        }
    }

    private async Task WaitForDurableCommitBatchWindowAsync()
    {
        if (!_flushPolicy.AllowsWriteConcurrencyDuringCommitFlush ||
            _durableCommitBatchWindow <= TimeSpan.Zero)
        {
            return;
        }

        Task? pendingSignalTask = null;
        lock (_pendingCommitSync)
        {
            if (_pendingCommitBatches.Count == 0)
                return;

            if (ShouldBypassBatchWindow_NoLock())
            {
                Interlocked.Increment(ref _batchWindowThresholdBypassCount);
                return;
            }

            _pendingCommitBatchWindowSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            pendingSignalTask = _pendingCommitBatchWindowSignal.Task;
            Interlocked.Increment(ref _batchWindowWaitCount);
        }

        Task delayTask = Task.Delay(_durableCommitBatchWindow);
        Task completedTask = await Task.WhenAny(delayTask, pendingSignalTask!).ConfigureAwait(false);
        if (ReferenceEquals(completedTask, pendingSignalTask))
            Interlocked.Increment(ref _batchWindowThresholdBypassCount);

        lock (_pendingCommitSync)
        {
            if (_pendingCommitBatchWindowSignal is not null &&
                ReferenceEquals(_pendingCommitBatchWindowSignal.Task, pendingSignalTask))
            {
                _pendingCommitBatchWindowSignal = null;
            }
        }
    }

    private bool ShouldBypassBatchWindow_NoLock()
    {
        return _pendingCommitBatches.Count >= DurableCommitBatchBypassPendingCommitThreshold ||
               _pendingCommitByteCount >= DurableCommitBatchBypassPendingByteThreshold;
    }

    private List<PendingCommitBatch> DrainCommittedBatches(long flushThroughSequence)
    {
        var committedBatches = new List<PendingCommitBatch>();
        int index = 0;
        long drainedByteCount = 0;
        while (index < _pendingCommitBatches.Count && _pendingCommitBatches[index].Sequence <= flushThroughSequence)
        {
            committedBatches.Add(_pendingCommitBatches[index]);
            drainedByteCount += _pendingCommitBatches[index].ByteCount;
            index++;
        }

        if (index > 0)
        {
            _pendingCommitBatches.RemoveRange(0, index);
            _pendingCommitByteCount -= drainedByteCount;
        }

        return committedBatches;
    }

    private void FailPendingCommits(Exception exception)
    {
        CSharpDbException fault = exception as CSharpDbException
            ?? new CSharpDbException(
                ErrorCode.WalError,
                $"WAL commit flush failed for '{_walPath}'. Reopen the database before issuing more writes.",
                exception);

        List<PendingCommitBatch> batches;
        lock (_pendingCommitSync)
        {
            _writeFault = fault;
            batches = new List<PendingCommitBatch>(_pendingCommitBatches);
            _pendingCommitBatches.Clear();
            _pendingCommitByteCount = 0;
            _pendingCommitBatchWindowSignal = null;
            _flushInProgress = false;
        }

        foreach (var batch in batches)
            batch.Completion.TrySetException(fault);
    }

    private async Task RewindPendingCommitBytesAsync()
    {
        long truncateAt;
        lock (_pendingCommitSync)
        {
            if (_pendingCommitBatches.Count == 0)
                return;

            truncateAt = _pendingCommitBatches[0].Entries[0].WalOffset;
        }

        if (_stream == null)
            return;

        await _streamMutex.WaitAsync().ConfigureAwait(false);
        try
        {
            _stream.SetLength(truncateAt);
            _stream.Position = truncateAt;
            _writePosition = truncateAt;
            _uncommittedStartOffset = truncateAt;
            _uncommittedFrames.Clear();
            ClearBufferedUncommittedFrames();
            _lastUncommittedDataChecksum = 0;
            await _stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _streamMutex.Release();
        }
    }

    private void ThrowIfWriteFaulted()
    {
        if (_writeFault is not null)
            throw _writeFault;
    }

    private async ValueTask AppendFrameDirectAsync(
        uint pageId,
        ReadOnlyMemory<byte> pageData,
        CancellationToken cancellationToken)
    {
        await _streamMutex.WaitAsync(cancellationToken);
        long frameOffset = 0;
        try
        {
            frameOffset = _writePosition;
            EnsureAppendCapacity_NoLock(PageConstants.WalFrameSize);
            // Checkpoint reads reuse the shared stream and can move its cursor away from the append tail.
            _stream!.Position = _writePosition;
            uint dataChecksum = WriteWalFrame(
                _appendFrameBuffer.AsSpan(0, PageConstants.WalFrameSize),
                pageId,
                pageData.Span,
                dbPageCount: 0u);
            await _stream.WriteAsync(_appendFrameBuffer.AsMemory(0, PageConstants.WalFrameSize), cancellationToken);
            _writePosition += PageConstants.WalFrameSize;
            _uncommittedFrames.Add((pageId, frameOffset));
            _lastUncommittedDataChecksum = dataChecksum;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append WAL frame for pageId={pageId} at walOffset={frameOffset}.",
                ex);
        }
        finally
        {
            _streamMutex.Release();
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
                $"Invalid WAL page payload size for pageId={pageId}. Expected {PageConstants.PageSize}, got {pageData.Length}.");
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

    private PendingCommitBatch CreatePendingBatch(ReadOnlyMemory<WalFrameWrite> frames, long firstFrameOffset)
    {
        var entries = new PendingCommitEntry[frames.Length];
        for (int i = 0; i < frames.Length; i++)
        {
            entries[i] = new PendingCommitEntry(
                frames.Span[i].PageId,
                firstFrameOffset + (long)i * PageConstants.WalFrameSize);
        }

        return new PendingCommitBatch(entries);
    }

    private PendingCommitBatch CreatePendingBatch(List<(uint PageId, long WalOffset)> frames, bool clearSource)
    {
        var entries = new PendingCommitEntry[frames.Count];
        for (int i = 0; i < frames.Count; i++)
        {
            entries[i] = new PendingCommitEntry(frames[i].PageId, frames[i].WalOffset);
        }

        if (clearSource)
        {
            frames.Clear();
            _lastUncommittedDataChecksum = 0;
        }

        return new PendingCommitBatch(entries);
    }

    private void PublishCommittedBatch(PendingCommitBatch batch)
    {
        _index.EnsurePageCapacity(batch.Entries.Length);

        foreach (var entry in batch.Entries)
        {
            _index.AddCommittedFrame(entry.PageId, entry.WalOffset);
        }

        _index.AdvanceCommit();
    }

    private uint WriteWalFrame(Span<byte> frameDestination, uint pageId, ReadOnlySpan<byte> pageData, uint dbPageCount)
    {
        if (pageData.Length != PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Invalid WAL page payload size for pageId={pageId}. Expected {PageConstants.PageSize}, got {pageData.Length}.");
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

    private async ValueTask<IncrementalCheckpointState?> EnsureIncrementalCheckpointStateAsync(CancellationToken cancellationToken)
    {
        if (_incrementalCheckpoint is not null)
            return _incrementalCheckpoint;

        if (_stream is null)
            return null;

        var indexSnapshot = _index.GetCommittedStateSnapshot();
        var committedPages = indexSnapshot.LatestPageMap;
        int committedFrameCount = indexSnapshot.FrameCount;
        long committedCommitCount = indexSnapshot.CommitCounter;
        long streamLength;

        await _streamMutex.WaitAsync(cancellationToken);
        try
        {
            if (_stream is null)
                return null;

            streamLength = _stream.Length;
            if (streamLength <= PageConstants.WalHeaderSize)
                return null;

            if (await RequiresCommittedStateRebuildAsync(
                committedPages,
                committedFrameCount,
                streamLength,
                cancellationToken))
            {
                committedPages = new Dictionary<uint, long>(Math.Max(4, committedPages.Count));
                var pendingCommitFrames = new List<(uint PageId, long WalOffset)>(capacity: 16);
                committedFrameCount = 0;
                committedCommitCount = 0;

                _stream.Position = PageConstants.WalHeaderSize;
                byte[] frameHeaderBuffer = _recoveryFrameHeaderBuffer;
                byte[] pageDataBuffer = _recoveryPageBuffer;

                while (_stream.Position + PageConstants.WalFrameSize <= streamLength)
                {
                    long frameOffset = _stream.Position;

                    int headerRead = await _stream.ReadAsync(
                        frameHeaderBuffer.AsMemory(0, PageConstants.WalFrameHeaderSize),
                        cancellationToken);
                    int dataRead = await _stream.ReadAsync(
                        pageDataBuffer.AsMemory(0, PageConstants.PageSize),
                        cancellationToken);

                    if (headerRead != PageConstants.WalFrameHeaderSize ||
                        dataRead != PageConstants.PageSize)
                    {
                        streamLength = frameOffset;
                        break;
                    }

                    uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
                    uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
                    if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
                    {
                        streamLength = frameOffset;
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

                    if (expectedHeaderChecksum != actualHeaderChecksum ||
                        expectedDataChecksum != actualDataChecksum)
                    {
                        streamLength = frameOffset;
                        break;
                    }

                    uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
                    uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4));

                    pendingCommitFrames.Add((pageId, frameOffset));

                    if (dbPageCount == 0)
                        continue;

                    committedFrameCount += pendingCommitFrames.Count;
                    committedCommitCount++;
                    foreach (var (committedPageId, committedOffset) in pendingCommitFrames)
                        committedPages[committedPageId] = committedOffset;

                    pendingCommitFrames.Clear();
                }

                if (pendingCommitFrames.Count > 0)
                    streamLength = pendingCommitFrames[0].WalOffset;

                if (_stream.Length != streamLength)
                {
                    _stream.SetLength(streamLength);
                    _writePosition = streamLength;
                    _uncommittedStartOffset = streamLength;
                }

                _index.OverwriteCommittedState(committedPages, committedFrameCount, committedCommitCount);
            }
        }
        finally
        {
            _streamMutex.Release();
        }

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
        _incrementalCheckpoint = new IncrementalCheckpointState(snapshot, committedPageCount, streamLength);
        return _incrementalCheckpoint;
    }

    private async ValueTask<bool> RequiresCommittedStateRebuildAsync(
        IReadOnlyDictionary<uint, long> committedPages,
        int committedFrameCount,
        long streamLength,
        CancellationToken cancellationToken)
    {
        if (committedFrameCount == 0)
            return committedPages.Count != 0 || streamLength > PageConstants.WalHeaderSize;

        if (committedPages.Count == 0 || streamLength < PageConstants.WalHeaderSize + PageConstants.WalFrameSize)
            return true;

        long maxFrameOffset = streamLength - PageConstants.WalFrameSize;
        byte[] frameHeaderBuffer = _recoveryFrameHeaderBuffer;
        foreach (var entry in committedPages)
        {
            uint expectedPageId = entry.Key;
            long walOffset = entry.Value;
            if (walOffset < PageConstants.WalHeaderSize || walOffset > maxFrameOffset)
                return true;

            if ((walOffset - PageConstants.WalHeaderSize) % PageConstants.WalFrameSize != 0)
                return true;

            await ReadWalRangeFromStreamAsync(
                walOffset,
                frameHeaderBuffer.AsMemory(0, PageConstants.WalFrameHeaderSize),
                cancellationToken);

            uint actualPageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
            if (actualPageId != expectedPageId)
                return true;

            uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
            uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
            if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
                return true;

            uint expectedHeaderChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(16, 4));
            uint actualHeaderChecksum = ComputeHeaderChecksum(
                actualPageId,
                BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4)),
                frameSalt1,
                frameSalt2,
                frameHeaderBuffer.AsSpan(0, 16));
            if (expectedHeaderChecksum != actualHeaderChecksum)
                return true;
        }

        return false;
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
        if (_stream is null)
            return;

        var checkpoint = _incrementalCheckpoint;
        if (checkpoint is null)
            return;

        long retainedByteCount = _stream.Length - checkpoint.RetainedWalStartOffset;
        if (retainedByteCount <= 0)
        {
            await ResetWalAsync(pageCount, generateNewSalts: true, cancellationToken);
            _incrementalCheckpoint = null;
            return;
        }

        await CompactRetainedFramesAsync(checkpoint.RetainedWalStartOffset, retainedByteCount, pageCount, cancellationToken);
        _incrementalCheckpoint = null;
    }

    private async ValueTask CompactRetainedFramesAsync(
        long retainedWalStartOffset,
        long retainedByteCount,
        uint pageCount,
        CancellationToken cancellationToken)
    {
        if (_stream is null)
            return;

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
            await ReadWalRangeIntoAsync(sourceOffset, moveBuffer.AsMemory(0, chunkLength), cancellationToken);
            _stream.Position = destinationOffset;
            await _stream.WriteAsync(moveBuffer.AsMemory(0, chunkLength), cancellationToken);
            CaptureRetainedFrameMetadata(
                moveBuffer.AsSpan(0, chunkLength),
                destinationOffset,
                retainedLatestPages,
                ref retainedFrameCount,
                ref retainedCommitCount);
            sourceOffset += chunkLength;
            destinationOffset += chunkLength;
        }

        _stream.SetLength(PageConstants.WalHeaderSize + retainedByteCount);
        await RewriteWalHeaderAsync(pageCount, cancellationToken);
        await FlushAsync(cancellationToken);

        _index.ReplaceCommittedState(retainedLatestPages, retainedFrameCount, retainedCommitCount);
        _writePosition = _stream.Length;
        _uncommittedStartOffset = _writePosition;
    }

    private async ValueTask ResetWalAsync(uint pageCount, bool generateNewSalts, CancellationToken cancellationToken)
    {
        if (_stream is null)
            return;

        _index.Reset();

        if (generateNewSalts)
        {
            _salt1 = (uint)Random.Shared.Next();
            _salt2 = (uint)Random.Shared.Next();
        }

        _stream.Position = 0;
        WriteWalHeader(_walHeaderBuffer, pageCount);
        await _stream.WriteAsync(_walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
        _stream.SetLength(PageConstants.WalHeaderSize);
        await FlushAsync(cancellationToken);
        _writePosition = PageConstants.WalHeaderSize;
        _uncommittedStartOffset = PageConstants.WalHeaderSize;
    }

    private async ValueTask RewriteWalHeaderAsync(uint pageCount, CancellationToken cancellationToken)
    {
        if (_stream is null)
            return;

        _stream.Position = 0;
        WriteWalHeader(_walHeaderBuffer, pageCount);
        await _stream.WriteAsync(_walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
    }

    private ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        if (_stream is null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        return FlushCoreAsync(_stream, cancellationToken);
    }

    private async ValueTask FlushCoreAsync(FileStream stream, CancellationToken cancellationToken)
    {
        await _flushPolicy.FlushBufferedWritesAsync(stream, cancellationToken);
        await _flushPolicy.FlushCommitAsync(stream, cancellationToken);
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
                $"Retained WAL suffix length {retainedBytes.Length} was not frame-aligned during compaction.");
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

    private readonly record struct PendingCommitEntry(uint PageId, long WalOffset);

    private sealed class PendingCommitBatch
    {
        public PendingCommitBatch(PendingCommitEntry[] entries)
        {
            Entries = entries;
            ByteCount = (long)entries.Length * PageConstants.WalFrameSize;
        }

        public long Sequence { get; set; }
        public PendingCommitEntry[] Entries { get; }
        public long ByteCount { get; }
        public TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly record struct BufferedUncommittedFrame(uint PageId, byte[] Buffer);
}

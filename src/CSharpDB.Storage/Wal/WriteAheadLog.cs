using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;
using Microsoft.Win32.SafeHandles;
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
public sealed class WriteAheadLog : IWriteAheadLog
{
    private const int WalStreamBufferSize = 64 * 1024;
    private const int AppendFrameChunkSize = 16;
    private const int CheckpointWriteChunkPages = 16;
    private static readonly IComparer<KeyValuePair<uint, long>> PageIdComparer =
        Comparer<KeyValuePair<uint, long>>.Create(static (left, right) => left.Key.CompareTo(right.Key));

    private readonly string _walPath;
    private FileStream? _stream;
    private SafeFileHandle? _readHandle; // separate read handle for concurrent reads
    private readonly WalIndex _index;
    private readonly IPageChecksumProvider _checksumProvider;
    private readonly bool _useAdditiveHeaderChecksum;
    private readonly IWalFlushPolicy _flushPolicy;

    // WAL header fields
    private uint _salt1;
    private uint _salt2;

    // Uncommitted frame tracking for current transaction
    private readonly List<(uint PageId, long WalOffset)> _uncommittedFrames = new(capacity: 256);
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

    public WriteAheadLog(
        string databasePath,
        WalIndex index,
        IPageChecksumProvider? checksumProvider = null,
        DurabilityMode durabilityMode = DurabilityMode.Durable)
        : this(databasePath, index, checksumProvider, WalFlushPolicy.Create(durabilityMode))
    {
    }

    internal WriteAheadLog(
        string databasePath,
        WalIndex index,
        IPageChecksumProvider? checksumProvider,
        IWalFlushPolicy flushPolicy)
    {
        _walPath = databasePath + ".wal";
        _index = index;
        _checksumProvider = checksumProvider ?? new AdditiveChecksumProvider();
        _useAdditiveHeaderChecksum = _checksumProvider is AdditiveChecksumProvider;
        _flushPolicy = flushPolicy;
    }

    public WalIndex Index => _index;
    public bool IsOpen => _stream != null;
    public bool HasPendingCheckpoint => _incrementalCheckpoint is not null;
    internal IWalFlushPolicy FlushPolicy => _flushPolicy;

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
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
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

            _uncommittedStartOffset = _stream.Position;
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
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _uncommittedStartOffset = _stream.Position;
    }

    /// <summary>
    /// Append a dirty page to the WAL as an uncommitted frame.
    /// </summary>
    public async ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData,
        CancellationToken cancellationToken = default)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        long frameOffset = _stream.Position;

        try
        {
            uint dataChecksum = WriteWalFrame(
                _appendFrameBuffer.AsSpan(0, PageConstants.WalFrameSize),
                pageId,
                pageData.Span,
                dbPageCount: 0u);
            await _stream.WriteAsync(_appendFrameBuffer.AsMemory(0, PageConstants.WalFrameSize), cancellationToken);
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
    }

    public async ValueTask AppendFramesAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        CancellationToken cancellationToken = default)
    {
        await AppendFramesCoreAsync(
            frames,
            commitOnLastFrame: false,
            newDbPageCount: 0u,
            trackUncommittedFrames: true,
            cancellationToken);
    }

    public async ValueTask AppendFramesAndCommitAsync(
        ReadOnlyMemory<WalFrameWrite> frames,
        uint newDbPageCount,
        CancellationToken cancellationToken = default)
    {
        if (frames.IsEmpty)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");
        if (_uncommittedFrames.Count != 0)
            throw new CSharpDbException(ErrorCode.WalError, "AppendFramesAndCommitAsync cannot be used with existing uncommitted frames.");
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        long firstFrameOffset = _stream.Position;
        await AppendFramesCoreAsync(
            frames,
            commitOnLastFrame: true,
            newDbPageCount,
            trackUncommittedFrames: false,
            cancellationToken);

        try
        {
            await FlushAsync(cancellationToken);
            PublishCommittedFramesFromBatch(frames, firstFrameOffset);
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
    public async ValueTask CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        if (_uncommittedFrames.Count == 0)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");

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

        try
        {
            _stream.Position = lastOffset;
            await _stream.WriteAsync(frameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);

            // Seek to end for next writes
            _stream.Position = lastOffset + PageConstants.WalFrameSize;

            // Flush to disk — this is the commit point
            await FlushAsync(cancellationToken);

            PublishCommittedFrames();
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to commit WAL transaction with {_uncommittedFrames.Count} frame(s), newDbPageCount={newDbPageCount}, commitFrameOffset={lastOffset}.",
                ex);
        }
    }

    /// <summary>
    /// Rollback: truncate the WAL file back to where the transaction started.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_stream == null) return;
        try
        {
            _stream.SetLength(_uncommittedStartOffset);
            _stream.Position = _uncommittedStartOffset;
            await FlushAsync(cancellationToken);
            _uncommittedFrames.Clear();
            _lastUncommittedDataChecksum = 0;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to rollback WAL transaction to offset {_uncommittedStartOffset}.",
                ex);
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
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
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

            _stream.Position = _stream.Length;
            _uncommittedStartOffset = _stream.Position;
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
            var checkpoint = EnsureIncrementalCheckpointState();
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
            _incrementalCheckpoint = null;
            _uncommittedFrames.Clear();
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
        _incrementalCheckpoint = null;
        _uncommittedFrames.Clear();
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

        if (hasContiguousWalOffsets)
        {
            int readByteCount = pageCount * PageConstants.WalFrameSize;
            await ReadWalRangeIntoAsync(startWalOffset, checkpointReadBuffer.AsMemory(0, readByteCount), cancellationToken);

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
            while (frameIndex < totalFrameCount)
            {
                int framesInChunk = Math.Min(AppendFrameChunkSize, totalFrameCount - frameIndex);
                long chunkStartOffset = _stream.Position;

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

    private void PublishCommittedFramesFromBatch(ReadOnlyMemory<WalFrameWrite> frames, long firstFrameOffset)
    {
        _index.EnsurePageCapacity(frames.Length);

        for (int i = 0; i < frames.Length; i++)
        {
            long frameOffset = firstFrameOffset + (long)i * PageConstants.WalFrameSize;
            _index.AddCommittedFrame(frames.Span[i].PageId, frameOffset);
        }

        _index.AdvanceCommit();
        _lastUncommittedDataChecksum = 0;
    }

    private void PublishCommittedFrames()
    {
        _index.EnsurePageCapacity(_uncommittedFrames.Count);

        foreach (var (pageId, walOffset) in _uncommittedFrames)
        {
            _index.AddCommittedFrame(pageId, walOffset);
        }

        _index.AdvanceCommit();
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
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

    private IncrementalCheckpointState? EnsureIncrementalCheckpointState()
    {
        if (_incrementalCheckpoint is not null)
            return _incrementalCheckpoint;

        if (_stream is null)
            return null;

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
        _incrementalCheckpoint = new IncrementalCheckpointState(snapshot, committedPageCount, _stream.Length);
        return _incrementalCheckpoint;
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
        _uncommittedStartOffset = _stream.Length;
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

        return _flushPolicy.FlushAsync(_stream, cancellationToken);
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
}

using CSharpDB.Core;
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
public sealed class WriteAheadLog : IWriteAheadLog
{
    private const int WalStreamBufferSize = 64 * 1024;
    private const int CheckpointWriteChunkPages = 16;
    private static readonly IComparer<KeyValuePair<uint, long>> PageIdComparer =
        Comparer<KeyValuePair<uint, long>>.Create(static (left, right) => left.Key.CompareTo(right.Key));

    private readonly string _walPath;
    private FileStream? _stream;
    private SafeFileHandle? _readHandle; // separate read handle for concurrent reads
    private readonly WalIndex _index;
    private readonly IPageChecksumProvider _checksumProvider;

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
    private readonly byte[] _recoveryFrameHeaderBuffer = new byte[PageConstants.WalFrameHeaderSize];
    private readonly byte[] _recoveryPageBuffer = new byte[PageConstants.PageSize];
    private readonly byte[] _checkpointWriteBuffer = new byte[PageConstants.PageSize * CheckpointWriteChunkPages];

    public WriteAheadLog(
        string databasePath,
        WalIndex index,
        IPageChecksumProvider? checksumProvider = null)
    {
        _walPath = databasePath + ".wal";
        _index = index;
        _checksumProvider = checksumProvider ?? new AdditiveChecksumProvider();
    }

    public WalIndex Index => _index;
    public bool IsOpen => _stream != null;

    // ============ Open / Create ============

    /// <summary>
    /// Open an existing WAL file or create a new one.
    /// If a WAL file exists, scan it to rebuild the index (recovery).
    /// </summary>
    public async ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default)
    {
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
            _stream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite,
                FileShare.Read, bufferSize: WalStreamBufferSize, useAsync: true);

            _salt1 = (uint)Random.Shared.Next();
            _salt2 = (uint)Random.Shared.Next();

            WriteWalHeader(_walHeaderBuffer, dbPageCount);
            await _stream.WriteAsync(_walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
            await _stream.FlushAsync(cancellationToken);

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

        var frameHeader = _appendFrameHeader;
        var frameHeaderSpan = frameHeader.AsSpan(0, PageConstants.WalFrameHeaderSize);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(0, 4), pageId);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(4, 4), 0u); // dbPageCount=0 means non-commit
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(8, 4), _salt1);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(12, 4), _salt2);

        uint headerChecksum = _checksumProvider.Compute(frameHeaderSpan.Slice(0, 16));
        uint dataChecksum = _checksumProvider.Compute(pageData.Span);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(16, 4), headerChecksum);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(20, 4), dataChecksum);

        try
        {
            frameHeaderSpan.CopyTo(_appendFrameBuffer.AsSpan(0, PageConstants.WalFrameHeaderSize));
            pageData.Span.CopyTo(_appendFrameBuffer.AsSpan(PageConstants.WalFrameHeaderSize, PageConstants.PageSize));
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

        uint headerChecksum = _checksumProvider.Compute(frameHeaderSpan.Slice(0, 16));
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(16, 4), headerChecksum);
        BinaryPrimitives.WriteUInt32LittleEndian(frameHeaderSpan.Slice(20, 4), lastDataChecksum);

        try
        {
            _stream.Position = lastOffset;
            await _stream.WriteAsync(frameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);

            // Seek to end for next writes
            _stream.Position = lastOffset + PageConstants.WalFrameSize;

            // Flush to disk — this is the commit point
            await _stream.FlushAsync(cancellationToken);

            // Update the in-memory WAL index
            foreach (var (pageId, walOffset) in _uncommittedFrames)
            {
                _index.AddCommittedFrame(pageId, walOffset);
            }
            _index.AdvanceCommit();
            _uncommittedFrames.Clear();
            _lastUncommittedDataChecksum = 0;
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
            await _stream.FlushAsync(cancellationToken);
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
            _stream = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite,
                FileShare.Read, bufferSize: WalStreamBufferSize, useAsync: true);

            // Read and validate header
            var header = _walHeaderBuffer;
            if (await _stream.ReadAsync(header.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken) != PageConstants.WalHeaderSize)
                throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: header too short.");

            if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
                throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: bad magic.");

            _salt1 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
            _salt2 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(20, 4));

            // Scan frames
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
                    // Partial frame — truncate
                    _stream.SetLength(frameOffset);
                    break;
                }

                // Validate salt
                uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
                uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
                if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
                {
                    _stream.SetLength(frameOffset);
                    break;
                }

                // Validate checksums
                uint expectedHeaderChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(16, 4));
                uint expectedDataChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(20, 4));
                uint actualHeaderChecksum = _checksumProvider.Compute(frameHeaderBuffer.AsSpan(0, 16));
                uint actualDataChecksum = _checksumProvider.Compute(pageDataBuffer);

                if (expectedHeaderChecksum != actualHeaderChecksum ||
                    expectedDataChecksum != actualDataChecksum)
                {
                    // Corrupt frame — truncate
                    _stream.SetLength(frameOffset);
                    break;
                }

                uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
                uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4));

                uncommittedBatch.Add((pageId, frameOffset));

                if (dbPageCount != 0)
                {
                    // Commit frame — all frames in this batch are committed
                    foreach (var (committedPageId, committedOffset) in uncommittedBatch)
                    {
                        _index.AddCommittedFrame(committedPageId, committedOffset);
                    }
                    _index.AdvanceCommit();
                    uncommittedBatch.Clear();
                }
            }

            // Remaining frames are from an incomplete transaction — truncate
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
        if (_stream == null) return;

        var committedPages = _index.GetAllCommittedPages();
        int committedPageCount = committedPages.Count;
        if (committedPageCount == 0) return;

        KeyValuePair<uint, long>[]? sortedCommittedPages = null;

        try
        {
            sortedCommittedPages = ArrayPool<KeyValuePair<uint, long>>.Shared.Rent(committedPageCount);
            int sortedCount = 0;
            bool isPageIdSortedAscending = true;
            uint previousPageId = 0;
            foreach (var committedPage in committedPages)
            {
                if (sortedCount > 0 && committedPage.Key < previousPageId)
                {
                    isPageIdSortedAscending = false;
                }

                sortedCommittedPages[sortedCount++] = committedPage;
                previousPageId = committedPage.Key;
            }

            if (!isPageIdSortedAscending && committedPageCount > 1)
            {
                Array.Sort(sortedCommittedPages, 0, committedPageCount, PageIdComparer);
            }

            // Ensure DB file is large enough
            long requiredLength = (long)pageCount * PageConstants.PageSize;
            if (device.Length < requiredLength)
            {
                await device.SetLengthAsync(requiredLength, cancellationToken);
            }

            uint batchStartPageId = 0;
            int batchPageCount = 0;

            for (int i = 0; i < committedPageCount; i++)
            {
                var committedPage = sortedCommittedPages[i];
                uint pageId = committedPage.Key;
                long walOffset = committedPage.Value;
                bool startsNewBatch = batchPageCount == 0 ||
                    (ulong)pageId != (ulong)batchStartPageId + (uint)batchPageCount ||
                    batchPageCount == CheckpointWriteChunkPages;

                if (startsNewBatch && batchPageCount > 0)
                {
                    await FlushCheckpointBatchAsync(device, batchStartPageId, batchPageCount, cancellationToken);
                    batchPageCount = 0;
                }

                if (batchPageCount == 0)
                    batchStartPageId = pageId;

                var destination = _checkpointWriteBuffer.AsMemory(
                    batchPageCount * PageConstants.PageSize,
                    PageConstants.PageSize);
                await ReadPageIntoFromStreamAsync(walOffset, destination, cancellationToken);
                batchPageCount++;
            }

            if (batchPageCount > 0)
            {
                await FlushCheckpointBatchAsync(device, batchStartPageId, batchPageCount, cancellationToken);
            }

            await device.FlushAsync(cancellationToken);

            // Reset WAL
            _index.Reset();

            _salt1 = (uint)Random.Shared.Next();
            _salt2 = (uint)Random.Shared.Next();

            // Rewrite WAL header with new salts
            _stream.Position = 0;
            WriteWalHeader(_walHeaderBuffer, pageCount);
            await _stream.WriteAsync(_walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);

            _stream.SetLength(PageConstants.WalHeaderSize);
            await _stream.FlushAsync(cancellationToken);
            _uncommittedStartOffset = PageConstants.WalHeaderSize;
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to checkpoint WAL '{_walPath}' with {committedPageCount} committed page(s).",
                ex);
        }
        finally
        {
            if (sortedCommittedPages != null)
            {
                ArrayPool<KeyValuePair<uint, long>>.Shared.Return(sortedCommittedPages, clearArray: false);
            }
        }
    }

    /// <summary>
    /// Delete the WAL file entirely. Called when closing the database after a final checkpoint.
    /// </summary>
    public async ValueTask CloseAndDeleteAsync()
    {
        try
        {
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

    private async ValueTask ReadPageIntoFromStreamAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        long dataOffset = walFrameOffset + PageConstants.WalFrameHeaderSize;

        try
        {
            _stream.Position = dataOffset;
            await _stream.ReadExactlyAsync(destination, cancellationToken);
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to read WAL page from stream at walFrameOffset={walFrameOffset}.",
                ex);
        }
    }

    private ValueTask FlushCheckpointBatchAsync(
        IStorageDevice device,
        uint startPageId,
        int pageCount,
        CancellationToken cancellationToken)
    {
        long dbOffset = (long)startPageId * PageConstants.PageSize;
        int byteCount = pageCount * PageConstants.PageSize;
        return device.WriteAsync(dbOffset, _checkpointWriteBuffer.AsMemory(0, byteCount), cancellationToken);
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
}

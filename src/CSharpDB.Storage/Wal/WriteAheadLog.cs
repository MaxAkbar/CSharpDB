using CSharpDB.Core;
using Microsoft.Win32.SafeHandles;

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
    private readonly string _walPath;
    private FileStream? _stream;
    private SafeFileHandle? _readHandle; // separate read handle for concurrent reads
    private readonly WalIndex _index;
    private readonly IPageChecksumProvider _checksumProvider;

    // WAL header fields
    private uint _salt1;
    private uint _salt2;

    // Uncommitted frame tracking for current transaction
    private readonly List<(uint PageId, long WalOffset)> _uncommittedFrames = new();
    private long _uncommittedStartOffset;

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
        _stream = new FileStream(_walPath, FileMode.Create, FileAccess.ReadWrite,
            FileShare.Read, bufferSize: 4096, useAsync: true);

        _salt1 = (uint)Random.Shared.Next();
        _salt2 = (uint)Random.Shared.Next();

        var header = new byte[PageConstants.WalHeaderSize];
        PageConstants.WalMagic.AsSpan().CopyTo(header);
        BitConverter.TryWriteBytes(header.AsSpan(4), 1); // version
        BitConverter.TryWriteBytes(header.AsSpan(8), PageConstants.PageSize);
        BitConverter.TryWriteBytes(header.AsSpan(12), dbPageCount);
        BitConverter.TryWriteBytes(header.AsSpan(16), _salt1);
        BitConverter.TryWriteBytes(header.AsSpan(20), _salt2);

        await _stream.WriteAsync(header, cancellationToken);
        await _stream.FlushAsync(cancellationToken);

        _uncommittedStartOffset = _stream.Position;
        OpenReadHandle();
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

        var frameHeader = new byte[PageConstants.WalFrameHeaderSize];
        BitConverter.TryWriteBytes(frameHeader.AsSpan(0), pageId);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(4), 0u); // dbPageCount=0 means non-commit
        BitConverter.TryWriteBytes(frameHeader.AsSpan(8), _salt1);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(12), _salt2);

        uint headerChecksum = _checksumProvider.Compute(frameHeader.AsSpan(0, 16));
        uint dataChecksum = _checksumProvider.Compute(pageData.Span);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(16), headerChecksum);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(20), dataChecksum);

        await _stream.WriteAsync(frameHeader, cancellationToken);
        await _stream.WriteAsync(pageData, cancellationToken);

        _uncommittedFrames.Add((pageId, frameOffset));
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
        var (_, lastOffset) = _uncommittedFrames[^1];

        // Re-read the first 16 bytes of the frame header to recalculate checksum
        _stream.Position = lastOffset;
        var lastFrameHeader = new byte[PageConstants.WalFrameHeaderSize];
        await _stream.ReadAsync(lastFrameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);

        // Update dbPageCount
        BitConverter.TryWriteBytes(lastFrameHeader.AsSpan(4), newDbPageCount);

        // Recalculate header checksum
        uint newHeaderChecksum = _checksumProvider.Compute(lastFrameHeader.AsSpan(0, 16));
        BitConverter.TryWriteBytes(lastFrameHeader.AsSpan(16), newHeaderChecksum);

        // Write updated header back
        _stream.Position = lastOffset;
        await _stream.WriteAsync(lastFrameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), cancellationToken);

        // Seek to end for next writes
        _stream.Position = _stream.Length;

        // Flush to disk — this is the commit point
        await _stream.FlushAsync(cancellationToken);

        // Update the in-memory WAL index
        foreach (var (pageId, walOffset) in _uncommittedFrames)
        {
            _index.AddCommittedFrame(pageId, walOffset);
        }
        _index.AdvanceCommit();

        _uncommittedFrames.Clear();
    }

    /// <summary>
    /// Rollback: truncate the WAL file back to where the transaction started.
    /// </summary>
    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_stream == null) return;
        _stream.SetLength(_uncommittedStartOffset);
        _stream.Position = _uncommittedStartOffset;
        await _stream.FlushAsync(cancellationToken);
        _uncommittedFrames.Clear();
    }

    // ============ Read operations ============

    /// <summary>
    /// Read a page from the WAL at the given frame offset.
    /// Uses a separate read handle for thread safety with concurrent readers.
    /// </summary>
    public async ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default)
    {
        var page = new byte[PageConstants.PageSize];
        long dataOffset = walFrameOffset + PageConstants.WalFrameHeaderSize;

        if (_readHandle != null)
        {
            await RandomAccess.ReadAsync(_readHandle, page, dataOffset, cancellationToken);
        }
        else if (_stream != null)
        {
            _stream.Position = dataOffset;
            int bytesRead = 0;
            while (bytesRead < PageConstants.PageSize)
            {
                int read = await _stream.ReadAsync(
                    page.AsMemory(bytesRead, PageConstants.PageSize - bytesRead), cancellationToken);
                if (read == 0) break;
                bytesRead += read;
            }
        }
        else
        {
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");
        }

        return page;
    }

    // ============ Recovery ============

    /// <summary>
    /// Scan an existing WAL file, validate frames, and rebuild the index
    /// with only committed frames.
    /// </summary>
    private async ValueTask RecoverAsync(CancellationToken cancellationToken)
    {
        _stream = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite,
            FileShare.Read, bufferSize: 4096, useAsync: true);

        // Read and validate header
        var header = new byte[PageConstants.WalHeaderSize];
        if (await _stream.ReadAsync(header, cancellationToken) != PageConstants.WalHeaderSize)
            throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: header too short.");

        if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
            throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: bad magic.");

        _salt1 = BitConverter.ToUInt32(header, 16);
        _salt2 = BitConverter.ToUInt32(header, 20);

        // Scan frames
        var uncommittedBatch = new List<(uint PageId, long WalOffset)>();
        var frameHeaderBuffer = new byte[PageConstants.WalFrameHeaderSize];
        var pageDataBuffer = new byte[PageConstants.PageSize];

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
            uint frameSalt1 = BitConverter.ToUInt32(frameHeaderBuffer, 8);
            uint frameSalt2 = BitConverter.ToUInt32(frameHeaderBuffer, 12);
            if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
            {
                _stream.SetLength(frameOffset);
                break;
            }

            // Validate checksums
            uint expectedHeaderChecksum = BitConverter.ToUInt32(frameHeaderBuffer, 16);
            uint expectedDataChecksum = BitConverter.ToUInt32(frameHeaderBuffer, 20);
            uint actualHeaderChecksum = _checksumProvider.Compute(frameHeaderBuffer.AsSpan(0, 16));
            uint actualDataChecksum = _checksumProvider.Compute(pageDataBuffer);

            if (expectedHeaderChecksum != actualHeaderChecksum ||
                expectedDataChecksum != actualDataChecksum)
            {
                // Corrupt frame — truncate
                _stream.SetLength(frameOffset);
                break;
            }

            uint pageId = BitConverter.ToUInt32(frameHeaderBuffer, 0);
            uint dbPageCount = BitConverter.ToUInt32(frameHeaderBuffer, 4);

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

        _stream.Position = _stream.Length;
        _uncommittedStartOffset = _stream.Position;
        OpenReadHandle();
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
        if (committedPages.Count == 0) return;

        // Ensure DB file is large enough
        long requiredLength = (long)pageCount * PageConstants.PageSize;
        if (device.Length < requiredLength)
        {
            await device.SetLengthAsync(requiredLength, cancellationToken);
        }

        // Read each committed page from WAL and write to DB file
        foreach (var (pageId, walOffset) in committedPages)
        {
            var pageData = await ReadPageAsync(walOffset, cancellationToken);
            await device.WriteAsync((long)pageId * PageConstants.PageSize, pageData, cancellationToken);
        }

        await device.FlushAsync(cancellationToken);

        // Reset WAL
        _index.Reset();
        CloseReadHandle();

        _salt1 = (uint)Random.Shared.Next();
        _salt2 = (uint)Random.Shared.Next();

        // Rewrite WAL header with new salts
        _stream.Position = 0;
        var header = new byte[PageConstants.WalHeaderSize];
        PageConstants.WalMagic.AsSpan().CopyTo(header);
        BitConverter.TryWriteBytes(header.AsSpan(4), 1);
        BitConverter.TryWriteBytes(header.AsSpan(8), PageConstants.PageSize);
        BitConverter.TryWriteBytes(header.AsSpan(12), pageCount);
        BitConverter.TryWriteBytes(header.AsSpan(16), _salt1);
        BitConverter.TryWriteBytes(header.AsSpan(20), _salt2);
        await _stream.WriteAsync(header, cancellationToken);

        _stream.SetLength(PageConstants.WalHeaderSize);
        await _stream.FlushAsync(cancellationToken);
        _uncommittedStartOffset = PageConstants.WalHeaderSize;

        OpenReadHandle();
    }

    /// <summary>
    /// Delete the WAL file entirely. Called when closing the database after a final checkpoint.
    /// </summary>
    public async ValueTask CloseAndDeleteAsync()
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

    private void CloseReadHandle()
    {
        if (_readHandle != null)
        {
            _readHandle.Dispose();
            _readHandle = null;
        }
    }
}

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

    // WAL header fields
    private uint _salt1;
    private uint _salt2;

    // Uncommitted frame tracking for current transaction
    private readonly List<(uint PageId, long WalOffset)> _uncommittedFrames = new();
    private long _uncommittedStartOffset;

    public WriteAheadLog(string databasePath, WalIndex index)
    {
        _walPath = databasePath + ".wal";
        _index = index;
    }

    public WalIndex Index => _index;
    public bool IsOpen => _stream != null;

    // ============ Open / Create ============

    /// <summary>
    /// Open an existing WAL file or create a new one.
    /// If a WAL file exists, scan it to rebuild the index (recovery).
    /// </summary>
    public async ValueTask OpenAsync(uint currentDbPageCount, CancellationToken ct = default)
    {
        if (File.Exists(_walPath))
        {
            await RecoverAsync(ct);
        }
        else
        {
            await CreateNewAsync(currentDbPageCount, ct);
        }
    }

    private async ValueTask CreateNewAsync(uint dbPageCount, CancellationToken ct)
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

        await _stream.WriteAsync(header, ct);
        await _stream.FlushAsync(ct);

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
        CancellationToken ct = default)
    {
        if (_stream == null)
            throw new CSharpDbException(ErrorCode.WalError, "WAL not open.");

        long frameOffset = _stream.Position;

        var frameHeader = new byte[PageConstants.WalFrameHeaderSize];
        BitConverter.TryWriteBytes(frameHeader.AsSpan(0), pageId);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(4), 0u); // dbPageCount=0 means non-commit
        BitConverter.TryWriteBytes(frameHeader.AsSpan(8), _salt1);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(12), _salt2);

        uint headerCksum = SimpleChecksum(frameHeader.AsSpan(0, 16));
        uint dataCksum = SimpleChecksum(pageData.Span);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(16), headerCksum);
        BitConverter.TryWriteBytes(frameHeader.AsSpan(20), dataCksum);

        await _stream.WriteAsync(frameHeader, ct);
        await _stream.WriteAsync(pageData, ct);

        _uncommittedFrames.Add((pageId, frameOffset));
    }

    /// <summary>
    /// Commit the current transaction. Marks the last frame as a commit frame,
    /// flushes to disk, then updates the in-memory WAL index.
    /// </summary>
    public async ValueTask CommitAsync(uint newDbPageCount, CancellationToken ct = default)
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
        await _stream.ReadAsync(lastFrameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), ct);

        // Update dbPageCount
        BitConverter.TryWriteBytes(lastFrameHeader.AsSpan(4), newDbPageCount);

        // Recalculate header checksum
        uint newHeaderCksum = SimpleChecksum(lastFrameHeader.AsSpan(0, 16));
        BitConverter.TryWriteBytes(lastFrameHeader.AsSpan(16), newHeaderCksum);

        // Write updated header back
        _stream.Position = lastOffset;
        await _stream.WriteAsync(lastFrameHeader.AsMemory(0, PageConstants.WalFrameHeaderSize), ct);

        // Seek to end for next writes
        _stream.Position = _stream.Length;

        // Flush to disk — this is the commit point
        await _stream.FlushAsync(ct);

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
    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_stream == null) return;
        _stream.SetLength(_uncommittedStartOffset);
        _stream.Position = _uncommittedStartOffset;
        await _stream.FlushAsync(ct);
        _uncommittedFrames.Clear();
    }

    // ============ Read operations ============

    /// <summary>
    /// Read a page from the WAL at the given frame offset.
    /// Uses a separate read handle for thread safety with concurrent readers.
    /// </summary>
    public async ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken ct = default)
    {
        var page = new byte[PageConstants.PageSize];
        long dataOffset = walFrameOffset + PageConstants.WalFrameHeaderSize;

        if (_readHandle != null)
        {
            await RandomAccess.ReadAsync(_readHandle, page, dataOffset, ct);
        }
        else if (_stream != null)
        {
            _stream.Position = dataOffset;
            int bytesRead = 0;
            while (bytesRead < PageConstants.PageSize)
            {
                int read = await _stream.ReadAsync(
                    page.AsMemory(bytesRead, PageConstants.PageSize - bytesRead), ct);
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
    private async ValueTask RecoverAsync(CancellationToken ct)
    {
        _stream = new FileStream(_walPath, FileMode.Open, FileAccess.ReadWrite,
            FileShare.Read, bufferSize: 4096, useAsync: true);

        // Read and validate header
        var header = new byte[PageConstants.WalHeaderSize];
        if (await _stream.ReadAsync(header, ct) != PageConstants.WalHeaderSize)
            throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: header too short.");

        if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
            throw new CSharpDbException(ErrorCode.WalError, "Invalid WAL file: bad magic.");

        _salt1 = BitConverter.ToUInt32(header, 16);
        _salt2 = BitConverter.ToUInt32(header, 20);

        // Scan frames
        var uncommittedBatch = new List<(uint PageId, long WalOffset)>();
        var frameHeaderBuf = new byte[PageConstants.WalFrameHeaderSize];
        var pageDataBuf = new byte[PageConstants.PageSize];

        while (_stream.Position + PageConstants.WalFrameSize <= _stream.Length)
        {
            long frameOffset = _stream.Position;

            int hdrRead = await _stream.ReadAsync(frameHeaderBuf, ct);
            int dataRead = await _stream.ReadAsync(pageDataBuf, ct);

            if (hdrRead != PageConstants.WalFrameHeaderSize ||
                dataRead != PageConstants.PageSize)
            {
                // Partial frame — truncate
                _stream.SetLength(frameOffset);
                break;
            }

            // Validate salt
            uint frameSalt1 = BitConverter.ToUInt32(frameHeaderBuf, 8);
            uint frameSalt2 = BitConverter.ToUInt32(frameHeaderBuf, 12);
            if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
            {
                _stream.SetLength(frameOffset);
                break;
            }

            // Validate checksums
            uint expectedHeaderCksum = BitConverter.ToUInt32(frameHeaderBuf, 16);
            uint expectedDataCksum = BitConverter.ToUInt32(frameHeaderBuf, 20);
            uint actualHeaderCksum = SimpleChecksum(frameHeaderBuf.AsSpan(0, 16));
            uint actualDataCksum = SimpleChecksum(pageDataBuf);

            if (expectedHeaderCksum != actualHeaderCksum ||
                expectedDataCksum != actualDataCksum)
            {
                // Corrupt frame — truncate
                _stream.SetLength(frameOffset);
                break;
            }

            uint pageId = BitConverter.ToUInt32(frameHeaderBuf, 0);
            uint dbPageCount = BitConverter.ToUInt32(frameHeaderBuf, 4);

            uncommittedBatch.Add((pageId, frameOffset));

            if (dbPageCount != 0)
            {
                // Commit frame — all frames in this batch are committed
                foreach (var (pid, off) in uncommittedBatch)
                {
                    _index.AddCommittedFrame(pid, off);
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
    public async ValueTask CheckpointAsync(IStorageDevice device, uint pageCount, CancellationToken ct = default)
    {
        if (_stream == null) return;

        var committedPages = _index.GetAllCommittedPages();
        if (committedPages.Count == 0) return;

        // Ensure DB file is large enough
        long requiredLength = (long)pageCount * PageConstants.PageSize;
        if (device.Length < requiredLength)
        {
            await device.SetLengthAsync(requiredLength, ct);
        }

        // Read each committed page from WAL and write to DB file
        foreach (var (pageId, walOffset) in committedPages)
        {
            var pageData = await ReadPageAsync(walOffset, ct);
            await device.WriteAsync((long)pageId * PageConstants.PageSize, pageData, ct);
        }

        await device.FlushAsync(ct);

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
        await _stream.WriteAsync(header, ct);

        _stream.SetLength(PageConstants.WalHeaderSize);
        await _stream.FlushAsync(ct);
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

    // ============ Checksum ============

    /// <summary>
    /// Simple additive checksum for detecting torn writes / corruption.
    /// </summary>
    internal static uint SimpleChecksum(ReadOnlySpan<byte> data)
    {
        uint sum = 0;
        int i = 0;
        for (; i + 3 < data.Length; i += 4)
        {
            sum += BitConverter.ToUInt32(data[i..]);
        }
        for (; i < data.Length; i++)
        {
            sum += data[i];
        }
        return sum;
    }
}

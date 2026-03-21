using CSharpDB.Primitives;
using CSharpDB.Storage.Device;
using System.Buffers.Binary;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory WAL implementation that preserves the same frame/header format as the file-backed WAL.
/// This allows load-from-disk recovery to run entirely in memory.
/// </summary>
public sealed class MemoryWriteAheadLog : IWriteAheadLog
{
    private const int AppendFrameChunkSize = 16;
    private const int CheckpointWriteChunkPages = 16;
    private static readonly IComparer<KeyValuePair<uint, long>> PageIdComparer =
        Comparer<KeyValuePair<uint, long>>.Create(static (left, right) => left.Key.CompareTo(right.Key));

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

    public MemoryWriteAheadLog(
        WalIndex index,
        IPageChecksumProvider? checksumProvider = null,
        ReadOnlyMemory<byte> initialBytes = default)
    {
        _storage = new MemoryStorageDevice(initialBytes);
        _seedBytes = initialBytes.IsEmpty ? Array.Empty<byte>() : initialBytes.ToArray();
        _index = index;
        _checksumProvider = checksumProvider ?? new AdditiveChecksumProvider();
        _useAdditiveHeaderChecksum = _checksumProvider is AdditiveChecksumProvider;
    }

    public async ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default)
    {
        if (_isOpen)
            return;

        if (!_seedConsumed && _seedBytes.Length > 0)
        {
            _seedConsumed = true;
            await RecoverAsync(cancellationToken);
            return;
        }

        _seedConsumed = true;
        await CreateNewAsync(currentDbPageCount, cancellationToken);
    }

    public bool HasPendingCheckpoint => _incrementalCheckpoint is not null;
    public bool IsOpen => _isOpen;

    public void BeginTransaction()
    {
        EnsureOpen();
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _uncommittedStartOffset = _storage.Length;
    }

    public async ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken cancellationToken = default)
    {
        EnsureOpen();

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

    public async ValueTask AppendFramesAsync(ReadOnlyMemory<WalFrameWrite> frames, CancellationToken cancellationToken = default)
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
        }
        catch (Exception ex) when (ex is not CSharpDbException && ex is not OperationCanceledException)
        {
            throw new CSharpDbException(
                ErrorCode.WalError,
                $"Failed to append+commit {frames.Length} in-memory WAL frame(s), newDbPageCount={newDbPageCount}.",
                ex);
        }
    }

    public async ValueTask CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        if (_uncommittedFrames.Count == 0)
            throw new CSharpDbException(ErrorCode.WalError, "No frames to commit.");

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
        if (!_isOpen)
            return;

        try
        {
            await _storage.SetLengthAsync(_uncommittedStartOffset, cancellationToken);
            await _storage.FlushAsync(cancellationToken);
            _uncommittedFrames.Clear();
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
        if (!_isOpen)
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
                $"Failed to checkpoint in-memory WAL with {committedPageCount} committed page(s), pageCount={pageCount}, requiredLength={requiredLength}, deviceLength={device.Length}.",
                ex);
        }
    }

    public ValueTask CloseAndDeleteAsync()
    {
        _isOpen = false;
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _incrementalCheckpoint = null;
        _index.Reset();
        _storage.SetLengthAsync(0).GetAwaiter().GetResult();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _isOpen = false;
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _incrementalCheckpoint = null;
        return ValueTask.CompletedTask;
    }

    private async ValueTask CreateNewAsync(uint dbPageCount, CancellationToken cancellationToken)
    {
        _incrementalCheckpoint = null;
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _index.Reset();
        _isOpen = true;
        _salt1 = (uint)Random.Shared.Next();
        _salt2 = (uint)Random.Shared.Next();
        WriteWalHeader(_walHeaderBuffer, dbPageCount);
        await _storage.SetLengthAsync(0, cancellationToken);
        await _storage.WriteAsync(0, _walHeaderBuffer.AsMemory(0, PageConstants.WalHeaderSize), cancellationToken);
        await _storage.FlushAsync(cancellationToken);
        _uncommittedStartOffset = PageConstants.WalHeaderSize;
    }

    private async ValueTask RecoverAsync(CancellationToken cancellationToken)
    {
        _incrementalCheckpoint = null;
        _uncommittedFrames.Clear();
        _lastUncommittedDataChecksum = 0;
        _recoverUncommittedBatch.Clear();
        _index.Reset();
        _isOpen = true;

        var header = _walHeaderBuffer;
        if (await _storage.ReadAsync(0, header, cancellationToken) != PageConstants.WalHeaderSize)
            throw new CSharpDbException(ErrorCode.WalError, "Invalid in-memory WAL: header too short.");

        if (!header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic))
            throw new CSharpDbException(ErrorCode.WalError, "Invalid in-memory WAL: bad magic.");

        _salt1 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4));
        _salt2 = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(20, 4));

        var uncommittedBatch = _recoverUncommittedBatch;
        uncommittedBatch.Clear();
        var frameHeaderBuffer = _recoveryFrameHeaderBuffer;
        var pageDataBuffer = _recoveryPageBuffer;
        long offset = PageConstants.WalHeaderSize;

        while (offset + PageConstants.WalFrameSize <= _storage.Length)
        {
            long frameOffset = offset;
            int headerRead = await _storage.ReadAsync(offset, frameHeaderBuffer, cancellationToken);
            int dataRead = await _storage.ReadAsync(offset + PageConstants.WalFrameHeaderSize, pageDataBuffer, cancellationToken);
            offset += PageConstants.WalFrameSize;

            if (headerRead != PageConstants.WalFrameHeaderSize || dataRead != PageConstants.PageSize)
            {
                await _storage.SetLengthAsync(frameOffset, cancellationToken);
                break;
            }

            uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(8, 4));
            uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(12, 4));
            if (frameSalt1 != _salt1 || frameSalt2 != _salt2)
            {
                await _storage.SetLengthAsync(frameOffset, cancellationToken);
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
                await _storage.SetLengthAsync(frameOffset, cancellationToken);
                break;
            }

            uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(0, 4));
            uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeaderBuffer.AsSpan(4, 4));
            uncommittedBatch.Add((pageId, frameOffset));

            if (dbPageCount != 0)
            {
                foreach (var (committedPageId, committedOffset) in uncommittedBatch)
                    _index.AddCommittedFrame(committedPageId, committedOffset);

                _index.AdvanceCommit();
                uncommittedBatch.Clear();
            }
        }

        if (uncommittedBatch.Count > 0)
        {
            long truncateAt = uncommittedBatch[0].WalOffset;
            await _storage.SetLengthAsync(truncateAt, cancellationToken);
        }

        uncommittedBatch.Clear();
        _uncommittedStartOffset = _storage.Length;
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

        _index.AdvanceCommit();
        _lastUncommittedDataChecksum = 0;
    }

    private void PublishCommittedFrames()
    {
        _index.EnsurePageCapacity(_uncommittedFrames.Count);

        foreach (var (pageId, walOffset) in _uncommittedFrames)
            _index.AddCommittedFrame(pageId, walOffset);

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
        _incrementalCheckpoint = new IncrementalCheckpointState(snapshot, committedPageCount, _storage.Length);
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
        var checkpoint = _incrementalCheckpoint;
        if (checkpoint is null)
            return;

        long retainedByteCount = _storage.Length - checkpoint.RetainedWalStartOffset;
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
        if (!_isOpen)
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
}

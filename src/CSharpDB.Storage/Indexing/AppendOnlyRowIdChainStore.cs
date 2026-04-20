using System.Buffers;
using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Storage.Indexing;

internal enum AppendableChainEncoding : byte
{
    Fixed64 = 0,
    DeltaVarint = 1,
}

internal static class AppendOnlyRowIdChainStore
{
    internal readonly record struct AppendableChainMetadata(
        uint LastPageId,
        int RowCount,
        long LastRowId,
        bool IsSortedAscending,
        AppendableChainEncoding Encoding);

    private const byte SortedAscendingFlag = 1;
    private const byte DeltaVarintEncodingFlag = 2;
    private const int AppendableMetadataFlagsOffset = 0;
    private const int AppendableMetadataRowCountOffset = AppendableMetadataFlagsOffset + sizeof(byte);
    private const int AppendableMetadataLastPageIdOffset = AppendableMetadataRowCountOffset + sizeof(int);
    private const int AppendableMetadataLastRowIdOffset = AppendableMetadataLastPageIdOffset + sizeof(uint);
    private const int AppendableMetadataSize = AppendableMetadataLastRowIdOffset + sizeof(long);
    private const int MaxVarUInt64Bytes = 10;

    private static int MaxOverflowPayloadBytes => PageConstants.PageSize - PageConstants.OverflowPageHeaderSize;
    private static int FixedWidthPayloadBytesPerPage =>
        (MaxOverflowPayloadBytes / RowIdPayloadCodec.RowIdSize) * RowIdPayloadCodec.RowIdSize;
    private static int FixedWidthAppendableFirstPagePayloadBytes =>
        ((MaxOverflowPayloadBytes - AppendableMetadataSize) / RowIdPayloadCodec.RowIdSize) * RowIdPayloadCodec.RowIdSize;
    private static int VariableWidthPayloadBytesPerPage => MaxOverflowPayloadBytes;
    private static int VariableWidthAppendableFirstPagePayloadBytes => MaxOverflowPayloadBytes - AppendableMetadataSize;

    public static async ValueTask<(uint FirstPageId, uint LastPageId)> WriteAsync(
        Pager pager,
        ReadOnlyMemory<byte> rowIdPayload,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (rowIdPayload.IsEmpty || rowIdPayload.Length % RowIdPayloadCodec.RowIdSize != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(rowIdPayload),
                "Row-id chain payloads must contain one or more complete row ids.");
        }

        var allocatedPages = new List<uint>();
        try
        {
            uint firstPageId = PageConstants.NullPageId;
            uint previousPageId = PageConstants.NullPageId;
            int offset = 0;

            while (offset < rowIdPayload.Length)
            {
                uint pageId = await pager.AllocatePageAsync(ct);
                allocatedPages.Add(pageId);
                if (firstPageId == PageConstants.NullPageId)
                    firstPageId = pageId;

                byte[] page = await pager.GetPageAsync(pageId, ct);
                page.AsSpan().Clear();
                page[PageConstants.PageTypeOffset] = PageConstants.PageTypeOverflow;

                int chunkLength = Math.Min(FixedWidthPayloadBytesPerPage, rowIdPayload.Length - offset);
                BinaryPrimitives.WriteUInt32LittleEndian(
                    page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                    PageConstants.NullPageId);
                BinaryPrimitives.WriteUInt16LittleEndian(
                    page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                    checked((ushort)chunkLength));
                rowIdPayload.Span[offset..(offset + chunkLength)]
                    .CopyTo(page.AsSpan(PageConstants.OverflowPageHeaderSize, chunkLength));
                await pager.MarkDirtyAsync(pageId, ct);

                if (previousPageId != PageConstants.NullPageId)
                {
                    byte[] previousPage = await pager.GetPageAsync(previousPageId, ct);
                    BinaryPrimitives.WriteUInt32LittleEndian(
                        previousPage.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                        pageId);
                    await pager.MarkDirtyAsync(previousPageId, ct);
                }

                previousPageId = pageId;
                offset += chunkLength;
            }

            return (firstPageId, previousPageId);
        }
        catch
        {
            foreach (uint pageId in allocatedPages)
            {
                try
                {
                    await pager.FreePageAsync(pageId, ct);
                }
                catch
                {
                    // Preserve the original failure.
                }
            }

            throw;
        }
    }

    public static async ValueTask<byte[]> ReadAsync(
        Pager pager,
        uint firstPageId,
        int rowCount,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));
        if (rowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));

        int payloadLength = checked(rowCount * RowIdPayloadCodec.RowIdSize);
        byte[] payload = GC.AllocateUninitializedArray<byte>(payloadLength);
        var visitedPages = new HashSet<uint>();
        int written = 0;
        uint pageId = firstPageId;

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id chain contains a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidatePage(pageId, page);

            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            if (chunkLength == 0 ||
                chunkLength % RowIdPayloadCodec.RowIdSize != 0 ||
                chunkLength > FixedWidthPayloadBytesPerPage ||
                written + chunkLength > payloadLength)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id page {pageId} has invalid chunk length {chunkLength}.");
            }

            page.AsSpan(PageConstants.OverflowPageHeaderSize, chunkLength)
                .CopyTo(payload.AsSpan(written, chunkLength));
            written += chunkLength;

            pageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
        }

        if (written != payloadLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id chain ended after {written} bytes; expected {payloadLength}.");
        }

        return payload;
    }

    public static async ValueTask<uint> AppendAsync(
        Pager pager,
        uint lastPageId,
        long rowId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (lastPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(lastPageId));

        byte[] page = await pager.GetPageAsync(lastPageId, ct);
        ValidatePage(lastPageId, page);

        ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
            page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
        if (chunkLength % RowIdPayloadCodec.RowIdSize != 0 || chunkLength > FixedWidthPayloadBytesPerPage)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {lastPageId} has invalid chunk length {chunkLength}.");
        }

        if (chunkLength + RowIdPayloadCodec.RowIdSize <= FixedWidthPayloadBytesPerPage)
        {
            BinaryPrimitives.WriteInt64LittleEndian(
                page.AsSpan(PageConstants.OverflowPageHeaderSize + chunkLength, RowIdPayloadCodec.RowIdSize),
                rowId);
            BinaryPrimitives.WriteUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                checked((ushort)(chunkLength + RowIdPayloadCodec.RowIdSize)));
            await pager.MarkDirtyAsync(lastPageId, ct);
            return lastPageId;
        }

        uint newPageId = await pager.AllocatePageAsync(ct);
        try
        {
            byte[] newPage = await pager.GetPageAsync(newPageId, ct);
            newPage.AsSpan().Clear();
            newPage[PageConstants.PageTypeOffset] = PageConstants.PageTypeOverflow;
            BinaryPrimitives.WriteUInt32LittleEndian(
                newPage.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                PageConstants.NullPageId);
            BinaryPrimitives.WriteUInt16LittleEndian(
                newPage.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                RowIdPayloadCodec.RowIdSize);
            BinaryPrimitives.WriteInt64LittleEndian(
                newPage.AsSpan(PageConstants.OverflowPageHeaderSize, RowIdPayloadCodec.RowIdSize),
                rowId);
            await pager.MarkDirtyAsync(newPageId, ct);

            BinaryPrimitives.WriteUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                newPageId);
            await pager.MarkDirtyAsync(lastPageId, ct);
            return newPageId;
        }
        catch
        {
            try
            {
                await pager.FreePageAsync(newPageId, ct);
            }
            catch
            {
                // Preserve the original failure.
            }

            throw;
        }
    }

    public static async ValueTask<(uint FirstPageId, AppendableChainMetadata Metadata)> WriteAppendableAsync(
        Pager pager,
        ReadOnlyMemory<byte> rowIdPayload,
        bool isSortedAscending,
        long lastRowId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (rowIdPayload.IsEmpty || rowIdPayload.Length % RowIdPayloadCodec.RowIdSize != 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(rowIdPayload),
                "Row-id chain payloads must contain one or more complete row ids.");
        }

        int rowCount = rowIdPayload.Length / RowIdPayloadCodec.RowIdSize;
        byte[] encodedPayload = EncodeAppendablePayload(rowIdPayload, isSortedAscending, out AppendableChainEncoding encoding);
        var allocatedPages = new List<uint>();
        try
        {
            uint firstPageId = PageConstants.NullPageId;
            uint previousPageId = PageConstants.NullPageId;
            int offset = 0;

            while (offset < encodedPayload.Length)
            {
                uint pageId = await pager.AllocatePageAsync(ct);
                allocatedPages.Add(pageId);
                if (firstPageId == PageConstants.NullPageId)
                    firstPageId = pageId;

                byte[] page = await pager.GetPageAsync(pageId, ct);
                page.AsSpan().Clear();
                page[PageConstants.PageTypeOffset] = PageConstants.PageTypeOverflow;
                BinaryPrimitives.WriteUInt32LittleEndian(
                    page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                    PageConstants.NullPageId);

                bool isFirstPage = pageId == firstPageId;
                int chunkCapacity = GetAppendableDataCapacity(isFirstPage, encoding);
                int chunkLength = Math.Min(chunkCapacity, encodedPayload.Length - offset);
                int payloadOffset = isFirstPage
                    ? PageConstants.OverflowPageHeaderSize + AppendableMetadataSize
                    : PageConstants.OverflowPageHeaderSize;
                int storedChunkLength = isFirstPage
                    ? chunkLength + AppendableMetadataSize
                    : chunkLength;
                BinaryPrimitives.WriteUInt16LittleEndian(
                    page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                    checked((ushort)storedChunkLength));
                encodedPayload.AsSpan(offset, chunkLength)
                    .CopyTo(page.AsSpan(payloadOffset, chunkLength));
                await pager.MarkDirtyAsync(pageId, ct);

                if (previousPageId != PageConstants.NullPageId)
                {
                    byte[] previousPage = await pager.GetPageAsync(previousPageId, ct);
                    BinaryPrimitives.WriteUInt32LittleEndian(
                        previousPage.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                        pageId);
                    await pager.MarkDirtyAsync(previousPageId, ct);
                }

                previousPageId = pageId;
                offset += chunkLength;
            }

            AppendableChainMetadata metadata = new(
                previousPageId,
                rowCount,
                lastRowId,
                isSortedAscending,
                encoding);
            byte[] firstPage = await pager.GetPageAsync(firstPageId, ct);
            WriteAppendableMetadata(firstPage, metadata);
            await pager.MarkDirtyAsync(firstPageId, ct);
            return (firstPageId, metadata);
        }
        catch
        {
            foreach (uint pageId in allocatedPages)
            {
                try
                {
                    await pager.FreePageAsync(pageId, ct);
                }
                catch
                {
                    // Preserve the original failure.
                }
            }

            throw;
        }
    }

    public static async ValueTask<AppendableChainMetadata> ReadAppendableMetadataAsync(
        Pager pager,
        uint firstPageId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));

        byte[] firstPage = await pager.GetPageAsync(firstPageId, ct);
        ValidatePage(firstPageId, firstPage);
        return ReadAppendableMetadata(firstPageId, firstPage);
    }

    public static async ValueTask<byte[]> ReadAsync(
        Pager pager,
        uint firstPageId,
        AppendableChainMetadata metadata,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));
        if (metadata.RowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(metadata));

        byte[] encodedPayload = await ReadAppendableEncodedPayloadAsync(pager, firstPageId, metadata, ct);
        if (metadata.Encoding == AppendableChainEncoding.Fixed64)
        {
            int payloadLength = checked(metadata.RowCount * RowIdPayloadCodec.RowIdSize);
            if (encodedPayload.Length != payloadLength)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id chain contained {encodedPayload.Length} bytes; expected {payloadLength}.");
            }

            return encodedPayload;
        }

        return DecodeSortedAscendingDeltaPayload(
            encodedPayload,
            metadata.RowCount,
            metadata.LastRowId);
    }

    public static async ValueTask<AppendableChainMetadata> AppendAsync(
        Pager pager,
        uint firstPageId,
        AppendableChainMetadata metadata,
        long rowId,
        bool isSortedAscending,
        CancellationToken ct = default)
    {
        long[] rowIds = [rowId];
        return await AppendBatchAsync(
            pager,
            firstPageId,
            metadata,
            rowIds,
            isSortedAscending,
            ct);
    }

    public static async ValueTask<AppendableChainMetadata> AppendBatchAsync(
        Pager pager,
        uint firstPageId,
        AppendableChainMetadata metadata,
        ReadOnlyMemory<long> rowIds,
        bool isSortedAscending,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));
        if (metadata.LastPageId == PageConstants.NullPageId || metadata.RowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(metadata));
        if (rowIds.IsEmpty)
            return metadata;

        if (metadata.Encoding == AppendableChainEncoding.DeltaVarint && (!metadata.IsSortedAscending || !isSortedAscending))
        {
            throw new InvalidOperationException(
                "Delta-encoded appendable chains only support sorted ascending appends.");
        }

        byte[] firstPage = await pager.GetPageAsync(firstPageId, ct);
        ValidatePage(firstPageId, firstPage);

        uint currentLastPageId = metadata.LastPageId;
        byte[] lastPage = currentLastPageId == firstPageId
            ? firstPage
            : await pager.GetPageAsync(currentLastPageId, ct);
        ValidatePage(currentLastPageId, lastPage);

        ushort lastChunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
            lastPage.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
        ValidateStoredChunkLength(
            currentLastPageId,
            lastChunkLength,
            isFirstPage: currentLastPageId == firstPageId,
            metadata.Encoding,
            isAppendable: true);

        byte[] appendedBytes = metadata.Encoding switch
        {
            AppendableChainEncoding.Fixed64 => EncodeFixedWidthRowIds(rowIds.Span),
            AppendableChainEncoding.DeltaVarint => EncodeSortedAscendingDeltaAppends(rowIds.Span, metadata.LastRowId),
            _ => throw new InvalidOperationException($"Unknown appendable chain encoding '{metadata.Encoding}'."),
        };

        int offset = 0;
        while (offset < appendedBytes.Length)
        {
            bool isFirstPage = currentLastPageId == firstPageId;
            int maxStoredChunkLength = GetAppendableStoredChunkLengthCapacity(isFirstPage, metadata.Encoding);
            if (lastChunkLength < maxStoredChunkLength)
            {
                int writable = Math.Min(maxStoredChunkLength - lastChunkLength, appendedBytes.Length - offset);
                appendedBytes.AsSpan(offset, writable)
                    .CopyTo(lastPage.AsSpan(PageConstants.OverflowPageHeaderSize + lastChunkLength, writable));
                lastChunkLength = checked((ushort)(lastChunkLength + writable));
                BinaryPrimitives.WriteUInt16LittleEndian(
                    lastPage.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                    lastChunkLength);
                await pager.MarkDirtyAsync(currentLastPageId, ct);
                offset += writable;
                continue;
            }

            uint newPageId = await pager.AllocatePageAsync(ct);
            try
            {
                byte[] newPage = await pager.GetPageAsync(newPageId, ct);
                newPage.AsSpan().Clear();
                newPage[PageConstants.PageTypeOffset] = PageConstants.PageTypeOverflow;
                BinaryPrimitives.WriteUInt32LittleEndian(
                    newPage.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                    PageConstants.NullPageId);
                BinaryPrimitives.WriteUInt16LittleEndian(
                    newPage.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                    0);
                await pager.MarkDirtyAsync(newPageId, ct);

                BinaryPrimitives.WriteUInt32LittleEndian(
                    lastPage.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                    newPageId);
                await pager.MarkDirtyAsync(currentLastPageId, ct);

                currentLastPageId = newPageId;
                lastPage = newPage;
                lastChunkLength = 0;
            }
            catch
            {
                try
                {
                    await pager.FreePageAsync(newPageId, ct);
                }
                catch
                {
                    // Preserve the original failure.
                }

                throw;
            }
        }

        AppendableChainMetadata updated = metadata with
        {
            LastPageId = currentLastPageId,
            RowCount = metadata.RowCount + rowIds.Length,
            LastRowId = rowIds.Span[^1],
            IsSortedAscending = isSortedAscending,
        };
        WriteAppendableMetadata(firstPage, updated);
        await pager.MarkDirtyAsync(firstPageId, ct);
        return updated;
    }

    public static async ValueTask<bool> ContainsAsync(
        Pager pager,
        uint firstPageId,
        AppendableChainMetadata metadata,
        long rowId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));
        if (metadata.RowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(metadata));

        byte[] rowIdPayload = await ReadAsync(pager, firstPageId, metadata, ct);
        int rowCount = RowIdPayloadCodec.GetCount(rowIdPayload);
        for (int i = 0; i < rowCount; i++)
        {
            long current = RowIdPayloadCodec.ReadAt(rowIdPayload, i);
            if (current == rowId)
                return true;

            if (metadata.IsSortedAscending && current > rowId)
                return false;
        }

        return false;
    }

    public static async ValueTask<bool> ContainsAsync(
        Pager pager,
        uint firstPageId,
        int rowCount,
        long rowId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            throw new ArgumentOutOfRangeException(nameof(firstPageId));
        if (rowCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(rowCount));

        var visitedPages = new HashSet<uint>();
        int observedCount = 0;
        uint pageId = firstPageId;

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id chain contains a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidatePage(pageId, page);

            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            if (chunkLength % RowIdPayloadCodec.RowIdSize != 0 || chunkLength > FixedWidthPayloadBytesPerPage)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id page {pageId} has invalid chunk length {chunkLength}.");
            }

            int localCount = chunkLength / RowIdPayloadCodec.RowIdSize;
            for (int i = 0; i < localCount; i++)
            {
                long current = BinaryPrimitives.ReadInt64LittleEndian(
                    page.AsSpan(
                        PageConstants.OverflowPageHeaderSize + (i * RowIdPayloadCodec.RowIdSize),
                        RowIdPayloadCodec.RowIdSize));
                observedCount++;
                if (current == rowId)
                    return true;
            }

            pageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
        }

        if (observedCount != rowCount)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id chain contained {observedCount} row ids; expected {rowCount}.");
        }

        return false;
    }

    public static async ValueTask ReclaimAsync(
        Pager pager,
        uint firstPageId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (firstPageId == PageConstants.NullPageId)
            return;

        var visitedPages = new HashSet<uint>();
        uint pageId = firstPageId;

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id reclaim encountered a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidatePage(pageId, page);
            uint nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
            await pager.FreePageAsync(pageId, ct);
            pageId = nextPageId;
        }
    }

    private static void ValidatePage(uint pageId, ReadOnlySpan<byte> page)
    {
        if (page.Length < PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} is shorter than the configured page size.");
        }

        if (page[PageConstants.PageTypeOffset] != PageConstants.PageTypeOverflow)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has unexpected page type 0x{page[PageConstants.PageTypeOffset]:X2}.");
        }
    }

    private static AppendableChainMetadata ReadAppendableMetadata(uint pageId, ReadOnlySpan<byte> page)
    {
        ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
            page.Slice(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
        if (chunkLength < AppendableMetadataSize + 1)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has invalid appendable chunk length {chunkLength}.");
        }

        ReadOnlySpan<byte> metadataSpan = page.Slice(PageConstants.OverflowPageHeaderSize, AppendableMetadataSize);
        byte flags = metadataSpan[AppendableMetadataFlagsOffset];
        bool isSortedAscending = (flags & SortedAscendingFlag) != 0;
        AppendableChainEncoding encoding = (flags & DeltaVarintEncodingFlag) != 0
            ? AppendableChainEncoding.DeltaVarint
            : AppendableChainEncoding.Fixed64;
        int rowCount = BinaryPrimitives.ReadInt32LittleEndian(metadataSpan.Slice(AppendableMetadataRowCountOffset, sizeof(int)));
        uint lastPageId = BinaryPrimitives.ReadUInt32LittleEndian(metadataSpan.Slice(AppendableMetadataLastPageIdOffset, sizeof(uint)));
        long lastRowId = BinaryPrimitives.ReadInt64LittleEndian(metadataSpan.Slice(AppendableMetadataLastRowIdOffset, sizeof(long)));

        if (rowCount <= 0 || lastPageId == PageConstants.NullPageId)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has invalid appendable metadata.");
        }

        int dataLength = chunkLength - AppendableMetadataSize;
        ValidateAppendableDataLength(pageId, dataLength, encoding, isFirstPage: true);
        return new AppendableChainMetadata(lastPageId, rowCount, lastRowId, isSortedAscending, encoding);
    }

    private static void WriteAppendableMetadata(Span<byte> page, AppendableChainMetadata metadata)
    {
        Span<byte> metadataSpan = page.Slice(PageConstants.OverflowPageHeaderSize, AppendableMetadataSize);
        metadataSpan.Clear();
        byte flags = metadata.IsSortedAscending ? SortedAscendingFlag : (byte)0;
        if (metadata.Encoding == AppendableChainEncoding.DeltaVarint)
            flags |= DeltaVarintEncodingFlag;

        metadataSpan[AppendableMetadataFlagsOffset] = flags;
        BinaryPrimitives.WriteInt32LittleEndian(
            metadataSpan.Slice(AppendableMetadataRowCountOffset, sizeof(int)),
            metadata.RowCount);
        BinaryPrimitives.WriteUInt32LittleEndian(
            metadataSpan.Slice(AppendableMetadataLastPageIdOffset, sizeof(uint)),
            metadata.LastPageId);
        BinaryPrimitives.WriteInt64LittleEndian(
            metadataSpan.Slice(AppendableMetadataLastRowIdOffset, sizeof(long)),
            metadata.LastRowId);
    }

    private static byte[] EncodeAppendablePayload(
        ReadOnlyMemory<byte> rowIdPayload,
        bool isSortedAscending,
        out AppendableChainEncoding encoding)
    {
        if (isSortedAscending)
        {
            byte[] deltaPayload = EncodeSortedAscendingDeltaPayload(rowIdPayload.Span);
            if (deltaPayload.Length < rowIdPayload.Length)
            {
                encoding = AppendableChainEncoding.DeltaVarint;
                return deltaPayload;
            }
        }

        encoding = AppendableChainEncoding.Fixed64;
        return rowIdPayload.ToArray();
    }

    private static async ValueTask<byte[]> ReadAppendableEncodedPayloadAsync(
        Pager pager,
        uint firstPageId,
        AppendableChainMetadata metadata,
        CancellationToken ct)
    {
        var encodedPayload = new ArrayBufferWriter<byte>();
        var visitedPages = new HashSet<uint>();
        uint pageId = firstPageId;

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Append-only row-id chain contains a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidatePage(pageId, page);

            bool isFirstPage = pageId == firstPageId;
            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            ValidateStoredChunkLength(pageId, chunkLength, isFirstPage, metadata.Encoding, isAppendable: true);

            int dataOffset = PageConstants.OverflowPageHeaderSize;
            int dataLength = chunkLength;
            if (isFirstPage)
            {
                dataOffset += AppendableMetadataSize;
                dataLength -= AppendableMetadataSize;
            }

            ValidateAppendableDataLength(pageId, dataLength, metadata.Encoding, isFirstPage);
            page.AsSpan(dataOffset, dataLength).CopyTo(encodedPayload.GetSpan(dataLength));
            encodedPayload.Advance(dataLength);

            pageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
        }

        return encodedPayload.WrittenSpan.ToArray();
    }

    private static void ValidateStoredChunkLength(
        uint pageId,
        int chunkLength,
        bool isFirstPage,
        AppendableChainEncoding encoding,
        bool isAppendable)
    {
        int minLength = isAppendable && isFirstPage
            ? AppendableMetadataSize + GetMinimumAppendableDataBytes(encoding)
            : GetMinimumAppendableDataBytes(encoding);
        int maxLength = isAppendable
            ? GetAppendableStoredChunkLengthCapacity(isFirstPage, encoding)
            : FixedWidthPayloadBytesPerPage;

        if (chunkLength < minLength || chunkLength > maxLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has invalid chunk length {chunkLength}.");
        }
    }

    private static void ValidateAppendableDataLength(
        uint pageId,
        int dataLength,
        AppendableChainEncoding encoding,
        bool isFirstPage)
    {
        int maxDataLength = GetAppendableDataCapacity(isFirstPage, encoding);
        if (dataLength <= 0 || dataLength > maxDataLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has invalid chunk length {dataLength}.");
        }

        if (encoding == AppendableChainEncoding.Fixed64 &&
            dataLength % RowIdPayloadCodec.RowIdSize != 0)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {pageId} has misaligned fixed-width row-id data.");
        }
    }

    private static int GetAppendableStoredChunkLengthCapacity(bool isFirstPage, AppendableChainEncoding encoding)
        => isFirstPage
            ? AppendableMetadataSize + GetAppendableDataCapacity(isFirstPage: true, encoding)
            : GetAppendableDataCapacity(isFirstPage: false, encoding);

    private static int GetAppendableDataCapacity(bool isFirstPage, AppendableChainEncoding encoding)
        => encoding switch
        {
            AppendableChainEncoding.Fixed64 => isFirstPage
                ? FixedWidthAppendableFirstPagePayloadBytes
                : FixedWidthPayloadBytesPerPage,
            AppendableChainEncoding.DeltaVarint => isFirstPage
                ? VariableWidthAppendableFirstPagePayloadBytes
                : VariableWidthPayloadBytesPerPage,
            _ => throw new InvalidOperationException($"Unknown appendable chain encoding '{encoding}'."),
        };

    private static int GetMinimumAppendableDataBytes(AppendableChainEncoding encoding)
        => encoding == AppendableChainEncoding.DeltaVarint ? 1 : RowIdPayloadCodec.RowIdSize;

    private static byte[] EncodeFixedWidthRowIds(ReadOnlySpan<long> rowIds)
    {
        byte[] payload = GC.AllocateUninitializedArray<byte>(checked(rowIds.Length * RowIdPayloadCodec.RowIdSize));
        for (int i = 0; i < rowIds.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(
                payload.AsSpan(i * RowIdPayloadCodec.RowIdSize, RowIdPayloadCodec.RowIdSize),
                rowIds[i]);
        }

        return payload;
    }

    private static byte[] EncodeSortedAscendingDeltaPayload(ReadOnlySpan<byte> rowIdPayload)
    {
        var writer = new ArrayBufferWriter<byte>(rowIdPayload.Length);
        long previousRowId = 0;

        for (int offset = 0; offset < rowIdPayload.Length; offset += RowIdPayloadCodec.RowIdSize)
        {
            long currentRowId = BinaryPrimitives.ReadInt64LittleEndian(
                rowIdPayload.Slice(offset, RowIdPayloadCodec.RowIdSize));
            if (currentRowId <= previousRowId)
            {
                throw new InvalidOperationException(
                    "Sorted appendable row-id chains require strictly increasing row ids.");
            }

            WriteVarUInt64(writer, checked((ulong)(currentRowId - previousRowId)));
            previousRowId = currentRowId;
        }

        return writer.WrittenSpan.ToArray();
    }

    private static byte[] EncodeSortedAscendingDeltaAppends(ReadOnlySpan<long> rowIds, long previousRowId)
    {
        var writer = new ArrayBufferWriter<byte>(Math.Max(rowIds.Length, 16));
        long lastRowId = previousRowId;

        for (int i = 0; i < rowIds.Length; i++)
        {
            long currentRowId = rowIds[i];
            if (currentRowId <= lastRowId)
            {
                throw new InvalidOperationException(
                    "Delta-encoded appendable row-id chains require strictly increasing appended row ids.");
            }

            WriteVarUInt64(writer, checked((ulong)(currentRowId - lastRowId)));
            lastRowId = currentRowId;
        }

        return writer.WrittenSpan.ToArray();
    }

    private static byte[] DecodeSortedAscendingDeltaPayload(
        ReadOnlySpan<byte> encodedPayload,
        int rowCount,
        long expectedLastRowId)
    {
        byte[] payload = GC.AllocateUninitializedArray<byte>(checked(rowCount * RowIdPayloadCodec.RowIdSize));
        int offset = 0;
        long currentRowId = 0;

        for (int i = 0; i < rowCount; i++)
        {
            if (!TryReadVarUInt64(encodedPayload, ref offset, out ulong delta) || delta == 0)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    "Append-only row-id delta chain contains an invalid varint payload.");
            }

            if (delta > long.MaxValue || currentRowId > long.MaxValue - (long)delta)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    "Append-only row-id delta chain overflowed the supported row-id range.");
            }

            currentRowId += (long)delta;
            BinaryPrimitives.WriteInt64LittleEndian(
                payload.AsSpan(i * RowIdPayloadCodec.RowIdSize, RowIdPayloadCodec.RowIdSize),
                currentRowId);
        }

        if (offset != encodedPayload.Length || currentRowId != expectedLastRowId)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Append-only row-id delta chain did not decode to the expected row-id set.");
        }

        return payload;
    }

    private static void WriteVarUInt64(ArrayBufferWriter<byte> writer, ulong value)
    {
        Span<byte> buffer = writer.GetSpan(MaxVarUInt64Bytes);
        int written = 0;
        do
        {
            byte next = (byte)(value & 0x7Fu);
            value >>= 7;
            if (value != 0)
                next |= 0x80;

            buffer[written++] = next;
        }
        while (value != 0);

        writer.Advance(written);
    }

    private static bool TryReadVarUInt64(ReadOnlySpan<byte> source, ref int offset, out ulong value)
    {
        value = 0;
        int shift = 0;
        while (offset < source.Length && shift < 64)
        {
            byte next = source[offset++];
            value |= (ulong)(next & 0x7F) << shift;
            if ((next & 0x80) == 0)
                return true;

            shift += 7;
        }

        value = 0;
        return false;
    }
}

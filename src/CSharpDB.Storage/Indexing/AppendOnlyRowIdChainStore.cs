using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Storage.Indexing;

internal static class AppendOnlyRowIdChainStore
{
    private static int RowIdsPerPage => (PageConstants.PageSize - PageConstants.OverflowPageHeaderSize) / RowIdPayloadCodec.RowIdSize;
    private static int PayloadBytesPerPage => RowIdsPerPage * RowIdPayloadCodec.RowIdSize;

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

                int chunkLength = Math.Min(PayloadBytesPerPage, rowIdPayload.Length - offset);
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
                chunkLength > PayloadBytesPerPage ||
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
        if (chunkLength % RowIdPayloadCodec.RowIdSize != 0 || chunkLength > PayloadBytesPerPage)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Append-only row-id page {lastPageId} has invalid chunk length {chunkLength}.");
        }

        if (chunkLength + RowIdPayloadCodec.RowIdSize <= PayloadBytesPerPage)
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
            if (chunkLength % RowIdPayloadCodec.RowIdSize != 0 || chunkLength > PayloadBytesPerPage)
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
}

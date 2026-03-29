using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Storage.Indexing;

internal static class IndexOverflowPageStore
{
    internal static int MaxInlinePayloadLength { get; } = ComputeMaxInlinePayloadLength();

    private static int OverflowPayloadBytesPerPage => PageConstants.PageSize - PageConstants.OverflowPageHeaderSize;

    public static bool RequiresOverflow(int payloadLength) => payloadLength > MaxInlinePayloadLength;

    public static async ValueTask<byte[]> WriteAsync(Pager pager, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);

        if (!RequiresOverflow(payload.Length))
            throw new InvalidOperationException("Inline payloads should not be written through the overflow page store.");

        var allocatedPages = new List<uint>();
        try
        {
            uint firstPageId = PageConstants.NullPageId;
            uint previousPageId = PageConstants.NullPageId;
            int offset = 0;

            while (offset < payload.Length)
            {
                uint pageId = await pager.AllocatePageAsync(ct);
                allocatedPages.Add(pageId);
                if (firstPageId == PageConstants.NullPageId)
                    firstPageId = pageId;

                byte[] page = await pager.GetPageAsync(pageId, ct);
                page.AsSpan().Clear();
                page[PageConstants.PageTypeOffset] = PageConstants.PageTypeOverflow;

                int chunkLength = Math.Min(OverflowPayloadBytesPerPage, payload.Length - offset);
                BinaryPrimitives.WriteUInt32LittleEndian(
                    page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)),
                    PageConstants.NullPageId);
                BinaryPrimitives.WriteUInt16LittleEndian(
                    page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)),
                    checked((ushort)chunkLength));
                payload.Span.Slice(offset, chunkLength)
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

            return IndexOverflowReferenceCodec.Encode(firstPageId, payload.Length);
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

    public static async ValueTask<byte[]> ReadAsync(Pager pager, ReadOnlyMemory<byte> referencePayload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);

        uint pageId = IndexOverflowReferenceCodec.ReadFirstPageId(referencePayload.Span);
        int payloadLength = IndexOverflowReferenceCodec.ReadPayloadLength(referencePayload.Span);
        byte[] payload = GC.AllocateUninitializedArray<byte>(payloadLength);
        var visitedPages = new HashSet<uint>();
        int written = 0;

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Index overflow chain contains a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidateOverflowPage(pageId, page);

            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            if (chunkLength > OverflowPayloadBytesPerPage || written + chunkLength > payloadLength)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Index overflow page {pageId} has invalid chunk length {chunkLength}.");
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
                $"Index overflow chain ended after {written} bytes; expected {payloadLength}.");
        }

        return payload;
    }

    public static async ValueTask ReclaimAsync(Pager pager, ReadOnlyMemory<byte> referencePayload, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);

        uint pageId = IndexOverflowReferenceCodec.ReadFirstPageId(referencePayload.Span);
        var visitedPages = new HashSet<uint>();

        while (pageId != PageConstants.NullPageId)
        {
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Index overflow reclaim encountered a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidateOverflowPage(pageId, page);
            uint nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
            await pager.FreePageAsync(pageId, ct);
            pageId = nextPageId;
        }
    }

    private static void ValidateOverflowPage(uint pageId, ReadOnlySpan<byte> page)
    {
        if (page.Length < PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Index overflow page {pageId} is shorter than the configured page size.");
        }

        if (page[PageConstants.PageTypeOffset] != PageConstants.PageTypeOverflow)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Index overflow page {pageId} has unexpected page type 0x{page[PageConstants.PageTypeOffset]:X2}.");
        }
    }

    private static int ComputeMaxInlinePayloadLength()
    {
        int maxLeafCellLength = PageConstants.PageSize - PageConstants.SlottedPageHeaderSize - PageConstants.CellPointerSize;
        for (int payloadLength = maxLeafCellLength; payloadLength >= 0; payloadLength--)
        {
            int payloadPart = sizeof(long) + payloadLength;
            int leafCellLength = Varint.SizeOf((ulong)payloadPart) + payloadPart;
            if (leafCellLength <= maxLeafCellLength)
                return payloadLength;
        }

        return 0;
    }
}

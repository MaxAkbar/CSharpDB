using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.Paging;

internal readonly record struct OverflowPageReference(uint FirstPageId, int PayloadLength);

/// <summary>
/// Stores payload bytes in linked overflow pages. Reference encoding is deliberately
/// left to the owning data structure so different owners cannot decode or reclaim
/// each other's chains accidentally.
/// </summary>
internal static class OverflowPageStore
{
    private readonly record struct ValidatedOverflowChunk(PageReadBuffer Page, ushort ChunkLength);

    internal static int MaxInlineBTreePayloadLength { get; } = ComputeMaxInlineBTreePayloadLength();

    private static int PayloadBytesPerPage => PageConstants.PageSize - PageConstants.OverflowPageHeaderSize;

    internal static async ValueTask<OverflowPageReference> WriteAsync(
        Pager pager,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        if (payload.IsEmpty)
            throw new ArgumentException("Overflow payloads cannot be empty.", nameof(payload));

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

                int chunkLength = Math.Min(PayloadBytesPerPage, payload.Length - offset);
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

            return new OverflowPageReference(firstPageId, payload.Length);
        }
        catch
        {
            foreach (uint pageId in allocatedPages)
            {
                try
                {
                    await pager.FreePageAsync(pageId, CancellationToken.None);
                }
                catch
                {
                    // Preserve the original storage failure.
                }
            }

            throw;
        }
    }

    internal static ValueTask<byte[]> ReadAsync(
        Pager pager,
        OverflowPageReference reference,
        CancellationToken ct = default)
        => ReadCoreAsync(pager, reference, snapshot: null, ct);

    internal static ValueTask<byte[]> ReadAsync(
        Pager pager,
        OverflowPageReference reference,
        WalSnapshot snapshot,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return ReadCoreAsync(pager, reference, snapshot, ct);
    }

    private static async ValueTask<byte[]> ReadCoreAsync(
        Pager pager,
        OverflowPageReference reference,
        WalSnapshot? snapshot,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(pager);
        uint pageCount = pager.PageCount;
        ValidateReference(reference, pageCount);

        var visitedPages = new HashSet<uint>();
        var chunks = new List<ValidatedOverflowChunk>();
        uint pageId = reference.FirstPageId;
        int validatedBytes = 0;

        while (pageId != PageConstants.NullPageId)
        {
            ValidatePageId(pageId, pageCount);
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Overflow chain contains a cycle at page {pageId}.");
            }

            PageReadBuffer page = snapshot is null
                ? await pager.GetPageReadAsync(pageId, ct)
                : await pager.GetSnapshotPageReadAsync(pageId, snapshot, ct);
            ReadOnlySpan<byte> pageSpan = page.Memory.Span;
            ValidateOverflowPage(pageId, pageSpan);

            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                pageSpan.Slice(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            if (chunkLength == 0 ||
                chunkLength > PayloadBytesPerPage ||
                chunkLength > reference.PayloadLength - validatedBytes)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Overflow page {pageId} has invalid chunk length {chunkLength}.");
            }

            chunks.Add(new ValidatedOverflowChunk(page, chunkLength));
            validatedBytes += chunkLength;

            pageId = BinaryPrimitives.ReadUInt32LittleEndian(
                pageSpan.Slice(PageConstants.OverflowNextOffset, sizeof(uint)));
            if (pageId != PageConstants.NullPageId)
                ValidatePageId(pageId, pageCount);
        }

        if (validatedBytes != reference.PayloadLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow chain ended after {validatedBytes} bytes; expected {reference.PayloadLength}.");
        }

        byte[] payload = GC.AllocateUninitializedArray<byte>(reference.PayloadLength);
        int written = 0;
        foreach (ValidatedOverflowChunk chunk in chunks)
        {
            chunk.Page.Memory.Span
                .Slice(PageConstants.OverflowPageHeaderSize, chunk.ChunkLength)
                .CopyTo(payload.AsSpan(written, chunk.ChunkLength));
            written += chunk.ChunkLength;
        }

        return payload;
    }

    internal static async ValueTask ReclaimAsync(
        Pager pager,
        OverflowPageReference reference,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(pager);
        uint pageCount = pager.PageCount;
        ValidateReference(reference, pageCount);

        uint pageId = reference.FirstPageId;
        var visitedPages = new HashSet<uint>();
        var chainPages = new List<uint>();
        int validatedBytes = 0;

        while (pageId != PageConstants.NullPageId)
        {
            ValidatePageId(pageId, pageCount);
            if (!visitedPages.Add(pageId))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Overflow reclaim encountered a cycle at page {pageId}.");
            }

            byte[] page = await pager.GetPageAsync(pageId, ct);
            ValidateOverflowPage(pageId, page);
            ushort chunkLength = BinaryPrimitives.ReadUInt16LittleEndian(
                page.AsSpan(PageConstants.OverflowChunkLengthOffset, sizeof(ushort)));
            if (chunkLength == 0 ||
                chunkLength > PayloadBytesPerPage ||
                chunkLength > reference.PayloadLength - validatedBytes)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Overflow page {pageId} has invalid chunk length {chunkLength}.");
            }

            validatedBytes += chunkLength;
            chainPages.Add(pageId);

            uint nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(
                page.AsSpan(PageConstants.OverflowNextOffset, sizeof(uint)));
            if (nextPageId != PageConstants.NullPageId)
                ValidatePageId(nextPageId, pageCount);

            pageId = nextPageId;
        }

        if (validatedBytes != reference.PayloadLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow chain ended after {validatedBytes} bytes; expected {reference.PayloadLength}.");
        }

        ct.ThrowIfCancellationRequested();
        foreach (uint chainPageId in chainPages)
            await pager.FreePageAsync(chainPageId, CancellationToken.None);
    }

    private static void ValidateReference(OverflowPageReference reference, uint pageCount)
    {
        if (reference.FirstPageId == PageConstants.NullPageId || reference.PayloadLength <= 0)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Overflow reference does not point to a valid payload.");
        }

        ValidatePageId(reference.FirstPageId, pageCount);

        long maximumPayloadLength = Math.Max(0L, (long)pageCount - 1) * PayloadBytesPerPage;
        if (reference.PayloadLength > maximumPayloadLength)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow payload length {reference.PayloadLength} exceeds the maximum " +
                $"possible length {maximumPayloadLength} for a {pageCount}-page database.");
        }
    }

    private static void ValidatePageId(uint pageId, uint pageCount)
    {
        if (pageId == PageConstants.NullPageId || pageId >= pageCount)
        {
            string validRange = pageCount > 1 ? $"1..{pageCount - 1}" : "empty";
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow chain references page {pageId}, outside the valid page range {validRange}.");
        }
    }

    private static void ValidateOverflowPage(uint pageId, ReadOnlySpan<byte> page)
    {
        if (page.Length < PageConstants.PageSize)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow page {pageId} is shorter than the configured page size.");
        }

        if (page[PageConstants.PageTypeOffset] != PageConstants.PageTypeOverflow)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Overflow page {pageId} has unexpected page type 0x{page[PageConstants.PageTypeOffset]:X2}.");
        }
    }

    private static int ComputeMaxInlineBTreePayloadLength()
    {
        int maxLeafCellLength = PageConstants.PageSize -
            PageConstants.SlottedPageHeaderSize -
            PageConstants.CellPointerSize;
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

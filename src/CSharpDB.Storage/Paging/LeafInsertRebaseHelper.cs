namespace CSharpDB.Storage.Paging;

internal static class LeafInsertRebaseHelper
{
    public static bool TryRebaseInsertOnlyLeafPage(
        uint pageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        ReadOnlyMemory<byte> transactionPage,
        out byte[]? rebasedPage)
    {
        var baseLeaf = new ReadOnlySlottedPage(basePage, pageId);
        var committedLeaf = new ReadOnlySlottedPage(committedPage, pageId);
        var transactionLeaf = new ReadOnlySlottedPage(transactionPage, pageId);

        rebasedPage = null;
        if (baseLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedLeaf.PageType != PageConstants.PageTypeLeaf ||
            transactionLeaf.PageType != PageConstants.PageTypeLeaf)
        {
            return false;
        }

        uint nextLeaf = baseLeaf.RightChildOrNextLeaf;
        if (committedLeaf.RightChildOrNextLeaf != nextLeaf ||
            transactionLeaf.RightChildOrNextLeaf != nextLeaf)
        {
            return false;
        }

        if (!TryCollectInsertedLeafCells(baseLeaf, committedLeaf, out _) ||
            !TryCollectInsertedLeafCells(baseLeaf, transactionLeaf, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0)
        {
            return false;
        }

        byte[] merged = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        committedPage.Span.CopyTo(merged);
        var mergedLeaf = new SlottedPage(merged, pageId);

        for (int i = 0; i < transactionInsertedCells.Count; i++)
        {
            byte[] insertedCell = transactionInsertedCells[i];
            long insertedKey = ReadLeafCellKey(insertedCell);
            int insertIndex = FindInsertPosition(mergedLeaf, insertedKey);
            if (insertIndex < mergedLeaf.CellCount && ReadLeafKey(mergedLeaf, insertIndex) == insertedKey)
                return false;

            if (!TryInsertLeafCell(ref mergedLeaf, insertIndex, insertedCell))
                return false;
        }

        rebasedPage = merged;
        return true;
    }

    private static bool TryCollectInsertedLeafCells(
        ReadOnlySlottedPage baseLeaf,
        ReadOnlySlottedPage candidateLeaf,
        out List<byte[]> insertedCells)
    {
        insertedCells = [];

        int baseIndex = 0;
        int candidateIndex = 0;
        while (baseIndex < baseLeaf.CellCount && candidateIndex < candidateLeaf.CellCount)
        {
            long baseKey = ReadLeafKey(baseLeaf, baseIndex);
            long candidateKey = ReadLeafKey(candidateLeaf, candidateIndex);
            if (candidateKey < baseKey)
            {
                insertedCells.Add(candidateLeaf.GetCellMemory(candidateIndex).ToArray());
                candidateIndex++;
                continue;
            }

            if (candidateKey != baseKey)
                return false;

            if (!candidateLeaf.GetCellMemory(candidateIndex).Span.SequenceEqual(baseLeaf.GetCellMemory(baseIndex).Span))
                return false;

            baseIndex++;
            candidateIndex++;
        }

        if (baseIndex != baseLeaf.CellCount)
            return false;

        while (candidateIndex < candidateLeaf.CellCount)
        {
            insertedCells.Add(candidateLeaf.GetCellMemory(candidateIndex).ToArray());
            candidateIndex++;
        }

        return true;
    }

    private static bool TryInsertLeafCell(ref SlottedPage leaf, int index, ReadOnlySpan<byte> cell)
    {
        if (leaf.InsertCell(index, cell))
            return true;

        leaf.Defragment();
        return leaf.InsertCell(index, cell);
    }

    private static int FindInsertPosition(SlottedPage leaf, long key)
    {
        int lo = 0;
        int hi = leaf.CellCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (ReadLeafKey(leaf, mid) < key)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    private static long ReadLeafKey(SlottedPage leaf, int index)
    {
        ReadOnlyMemory<byte> cell = leaf.GetCellMemory(index);
        ReadVarintFast(cell.Span, out int headerBytes);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(cell.Span.Slice(headerBytes, 8));
    }

    private static long ReadLeafKey(ReadOnlySlottedPage leaf, int index)
    {
        ReadOnlyMemory<byte> cell = leaf.GetCellMemory(index);
        ReadVarintFast(cell.Span, out int headerBytes);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(cell.Span.Slice(headerBytes, 8));
    }

    private static long ReadLeafCellKey(ReadOnlySpan<byte> cell)
    {
        ReadVarintFast(cell, out int headerBytes);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(headerBytes, 8));
    }

    private static ulong ReadVarintFast(ReadOnlySpan<byte> source, out int bytesRead)
    {
        byte first = source[0];
        if ((first & 0x80) == 0)
        {
            bytesRead = 1;
            return first;
        }

        return Varint.Read(source, out bytesRead);
    }
}

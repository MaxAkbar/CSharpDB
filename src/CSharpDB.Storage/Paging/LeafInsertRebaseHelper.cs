using CSharpDB.Primitives;

namespace CSharpDB.Storage.Paging;

internal static class LeafInsertRebaseHelper
{
    public static InsertOnlyRebaseResult TryRebaseInsertOnlyLeafPage(
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
            return InsertOnlyRebaseResult.NotApplicable;
        }

        uint nextLeaf = baseLeaf.RightChildOrNextLeaf;
        if (committedLeaf.RightChildOrNextLeaf != nextLeaf ||
            transactionLeaf.RightChildOrNextLeaf != nextLeaf)
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TryCollectInsertedLeafCells(baseLeaf, committedLeaf, out _) ||
            !TryCollectInsertedLeafCells(baseLeaf, transactionLeaf, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0)
        {
            return InsertOnlyRebaseResult.StructuralReject;
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
                return InsertOnlyRebaseResult.StructuralReject;

            if (!TryInsertLeafCell(ref mergedLeaf, insertIndex, insertedCell))
                return InsertOnlyRebaseResult.CapacityReject;
        }

        rebasedPage = merged;
        return InsertOnlyRebaseResult.Success;
    }

    public static InsertOnlyRebaseResult TryPlanSplitInsertOnlyLeafPage(
        uint pageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        ReadOnlyMemory<byte> transactionPage,
        out LeafInsertSplitPlan? splitPlan)
    {
        var baseLeaf = new ReadOnlySlottedPage(basePage, pageId);
        var committedLeaf = new ReadOnlySlottedPage(committedPage, pageId);
        var transactionLeaf = new ReadOnlySlottedPage(transactionPage, pageId);

        splitPlan = null;
        if (baseLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedLeaf.PageType != PageConstants.PageTypeLeaf ||
            transactionLeaf.PageType != PageConstants.PageTypeLeaf)
        {
            return InsertOnlyRebaseResult.NotApplicable;
        }

        uint nextLeaf = baseLeaf.RightChildOrNextLeaf;
        if (committedLeaf.RightChildOrNextLeaf != nextLeaf ||
            transactionLeaf.RightChildOrNextLeaf != nextLeaf)
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TryCollectInsertedLeafCells(baseLeaf, committedLeaf, out _) ||
            !TryCollectInsertedLeafCells(baseLeaf, transactionLeaf, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0 ||
            !TryMergeCommittedAndTransactionLeafCells(committedLeaf, transactionInsertedCells, out byte[][]? mergedCells, out bool rightEdgeSplit))
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TrySelectLeafSplitIndex(mergedCells, rightEdgeSplit, out int splitIndex))
            return InsertOnlyRebaseResult.CapacityReject;

        byte[][] leftCells = new byte[splitIndex][];
        byte[][] rightCells = new byte[mergedCells.Length - splitIndex][];
        Array.Copy(mergedCells, 0, leftCells, 0, leftCells.Length);
        Array.Copy(mergedCells, splitIndex, rightCells, 0, rightCells.Length);

        splitPlan = new LeafInsertSplitPlan(
            leftCells,
            rightCells,
            ReadLeafCellKey(rightCells[0]),
            committedLeaf.RightChildOrNextLeaf,
            rightEdgeSplit);
        return InsertOnlyRebaseResult.Success;
    }

    public static byte[] BuildLeafPage(uint pageId, IReadOnlyList<byte[]> cells, uint nextLeafPageId)
    {
        byte[] page = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        var leaf = new SlottedPage(page, pageId);
        leaf.Initialize(PageConstants.PageTypeLeaf);
        leaf.RightChildOrNextLeaf = nextLeafPageId;

        for (int i = 0; i < cells.Count; i++)
        {
            if (leaf.InsertCell(i, cells[i]))
                continue;

            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Commit-time leaf split materialization overflowed page {pageId}.");
        }

        return page;
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

    private static bool TryMergeCommittedAndTransactionLeafCells(
        ReadOnlySlottedPage committedLeaf,
        List<byte[]> transactionInsertedCells,
        out byte[][] mergedCells,
        out bool rightEdgeSplit)
    {
        mergedCells = [];
        rightEdgeSplit = false;

        int committedCount = committedLeaf.CellCount;
        var orderedCells = new byte[committedCount + transactionInsertedCells.Count][];
        int committedIndex = 0;
        int insertedIndex = 0;
        int writeIndex = 0;

        long maxCommittedKey = committedCount > 0
            ? ReadLeafKey(committedLeaf, committedCount - 1)
            : long.MinValue;
        rightEdgeSplit =
            committedLeaf.RightChildOrNextLeaf == PageConstants.NullPageId &&
            transactionInsertedCells.Count > 0 &&
            (committedCount == 0 || ReadLeafCellKey(transactionInsertedCells[0]) > maxCommittedKey);

        while (committedIndex < committedCount && insertedIndex < transactionInsertedCells.Count)
        {
            long committedKey = ReadLeafKey(committedLeaf, committedIndex);
            byte[] insertedCell = transactionInsertedCells[insertedIndex];
            long insertedKey = ReadLeafCellKey(insertedCell);
            if (insertedKey < committedKey)
            {
                orderedCells[writeIndex++] = insertedCell;
                insertedIndex++;
                continue;
            }

            if (insertedKey == committedKey)
                return false;

            orderedCells[writeIndex++] = committedLeaf.GetCellMemory(committedIndex).ToArray();
            committedIndex++;
        }

        while (committedIndex < committedCount)
        {
            orderedCells[writeIndex++] = committedLeaf.GetCellMemory(committedIndex).ToArray();
            committedIndex++;
        }

        while (insertedIndex < transactionInsertedCells.Count)
        {
            orderedCells[writeIndex++] = transactionInsertedCells[insertedIndex];
            insertedIndex++;
        }

        mergedCells = orderedCells;
        return true;
    }

    private static bool TrySelectLeafSplitIndex(
        byte[][] mergedCells,
        bool preferSparseRightPage,
        out int splitIndex)
    {
        int totalCellCount = mergedCells.Length;
        splitIndex = -1;
        if (totalCellCount < 2)
            return false;

        int[] cellOffsets = new int[totalCellCount + 1];
        int totalCellBytes = 0;
        for (int i = 0; i < totalCellCount; i++)
        {
            cellOffsets[i] = totalCellBytes;
            totalCellBytes += mergedCells[i].Length;
        }

        cellOffsets[totalCellCount] = totalCellBytes;
        int capacity = PageConstants.PageSize - PageConstants.SlottedPageHeaderSize;
        long bestOccupancySkew = long.MaxValue;

        for (int candidateSplitIndex = 1; candidateSplitIndex < totalCellCount; candidateSplitIndex++)
        {
            int leftCellBytes = cellOffsets[candidateSplitIndex];
            int rightCellBytes = totalCellBytes - leftCellBytes;
            int leftCellCount = candidateSplitIndex;
            int rightCellCount = totalCellCount - candidateSplitIndex;
            int leftUsedBytes = leftCellBytes + leftCellCount * PageConstants.CellPointerSize;
            int rightUsedBytes = rightCellBytes + rightCellCount * PageConstants.CellPointerSize;
            if (leftUsedBytes > capacity || rightUsedBytes > capacity)
                continue;

            if (preferSparseRightPage)
                splitIndex = candidateSplitIndex;

            long occupancySkew = Math.Abs((long)leftUsedBytes - rightUsedBytes);
            if (occupancySkew < bestOccupancySkew)
            {
                bestOccupancySkew = occupancySkew;
                splitIndex = candidateSplitIndex;
            }
        }

        return splitIndex > 0;
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

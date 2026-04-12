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
        => TryRebaseInsertOnlyLeafPage(
            pageId,
            basePage,
            committedPage,
            transactionPage,
            out rebasedPage,
            out _);

    public static InsertOnlyRebaseResult TryRebaseInsertOnlyLeafPage(
        uint pageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        ReadOnlyMemory<byte> transactionPage,
        out byte[]? rebasedPage,
        out LeafInsertRebaseRejectReason rejectReason)
    {
        var baseLeaf = new ReadOnlySlottedPage(basePage, pageId);
        var committedLeaf = new ReadOnlySlottedPage(committedPage, pageId);
        var transactionLeaf = new ReadOnlySlottedPage(transactionPage, pageId);

        rebasedPage = null;
        rejectReason = LeafInsertRebaseRejectReason.None;
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
            rejectReason = LeafInsertRebaseRejectReason.NextLeafChanged;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TryCollectInsertedLeafCells(baseLeaf, committedLeaf, out _) ||
            !TryCollectInsertedLeafCells(baseLeaf, transactionLeaf, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0)
        {
            rejectReason = LeafInsertRebaseRejectReason.NonInsertOnlyDelta;
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
            {
                rejectReason = LeafInsertRebaseRejectReason.DuplicateKey;
                return InsertOnlyRebaseResult.StructuralReject;
            }

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
        splitPlan = null;
        try
        {
            var baseLeaf = new ReadOnlySlottedPage(basePage, pageId);
            var committedLeaf = new ReadOnlySlottedPage(committedPage, pageId);
            var transactionLeaf = new ReadOnlySlottedPage(transactionPage, pageId);

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
        catch (ArgumentOutOfRangeException)
        {
            splitPlan = null;
            return InsertOnlyRebaseResult.StructuralReject;
        }
        catch (IndexOutOfRangeException)
        {
            splitPlan = null;
            return InsertOnlyRebaseResult.StructuralReject;
        }
    }

    public static InsertOnlyRebaseResult TryRebaseCommittedSplitLeafPages(
        uint leftPageId,
        uint rightPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedLeftPage,
        ReadOnlyMemory<byte> committedRightPage,
        ReadOnlyMemory<byte> transactionPage,
        out byte[]? rebasedLeftPage,
        out byte[]? rebasedRightPage)
        => TryRebaseCommittedSplitLeafPages(
            leftPageId,
            rightPageId,
            basePage,
            committedLeftPage,
            committedRightPage,
            transactionPage,
            out rebasedLeftPage,
            out rebasedRightPage,
            out _);

    public static InsertOnlyRebaseResult TryRebaseCommittedSplitLeafPages(
        uint leftPageId,
        uint rightPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedLeftPage,
        ReadOnlyMemory<byte> committedRightPage,
        ReadOnlyMemory<byte> transactionPage,
        out byte[]? rebasedLeftPage,
        out byte[]? rebasedRightPage,
        out LeafInsertRebaseRejectReason rejectReason)
    {
        var baseLeaf = new ReadOnlySlottedPage(basePage, leftPageId);
        var committedLeftLeaf = new ReadOnlySlottedPage(committedLeftPage, leftPageId);
        var committedRightLeaf = new ReadOnlySlottedPage(committedRightPage, rightPageId);
        var transactionLeaf = new ReadOnlySlottedPage(transactionPage, leftPageId);

        rebasedLeftPage = null;
        rebasedRightPage = null;
        rejectReason = LeafInsertRebaseRejectReason.None;
        if (baseLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedLeftLeaf.PageType != PageConstants.PageTypeLeaf ||
            committedRightLeaf.PageType != PageConstants.PageTypeLeaf ||
            transactionLeaf.PageType != PageConstants.PageTypeLeaf)
        {
            return InsertOnlyRebaseResult.NotApplicable;
        }

        uint originalNextLeaf = baseLeaf.RightChildOrNextLeaf;
        if (transactionLeaf.RightChildOrNextLeaf != originalNextLeaf)
        {
            rejectReason = LeafInsertRebaseRejectReason.NonInsertOnlyDelta;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (committedLeftLeaf.RightChildOrNextLeaf != rightPageId ||
            committedRightLeaf.RightChildOrNextLeaf != originalNextLeaf ||
            !TryReadOrderedSplitLeafCells(committedLeftLeaf, committedRightLeaf, out byte[][] committedCells) ||
            !TryReadOrderedLeafCells(baseLeaf, out byte[][] baseCells) ||
            !TryCollectInsertedLeafCells(baseCells, committedCells, out _))
        {
            rejectReason = LeafInsertRebaseRejectReason.InvalidCommittedSplitShape;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TryCollectInsertedLeafCells(baseLeaf, transactionLeaf, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0)
        {
            rejectReason = LeafInsertRebaseRejectReason.NonInsertOnlyDelta;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        return TryRebaseCommittedSplitLeafPages(
            leftPageId,
            rightPageId,
            basePage,
            committedLeftPage,
            committedRightPage,
            transactionInsertedCells,
            out rebasedLeftPage,
            out rebasedRightPage,
            out rejectReason);
    }

    public static InsertOnlyRebaseResult TryCollectInsertedLeafCellsFromSplitPages(
        uint leftPageId,
        uint rightPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> splitLeftPage,
        ReadOnlyMemory<byte> splitRightPage,
        out List<byte[]>? insertedCells,
        out LeafInsertRebaseRejectReason rejectReason)
    {
        var baseLeaf = new ReadOnlySlottedPage(basePage, leftPageId);
        var splitLeftLeaf = new ReadOnlySlottedPage(splitLeftPage, leftPageId);
        var splitRightLeaf = new ReadOnlySlottedPage(splitRightPage, rightPageId);

        insertedCells = null;
        rejectReason = LeafInsertRebaseRejectReason.None;
        if (baseLeaf.PageType != PageConstants.PageTypeLeaf ||
            splitLeftLeaf.PageType != PageConstants.PageTypeLeaf ||
            splitRightLeaf.PageType != PageConstants.PageTypeLeaf)
        {
            return InsertOnlyRebaseResult.NotApplicable;
        }

        uint originalNextLeaf = baseLeaf.RightChildOrNextLeaf;
        if (splitLeftLeaf.RightChildOrNextLeaf != rightPageId ||
            splitRightLeaf.RightChildOrNextLeaf != originalNextLeaf)
        {
            rejectReason = LeafInsertRebaseRejectReason.InvalidCommittedSplitShape;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (!TryReadOrderedSplitLeafCells(splitLeftLeaf, splitRightLeaf, out byte[][] splitCells) ||
            !TryReadOrderedLeafCells(baseLeaf, out byte[][] baseCells) ||
            !TryCollectInsertedLeafCells(baseCells, splitCells, out List<byte[]>? transactionInsertedCells) ||
            transactionInsertedCells.Count == 0)
        {
            rejectReason = LeafInsertRebaseRejectReason.NonInsertOnlyDelta;
            return InsertOnlyRebaseResult.StructuralReject;
        }

        insertedCells = transactionInsertedCells;
        return InsertOnlyRebaseResult.Success;
    }

    public static InsertOnlyRebaseResult TryRebaseCommittedSplitLeafPagesWithInsertedCells(
        uint leftPageId,
        uint rightPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedLeftPage,
        ReadOnlyMemory<byte> committedRightPage,
        IReadOnlyList<byte[]> transactionInsertedCells,
        out byte[]? rebasedLeftPage,
        out byte[]? rebasedRightPage,
        out LeafInsertRebaseRejectReason rejectReason)
        => TryRebaseCommittedSplitLeafPages(
            leftPageId,
            rightPageId,
            basePage,
            committedLeftPage,
            committedRightPage,
            transactionInsertedCells,
            out rebasedLeftPage,
            out rebasedRightPage,
            out rejectReason);

    private static InsertOnlyRebaseResult TryRebaseCommittedSplitLeafPages(
        uint leftPageId,
        uint rightPageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedLeftPage,
        ReadOnlyMemory<byte> committedRightPage,
        IReadOnlyList<byte[]> transactionInsertedCells,
        out byte[]? rebasedLeftPage,
        out byte[]? rebasedRightPage,
        out LeafInsertRebaseRejectReason rejectReason)
    {
        var committedLeftLeaf = new ReadOnlySlottedPage(committedLeftPage, leftPageId);
        var committedRightLeaf = new ReadOnlySlottedPage(committedRightPage, rightPageId);

        rebasedLeftPage = null;
        rebasedRightPage = null;
        rejectReason = LeafInsertRebaseRejectReason.None;
        byte[] leftPage = committedLeftPage.ToArray();
        byte[] rightPage = committedRightPage.ToArray();
        var mergedLeftLeaf = new SlottedPage(leftPage, leftPageId);
        var mergedRightLeaf = new SlottedPage(rightPage, rightPageId);
        long splitKey = ReadLeafKey(committedRightLeaf, 0);

        for (int i = 0; i < transactionInsertedCells.Count; i++)
        {
            byte[] insertedCell = transactionInsertedCells[i];
            long insertedKey = ReadLeafCellKey(insertedCell);
            if (insertedKey < splitKey)
            {
                int insertIndex = FindInsertPosition(mergedLeftLeaf, insertedKey);
                if (insertIndex < mergedLeftLeaf.CellCount &&
                    ReadLeafKey(mergedLeftLeaf, insertIndex) == insertedKey)
                {
                    rejectReason = LeafInsertRebaseRejectReason.DuplicateKey;
                    return InsertOnlyRebaseResult.StructuralReject;
                }

                if (!TryInsertLeafCell(ref mergedLeftLeaf, insertIndex, insertedCell))
                    return InsertOnlyRebaseResult.CapacityReject;

                continue;
            }

            int rightInsertIndex = FindInsertPosition(mergedRightLeaf, insertedKey);
            if (rightInsertIndex < mergedRightLeaf.CellCount &&
                ReadLeafKey(mergedRightLeaf, rightInsertIndex) == insertedKey)
            {
                rejectReason = LeafInsertRebaseRejectReason.DuplicateKey;
                return InsertOnlyRebaseResult.StructuralReject;
            }

            if (!TryInsertLeafCell(ref mergedRightLeaf, rightInsertIndex, insertedCell))
                return InsertOnlyRebaseResult.CapacityReject;
        }

        rebasedLeftPage = leftPage;
        rebasedRightPage = rightPage;
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

    public static bool TryReadFirstLeafKey(
        uint pageId,
        ReadOnlyMemory<byte> page,
        out long key)
    {
        var leaf = new ReadOnlySlottedPage(page, pageId);
        if (leaf.PageType != PageConstants.PageTypeLeaf || leaf.CellCount == 0)
        {
            key = default;
            return false;
        }

        key = ReadLeafKey(leaf, 0);
        return true;
    }

    private static bool TryCollectInsertedLeafCells(
        ReadOnlySlottedPage baseLeaf,
        ReadOnlySlottedPage candidateLeaf,
        out List<byte[]> insertedCells)
    {
        if (!TryReadOrderedLeafCells(baseLeaf, out byte[][] baseCells) ||
            !TryReadOrderedLeafCells(candidateLeaf, out byte[][] candidateCells))
        {
            insertedCells = [];
            return false;
        }

        return TryCollectInsertedLeafCells(baseCells, candidateCells, out insertedCells);
    }

    private static bool TryCollectInsertedLeafCells(
        IReadOnlyList<byte[]> baseCells,
        IReadOnlyList<byte[]> candidateCells,
        out List<byte[]> insertedCells)
    {
        insertedCells = [];
        try
        {
            int baseIndex = 0;
            int candidateIndex = 0;
            while (baseIndex < baseCells.Count && candidateIndex < candidateCells.Count)
            {
                byte[] baseCell = baseCells[baseIndex];
                byte[] candidateCell = candidateCells[candidateIndex];
                long baseKey = ReadLeafCellKey(baseCell);
                long candidateKey = ReadLeafCellKey(candidateCell);
                if (candidateKey < baseKey)
                {
                    insertedCells.Add(candidateCell);
                    candidateIndex++;
                    continue;
                }

                if (candidateKey != baseKey)
                    return false;

                if (!candidateCell.AsSpan().SequenceEqual(baseCell))
                    return false;

                baseIndex++;
                candidateIndex++;
            }

            if (baseIndex != baseCells.Count)
                return false;

            while (candidateIndex < candidateCells.Count)
            {
                insertedCells.Add(candidateCells[candidateIndex]);
                candidateIndex++;
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            insertedCells = [];
            return false;
        }
        catch (IndexOutOfRangeException)
        {
            insertedCells = [];
            return false;
        }

        return true;
    }

    private static bool TryReadOrderedSplitLeafCells(
        ReadOnlySlottedPage leftLeaf,
        ReadOnlySlottedPage rightLeaf,
        out byte[][] cells)
    {
        cells = [];
        if (!TryReadOrderedLeafCells(leftLeaf, out byte[][] leftCells) ||
            !TryReadOrderedLeafCells(rightLeaf, out byte[][] rightCells) ||
            rightCells.Length == 0)
        {
            return false;
        }

        if (leftCells.Length > 0 &&
            ReadLeafCellKey(leftCells[^1]) >= ReadLeafCellKey(rightCells[0]))
        {
            return false;
        }

        cells = new byte[leftCells.Length + rightCells.Length][];
        Array.Copy(leftCells, 0, cells, 0, leftCells.Length);
        Array.Copy(rightCells, 0, cells, leftCells.Length, rightCells.Length);
        return true;
    }

    private static bool TryReadOrderedLeafCells(ReadOnlySlottedPage leaf, out byte[][] cells)
    {
        cells = new byte[leaf.CellCount][];
        try
        {
            long previousKey = long.MinValue;
            for (int i = 0; i < leaf.CellCount; i++)
            {
                byte[] cell = leaf.GetCellMemory(i).ToArray();
                long key = ReadLeafCellKey(cell);
                if (i > 0 && key <= previousKey)
                {
                    cells = [];
                    return false;
                }

                cells[i] = cell;
                previousKey = key;
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            cells = [];
            return false;
        }
        catch (IndexOutOfRangeException)
        {
            cells = [];
            return false;
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

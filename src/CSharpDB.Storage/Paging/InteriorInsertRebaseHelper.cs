using System.Buffers.Binary;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Paging;

internal static class InteriorInsertRebaseHelper
{
    public static InsertOnlyRebaseResult TryRebaseInsertOnlyInteriorPage(
        uint pageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> committedPage,
        ReadOnlyMemory<byte> transactionPage,
        out byte[]? rebasedPage)
    {
        var baseInterior = new ReadOnlySlottedPage(basePage, pageId);
        var committedInterior = new ReadOnlySlottedPage(committedPage, pageId);
        var transactionInterior = new ReadOnlySlottedPage(transactionPage, pageId);

        rebasedPage = null;
        if (baseInterior.PageType != PageConstants.PageTypeInterior ||
            committedInterior.PageType != PageConstants.PageTypeInterior ||
            transactionInterior.PageType != PageConstants.PageTypeInterior)
        {
            return InsertOnlyRebaseResult.NotApplicable;
        }

        if (!TryCollectInsertedInteriorEntries(baseInterior, committedInterior, out _) ||
            !TryCollectInsertedInteriorEntries(baseInterior, transactionInterior, out List<InteriorInsertion>? transactionInsertions) ||
            transactionInsertions.Count == 0)
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        byte[] merged = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        committedPage.Span.CopyTo(merged);
        var mergedInterior = new SlottedPage(merged, pageId);

        for (int i = 0; i < transactionInsertions.Count; i++)
        {
            InsertOnlyRebaseResult applyResult = TryApplyInsertion(ref mergedInterior, transactionInsertions[i]);
            if (applyResult != InsertOnlyRebaseResult.Success)
                return applyResult;
        }

        rebasedPage = merged;
        return InsertOnlyRebaseResult.Success;
    }

    public static InsertOnlyRebaseResult TryApplyCommittedInteriorInsertion(
        uint pageId,
        ReadOnlyMemory<byte> committedPage,
        uint leftChild,
        uint rightBoundaryChild,
        long key,
        uint newChild,
        out byte[]? rebasedPage)
    {
        var committedInterior = new ReadOnlySlottedPage(committedPage, pageId);

        rebasedPage = null;
        if (committedInterior.PageType != PageConstants.PageTypeInterior)
            return InsertOnlyRebaseResult.NotApplicable;

        byte[] merged = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        committedPage.Span.CopyTo(merged);
        var mergedInterior = new SlottedPage(merged, pageId);
        InsertOnlyRebaseResult result = TryApplyInsertion(
            ref mergedInterior,
            new InteriorInsertion(leftChild, rightBoundaryChild, key, newChild));
        if (result == InsertOnlyRebaseResult.Success)
            rebasedPage = merged;

        return result;
    }

    public static InsertOnlyRebaseResult TryApplyCommittedInteriorChildSplit(
        uint pageId,
        ReadOnlyMemory<byte> committedPage,
        uint leftChild,
        uint currentRightChild,
        uint rightBoundaryChild,
        long currentRightChildKey,
        long newChildKey,
        uint newChild,
        out byte[]? rebasedPage)
    {
        var committedInterior = new ReadOnlySlottedPage(committedPage, pageId);

        rebasedPage = null;
        if (committedInterior.PageType != PageConstants.PageTypeInterior)
            return InsertOnlyRebaseResult.NotApplicable;

        var keys = new List<long>(committedInterior.CellCount + 1);
        var children = new List<uint>(committedInterior.CellCount + 2);
        ReadInteriorNode(committedInterior, keys, children);

        int leftChildIndex = children.IndexOf(leftChild);
        if (leftChildIndex < 0 ||
            leftChildIndex + 1 >= children.Count ||
            children[leftChildIndex + 1] != currentRightChild ||
            children.Contains(newChild))
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        if (rightBoundaryChild == PageConstants.NullPageId)
        {
            if (leftChildIndex + 1 != children.Count - 1)
                return InsertOnlyRebaseResult.StructuralReject;
        }
        else if (leftChildIndex + 2 >= children.Count || children[leftChildIndex + 2] != rightBoundaryChild)
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        int currentRightKeyIndex = leftChildIndex;
        if (currentRightKeyIndex >= keys.Count ||
            currentRightChildKey >= newChildKey ||
            (currentRightKeyIndex > 0 && keys[currentRightKeyIndex - 1] >= currentRightChildKey) ||
            (currentRightKeyIndex + 1 < keys.Count && newChildKey >= keys[currentRightKeyIndex + 1]))
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        keys[currentRightKeyIndex] = currentRightChildKey;
        keys.Insert(currentRightKeyIndex + 1, newChildKey);
        children.Insert(leftChildIndex + 2, newChild);

        byte[] merged = GC.AllocateUninitializedArray<byte>(PageConstants.PageSize);
        committedPage.Span.CopyTo(merged);
        var mergedInterior = new SlottedPage(merged, pageId);
        if (!TryWriteInteriorNode(ref mergedInterior, keys, children))
            return InsertOnlyRebaseResult.CapacityReject;

        rebasedPage = merged;
        return InsertOnlyRebaseResult.Success;
    }

    public static InsertOnlyRebaseResult TryDescribeSingleInsertedInteriorEntry(
        uint pageId,
        ReadOnlyMemory<byte> basePage,
        ReadOnlyMemory<byte> candidatePage,
        out InteriorInsertion insertion)
    {
        var baseInterior = new ReadOnlySlottedPage(basePage, pageId);
        var candidateInterior = new ReadOnlySlottedPage(candidatePage, pageId);

        insertion = default;
        if (baseInterior.PageType != PageConstants.PageTypeInterior ||
            candidateInterior.PageType != PageConstants.PageTypeInterior)
        {
            return InsertOnlyRebaseResult.NotApplicable;
        }

        if (!TryCollectInsertedInteriorEntries(baseInterior, candidateInterior, out List<InteriorInsertion> insertedEntries) ||
            insertedEntries.Count != 1)
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        insertion = insertedEntries[0];
        return InsertOnlyRebaseResult.Success;
    }

    private static bool TryCollectInsertedInteriorEntries(
        ReadOnlySlottedPage baseInterior,
        ReadOnlySlottedPage candidateInterior,
        out List<InteriorInsertion> insertedEntries)
    {
        insertedEntries = [];

        var baseKeys = new List<long>(baseInterior.CellCount);
        var baseChildren = new List<uint>(baseInterior.CellCount + 1);
        var candidateKeys = new List<long>(candidateInterior.CellCount);
        var candidateChildren = new List<uint>(candidateInterior.CellCount + 1);
        ReadInteriorNode(baseInterior, baseKeys, baseChildren);
        ReadInteriorNode(candidateInterior, candidateKeys, candidateChildren);

        if (candidateKeys.Count < baseKeys.Count ||
            candidateChildren.Count < baseChildren.Count ||
            candidateChildren.Count != candidateKeys.Count + 1 ||
            baseChildren.Count == 0 ||
            candidateChildren[0] != baseChildren[0])
        {
            return false;
        }

        int candidateKeyIndex = 0;
        int candidateChildIndex = 0;
        for (int baseKeyIndex = 0; baseKeyIndex < baseKeys.Count; baseKeyIndex++)
        {
            long baseKey = baseKeys[baseKeyIndex];
            uint baseRightChild = baseChildren[baseKeyIndex + 1];

            while (candidateKeyIndex < candidateKeys.Count &&
                   candidateKeys[candidateKeyIndex] != baseKey)
            {
                if (candidateChildIndex + 1 >= candidateChildren.Count)
                    return false;

                insertedEntries.Add(new InteriorInsertion(
                    candidateChildren[candidateChildIndex],
                    baseRightChild,
                    candidateKeys[candidateKeyIndex],
                    candidateChildren[candidateChildIndex + 1]));
                candidateKeyIndex++;
                candidateChildIndex++;
            }

            if (candidateKeyIndex >= candidateKeys.Count ||
                candidateKeys[candidateKeyIndex] != baseKey ||
                candidateChildIndex + 1 >= candidateChildren.Count ||
                candidateChildren[candidateChildIndex + 1] != baseRightChild)
            {
                return false;
            }

            candidateKeyIndex++;
            candidateChildIndex++;
        }

        while (candidateKeyIndex < candidateKeys.Count)
        {
            if (candidateChildIndex + 1 >= candidateChildren.Count)
                return false;

            insertedEntries.Add(new InteriorInsertion(
                candidateChildren[candidateChildIndex],
                PageConstants.NullPageId,
                candidateKeys[candidateKeyIndex],
                candidateChildren[candidateChildIndex + 1]));
            candidateKeyIndex++;
            candidateChildIndex++;
        }

        return candidateChildIndex == candidateChildren.Count - 1;
    }

    private static InsertOnlyRebaseResult TryApplyInsertion(ref SlottedPage mergedInterior, InteriorInsertion insertion)
    {
        var keys = new List<long>(mergedInterior.CellCount);
        var children = new List<uint>(mergedInterior.CellCount + 1);
        ReadInteriorNode(mergedInterior, keys, children);

        int leftChildIndex = children.IndexOf(insertion.LeftChild);
        if (leftChildIndex < 0 || children.Contains(insertion.NewChild))
            return InsertOnlyRebaseResult.StructuralReject;

        int maxInsertIndexExclusive;
        if (insertion.RightBoundaryChild == PageConstants.NullPageId)
        {
            maxInsertIndexExclusive = keys.Count + 1;
        }
        else
        {
            int rightChildIndex = children.IndexOf(insertion.RightBoundaryChild, leftChildIndex + 1);
            if (rightChildIndex < 0)
                return InsertOnlyRebaseResult.StructuralReject;

            maxInsertIndexExclusive = rightChildIndex;
        }

        int insertKeyIndex = LowerBound(keys, insertion.Key);
        if (insertKeyIndex < leftChildIndex ||
            insertKeyIndex >= maxInsertIndexExclusive ||
            (insertKeyIndex < keys.Count && keys[insertKeyIndex] == insertion.Key))
        {
            return InsertOnlyRebaseResult.StructuralReject;
        }

        keys.Insert(insertKeyIndex, insertion.Key);
        children.Insert(insertKeyIndex + 1, insertion.NewChild);
        return TryWriteInteriorNode(ref mergedInterior, keys, children)
            ? InsertOnlyRebaseResult.Success
            : InsertOnlyRebaseResult.CapacityReject;
    }

    private static void ReadInteriorNode(ReadOnlySlottedPage interior, List<long> keys, List<uint> children)
    {
        keys.Clear();
        children.Clear();

        int keyCount = interior.CellCount;
        for (int i = 0; i < keyCount; i++)
        {
            keys.Add(ReadInteriorKey(interior, i));
            children.Add(ReadInteriorLeftChild(interior, i));
        }

        children.Add(interior.RightChildOrNextLeaf);
    }

    private static void ReadInteriorNode(SlottedPage interior, List<long> keys, List<uint> children)
    {
        keys.Clear();
        children.Clear();

        int keyCount = interior.CellCount;
        for (int i = 0; i < keyCount; i++)
        {
            keys.Add(ReadInteriorKey(interior, i));
            children.Add(ReadInteriorLeftChild(interior, i));
        }

        children.Add(interior.RightChildOrNextLeaf);
    }

    private static bool TryWriteInteriorNode(ref SlottedPage interior, IReadOnlyList<long> keys, IReadOnlyList<uint> children)
    {
        if (children.Count != keys.Count + 1)
            return false;

        interior.Initialize(PageConstants.PageTypeInterior);
        interior.RightChildOrNextLeaf = children[^1];

        Span<byte> cell = stackalloc byte[13];
        for (int i = 0; i < keys.Count; i++)
        {
            WriteInteriorCell(cell, children[i], keys[i]);
            if (interior.InsertCell(i, cell))
                continue;

            interior.Defragment();
            if (!interior.InsertCell(i, cell))
                return false;
        }

        return true;
    }

    private static int LowerBound(IReadOnlyList<long> keys, long key)
    {
        int lo = 0;
        int hi = keys.Count;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (keys[mid] < key)
                lo = mid + 1;
            else
                hi = mid;
        }

        return lo;
    }

    private static long ReadInteriorKey(ReadOnlySlottedPage interior, int index)
    {
        ReadOnlyMemory<byte> cell = interior.GetCellMemory(index);
        return BinaryPrimitives.ReadInt64LittleEndian(cell.Span.Slice(5, 8));
    }

    private static long ReadInteriorKey(SlottedPage interior, int index)
    {
        ReadOnlyMemory<byte> cell = interior.GetCellMemory(index);
        return BinaryPrimitives.ReadInt64LittleEndian(cell.Span.Slice(5, 8));
    }

    private static uint ReadInteriorLeftChild(ReadOnlySlottedPage interior, int index)
    {
        ReadOnlyMemory<byte> cell = interior.GetCellMemory(index);
        return BinaryPrimitives.ReadUInt32LittleEndian(cell.Span.Slice(1, 4));
    }

    private static uint ReadInteriorLeftChild(SlottedPage interior, int index)
    {
        ReadOnlyMemory<byte> cell = interior.GetCellMemory(index);
        return BinaryPrimitives.ReadUInt32LittleEndian(cell.Span.Slice(1, 4));
    }

    private static void WriteInteriorCell(Span<byte> destination, uint leftChild, long key)
    {
        destination[0] = 12;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), leftChild);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(5, 8), key);
    }

    internal readonly record struct InteriorInsertion(
        uint LeftChild,
        uint RightBoundaryChild,
        long Key,
        uint NewChild);
}

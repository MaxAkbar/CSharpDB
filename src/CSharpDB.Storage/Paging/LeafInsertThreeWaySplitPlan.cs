namespace CSharpDB.Storage.Paging;

internal sealed class LeafInsertThreeWaySplitPlan
{
    public LeafInsertThreeWaySplitPlan(
        byte[][] leftCells,
        byte[][] middleCells,
        byte[][] rightCells,
        long middleSplitKey,
        long rightSplitKey,
        uint originalNextLeafPageId,
        bool rightEdgeSplit)
    {
        LeftCells = leftCells ?? throw new ArgumentNullException(nameof(leftCells));
        MiddleCells = middleCells ?? throw new ArgumentNullException(nameof(middleCells));
        RightCells = rightCells ?? throw new ArgumentNullException(nameof(rightCells));
        MiddleSplitKey = middleSplitKey;
        RightSplitKey = rightSplitKey;
        OriginalNextLeafPageId = originalNextLeafPageId;
        RightEdgeSplit = rightEdgeSplit;
    }

    public byte[][] LeftCells { get; }

    public byte[][] MiddleCells { get; }

    public byte[][] RightCells { get; }

    public long MiddleSplitKey { get; }

    public long RightSplitKey { get; }

    public uint OriginalNextLeafPageId { get; }

    public bool RightEdgeSplit { get; }
}

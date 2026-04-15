namespace CSharpDB.Storage.Paging;

internal sealed class LeafInsertSplitPlan
{
    public LeafInsertSplitPlan(
        byte[][] leftCells,
        byte[][] rightCells,
        long splitKey,
        uint originalNextLeafPageId,
        bool rightEdgeSplit)
    {
        LeftCells = leftCells ?? throw new ArgumentNullException(nameof(leftCells));
        RightCells = rightCells ?? throw new ArgumentNullException(nameof(rightCells));
        SplitKey = splitKey;
        OriginalNextLeafPageId = originalNextLeafPageId;
        RightEdgeSplit = rightEdgeSplit;
    }

    public byte[][] LeftCells { get; }

    public byte[][] RightCells { get; }

    public long SplitKey { get; }

    public uint OriginalNextLeafPageId { get; }

    public bool RightEdgeSplit { get; }
}

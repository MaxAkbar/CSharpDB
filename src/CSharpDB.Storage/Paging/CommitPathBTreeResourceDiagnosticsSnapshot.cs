namespace CSharpDB.Storage.Paging;

public readonly record struct CommitPathBTreeResourceDiagnosticsSnapshot(
    string ResourceName,
    long LeafSplitCount,
    long RightEdgeLeafSplitCount,
    long InteriorInsertCount,
    long RightEdgeInteriorInsertCount,
    long InteriorSplitCount,
    long RightEdgeInteriorSplitCount,
    long RootSplitCount)
{
    public long NonRightEdgeLeafSplitCount => LeafSplitCount - RightEdgeLeafSplitCount;

    public long NonRightEdgeInteriorInsertCount => InteriorInsertCount - RightEdgeInteriorInsertCount;

    public long NonRightEdgeInteriorSplitCount => InteriorSplitCount - RightEdgeInteriorSplitCount;

    public long NonRightEdgeStructuralEventCount =>
        NonRightEdgeLeafSplitCount +
        NonRightEdgeInteriorInsertCount +
        NonRightEdgeInteriorSplitCount +
        RootSplitCount;

    public long StructuralEventCount =>
        LeafSplitCount +
        InteriorInsertCount +
        InteriorSplitCount +
        RootSplitCount;
}

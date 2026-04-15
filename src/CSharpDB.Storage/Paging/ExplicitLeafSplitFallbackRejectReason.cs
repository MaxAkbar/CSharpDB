namespace CSharpDB.Storage.Paging;

internal enum ExplicitLeafSplitFallbackRejectReason
{
    None = 0,
    MissingTraversal = 1,
    DirtyAncestor = 2,
    ParentBoundaryMissing = 3,
    TargetPageDirty = 4,
    InvalidCommittedShape = 5,
}

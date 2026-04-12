namespace CSharpDB.Storage.Paging;

internal enum DirtyParentLeafSplitRecoveryRejectReason
{
    None = 0,
    MissingParentPage = 1,
    TransactionLeafNotSplit = 2,
    BaseParentBoundaryMissing = 3,
    ParentInsertionShape = 4,
    ParentInsertionMismatch = 5,
    MissingLocalRightPage = 6,
    LocalSplitShape = 7,
    RebaseFailure = 8,
}

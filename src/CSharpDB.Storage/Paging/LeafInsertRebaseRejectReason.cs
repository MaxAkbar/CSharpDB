namespace CSharpDB.Storage.Paging;

internal enum LeafInsertRebaseRejectReason
{
    None = 0,
    NextLeafChanged = 1,
    NonInsertOnlyDelta = 2,
    DuplicateKey = 3,
    InvalidCommittedSplitShape = 4,
}

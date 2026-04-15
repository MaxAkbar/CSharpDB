namespace CSharpDB.Storage.Paging;

internal enum ExplicitLeafStructuralRejectReason
{
    None = 0,
    NonInsertOnlyDelta = 1,
    DuplicateKey = 2,
    SplitFallbackPrecondition = 3,
    SplitFallbackShape = 4,
    Other = 5,
}

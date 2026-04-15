namespace CSharpDB.Storage.Paging;

internal enum InsertOnlyRebaseResult
{
    NotApplicable = 0,
    Success = 1,
    StructuralReject = 2,
    CapacityReject = 3,
}

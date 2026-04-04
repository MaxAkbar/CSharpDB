namespace CSharpDB.Admin.Forms.Models;

public sealed record FormRecordPage(
    int PageNumber,
    int PageSize,
    int TotalCount,
    IReadOnlyList<Dictionary<string, object?>> Records);

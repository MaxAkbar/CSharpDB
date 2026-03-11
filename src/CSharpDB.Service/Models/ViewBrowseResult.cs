namespace CSharpDB.Service.Models;

[Obsolete("CSharpDB.Service models are deprecated and will be removed in v2.0.0. Use CSharpDB.Client.Models instead.")]
public sealed class ViewBrowseResult
{
    public required string ViewName { get; init; }
    public required string[] ColumnNames { get; init; }
    public required IReadOnlyList<object?[]> Rows { get; init; }
    public required int TotalRows { get; init; }
    public required int Page { get; init; }
    public required int PageSize { get; init; }

    public int TotalPages => Math.Max(1, (int)Math.Ceiling((double)TotalRows / PageSize));
}

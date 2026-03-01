using CSharpDB.Core;

namespace CSharpDB.Service.Models;

public sealed class TableBrowseResult
{
    public required string TableName { get; init; }
    public required TableSchema Schema { get; init; }
    public required List<object?[]> Rows { get; init; }
    public required int TotalRows { get; init; }
    public required int Page { get; init; }
    public required int PageSize { get; init; }
    public int TotalPages => Math.Max(1, (int)Math.Ceiling((double)TotalRows / PageSize));
}

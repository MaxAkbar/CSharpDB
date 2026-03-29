namespace CSharpDB.Admin.Models;

public sealed class QueryResultsStatus
{
    public int TotalRows { get; init; }
    public int VisibleRows { get; init; }
    public int Page { get; init; }
    public int PageSize { get; init; }
    public TimeSpan? Elapsed { get; init; }
    public string? Error { get; init; }
}

namespace CSharpDB.Client.Models;

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

public sealed class ViewBrowseResult
{
    public required string ViewName { get; init; }
    public required string[] ColumnNames { get; init; }
    public required List<object?[]> Rows { get; init; }
    public required int TotalRows { get; init; }
    public required int Page { get; init; }
    public required int PageSize { get; init; }
    public int TotalPages => Math.Max(1, (int)Math.Ceiling((double)TotalRows / PageSize));
}

public sealed class SqlExecutionResult
{
    public bool IsQuery { get; init; }
    public string[]? ColumnNames { get; init; }
    public List<object?[]>? Rows { get; init; }
    public int RowsAffected { get; init; }
    public string? Error { get; init; }
    public TimeSpan Elapsed { get; init; }
}

public sealed class DatabaseInfo
{
    public required string DataSource { get; init; }
    public int TableCount { get; init; }
    public int IndexCount { get; init; }
    public int ViewCount { get; init; }
    public int TriggerCount { get; init; }
    public int ProcedureCount { get; init; }
    public int CollectionCount { get; init; }
    public int SavedQueryCount { get; init; }
}

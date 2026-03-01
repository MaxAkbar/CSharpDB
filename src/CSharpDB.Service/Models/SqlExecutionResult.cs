namespace CSharpDB.Service.Models;

public sealed class SqlExecutionResult
{
    public bool IsQuery { get; init; }
    public string[]? ColumnNames { get; init; }
    public List<object?[]>? Rows { get; init; }
    public int RowsAffected { get; init; }
    public string? Error { get; init; }
    public TimeSpan Elapsed { get; init; }
}

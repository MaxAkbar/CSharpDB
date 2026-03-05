namespace CSharpDB.Service.Models;

public sealed class ProcedureStatementExecutionResult
{
    public int StatementIndex { get; init; }
    public string StatementText { get; init; } = string.Empty;
    public bool IsQuery { get; init; }
    public string[]? ColumnNames { get; init; }
    public List<object?[]>? Rows { get; init; }
    public int RowsAffected { get; init; }
    public TimeSpan Elapsed { get; init; }
}

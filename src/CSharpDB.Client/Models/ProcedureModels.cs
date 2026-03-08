namespace CSharpDB.Client.Models;

public sealed class ProcedureDefinition
{
    public required string Name { get; init; }
    public required string BodySql { get; init; }
    public IReadOnlyList<ProcedureParameterDefinition> Parameters { get; init; } = [];
    public string? Description { get; init; }
    public bool IsEnabled { get; init; } = true;
    public DateTime CreatedUtc { get; init; }
    public DateTime UpdatedUtc { get; init; }
}

public sealed class ProcedureParameterDefinition
{
    public required string Name { get; init; }
    public DbType Type { get; init; }
    public bool Required { get; init; }
    public object? Default { get; init; }
    public string? Description { get; init; }
}

public sealed class ProcedureExecutionResult
{
    public required string ProcedureName { get; init; }
    public bool Succeeded { get; init; }
    public IReadOnlyList<ProcedureStatementExecutionResult> Statements { get; init; } = [];
    public string? Error { get; init; }
    public int? FailedStatementIndex { get; init; }
    public TimeSpan Elapsed { get; init; }
}

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

public sealed class SavedQueryDefinition
{
    public long Id { get; init; }
    public required string Name { get; init; }
    public required string SqlText { get; init; }
    public DateTime CreatedUtc { get; init; }
    public DateTime UpdatedUtc { get; init; }
}

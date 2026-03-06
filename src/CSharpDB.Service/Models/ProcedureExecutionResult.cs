namespace CSharpDB.Service.Models;

public sealed class ProcedureExecutionResult
{
    public required string ProcedureName { get; init; }
    public bool Succeeded { get; init; }
    public IReadOnlyList<ProcedureStatementExecutionResult> Statements { get; init; } = [];
    public string? Error { get; init; }
    public int? FailedStatementIndex { get; init; }
    public TimeSpan Elapsed { get; init; }
}

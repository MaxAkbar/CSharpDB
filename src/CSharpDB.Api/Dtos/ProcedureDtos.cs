namespace CSharpDB.Api.Dtos;

public sealed record ProcedureParameterRequest(
    string Name,
    string Type,
    bool Required,
    object? Default = null,
    string? Description = null);

public sealed record CreateProcedureRequest(
    string Name,
    string BodySql,
    IReadOnlyList<ProcedureParameterRequest>? Parameters = null,
    string? Description = null,
    bool IsEnabled = true);

public sealed record UpdateProcedureRequest(
    string NewName,
    string BodySql,
    IReadOnlyList<ProcedureParameterRequest>? Parameters = null,
    string? Description = null,
    bool IsEnabled = true);

public sealed record ExecuteProcedureRequest(Dictionary<string, object?>? Args = null);

public sealed record ProcedureParameterResponse(
    string Name,
    string Type,
    bool Required,
    object? Default,
    string? Description);

public sealed record ProcedureSummaryResponse(
    string Name,
    string? Description,
    bool IsEnabled,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record ProcedureDetailResponse(
    string Name,
    string BodySql,
    IReadOnlyList<ProcedureParameterResponse> Parameters,
    string? Description,
    bool IsEnabled,
    DateTime CreatedUtc,
    DateTime UpdatedUtc);

public sealed record ProcedureStatementResultResponse(
    int StatementIndex,
    string StatementText,
    bool IsQuery,
    string[]? ColumnNames,
    IReadOnlyList<Dictionary<string, object?>>? Rows,
    int RowsAffected,
    double ElapsedMs);

public sealed record ProcedureExecutionResponse(
    string ProcedureName,
    bool Succeeded,
    IReadOnlyList<ProcedureStatementResultResponse> Statements,
    string? Error,
    int? FailedStatementIndex,
    double ElapsedMs);

namespace CSharpDB.Service.Models;

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

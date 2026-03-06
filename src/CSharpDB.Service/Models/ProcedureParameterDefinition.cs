using CSharpDB.Core;

namespace CSharpDB.Service.Models;

public sealed class ProcedureParameterDefinition
{
    public required string Name { get; init; }
    public DbType Type { get; init; }
    public bool Required { get; init; }
    public object? Default { get; init; }
    public string? Description { get; init; }
}

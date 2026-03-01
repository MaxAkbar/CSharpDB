namespace CSharpDB.Service.Models;

public sealed class ViewDefinition
{
    public required string Name { get; init; }
    public required string Sql { get; init; }
}

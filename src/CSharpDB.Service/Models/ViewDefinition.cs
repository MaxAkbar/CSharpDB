namespace CSharpDB.Service.Models;

[Obsolete("CSharpDB.Service models are deprecated and will be removed in v2.0.0. Use CSharpDB.Client.Models instead.")]
public sealed class ViewDefinition
{
    public required string Name { get; init; }
    public required string Sql { get; init; }
}

namespace CSharpDB.Service.Models;

[Obsolete("CSharpDB.Service models are deprecated and will be removed in v2.0.0. Use CSharpDB.Client.Models instead.")]
public sealed class SavedQueryDefinition
{
    public long Id { get; init; }
    public required string Name { get; init; }
    public required string SqlText { get; init; }
    public DateTime CreatedUtc { get; init; }
    public DateTime UpdatedUtc { get; init; }
}

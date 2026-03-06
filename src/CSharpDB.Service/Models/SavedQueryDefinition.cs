namespace CSharpDB.Service.Models;

public sealed class SavedQueryDefinition
{
    public long Id { get; init; }
    public required string Name { get; init; }
    public required string SqlText { get; init; }
    public DateTime CreatedUtc { get; init; }
    public DateTime UpdatedUtc { get; init; }
}

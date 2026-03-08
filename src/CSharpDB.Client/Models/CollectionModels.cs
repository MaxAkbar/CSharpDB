using System.Text.Json;

namespace CSharpDB.Client.Models;

public sealed class CollectionDocument
{
    public required string Key { get; init; }
    public required JsonElement Document { get; init; }
}

public sealed class CollectionBrowseResult
{
    public required string CollectionName { get; init; }
    public required IReadOnlyList<CollectionDocument> Documents { get; init; }
    public required int TotalCount { get; init; }
    public required int Page { get; init; }
    public required int PageSize { get; init; }
    public int TotalPages => Math.Max(1, (int)Math.Ceiling((double)TotalCount / PageSize));
}

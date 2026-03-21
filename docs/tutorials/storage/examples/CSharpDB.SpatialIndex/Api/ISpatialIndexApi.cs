using CSharpDB.SpatialIndex;

internal interface ISpatialIndexApi : IAsyncDisposable
{
    Task ResetAsync(CancellationToken ct);
    Task<SpatialPoint> AddAsync(double latitude, double longitude, string name, string? category, string? description, Dictionary<string, string>? tags, CancellationToken ct);
    Task DeleteAsync(long hilbertKey, CancellationToken ct);
    Task<SpatialPoint?> GetAsync(long hilbertKey, CancellationToken ct);
    Task<SpatialQueryResult> QueryNearbyAsync(double latitude, double longitude, double radiusKm, string? category, int maxResults, CancellationToken ct);
    Task<SpatialQueryResult> QueryBoundingBoxAsync(double minLat, double minLon, double maxLat, double maxLon, string? category, int maxResults, CancellationToken ct);
    Task<long> CountAsync(CancellationToken ct);
}

internal sealed record AddPointRequest(double Latitude, double Longitude, string Name, string? Category, string? Description, Dictionary<string, string>? Tags);

internal sealed record NearbyQueryRequest(double Latitude, double Longitude, double RadiusKm, string? Category, int MaxResults = 100);

internal sealed record BoundingBoxQueryRequest(double MinLat, double MinLon, double MaxLat, double MaxLon, string? Category, int MaxResults = 10_000);

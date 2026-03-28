using CSharpDB.SpatialIndex;

internal sealed class InProcessSpatialIndexApiClient : ISpatialIndexApi, IAsyncDisposable
{
    private readonly SpatialIndexApiService _service;

    public InProcessSpatialIndexApiClient(string databasePath)
    {
        _service = new SpatialIndexApiService(databasePath);
    }

    public Task ResetAsync(CancellationToken ct) => _service.ResetAsync(ct);

    public Task<SpatialPoint> AddAsync(double latitude, double longitude, string name, string? category, string? description, Dictionary<string, string>? tags, CancellationToken ct)
        => _service.AddAsync(latitude, longitude, name, category, description, tags, ct);

    public Task DeleteAsync(long hilbertKey, CancellationToken ct) => _service.DeleteAsync(hilbertKey, ct);

    public Task<SpatialPoint?> GetAsync(long hilbertKey, CancellationToken ct) => _service.GetAsync(hilbertKey, ct);

    public Task<SpatialQueryResult> QueryNearbyAsync(double latitude, double longitude, double radiusKm, string? category, int maxResults, CancellationToken ct)
        => _service.QueryNearbyAsync(latitude, longitude, radiusKm, category, maxResults, ct);

    public Task<SpatialQueryResult> QueryBoundingBoxAsync(double minLat, double minLon, double maxLat, double maxLon, string? category, int maxResults, CancellationToken ct)
        => _service.QueryBoundingBoxAsync(minLat, minLon, maxLat, maxLon, category, maxResults, ct);

    public Task<long> CountAsync(CancellationToken ct) => _service.CountAsync(ct);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

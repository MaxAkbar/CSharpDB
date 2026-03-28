using CSharpDB.SpatialIndex;

internal sealed class SpatialIndexApiService : IAsyncDisposable
{
    private readonly string _databasePath;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private SpatialIndexDatabase? _database;

    public SpatialIndexApiService(string databasePath)
    {
        _databasePath = databasePath;
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (_database is not null)
            {
                await _database.DisposeAsync();
                _database = null;
            }

            SpatialIndexDatabaseUtility.DeleteDatabaseFiles(_databasePath);
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<SpatialPoint> AddAsync(
        double latitude, double longitude, string name,
        string? category, string? description, Dictionary<string, string>? tags,
        CancellationToken ct)
    {
        return ExecuteAsync(db => db.AddAsync(latitude, longitude, name, category, description, tags, ct), ct);
    }

    public Task DeleteAsync(long hilbertKey, CancellationToken ct)
    {
        return ExecuteAsync(async db =>
        {
            await db.DeleteAsync(hilbertKey, ct);
            return true;
        }, ct);
    }

    public Task<SpatialPoint?> GetAsync(long hilbertKey, CancellationToken ct)
    {
        return ExecuteAsync(db => db.GetAsync(hilbertKey, ct), ct);
    }

    public Task<SpatialQueryResult> QueryNearbyAsync(
        double latitude, double longitude, double radiusKm,
        string? category, int maxResults, CancellationToken ct)
    {
        return ExecuteAsync(db => db.QueryNearbyAsync(latitude, longitude, radiusKm, category, maxResults, ct), ct);
    }

    public Task<SpatialQueryResult> QueryBoundingBoxAsync(
        double minLat, double minLon, double maxLat, double maxLon,
        string? category, int maxResults, CancellationToken ct)
    {
        return ExecuteAsync(db => db.QueryBoundingBoxAsync(minLat, minLon, maxLat, maxLon, category, maxResults, ct), ct);
    }

    public Task<long> CountAsync(CancellationToken ct)
    {
        return ExecuteAsync(db => db.CountAsync(ct), ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_database is not null)
            {
                await _database.DisposeAsync();
                _database = null;
            }
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async Task<T> ExecuteAsync<T>(Func<SpatialIndexDatabase, Task<T>> operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await _gate.WaitAsync(ct);
        try
        {
            _database ??= await SpatialIndexDatabase.OpenAsync(_databasePath, ct);
            return await operation(_database);
        }
        finally
        {
            _gate.Release();
        }
    }
}

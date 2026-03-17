using CSharpDB.TimeSeries;

internal sealed class TimeSeriesApiService : IAsyncDisposable
{
    private readonly string _databasePath;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private TimeSeriesDatabase? _database;

    public TimeSeriesApiService(string databasePath)
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

            TimeSeriesDatabaseUtility.DeleteDatabaseFiles(_databasePath);
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<TimeSeriesPoint> RecordAsync(
        string metric, double value, string? unit,
        Dictionary<string, string>? tags, DateTime? timestampUtc,
        CancellationToken ct)
    {
        return ExecuteAsync(db => db.RecordAsync(metric, value, unit, tags, timestampUtc, ct), ct);
    }

    public Task DeleteAsync(long timestampTicks, CancellationToken ct)
    {
        return ExecuteAsync(async db =>
        {
            await db.DeleteAsync(timestampTicks, ct);
            return true;
        }, ct);
    }

    public Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct)
    {
        return ExecuteAsync(db => db.GetPointAsync(timestampTicks, ct), ct);
    }

    public Task<TimeSeriesQueryResult> QueryAsync(
        DateTime from, DateTime to, string? metric, int maxResults, CancellationToken ct)
    {
        return ExecuteAsync(db => db.QueryAsync(from, to, metric, maxResults, ct), ct);
    }

    public Task<TimeSeriesPoint?> GetLatestAsync(CancellationToken ct)
    {
        return ExecuteAsync(db => db.GetLatestAsync(ct), ct);
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

    private async Task<T> ExecuteAsync<T>(Func<TimeSeriesDatabase, Task<T>> operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await _gate.WaitAsync(ct);
        try
        {
            _database ??= await TimeSeriesDatabase.OpenAsync(_databasePath, ct);
            return await operation(_database);
        }
        finally
        {
            _gate.Release();
        }
    }
}

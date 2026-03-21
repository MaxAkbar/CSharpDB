using CSharpDB.TimeSeries;

internal sealed class InProcessTimeSeriesApiClient : ITimeSeriesApi, IAsyncDisposable
{
    private readonly TimeSeriesApiService _service;

    public InProcessTimeSeriesApiClient(string databasePath)
    {
        _service = new TimeSeriesApiService(databasePath);
    }

    public Task ResetAsync(CancellationToken ct) => _service.ResetAsync(ct);

    public Task<TimeSeriesPoint> RecordAsync(string metric, double value, string? unit, Dictionary<string, string>? tags, DateTime? timestampUtc, CancellationToken ct)
        => _service.RecordAsync(metric, value, unit, tags, timestampUtc, ct);

    public Task DeleteAsync(long timestampTicks, CancellationToken ct) => _service.DeleteAsync(timestampTicks, ct);

    public Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct) => _service.GetPointAsync(timestampTicks, ct);

    public Task<TimeSeriesQueryResult> QueryAsync(DateTime from, DateTime to, string? metric, int maxResults, CancellationToken ct)
        => _service.QueryAsync(from, to, metric, maxResults, ct);

    public Task<TimeSeriesPoint?> GetLatestAsync(CancellationToken ct) => _service.GetLatestAsync(ct);

    public Task<long> CountAsync(CancellationToken ct) => _service.CountAsync(ct);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

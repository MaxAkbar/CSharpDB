using CSharpDB.TimeSeries;

internal interface ITimeSeriesApi : IAsyncDisposable
{
    Task ResetAsync(CancellationToken ct);
    Task<TimeSeriesPoint> RecordAsync(string metric, double value, string? unit, Dictionary<string, string>? tags, DateTime? timestampUtc, CancellationToken ct);
    Task DeleteAsync(long timestampTicks, CancellationToken ct);
    Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct);
    Task<TimeSeriesQueryResult> QueryAsync(DateTime from, DateTime to, string? metric, int maxResults, CancellationToken ct);
    Task<TimeSeriesPoint?> GetLatestAsync(CancellationToken ct);
    Task<long> CountAsync(CancellationToken ct);
}

internal sealed record RecordPointRequest(string Metric, double Value, string? Unit, Dictionary<string, string>? Tags, DateTime? TimestampUtc);

internal sealed record QueryRequest(DateTime From, DateTime To, string? Metric, int MaxResults = 10_000);

internal sealed record DeletePointRequest(long TimestampTicks);

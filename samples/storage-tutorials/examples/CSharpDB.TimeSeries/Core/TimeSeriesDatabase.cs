using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.TimeSeries;

// ──────────────────────────────────────────────────────────────
//  Time-series database built on a single CSharpDB.Storage B+tree
// ──────────────────────────────────────────────────────────────

/// <summary>
/// A time-series database that uses <c>DateTime.UtcNow.Ticks</c> (long) as the
/// B+tree key and stores sensor readings / metrics as JSON payloads.
///
/// Because ticks increase monotonically, inserts are always append-like and
/// <see cref="CSharpDB.Storage.BTrees.BTreeCursor.SeekAsync"/> provides
/// efficient time-range queries for free.
///
/// Use cases: IoT telemetry, application metrics, stock price history.
/// </summary>
public sealed class TimeSeriesDatabase : IAsyncDisposable
{
    private readonly ITimeSeriesStore _store;

    private TimeSeriesDatabase(ITimeSeriesStore store)
    {
        _store = store;
    }

    // ──────────────────────────────────────────────────────────
    //  Factory – open or create
    // ──────────────────────────────────────────────────────────

    /// <summary>
    /// Open (or create) a time-series database backed by <paramref name="filePath"/>.
    /// </summary>
    public static async Task<TimeSeriesDatabase> OpenAsync(string filePath, CancellationToken ct = default)
    {
        var isNew = !File.Exists(filePath);

        var options = new StorageEngineOptionsBuilder()
            .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
            .UseBTreeIndexes()
            .Build();

        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(filePath, options, ct);

        var store = new TimeSeriesStore(context.Pager);
        var db = new TimeSeriesDatabase(store);

        if (isNew)
        {
            await store.InitializeNewAsync(ct);
        }
        else
        {
            await store.LoadAsync(ct);
        }

        return db;
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – write
    // ──────────────────────────────────────────────────────────

    /// <summary>Record a data point. Timestamp defaults to now if not set.</summary>
    public async Task<TimeSeriesPoint> RecordAsync(
        string metric, double value, string? unit = null,
        Dictionary<string, string>? tags = null,
        DateTime? timestampUtc = null,
        CancellationToken ct = default)
    {
        var ts = timestampUtc ?? DateTime.UtcNow;

        var point = new TimeSeriesPoint
        {
            TimestampTicks = ts.Ticks,
            Metric = metric,
            Value = value,
            Unit = unit,
            Tags = tags,
        };

        await _store.InsertPointAsync(point, ct);
        return point;
    }

    /// <summary>Delete a specific data point by its exact timestamp ticks.</summary>
    public Task DeleteAsync(long timestampTicks, CancellationToken ct = default)
    {
        return _store.DeletePointAsync(timestampTicks, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – read / query
    // ──────────────────────────────────────────────────────────

    /// <summary>Retrieve a single data point by exact timestamp.</summary>
    public Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct = default)
    {
        return _store.GetPointAsync(timestampTicks, ct);
    }

    /// <summary>
    /// Query a time range. This is the key showcase: the BTreeCursor seeks to
    /// <paramref name="from"/> and scans forward to <paramref name="to"/>.
    /// </summary>
    public async Task<TimeSeriesQueryResult> QueryAsync(
        DateTime from, DateTime to,
        string? metric = null,
        int maxResults = 10_000,
        CancellationToken ct = default)
    {
        var points = await _store.QueryRangeAsync(
            from.Ticks, to.Ticks, metric, maxResults, ct);

        return new TimeSeriesQueryResult
        {
            Points = points,
            Aggregation = TimeSeriesAggregation.Compute(points),
        };
    }

    /// <summary>Get the most recently recorded data point.</summary>
    public Task<TimeSeriesPoint?> GetLatestAsync(CancellationToken ct = default)
    {
        return _store.GetLatestPointAsync(ct);
    }

    /// <summary>Count total stored data points.</summary>
    public Task<long> CountAsync(CancellationToken ct = default)
    {
        return _store.CountPointsAsync(ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Dispose
    // ──────────────────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        return _store.DisposeAsync();
    }
}

using System.Buffers.Binary;
using System.Text.Json;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;

namespace CSharpDB.TimeSeries;

// ──────────────────────────────────────────────────────────────
//  Low-level storage layer – owns the B+trees and the Pager
// ──────────────────────────────────────────────────────────────

internal interface ITimeSeriesStore : IAsyncDisposable
{
    Pager Pager { get; }

    Task InitializeNewAsync(CancellationToken ct);
    Task LoadAsync(CancellationToken ct);
    Task InsertPointAsync(TimeSeriesPoint point, CancellationToken ct);
    Task DeletePointAsync(long timestampTicks, CancellationToken ct);
    Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct);
    Task<List<TimeSeriesPoint>> QueryRangeAsync(long startTicks, long endTicks, string? metricFilter, int maxResults, CancellationToken ct);
    Task<TimeSeriesPoint?> GetLatestPointAsync(CancellationToken ct);
    Task<long> CountPointsAsync(CancellationToken ct);
}

internal sealed class TimeSeriesStore(Pager pager) : ITimeSeriesStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // ── B+trees ──────────────────────────────────────────────
    // _data:  key = DateTime.Ticks (long)  →  value = JSON(TimeSeriesPoint)
    //         Natural sort order = chronological.  BTreeCursor.SeekAsync(startTicks)
    //         gives efficient time-range queries for free.
    private BTree _data = null!;

    public Pager Pager { get; } = pager;

    // ── Initialisation ───────────────────────────────────────

    public async Task InitializeNewAsync(CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var dataRoot = await BTree.CreateNewAsync(Pager, ct);
            _data = new BTree(Pager, dataRoot);

            // Persist superblock at key 0 (reserved – no real timestamp uses key 0).
            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task LoadAsync(CancellationToken ct)
    {
        // The data tree lives at page 1 (the first allocated page after the file header).
        // Read the superblock stored at key 0 to recover the root page id.
        _data = new BTree(Pager, 1);
        var superblock = await _data.FindAsync(0, ct);
        if (superblock is null)
        {
            throw new InvalidOperationException("Superblock not found – file is corrupted or not a time-series database.");
        }

        var dataRootPageId = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(0));
        _data = new BTree(Pager, dataRootPageId);
    }

    // ── Write operations ─────────────────────────────────────

    public async Task InsertPointAsync(TimeSeriesPoint point, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(point, JsonOptions);

        await Pager.BeginTransactionAsync(ct);
        try
        {
            // If a point with the exact same tick already exists, overwrite it.
            await _data.DeleteAsync(point.TimestampTicks, ct);
            await _data.InsertAsync(point.TimestampTicks, json, ct);
            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task DeletePointAsync(long timestampTicks, CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var deleted = await _data.DeleteAsync(timestampTicks, ct);
            if (!deleted)
            {
                await Pager.RollbackAsync(ct);
                throw new KeyNotFoundException($"No data point found at ticks {timestampTicks}.");
            }

            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch (KeyNotFoundException)
        {
            throw;
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    // ── Read operations ──────────────────────────────────────

    public async Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct)
    {
        var data = await _data.FindAsync(timestampTicks, ct);
        return data is null ? null : JsonSerializer.Deserialize<TimeSeriesPoint>(data, JsonOptions);
    }

    /// <summary>
    /// Range query using BTreeCursor.SeekAsync – the core showcase of this sample.
    /// Seeks to <paramref name="startTicks"/> and scans forward until <paramref name="endTicks"/>.
    /// </summary>
    public async Task<List<TimeSeriesPoint>> QueryRangeAsync(
        long startTicks, long endTicks, string? metricFilter, int maxResults, CancellationToken ct)
    {
        var results = new List<TimeSeriesPoint>();
        var cursor = _data.CreateCursor();

        if (!await cursor.SeekAsync(startTicks, ct))
        {
            return results;
        }

        do
        {
            // Skip the reserved superblock at key 0.
            if (cursor.CurrentKey == 0)
            {
                continue;
            }

            if (cursor.CurrentKey > endTicks)
            {
                break;
            }

            var point = JsonSerializer.Deserialize<TimeSeriesPoint>(cursor.CurrentValue.Span, JsonOptions);
            if (point is null)
            {
                continue;
            }

            // Optional metric name filter.
            if (metricFilter is not null
                && !point.Metric.Equals(metricFilter, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            results.Add(point);

            if (results.Count >= maxResults)
            {
                break;
            }
        }
        while (await cursor.MoveNextAsync(ct));

        return results;
    }

    /// <summary>
    /// Find the most recent data point by scanning backwards from the max key.
    /// Uses a cursor seeking near long.MaxValue and scanning to the last real key.
    /// </summary>
    public async Task<TimeSeriesPoint?> GetLatestPointAsync(CancellationToken ct)
    {
        // Walk the entire data set backwards – we seek to MaxValue so the cursor
        // lands at the very last leaf.  The last entry in that leaf is our latest point.
        // A more efficient approach would be FindMaxKeyAsync, but for the sample
        // we demonstrate the cursor pattern.
        var cursor = _data.CreateCursor();

        if (!await cursor.SeekAsync(1, ct))
        {
            return null;
        }

        TimeSeriesPoint? latest = null;
        do
        {
            if (cursor.CurrentKey == 0) continue;
            latest = JsonSerializer.Deserialize<TimeSeriesPoint>(cursor.CurrentValue.Span, JsonOptions);
        }
        while (await cursor.MoveNextAsync(ct));

        return latest;
    }

    public async Task<long> CountPointsAsync(CancellationToken ct)
    {
        // Count by traversing – subtract 1 for the reserved superblock entry at key 0.
        var total = await _data.CountEntriesAsync(ct);
        return Math.Max(0, total - 1);
    }

    // ── Superblock persistence ───────────────────────────────

    private async Task PersistSuperblockAsync(CancellationToken ct)
    {
        var buffer = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0), _data.RootPageId);

        await _data.DeleteAsync(0, ct);
        await _data.InsertAsync(0, buffer, ct);
    }

    // ── Dispose ──────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        return Pager.DisposeAsync();
    }
}

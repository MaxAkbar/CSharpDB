using System.Buffers.Binary;
using System.Text.Json;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;

namespace CSharpDB.SpatialIndex;

// ──────────────────────────────────────────────────────────────
//  Low-level storage layer – owns the B+tree and the Pager
// ──────────────────────────────────────────────────────────────

internal interface ISpatialIndexStore : IAsyncDisposable
{
    Pager Pager { get; }

    Task InitializeNewAsync(CancellationToken ct);
    Task LoadAsync(CancellationToken ct);
    Task InsertPointAsync(SpatialPoint point, CancellationToken ct);
    Task DeletePointAsync(long hilbertKey, CancellationToken ct);
    Task<SpatialPoint?> GetPointAsync(long hilbertKey, CancellationToken ct);
    Task<(List<SpatialPoint> Points, int Scanned)> ScanRangeAsync(long startKey, long endKey, string? categoryFilter, int maxResults, CancellationToken ct);
    Task<long> CountPointsAsync(CancellationToken ct);
}

internal sealed class SpatialIndexStore(Pager pager) : ISpatialIndexStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // ── B+tree ───────────────────────────────────────────────
    // _data:  key = HilbertCurve.Encode(lat, lon) (long)  →  value = JSON(SpatialPoint)
    //         Hilbert curve preserves spatial locality, so nearby points have
    //         nearby keys.  BTreeCursor range scans approximate spatial queries.
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
        _data = new BTree(Pager, 1);
        var superblock = await _data.FindAsync(0, ct);
        if (superblock is null)
        {
            throw new InvalidOperationException("Superblock not found – file is corrupted or not a spatial index.");
        }

        var dataRootPageId = BinaryPrimitives.ReadUInt32LittleEndian(superblock.AsSpan(0));
        _data = new BTree(Pager, dataRootPageId);
    }

    // ── Write operations ─────────────────────────────────────

    public async Task InsertPointAsync(SpatialPoint point, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(point, JsonOptions);

        await Pager.BeginTransactionAsync(ct);
        try
        {
            // Upsert: if a point with the same Hilbert key exists, overwrite it.
            await _data.DeleteAsync(point.HilbertKey, ct);
            await _data.InsertAsync(point.HilbertKey, json, ct);
            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task DeletePointAsync(long hilbertKey, CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var deleted = await _data.DeleteAsync(hilbertKey, ct);
            if (!deleted)
            {
                await Pager.RollbackAsync(ct);
                throw new KeyNotFoundException($"No point found at Hilbert key {hilbertKey}.");
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

    public async Task<SpatialPoint?> GetPointAsync(long hilbertKey, CancellationToken ct)
    {
        var data = await _data.FindAsync(hilbertKey, ct);
        return data is null ? null : JsonSerializer.Deserialize<SpatialPoint>(data, JsonOptions);
    }

    /// <summary>
    /// Range scan using BTreeCursor.SeekAsync – the core showcase of this sample.
    /// Seeks to <paramref name="startKey"/> and scans forward to <paramref name="endKey"/>.
    /// Returns both the matching points and the total number of entries scanned
    /// (so the caller can report scan efficiency).
    /// </summary>
    public async Task<(List<SpatialPoint> Points, int Scanned)> ScanRangeAsync(
        long startKey, long endKey, string? categoryFilter, int maxResults, CancellationToken ct)
    {
        var results = new List<SpatialPoint>();
        var scanned = 0;
        var cursor = _data.CreateCursor();

        if (!await cursor.SeekAsync(startKey, ct))
        {
            return (results, scanned);
        }

        do
        {
            // Skip the reserved superblock at key 0.
            if (cursor.CurrentKey == 0)
            {
                continue;
            }

            if (cursor.CurrentKey > endKey)
            {
                break;
            }

            scanned++;

            var point = JsonSerializer.Deserialize<SpatialPoint>(cursor.CurrentValue.Span, JsonOptions);
            if (point is null)
            {
                continue;
            }

            // Optional category filter.
            if (categoryFilter is not null
                && !string.Equals(point.Category, categoryFilter, StringComparison.OrdinalIgnoreCase))
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

        return (results, scanned);
    }

    public async Task<long> CountPointsAsync(CancellationToken ct)
    {
        var total = await _data.CountEntriesAsync(ct);
        return Math.Max(0, total - 1); // subtract 1 for the superblock at key 0
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

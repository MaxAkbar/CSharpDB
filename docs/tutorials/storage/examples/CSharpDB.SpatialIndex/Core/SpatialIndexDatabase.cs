using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.SpatialIndex;

// ──────────────────────────────────────────────────────────────
//  Spatial index database built on a single CSharpDB.Storage B+tree
// ──────────────────────────────────────────────────────────────

/// <summary>
/// A spatial index that uses <see cref="HilbertCurve.Encode"/> to map
/// (latitude, longitude) to a single <c>long</c> B+tree key.
///
/// Because the Hilbert curve preserves spatial locality, nearby geographic
/// points tend to have nearby keys.  This means:
///
///   • <see cref="QueryNearbyAsync"/> scans a key range around the centre
///     point using <c>BTreeCursor.SeekAsync</c>, then post-filters by
///     Haversine distance.
///
///   • <see cref="QueryBoundingBoxAsync"/> computes the Hilbert key range
///     for a bounding box, scans it, and post-filters by actual coordinates.
///
/// Use cases: maps, GIS, fleet tracking, nearest-neighbour search, geofencing.
/// </summary>
public sealed class SpatialIndexDatabase : IAsyncDisposable
{
    private readonly ISpatialIndexStore _store;

    private SpatialIndexDatabase(ISpatialIndexStore store)
    {
        _store = store;
    }

    // ──────────────────────────────────────────────────────────
    //  Factory – open or create
    // ──────────────────────────────────────────────────────────

    public static async Task<SpatialIndexDatabase> OpenAsync(string filePath, CancellationToken ct = default)
    {
        var isNew = !File.Exists(filePath);

        var options = new StorageEngineOptionsBuilder()
            .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
            .UseBTreeIndexes()
            .Build();

        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(filePath, options, ct);

        var store = new SpatialIndexStore(context.Pager);
        var db = new SpatialIndexDatabase(store);

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

    /// <summary>Add a geographic point to the index.</summary>
    public async Task<SpatialPoint> AddAsync(
        double latitude, double longitude, string name,
        string? category = null, string? description = null,
        Dictionary<string, string>? tags = null,
        CancellationToken ct = default)
    {
        var point = new SpatialPoint
        {
            HilbertKey = HilbertCurve.Encode(latitude, longitude),
            Latitude = latitude,
            Longitude = longitude,
            Name = name,
            Category = category,
            Description = description,
            Tags = tags,
        };

        await _store.InsertPointAsync(point, ct);
        return point;
    }

    /// <summary>Delete a point by its Hilbert key.</summary>
    public Task DeleteAsync(long hilbertKey, CancellationToken ct = default)
    {
        return _store.DeletePointAsync(hilbertKey, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – read / query
    // ──────────────────────────────────────────────────────────

    /// <summary>Retrieve a single point by exact Hilbert key.</summary>
    public Task<SpatialPoint?> GetAsync(long hilbertKey, CancellationToken ct = default)
    {
        return _store.GetPointAsync(hilbertKey, ct);
    }

    /// <summary>
    /// Find points near a geographic location.  This is the key showcase:
    /// the Hilbert curve maps the search radius to a B+tree key range,
    /// the cursor scans that range, and results are post-filtered by
    /// Haversine distance.
    /// </summary>
    public async Task<SpatialQueryResult> QueryNearbyAsync(
        double latitude, double longitude, double radiusKm,
        string? category = null, int maxResults = 100,
        CancellationToken ct = default)
    {
        var centerKey = HilbertCurve.Encode(latitude, longitude);
        var delta = HilbertCurve.EstimateRadiusDelta(latitude, radiusKm);

        var startKey = Math.Max(1, centerKey - delta);
        var endKey = centerKey + delta;

        var (rawPoints, scanned) = await _store.ScanRangeAsync(startKey, endKey, category, maxResults * 10, ct);

        // Post-filter by actual Haversine distance and annotate with distance.
        var results = new List<SpatialPointWithDistance>();
        foreach (var point in rawPoints)
        {
            var distance = GeoMath.HaversineDistanceKm(latitude, longitude, point.Latitude, point.Longitude);
            if (distance <= radiusKm)
            {
                results.Add(new SpatialPointWithDistance
                {
                    Point = point,
                    DistanceKm = Math.Round(distance, 2),
                });
            }
        }

        // Sort by distance ascending and limit.
        results.Sort((a, b) => a.DistanceKm.CompareTo(b.DistanceKm));
        if (results.Count > maxResults)
        {
            results.RemoveRange(maxResults, results.Count - maxResults);
        }

        return new SpatialQueryResult
        {
            Points = results,
            Statistics = SpatialQueryStatistics.Compute(results, scanned),
        };
    }

    /// <summary>
    /// Find all points within a geographic bounding box.
    /// Computes the Hilbert key range for the box, scans it,
    /// and post-filters by actual lat/lon bounds.
    /// </summary>
    public async Task<SpatialQueryResult> QueryBoundingBoxAsync(
        double minLat, double minLon, double maxLat, double maxLon,
        string? category = null, int maxResults = 10_000,
        CancellationToken ct = default)
    {
        var (startKey, endKey) = HilbertCurve.BoundingBoxRange(minLat, minLon, maxLat, maxLon);

        var (rawPoints, scanned) = await _store.ScanRangeAsync(startKey, endKey, category, maxResults * 5, ct);

        // Post-filter by actual bounding box.
        var results = new List<SpatialPointWithDistance>();
        foreach (var point in rawPoints)
        {
            if (GeoMath.IsInBoundingBox(point.Latitude, point.Longitude, minLat, minLon, maxLat, maxLon))
            {
                results.Add(new SpatialPointWithDistance
                {
                    Point = point,
                    DistanceKm = 0,
                });
            }
        }

        if (results.Count > maxResults)
        {
            results.RemoveRange(maxResults, results.Count - maxResults);
        }

        var area = GeoMath.BoundingBoxAreaSqKm(minLat, minLon, maxLat, maxLon);

        return new SpatialQueryResult
        {
            Points = results,
            Statistics = SpatialQueryStatistics.Compute(results, scanned, area),
        };
    }

    /// <summary>Count total stored points.</summary>
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

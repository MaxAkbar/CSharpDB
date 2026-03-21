namespace CSharpDB.SpatialIndex;

/// <summary>
/// The result of a spatial query, containing matching points with distances
/// and statistics about the query execution (including B+tree scan efficiency).
/// </summary>
public sealed class SpatialQueryResult
{
    public required IReadOnlyList<SpatialPointWithDistance> Points { get; init; }
    public required SpatialQueryStatistics Statistics { get; init; }
}

/// <summary>
/// A spatial point annotated with its distance from the query centre.
/// </summary>
public sealed class SpatialPointWithDistance
{
    public required SpatialPoint Point { get; init; }

    /// <summary>Distance from the query centre in kilometres (0 for bounding-box queries).</summary>
    public double DistanceKm { get; set; }
}

/// <summary>
/// Statistics about a spatial query's execution.
/// The ratio of <see cref="TotalResults"/> to <see cref="ScannedEntries"/> shows
/// how efficiently the Hilbert curve approximation maps to real spatial proximity.
/// </summary>
public sealed class SpatialQueryStatistics
{
    public int TotalResults { get; set; }

    /// <summary>How many B+tree entries were scanned (before post-filtering).</summary>
    public int ScannedEntries { get; set; }

    public double MinDistanceKm { get; set; }
    public double MaxDistanceKm { get; set; }
    public double BoundingBoxAreaSqKm { get; set; }

    /// <summary>Ratio of results to scanned entries (1.0 = perfect, lower = more over-scan).</summary>
    public double Efficiency => ScannedEntries > 0 ? (double)TotalResults / ScannedEntries : 0;

    public static SpatialQueryStatistics Compute(
        IReadOnlyList<SpatialPointWithDistance> points, int scanned,
        double bboxAreaSqKm = 0)
    {
        var stats = new SpatialQueryStatistics
        {
            TotalResults = points.Count,
            ScannedEntries = scanned,
            BoundingBoxAreaSqKm = bboxAreaSqKm,
        };

        if (points.Count > 0)
        {
            stats.MinDistanceKm = points.Min(p => p.DistanceKm);
            stats.MaxDistanceKm = points.Max(p => p.DistanceKm);
        }

        return stats;
    }
}

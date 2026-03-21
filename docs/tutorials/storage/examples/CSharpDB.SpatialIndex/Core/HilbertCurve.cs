namespace CSharpDB.SpatialIndex;

// ──────────────────────────────────────────────────────────────
//  Hilbert curve: maps 2D (latitude, longitude) → single long key
// ──────────────────────────────────────────────────────────────

/// <summary>
/// Maps geographic coordinates to a single <c>long</c> key via a Hilbert curve.
///
/// The Hilbert curve is a continuous space-filling curve that preserves spatial
/// locality: nearby points in 2D tend to have nearby Hilbert indices.  This means
/// a B+tree keyed by Hilbert index stores geographically close points on nearby
/// leaf pages, and <see cref="CSharpDB.Storage.BTrees.BTreeCursor.SeekAsync"/>
/// range scans approximate spatial proximity queries.
///
/// Resolution: 28 bits per axis → 56-bit Hilbert key.
/// Each grid cell ≈ 0.00067° ≈ 0.07 m at the equator.
/// Key 0 is reserved for the superblock, so all keys are offset by +1.
/// </summary>
public static class HilbertCurve
{
    /// <summary>Bits per axis.  28 bits gives ~268 million cells per axis.</summary>
    public const int Resolution = 28;

    /// <summary>Grid size = 2^Resolution.</summary>
    public const uint GridSize = 1u << Resolution;

    // ── Encode: (lat, lon) → Hilbert key ─────────────────────

    /// <summary>
    /// Encode a geographic coordinate to a Hilbert curve key.
    /// </summary>
    public static long Encode(double latitude, double longitude)
    {
        var x = NormalizeLongitude(longitude);
        var y = NormalizeLatitude(latitude);
        return XYToHilbert(x, y) + 1; // +1 to reserve key 0 for superblock
    }

    // ── Decode: Hilbert key → (lat, lon) ─────────────────────

    /// <summary>
    /// Decode a Hilbert curve key back to geographic coordinates.
    /// </summary>
    public static (double Latitude, double Longitude) Decode(long hilbertKey)
    {
        var (x, y) = HilbertToXY(hilbertKey - 1); // undo the +1 offset
        return (DenormalizeLatitude(y), DenormalizeLongitude(x));
    }

    // ── Range estimation for spatial queries ─────────────────

    /// <summary>
    /// Estimate the Hilbert key delta corresponding to a radius in kilometres.
    /// This is necessarily an over-estimate because the Hilbert curve is not
    /// uniform in geographic space.  The caller must post-filter by actual distance.
    /// </summary>
    public static long EstimateRadiusDelta(double centerLat, double radiusKm)
    {
        // Convert km to approximate degrees (1° latitude ≈ 111.32 km).
        var degDelta = radiusKm / 111.32;

        // At the given latitude, 1° longitude is shorter by cos(lat).
        var cosLat = Math.Cos(centerLat * Math.PI / 180.0);
        var lonDegDelta = cosLat > 0.001 ? degDelta / cosLat : 360.0;

        // Compute Hilbert keys at the four cardinal points at the radius distance.
        var centerKey = Encode(centerLat, 0); // use lon=0 as reference
        var northKey = Encode(Math.Min(90, centerLat + degDelta), 0);
        var southKey = Encode(Math.Max(-90, centerLat - degDelta), 0);
        var eastKey = Encode(centerLat, lonDegDelta);
        var westKey = Encode(centerLat, -lonDegDelta);

        // Take the maximum absolute difference and apply a generous multiplier
        // to account for the curve's non-linear behaviour.
        var maxDelta = Math.Max(
            Math.Max(Math.Abs(northKey - centerKey), Math.Abs(southKey - centerKey)),
            Math.Max(Math.Abs(eastKey - centerKey), Math.Abs(westKey - centerKey)));

        // Multiply by 4 to be conservative – it's better to scan more entries
        // and post-filter than to miss nearby points.
        return Math.Max(maxDelta * 4, 1_000);
    }

    /// <summary>
    /// Compute the minimum and maximum Hilbert keys that could appear within
    /// a geographic bounding box.  The range [MinKey, MaxKey] will over-cover
    /// the box; the caller must post-filter by actual coordinates.
    /// </summary>
    public static (long MinKey, long MaxKey) BoundingBoxRange(
        double minLat, double minLon, double maxLat, double maxLon)
    {
        // Sample the bounding box boundary and interior to find the key extremes.
        long min = long.MaxValue;
        long max = long.MinValue;

        const int samples = 16;
        for (var i = 0; i <= samples; i++)
        {
            var lat = minLat + (maxLat - minLat) * i / samples;
            for (var j = 0; j <= samples; j++)
            {
                var lon = minLon + (maxLon - minLon) * j / samples;
                var key = Encode(lat, lon);
                if (key < min) min = key;
                if (key > max) max = key;
            }
        }

        return (Math.Max(1, min), max);
    }

    // ── Core Hilbert algorithm ───────────────────────────────

    /// <summary>
    /// Convert (x, y) grid coordinates to a Hilbert curve index.
    /// Standard iterative algorithm using quadrant rotation.
    /// </summary>
    private static long XYToHilbert(uint x, uint y)
    {
        long d = 0;
        for (var s = GridSize >> 1; s > 0; s >>= 1)
        {
            var rx = (x & s) > 0 ? 1u : 0u;
            var ry = (y & s) > 0 ? 1u : 0u;
            d += (long)s * s * ((3 * rx) ^ ry);

            // Rotate the quadrant
            if (ry == 0)
            {
                if (rx == 1)
                {
                    x = s - 1 - x;
                    y = s - 1 - y;
                }

                (x, y) = (y, x);
            }
        }

        return d;
    }

    /// <summary>
    /// Convert a Hilbert curve index back to (x, y) grid coordinates.
    /// Inverse of <see cref="XYToHilbert"/>.
    /// </summary>
    private static (uint X, uint Y) HilbertToXY(long d)
    {
        uint x = 0, y = 0;
        for (uint s = 1; s < GridSize; s <<= 1)
        {
            var rx = 1u & (uint)(d / 2);
            var ry = 1u & ((uint)d ^ rx);

            // Inverse rotation
            if (ry == 0)
            {
                if (rx == 1)
                {
                    x = s - 1 - x;
                    y = s - 1 - y;
                }

                (x, y) = (y, x);
            }

            x += s * rx;
            y += s * ry;
            d /= 4;
        }

        return (x, y);
    }

    // ── Coordinate normalisation ─────────────────────────────

    private static uint NormalizeLatitude(double latitude)
    {
        // [-90, 90] → [0, GridSize - 1]
        var clamped = Math.Clamp(latitude, -90.0, 90.0);
        var normalised = (clamped + 90.0) / 180.0;
        return (uint)Math.Min(normalised * GridSize, GridSize - 1);
    }

    private static uint NormalizeLongitude(double longitude)
    {
        // [-180, 180] → [0, GridSize - 1]
        var clamped = Math.Clamp(longitude, -180.0, 180.0);
        var normalised = (clamped + 180.0) / 360.0;
        return (uint)Math.Min(normalised * GridSize, GridSize - 1);
    }

    private static double DenormalizeLatitude(uint y)
    {
        return (double)y / GridSize * 180.0 - 90.0;
    }

    private static double DenormalizeLongitude(uint x)
    {
        return (double)x / GridSize * 360.0 - 180.0;
    }
}

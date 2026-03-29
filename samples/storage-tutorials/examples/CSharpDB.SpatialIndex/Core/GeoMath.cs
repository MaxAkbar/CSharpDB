namespace CSharpDB.SpatialIndex;

/// <summary>
/// Geographic utility functions: distance calculation and bounds checking.
/// </summary>
public static class GeoMath
{
    private const double EarthRadiusKm = 6371.0;

    /// <summary>
    /// Compute the great-circle distance between two points using the Haversine formula.
    /// </summary>
    public static double HaversineDistanceKm(double lat1, double lon1, double lat2, double lon2)
    {
        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);

        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
              + Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2))
              * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return EarthRadiusKm * c;
    }

    /// <summary>
    /// Check whether a point lies within a bounding box.
    /// </summary>
    public static bool IsInBoundingBox(
        double lat, double lon,
        double minLat, double minLon, double maxLat, double maxLon)
    {
        return lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon;
    }

    /// <summary>
    /// Approximate the area of a bounding box in square kilometres.
    /// </summary>
    public static double BoundingBoxAreaSqKm(
        double minLat, double minLon, double maxLat, double maxLon)
    {
        var heightKm = HaversineDistanceKm(minLat, minLon, maxLat, minLon);
        var midLat = (minLat + maxLat) / 2;
        var widthKm = HaversineDistanceKm(midLat, minLon, midLat, maxLon);
        return heightKm * widthKm;
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180.0;
}

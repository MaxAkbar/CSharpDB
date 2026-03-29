namespace CSharpDB.SpatialIndex;

/// <summary>
/// A geographic point stored in the spatial index.
/// The B+tree key is <see cref="HilbertKey"/>, computed from
/// (<see cref="Latitude"/>, <see cref="Longitude"/>) via the Hilbert curve.
/// </summary>
public sealed class SpatialPoint
{
    /// <summary>Hilbert curve key (the B+tree key, computed from lat/lon).</summary>
    public long HilbertKey { get; set; }

    /// <summary>Latitude in degrees [-90, 90].</summary>
    public double Latitude { get; set; }

    /// <summary>Longitude in degrees [-180, 180].</summary>
    public double Longitude { get; set; }

    /// <summary>Human-readable name, e.g. "Eiffel Tower".</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Optional category, e.g. "landmark", "restaurant", "city".</summary>
    public string? Category { get; set; }

    /// <summary>Optional description.</summary>
    public string? Description { get; set; }

    /// <summary>Optional key-value tags for filtering.</summary>
    public Dictionary<string, string>? Tags { get; set; }
}

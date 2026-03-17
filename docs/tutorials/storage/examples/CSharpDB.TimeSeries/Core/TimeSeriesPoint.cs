namespace CSharpDB.TimeSeries;

/// <summary>
/// A single data point in a time series.
/// The key in the B+tree is <see cref="TimestampTicks"/> (DateTime.UtcNow.Ticks),
/// so all points are stored in chronological order and range scans are free.
/// </summary>
public sealed class TimeSeriesPoint
{
    /// <summary>UTC timestamp as ticks (100-nanosecond intervals since 0001-01-01).</summary>
    public long TimestampTicks { get; set; }

    /// <summary>UTC timestamp derived from <see cref="TimestampTicks"/>.</summary>
    public DateTime TimestampUtc
    {
        get => new(TimestampTicks, DateTimeKind.Utc);
        set => TimestampTicks = value.Ticks;
    }

    /// <summary>The name of the metric or sensor, e.g. "cpu_percent" or "temperature_c".</summary>
    public string Metric { get; set; } = string.Empty;

    /// <summary>Numeric value of the data point.</summary>
    public double Value { get; set; }

    /// <summary>Optional unit label (e.g. "%", "C", "ms", "USD").</summary>
    public string? Unit { get; set; }

    /// <summary>Optional key-value tags for grouping / filtering.</summary>
    public Dictionary<string, string>? Tags { get; set; }
}

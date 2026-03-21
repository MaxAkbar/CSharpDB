namespace CSharpDB.TimeSeries;

/// <summary>
/// The result of a time-range query, containing the matching points
/// plus aggregate statistics computed over the range.
/// </summary>
public sealed class TimeSeriesQueryResult
{
    public required IReadOnlyList<TimeSeriesPoint> Points { get; init; }
    public required TimeSeriesAggregation Aggregation { get; init; }
}

/// <summary>
/// Pre-computed aggregates over a set of data points.
/// </summary>
public sealed class TimeSeriesAggregation
{
    public int Count { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public double Sum { get; set; }
    public double Average => Count > 0 ? Sum / Count : 0;
    public double First { get; set; }
    public double Last { get; set; }

    public static TimeSeriesAggregation Compute(IReadOnlyList<TimeSeriesPoint> points)
    {
        if (points.Count == 0)
        {
            return new TimeSeriesAggregation();
        }

        var min = double.MaxValue;
        var max = double.MinValue;
        var sum = 0.0;

        foreach (var point in points)
        {
            if (point.Value < min) min = point.Value;
            if (point.Value > max) max = point.Value;
            sum += point.Value;
        }

        return new TimeSeriesAggregation
        {
            Count = points.Count,
            Min = min,
            Max = max,
            Sum = sum,
            First = points[0].Value,
            Last = points[^1].Value,
        };
    }
}

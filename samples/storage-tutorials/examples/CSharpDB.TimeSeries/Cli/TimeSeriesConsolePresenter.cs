using CSharpDB.TimeSeries;

internal sealed class TimeSeriesConsolePresenter
{
    private readonly AnsiConsoleWriter _console;

    public TimeSeriesConsolePresenter(AnsiConsoleWriter console)
    {
        _console = console;
    }

    public async Task ShowPointAsync(TimeSeriesPoint point)
    {
        await _console.WriteKeyValueAsync("Timestamp", point.TimestampUtc.ToString("yyyy-MM-dd HH:mm:ss.fff UTC"));
        await _console.WriteKeyValueAsync("Ticks", point.TimestampTicks);
        await _console.WriteKeyValueAsync("Metric", point.Metric);
        await _console.WriteKeyValueAsync("Value", FormatValue(point.Value, point.Unit));
        if (point.Tags is { Count: > 0 })
        {
            await _console.WriteKeyValueAsync("Tags", string.Join(", ", point.Tags.Select(kv => $"{kv.Key}={kv.Value}")));
        }
    }

    public async Task ShowQueryResultAsync(TimeSeriesQueryResult result, string? metric)
    {
        var agg = result.Aggregation;
        await _console.WriteInfoAsync($"Found {agg.Count} points{(metric is not null ? $" for '{metric}'" : "")}");
        await _console.WriteBlankLineAsync();

        if (agg.Count > 0)
        {
            await _console.WriteKeyValueAsync("Min", $"{agg.Min:F2}");
            await _console.WriteKeyValueAsync("Max", $"{agg.Max:F2}");
            await _console.WriteKeyValueAsync("Average", $"{agg.Average:F2}");
            await _console.WriteKeyValueAsync("Sum", $"{agg.Sum:F2}");
            await _console.WriteKeyValueAsync("First", $"{agg.First:F2}");
            await _console.WriteKeyValueAsync("Last", $"{agg.Last:F2}");
            await _console.WriteBlankLineAsync();
        }

        if (result.Points.Count == 0)
        {
            return;
        }

        var rows = result.Points
            .Select(p => new[]
            {
                p.TimestampUtc.ToString("yyyy-MM-dd HH:mm:ss.fff"),
                p.Metric,
                FormatValue(p.Value, p.Unit),
            })
            .ToArray();

        await _console.WriteTableAsync(["Timestamp (UTC)", "Metric", "Value"], rows);
    }

    public async Task ShowAsciiChartAsync(IReadOnlyList<TimeSeriesPoint> points, int width = 60, int height = 12)
    {
        if (points.Count < 2)
        {
            await _console.WriteInfoAsync("Not enough data points for a chart (need at least 2).");
            return;
        }

        var values = points.Select(p => p.Value).ToArray();
        var min = values.Min();
        var max = values.Max();
        var range = max - min;
        if (range == 0) range = 1;

        // Build the chart grid
        var chart = new char[height, width];
        for (var y = 0; y < height; y++)
        for (var x = 0; x < width; x++)
            chart[y, x] = ' ';

        // Plot data points
        for (var i = 0; i < width && i < points.Count; i++)
        {
            var sampleIndex = (int)((long)i * (points.Count - 1) / (width - 1));
            var normalized = (values[sampleIndex] - min) / range;
            var y = (int)(normalized * (height - 1));
            y = Math.Clamp(y, 0, height - 1);
            chart[height - 1 - y, i] = '█';

            // Fill below the point for a filled area chart
            for (var fill = height - 1; fill > height - 1 - y; fill--)
            {
                if (chart[fill, i] == ' ')
                    chart[fill, i] = '░';
            }
        }

        // Render
        var labelWidth = Math.Max(max.ToString("F1").Length, min.ToString("F1").Length);
        for (var y = 0; y < height; y++)
        {
            var value = max - (y * range / (height - 1));
            var label = y == 0 ? max.ToString("F1").PadLeft(labelWidth)
                : y == height - 1 ? min.ToString("F1").PadLeft(labelWidth)
                : new string(' ', labelWidth);

            var line = new string(Enumerable.Range(0, width).Select(x => chart[y, x]).ToArray());
            await _console.WriteInfoAsync($"{label} │{line}");
        }

        var axisLine = new string(' ', labelWidth) + " └" + new string('─', width);
        await _console.WriteInfoAsync(axisLine);

        var startLabel = points[0].TimestampUtc.ToString("HH:mm:ss");
        var endLabel = points[^1].TimestampUtc.ToString("HH:mm:ss");
        var padding = width - startLabel.Length - endLabel.Length;
        if (padding < 1) padding = 1;
        await _console.WriteInfoAsync($"{new string(' ', labelWidth + 2)}{startLabel}{new string(' ', padding)}{endLabel}");
    }

    private static string FormatValue(double value, string? unit)
    {
        var formatted = value % 1 == 0 ? value.ToString("F0") : value.ToString("F2");
        return unit is not null ? $"{formatted} {unit}" : formatted;
    }
}

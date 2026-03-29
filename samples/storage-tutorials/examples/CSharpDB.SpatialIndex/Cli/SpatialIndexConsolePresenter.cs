using CSharpDB.SpatialIndex;

internal sealed class SpatialIndexConsolePresenter
{
    private readonly AnsiConsoleWriter _console;

    public SpatialIndexConsolePresenter(AnsiConsoleWriter console) => _console = console;

    public async Task ShowPointAsync(SpatialPoint point)
    {
        await _console.WriteKeyValueAsync("Name", point.Name);
        await _console.WriteKeyValueAsync("Latitude", $"{point.Latitude:F6}");
        await _console.WriteKeyValueAsync("Longitude", $"{point.Longitude:F6}");
        await _console.WriteKeyValueAsync("Hilbert Key", point.HilbertKey);
        if (point.Category is not null) await _console.WriteKeyValueAsync("Category", point.Category);
        if (point.Description is not null) await _console.WriteKeyValueAsync("Description", point.Description);
        if (point.Tags is { Count: > 0 })
            await _console.WriteKeyValueAsync("Tags", string.Join(", ", point.Tags.Select(kv => $"{kv.Key}={kv.Value}")));
    }

    public async Task ShowNearbyResultAsync(SpatialQueryResult result, double centerLat, double centerLon)
    {
        var stats = result.Statistics;
        await _console.WriteInfoAsync($"Found {stats.TotalResults} points within radius (scanned {stats.ScannedEntries} B+tree entries, efficiency {stats.Efficiency:P0})");
        if (stats.TotalResults > 0)
        {
            await _console.WriteKeyValueAsync("Nearest", $"{stats.MinDistanceKm:F2} km");
            await _console.WriteKeyValueAsync("Farthest", $"{stats.MaxDistanceKm:F2} km");
        }

        await _console.WriteBlankLineAsync();

        if (result.Points.Count == 0) return;

        var rows = result.Points.Select(p => new[]
        {
            p.Point.Name,
            p.Point.Category ?? "—",
            $"{p.Point.Latitude:F4}",
            $"{p.Point.Longitude:F4}",
            $"{p.DistanceKm:F2} km",
        }).ToArray();

        await _console.WriteTableAsync(["Name", "Category", "Lat", "Lon", "Distance"], rows);

        await _console.WriteBlankLineAsync();
        await ShowAsciiMapAsync(result.Points, centerLat, centerLon);
    }

    public async Task ShowBboxResultAsync(SpatialQueryResult result)
    {
        var stats = result.Statistics;
        await _console.WriteInfoAsync($"Found {stats.TotalResults} points in bounding box (scanned {stats.ScannedEntries} B+tree entries, efficiency {stats.Efficiency:P0})");
        if (stats.BoundingBoxAreaSqKm > 0)
            await _console.WriteKeyValueAsync("Box Area", $"{stats.BoundingBoxAreaSqKm:N0} km²");
        await _console.WriteBlankLineAsync();

        if (result.Points.Count == 0) return;

        var rows = result.Points.Select(p => new[]
        {
            p.Point.Name,
            p.Point.Category ?? "—",
            $"{p.Point.Latitude:F4}",
            $"{p.Point.Longitude:F4}",
        }).ToArray();

        await _console.WriteTableAsync(["Name", "Category", "Lat", "Lon"], rows);
    }

    public async Task ShowAsciiMapAsync(IReadOnlyList<SpatialPointWithDistance> points,
        double centerLat = double.NaN, double centerLon = double.NaN)
    {
        if (points.Count < 1) return;

        const int width = 60;
        const int height = 18;

        var allLats = points.Select(p => p.Point.Latitude).ToList();
        var allLons = points.Select(p => p.Point.Longitude).ToList();
        var minLat = allLats.Min(); var maxLat = allLats.Max();
        var minLon = allLons.Min(); var maxLon = allLons.Max();

        // Add a small margin
        var latMargin = Math.Max((maxLat - minLat) * 0.1, 0.5);
        var lonMargin = Math.Max((maxLon - minLon) * 0.1, 0.5);
        minLat -= latMargin; maxLat += latMargin;
        minLon -= lonMargin; maxLon += lonMargin;

        var latRange = maxLat - minLat;
        var lonRange = maxLon - minLon;
        if (latRange == 0) latRange = 1;
        if (lonRange == 0) lonRange = 1;

        var grid = new char[height, width];
        for (var y = 0; y < height; y++)
        for (var x = 0; x < width; x++)
            grid[y, x] = '·';

        // Plot centre marker
        if (!double.IsNaN(centerLat) && !double.IsNaN(centerLon))
        {
            var cx = (int)((centerLon - minLon) / lonRange * (width - 1));
            var cy = height - 1 - (int)((centerLat - minLat) / latRange * (height - 1));
            cx = Math.Clamp(cx, 0, width - 1);
            cy = Math.Clamp(cy, 0, height - 1);
            grid[cy, cx] = '+';
        }

        // Plot points
        foreach (var p in points)
        {
            var px = (int)((p.Point.Longitude - minLon) / lonRange * (width - 1));
            var py = height - 1 - (int)((p.Point.Latitude - minLat) / latRange * (height - 1));
            px = Math.Clamp(px, 0, width - 1);
            py = Math.Clamp(py, 0, height - 1);
            grid[py, px] = p.Point.Category?.ToLowerInvariant() switch
            {
                "landmark" => '*',
                "city" => 'o',
                "restaurant" => '#',
                _ => '*',
            };
        }

        // Render
        var labelWidth = 7;
        for (var y = 0; y < height; y++)
        {
            var lat = maxLat - (y * latRange / (height - 1));
            var label = y == 0 ? $"{maxLat:F1}".PadLeft(labelWidth)
                      : y == height - 1 ? $"{minLat:F1}".PadLeft(labelWidth)
                      : new string(' ', labelWidth);
            var line = new string(Enumerable.Range(0, width).Select(x => grid[y, x]).ToArray());
            await _console.WriteInfoAsync($"{label} │{line}");
        }

        await _console.WriteInfoAsync($"{new string(' ', labelWidth)} └{new string('─', width)}");
        var padding = width - $"{minLon:F1}".Length - $"{maxLon:F1}".Length;
        if (padding < 1) padding = 1;
        await _console.WriteInfoAsync($"{new string(' ', labelWidth + 1)}{minLon:F1}{new string(' ', padding)}{maxLon:F1}");
        await _console.WriteInfoAsync("  Legend: * landmark  o city  # restaurant  + centre");
    }
}

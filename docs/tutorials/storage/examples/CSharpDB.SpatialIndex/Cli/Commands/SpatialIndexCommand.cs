using System.Globalization;
using CSharpDB.SpatialIndex;

internal sealed class SpatialIndexCommand : IReplCommand
{
    public string Name => "spatial";
    public string Description => "Add, query, and inspect spatial data.";
    public string Usage => "spatial <add|nearby|bbox|get|delete|count|reset> [...]";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count == 0 || arguments[0].Equals("help", StringComparison.OrdinalIgnoreCase))
        {
            await WriteHelpAsync(context.Console);
            return;
        }

        var action = arguments[0].ToLowerInvariant();
        switch (action)
        {
            case "add": await AddAsync(context, arguments, ct); break;
            case "nearby": await NearbyAsync(context, arguments, ct); break;
            case "bbox": await BboxAsync(context, arguments, ct); break;
            case "get": await GetAsync(context, arguments, ct); break;
            case "delete": await DeleteAsync(context, arguments, ct); break;
            case "count":
                var count = await context.Client.CountAsync(ct);
                await context.Console.WriteInfoAsync($"Total stored points: {count:N0}");
                break;
            case "reset":
                await context.Client.ResetAsync(ct);
                await context.Console.WriteSuccessAsync("Database reset.");
                break;
            default:
                await context.Console.WriteErrorAsync($"Unknown action '{arguments[0]}'.");
                await WriteHelpAsync(context.Console);
                break;
        }
    }

    // ── add <lat> <lon> <name> [category:<cat>] [tag:k=v] ───
    private static async Task AddAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 4);
        var lat = double.Parse(arguments[1], CultureInfo.InvariantCulture);
        var lon = double.Parse(arguments[2], CultureInfo.InvariantCulture);
        var name = arguments[3];
        string? category = null;
        string? description = null;
        Dictionary<string, string>? tags = null;

        for (var i = 4; i < arguments.Count; i++)
        {
            var arg = arguments[i];
            if (arg.StartsWith("category:", StringComparison.OrdinalIgnoreCase))
                category = arg[9..];
            else if (arg.StartsWith("desc:", StringComparison.OrdinalIgnoreCase))
                description = arg[5..];
            else if (arg.StartsWith("tag:", StringComparison.OrdinalIgnoreCase))
            {
                var kv = arg[4..].Split('=', 2);
                if (kv.Length == 2) { tags ??= new(); tags[kv[0]] = kv[1]; }
            }
        }

        var point = await context.Client.AddAsync(lat, lon, name, category, description, tags, ct);
        var presenter = new SpatialIndexConsolePresenter(context.Console);
        await context.Console.WriteSuccessAsync("Point added:");
        await presenter.ShowPointAsync(point);
    }

    // ── nearby <lat> <lon> <radiusKm> [category:<cat>] [max:N] ──
    private static async Task NearbyAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 4);
        var lat = double.Parse(arguments[1], CultureInfo.InvariantCulture);
        var lon = double.Parse(arguments[2], CultureInfo.InvariantCulture);
        var radius = double.Parse(arguments[3], CultureInfo.InvariantCulture);
        string? category = null;
        var maxResults = 100;

        for (var i = 4; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("category:", StringComparison.OrdinalIgnoreCase))
                category = arguments[i][9..];
            else if (arguments[i].StartsWith("max:", StringComparison.OrdinalIgnoreCase))
                maxResults = int.Parse(arguments[i][4..], CultureInfo.InvariantCulture);
        }

        var result = await context.Client.QueryNearbyAsync(lat, lon, radius, category, maxResults, ct);
        var presenter = new SpatialIndexConsolePresenter(context.Console);
        await presenter.ShowNearbyResultAsync(result, lat, lon);
    }

    // ── bbox <minLat> <minLon> <maxLat> <maxLon> [category:<cat>] ──
    private static async Task BboxAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 5);
        var minLat = double.Parse(arguments[1], CultureInfo.InvariantCulture);
        var minLon = double.Parse(arguments[2], CultureInfo.InvariantCulture);
        var maxLat = double.Parse(arguments[3], CultureInfo.InvariantCulture);
        var maxLon = double.Parse(arguments[4], CultureInfo.InvariantCulture);
        string? category = null;
        var maxResults = 10_000;

        for (var i = 5; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("category:", StringComparison.OrdinalIgnoreCase))
                category = arguments[i][9..];
            else if (arguments[i].StartsWith("max:", StringComparison.OrdinalIgnoreCase))
                maxResults = int.Parse(arguments[i][4..], CultureInfo.InvariantCulture);
        }

        var result = await context.Client.QueryBoundingBoxAsync(minLat, minLon, maxLat, maxLon, category, maxResults, ct);
        var presenter = new SpatialIndexConsolePresenter(context.Console);
        await presenter.ShowBboxResultAsync(result);
    }

    // ── get <hilbertKey> ─────────────────────────────────────
    private static async Task GetAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var key = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var point = await context.Client.GetAsync(key, ct);
        if (point is null) { await context.Console.WriteInfoAsync("No point found."); return; }
        var presenter = new SpatialIndexConsolePresenter(context.Console);
        await presenter.ShowPointAsync(point);
    }

    // ── delete <hilbertKey> ──────────────────────────────────
    private static async Task DeleteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var key = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        await context.Client.DeleteAsync(key, ct);
        await context.Console.WriteSuccessAsync($"Deleted point at Hilbert key {key}.");
    }

    private static void Require(IReadOnlyList<string> args, int min)
    {
        if (args.Count < min) throw new InvalidOperationException("Not enough arguments.");
    }

    private static async Task WriteHelpAsync(AnsiConsoleWriter console)
    {
        await console.WriteSectionAsync("spatial command");
        await console.WriteTableAsync(
            ["Action", "Description", "Example"],
            [
                ["add", "Add a geographic point.", "add 48.8584 2.2945 \"Eiffel Tower\" category:landmark"],
                ["nearby", "Find nearby points (radius).", "nearby 48.8566 2.3522 50 category:landmark"],
                ["bbox", "Bounding box query.", "bbox 35 -10 70 40"],
                ["get", "Get a point by Hilbert key.", "get 123456789"],
                ["delete", "Delete a point by key.", "delete 123456789"],
                ["count", "Count all stored points.", "count"],
                ["reset", "Delete the database.", "reset"],
            ]);
    }
}

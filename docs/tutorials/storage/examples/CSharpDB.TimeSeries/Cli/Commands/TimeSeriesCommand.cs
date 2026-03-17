using System.Globalization;
using CSharpDB.TimeSeries;

internal sealed class TimeSeriesCommand : IReplCommand
{
    public string Name => "timeseries";

    public string Description => "Record, query, and inspect time-series data.";

    public string Usage => "timeseries <record|query|get|delete|latest|count|chart|reset> [...]";

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
            case "record":
                await RecordAsync(context, arguments, ct);
                break;

            case "query":
                await QueryAsync(context, arguments, ct);
                break;

            case "get":
                await GetAsync(context, arguments, ct);
                break;

            case "delete":
                await DeleteAsync(context, arguments, ct);
                break;

            case "latest":
                await LatestAsync(context, ct);
                break;

            case "count":
                {
                    var count = await context.Client.CountAsync(ct);
                    await context.Console.WriteInfoAsync($"Total data points: {count:N0}");
                }
                break;

            case "chart":
                await ChartAsync(context, arguments, ct);
                break;

            case "reset":
                RequireArgumentCount(arguments, 1);
                await context.Client.ResetAsync(ct);
                await context.Console.WriteSuccessAsync("Database reset.");
                break;

            default:
                await context.Console.WriteErrorAsync($"Unknown action '{arguments[0]}'.");
                await WriteHelpAsync(context.Console);
                break;
        }
    }

    // ── record <metric> <value> [unit] [tag:key=value ...] ──
    private static async Task RecordAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        RequireArgumentCount(arguments, 3);

        var metric = arguments[1];
        if (!double.TryParse(arguments[2], CultureInfo.InvariantCulture, out var value))
        {
            throw new InvalidOperationException($"'{arguments[2]}' is not a valid number.");
        }

        string? unit = null;
        Dictionary<string, string>? tags = null;

        for (var i = 3; i < arguments.Count; i++)
        {
            var arg = arguments[i];
            if (arg.StartsWith("tag:", StringComparison.OrdinalIgnoreCase))
            {
                var kv = arg[4..].Split('=', 2);
                if (kv.Length == 2)
                {
                    tags ??= new Dictionary<string, string>();
                    tags[kv[0]] = kv[1];
                }
            }
            else
            {
                unit = arg;
            }
        }

        var point = await context.Client.RecordAsync(metric, value, unit, tags, null, ct);
        var presenter = new TimeSeriesConsolePresenter(context.Console);
        await context.Console.WriteSuccessAsync("Data point recorded:");
        await presenter.ShowPointAsync(point);
    }

    // ── query <from> <to> [metric] [max:N] ──────────────────
    private static async Task QueryAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        RequireArgumentCount(arguments, 3);

        var from = ParseDateTime(arguments[1]);
        var to = ParseDateTime(arguments[2]);
        string? metric = null;
        var maxResults = 10_000;

        for (var i = 3; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("max:", StringComparison.OrdinalIgnoreCase))
            {
                maxResults = int.Parse(arguments[i][4..], CultureInfo.InvariantCulture);
            }
            else
            {
                metric = arguments[i];
            }
        }

        var result = await context.Client.QueryAsync(from, to, metric, maxResults, ct);
        var presenter = new TimeSeriesConsolePresenter(context.Console);
        await presenter.ShowQueryResultAsync(result, metric);
    }

    // ── get <ticks> ──────────────────────────────────────────
    private static async Task GetAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        RequireArgumentCount(arguments, 2);
        var ticks = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var point = await context.Client.GetPointAsync(ticks, ct);
        if (point is null)
        {
            await context.Console.WriteInfoAsync("No data point found at that timestamp.");
            return;
        }

        var presenter = new TimeSeriesConsolePresenter(context.Console);
        await presenter.ShowPointAsync(point);
    }

    // ── delete <ticks> ───────────────────────────────────────
    private static async Task DeleteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        RequireArgumentCount(arguments, 2);
        var ticks = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        await context.Client.DeleteAsync(ticks, ct);
        await context.Console.WriteSuccessAsync($"Deleted data point at ticks {ticks}.");
    }

    // ── latest ───────────────────────────────────────────────
    private static async Task LatestAsync(ReplCommandContext context, CancellationToken ct)
    {
        var point = await context.Client.GetLatestAsync(ct);
        if (point is null)
        {
            await context.Console.WriteInfoAsync("No data points recorded yet.");
            return;
        }

        var presenter = new TimeSeriesConsolePresenter(context.Console);
        await context.Console.WriteSuccessAsync("Latest data point:");
        await presenter.ShowPointAsync(point);
    }

    // ── chart <from> <to> [metric] ──────────────────────────
    private static async Task ChartAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        RequireArgumentCount(arguments, 3);

        var from = ParseDateTime(arguments[1]);
        var to = ParseDateTime(arguments[2]);
        var metric = arguments.Count > 3 ? arguments[3] : null;

        var result = await context.Client.QueryAsync(from, to, metric, 10_000, ct);
        var presenter = new TimeSeriesConsolePresenter(context.Console);
        await presenter.ShowAsciiChartAsync(result.Points);
    }

    // ── Helpers ──────────────────────────────────────────────

    private static DateTime ParseDateTime(string input)
    {
        // Support common shortcuts
        if (input.Equals("now", StringComparison.OrdinalIgnoreCase))
            return DateTime.UtcNow;

        if (input.Equals("today", StringComparison.OrdinalIgnoreCase))
            return DateTime.UtcNow.Date;

        if (input.Equals("yesterday", StringComparison.OrdinalIgnoreCase))
            return DateTime.UtcNow.Date.AddDays(-1);

        // Relative: -1h, -30m, -7d
        if (input.StartsWith('-') && input.Length >= 2)
        {
            var unit = input[^1];
            if (double.TryParse(input[1..^1], CultureInfo.InvariantCulture, out var amount))
            {
                return unit switch
                {
                    's' => DateTime.UtcNow.AddSeconds(-amount),
                    'm' => DateTime.UtcNow.AddMinutes(-amount),
                    'h' => DateTime.UtcNow.AddHours(-amount),
                    'd' => DateTime.UtcNow.AddDays(-amount),
                    _ => throw new InvalidOperationException($"Unknown time unit '{unit}'. Use s, m, h, or d."),
                };
            }
        }

        return DateTime.Parse(input, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
    }

    private static void RequireArgumentCount(IReadOnlyList<string> arguments, int minimumCount)
    {
        if (arguments.Count < minimumCount)
        {
            throw new InvalidOperationException("Not enough arguments.");
        }
    }

    private static async Task WriteHelpAsync(AnsiConsoleWriter console)
    {
        await console.WriteSectionAsync("timeseries command");
        await console.WriteTableAsync(
            ["Action", "Description", "Example"],
            [
                ["record", "Record a data point.", "record cpu_percent 73.5 % tag:host=web01"],
                ["query", "Query a time range.", "query -1h now cpu_percent"],
                ["get", "Get a single point by ticks.", "get 638700000000000000"],
                ["delete", "Delete a point by ticks.", "delete 638700000000000000"],
                ["latest", "Show the most recent point.", "latest"],
                ["count", "Count all stored data points.", "count"],
                ["chart", "Show an ASCII chart.", "chart -1h now temperature_c"],
                ["reset", "Delete the database.", "reset"],
            ]);
        await console.WriteBlankLineAsync();
        await console.WriteSectionAsync("Time format");
        await console.WriteInfoAsync("Absolute: 2024-01-15T10:30:00Z, 2024-01-15");
        await console.WriteInfoAsync("Relative: -1h, -30m, -7d, -60s");
        await console.WriteInfoAsync("Keywords: now, today, yesterday");
    }
}

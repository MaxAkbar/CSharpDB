class Program
{
    static async Task Main(string[] args)
    {
        const string databasePath = "timeseries.cdb";

        if (args.Length > 0 && args[0].Equals("serve", StringComparison.OrdinalIgnoreCase))
        {
            await TimeSeriesWebHost.RunAsync(args.Skip(1).ToArray(), databasePath);
            return;
        }

        var console = new AnsiConsoleWriter();
        await using var client = CreateClient(args, databasePath);
        var context = new ReplCommandContext(console, databasePath, client);
        var timeSeriesCommand = new TimeSeriesCommand();
        var commands = new IReplCommand[]
        {
            new SampleCommand(),
            new PrefixedReplCommand("record", "Record a data point.", "record <metric> <value> [unit] [tag:key=value]", timeSeriesCommand, "record"),
            new PrefixedReplCommand("query", "Query a time range.", "query <from> <to> [metric] [max:N]", timeSeriesCommand, "query"),
            new PrefixedReplCommand("get", "Get a point by ticks.", "get <ticks>", timeSeriesCommand, "get"),
            new PrefixedReplCommand("delete", "Delete a point by ticks.", "delete <ticks>", timeSeriesCommand, "delete"),
            new PrefixedReplCommand("latest", "Show the most recent point.", "latest", timeSeriesCommand, "latest"),
            new PrefixedReplCommand("count", "Count all stored data points.", "count", timeSeriesCommand, "count"),
            new PrefixedReplCommand("chart", "Show an ASCII chart.", "chart <from> <to> [metric]", timeSeriesCommand, "chart"),
            new PrefixedReplCommand("reset", "Delete the database files.", "reset", timeSeriesCommand, "reset"),
        };

        var host = new ReplHost(commands, console);
        await host.RunAsync(context, CancellationToken.None);
    }

    private static ITimeSeriesApi CreateClient(string[] args, string databasePath)
    {
        var apiIndex = Array.FindIndex(args, arg => arg.Equals("--api", StringComparison.OrdinalIgnoreCase));
        if (apiIndex >= 0)
        {
            if (apiIndex == args.Length - 1)
            {
                throw new InvalidOperationException("Expected a base URL after --api.");
            }

            return new HttpTimeSeriesApiClient(args[apiIndex + 1]);
        }

        return new InProcessTimeSeriesApiClient(databasePath);
    }
}

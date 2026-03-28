class Program
{
    static async Task Main(string[] args)
    {
        const string databasePath = "spatialindex.cdb";

        if (args.Length > 0 && args[0].Equals("serve", StringComparison.OrdinalIgnoreCase))
        {
            await SpatialIndexWebHost.RunAsync(args.Skip(1).ToArray(), databasePath);
            return;
        }

        var console = new AnsiConsoleWriter();
        await using var client = CreateClient(args, databasePath);
        var context = new ReplCommandContext(console, databasePath, client);
        var spatialCommand = new SpatialIndexCommand();
        var commands = new IReplCommand[]
        {
            new SampleCommand(),
            new PrefixedReplCommand("add", "Add a geographic point.", "add <lat> <lon> <name> [category:<cat>]", spatialCommand, "add"),
            new PrefixedReplCommand("nearby", "Find nearby points.", "nearby <lat> <lon> <radiusKm> [category:<cat>]", spatialCommand, "nearby"),
            new PrefixedReplCommand("bbox", "Query bounding box.", "bbox <minLat> <minLon> <maxLat> <maxLon>", spatialCommand, "bbox"),
            new PrefixedReplCommand("get", "Get a point by Hilbert key.", "get <hilbertKey>", spatialCommand, "get"),
            new PrefixedReplCommand("delete", "Delete a point.", "delete <hilbertKey>", spatialCommand, "delete"),
            new PrefixedReplCommand("count", "Count stored points.", "count", spatialCommand, "count"),
            new PrefixedReplCommand("reset", "Delete database.", "reset", spatialCommand, "reset"),
        };

        var host = new ReplHost(commands, console);
        await host.RunAsync(context, CancellationToken.None);
    }

    private static ISpatialIndexApi CreateClient(string[] args, string databasePath)
    {
        var apiIndex = Array.FindIndex(args, arg => arg.Equals("--api", StringComparison.OrdinalIgnoreCase));
        if (apiIndex >= 0)
        {
            if (apiIndex == args.Length - 1)
                throw new InvalidOperationException("Expected a base URL after --api.");
            return new HttpSpatialIndexApiClient(args[apiIndex + 1]);
        }

        return new InProcessSpatialIndexApiClient(databasePath);
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        const string databasePath = "graphdb.cdb";

        if (args.Length > 0 && args[0].Equals("serve", StringComparison.OrdinalIgnoreCase))
        {
            await GraphWebHost.RunAsync(args.Skip(1).ToArray(), databasePath);
            return;
        }

        var console = new AnsiConsoleWriter();
        await using var client = CreateClient(args, databasePath);
        var context = new ReplCommandContext(console, databasePath, client);
        var graphCommand = new GraphCommand();
        var commands = new IReplCommand[]
        {
            new SampleCommand(),
            new PrefixedReplCommand("add-node",    "Add a node.",                  "add-node <label> [type:<type>]",         graphCommand, "add-node"),
            new PrefixedReplCommand("add-edge",    "Add a directed edge.",         "add-edge <src> <tgt> <label> [weight:N]",graphCommand, "add-edge"),
            new PrefixedReplCommand("get-node",    "Lookup a node by ID.",         "get-node <nodeId>",                      graphCommand, "get-node"),
            new PrefixedReplCommand("get-edge",    "Lookup an edge.",              "get-edge <srcId> <tgtId>",               graphCommand, "get-edge"),
            new PrefixedReplCommand("out",         "List outgoing edges.",         "out <nodeId> [label:<filter>]",          graphCommand, "out"),
            new PrefixedReplCommand("in",          "List incoming edges.",         "in <nodeId> [label:<filter>]",           graphCommand, "in"),
            new PrefixedReplCommand("bfs",         "BFS traversal.",              "bfs <startId> [depth:N] [dir:out|in|both]", graphCommand, "bfs"),
            new PrefixedReplCommand("path",        "Shortest path (BFS).",        "path <srcId> <tgtId> [depth:N]",         graphCommand, "path"),
            new PrefixedReplCommand("nodes",       "List all nodes.",             "nodes",                                   graphCommand, "nodes"),
            new PrefixedReplCommand("count",       "Count nodes and edges.",      "count",                                   graphCommand, "count"),
            new PrefixedReplCommand("delete-node", "Delete a node + edges.",      "delete-node <nodeId>",                   graphCommand, "delete-node"),
            new PrefixedReplCommand("delete-edge", "Delete an edge.",             "delete-edge <srcId> <tgtId>",            graphCommand, "delete-edge"),
            new PrefixedReplCommand("reset",       "Delete the database.",        "reset",                                   graphCommand, "reset"),
        };

        var host = new ReplHost(commands, console);
        await host.RunAsync(context, CancellationToken.None);
    }

    private static IGraphApi CreateClient(string[] args, string databasePath)
    {
        var apiIndex = Array.FindIndex(args, arg => arg.Equals("--api", StringComparison.OrdinalIgnoreCase));
        if (apiIndex >= 0)
        {
            if (apiIndex == args.Length - 1)
                throw new InvalidOperationException("Expected a base URL after --api.");
            return new HttpGraphApiClient(args[apiIndex + 1]);
        }

        return new InProcessGraphApiClient(databasePath);
    }
}

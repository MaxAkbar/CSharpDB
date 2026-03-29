using System.Globalization;
using CSharpDB.GraphDB;

internal sealed class GraphCommand : IReplCommand
{
    public string Name => "graph";
    public string Description => "Manage nodes, edges, and run traversals.";
    public string Usage => "graph <add-node|add-edge|get-node|get-edge|out|in|bfs|path|delete-node|delete-edge|count|reset> [...]";

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
            case "add-node": await AddNodeAsync(context, arguments, ct); break;
            case "add-edge": await AddEdgeAsync(context, arguments, ct); break;
            case "get-node": await GetNodeAsync(context, arguments, ct); break;
            case "get-edge": await GetEdgeAsync(context, arguments, ct); break;
            case "out": await OutgoingEdgesAsync(context, arguments, ct); break;
            case "in": await IncomingEdgesAsync(context, arguments, ct); break;
            case "bfs": await BfsAsync(context, arguments, ct); break;
            case "path": await ShortestPathAsync(context, arguments, ct); break;
            case "nodes": await ListNodesAsync(context, ct); break;
            case "delete-node": await DeleteNodeAsync(context, arguments, ct); break;
            case "delete-edge": await DeleteEdgeAsync(context, arguments, ct); break;
            case "count":
                var nodeCount = await context.Client.CountNodesAsync(ct);
                var edgeCount = await context.Client.CountEdgesAsync(ct);
                await context.Console.WriteInfoAsync($"Nodes: {nodeCount:N0}   Edges: {edgeCount:N0}");
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

    // ── add-node <label> [type:<type>] [prop:k=v] ─────────────
    private static async Task AddNodeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var label = arguments[1];
        string? type = null;
        Dictionary<string, string>? props = null;

        for (var i = 2; i < arguments.Count; i++)
        {
            var arg = arguments[i];
            if (arg.StartsWith("type:", StringComparison.OrdinalIgnoreCase))
                type = arg[5..];
            else if (arg.StartsWith("prop:", StringComparison.OrdinalIgnoreCase))
            {
                var kv = arg[5..].Split('=', 2);
                if (kv.Length == 2) { props ??= new(); props[kv[0]] = kv[1]; }
            }
        }

        var node = await context.Client.AddNodeAsync(label, type, props, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await context.Console.WriteSuccessAsync("Node added:");
        await presenter.ShowNodeAsync(node);
    }

    // ── add-edge <sourceId> <targetId> <label> [weight:N] ─────
    private static async Task AddEdgeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 4);
        var sourceId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var targetId = long.Parse(arguments[2], CultureInfo.InvariantCulture);
        var label = arguments[3];
        double weight = 0;
        Dictionary<string, string>? props = null;

        for (var i = 4; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("weight:", StringComparison.OrdinalIgnoreCase))
                weight = double.Parse(arguments[i][7..], CultureInfo.InvariantCulture);
            else if (arguments[i].StartsWith("prop:", StringComparison.OrdinalIgnoreCase))
            {
                var kv = arguments[i][5..].Split('=', 2);
                if (kv.Length == 2) { props ??= new(); props[kv[0]] = kv[1]; }
            }
        }

        var edge = await context.Client.AddEdgeAsync(sourceId, targetId, label, weight, props, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await context.Console.WriteSuccessAsync("Edge added:");
        await presenter.ShowEdgeAsync(edge);
    }

    // ── get-node <nodeId> ─────────────────────────────────────
    private static async Task GetNodeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var nodeId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var node = await context.Client.GetNodeAsync(nodeId, ct);
        if (node is null) { await context.Console.WriteInfoAsync("No node found."); return; }
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowNodeAsync(node);
    }

    // ── get-edge <sourceId> <targetId> ────────────────────────
    private static async Task GetEdgeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 3);
        var sourceId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var targetId = long.Parse(arguments[2], CultureInfo.InvariantCulture);
        var edge = await context.Client.GetEdgeAsync(sourceId, targetId, ct);
        if (edge is null) { await context.Console.WriteInfoAsync("No edge found."); return; }
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowEdgeAsync(edge);
    }

    // ── out <nodeId> [label:<filter>] ─────────────────────────
    private static async Task OutgoingEdgesAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var nodeId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        string? labelFilter = null;
        for (var i = 2; i < arguments.Count; i++)
            if (arguments[i].StartsWith("label:", StringComparison.OrdinalIgnoreCase))
                labelFilter = arguments[i][6..];

        var (edges, scanned) = await context.Client.GetOutgoingEdgesAsync(nodeId, labelFilter, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowEdgeListAsync($"Outgoing edges from node {nodeId}", edges, scanned);
    }

    // ── in <nodeId> [label:<filter>] ──────────────────────────
    private static async Task IncomingEdgesAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var nodeId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        string? labelFilter = null;
        for (var i = 2; i < arguments.Count; i++)
            if (arguments[i].StartsWith("label:", StringComparison.OrdinalIgnoreCase))
                labelFilter = arguments[i][6..];

        var (edges, scanned) = await context.Client.GetIncomingEdgesAsync(nodeId, labelFilter, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowEdgeListAsync($"Incoming edges to node {nodeId}", edges, scanned);
    }

    // ── bfs <startNodeId> [depth:N] [label:<filter>] [dir:out|in|both] ──
    private static async Task BfsAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var startNodeId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var maxDepth = 3;
        string? labelFilter = null;
        var direction = "outgoing";

        for (var i = 2; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("depth:", StringComparison.OrdinalIgnoreCase))
                maxDepth = int.Parse(arguments[i][6..], CultureInfo.InvariantCulture);
            else if (arguments[i].StartsWith("label:", StringComparison.OrdinalIgnoreCase))
                labelFilter = arguments[i][6..];
            else if (arguments[i].StartsWith("dir:", StringComparison.OrdinalIgnoreCase))
                direction = arguments[i][4..];
        }

        var result = await context.Client.TraverseBfsAsync(startNodeId, maxDepth, labelFilter, direction, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowTraversalResultAsync("BFS Traversal", result);
    }

    // ── path <sourceId> <targetId> [depth:N] [label:<filter>] ──
    private static async Task ShortestPathAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 3);
        var sourceId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var targetId = long.Parse(arguments[2], CultureInfo.InvariantCulture);
        var maxDepth = 10;
        string? labelFilter = null;

        for (var i = 3; i < arguments.Count; i++)
        {
            if (arguments[i].StartsWith("depth:", StringComparison.OrdinalIgnoreCase))
                maxDepth = int.Parse(arguments[i][6..], CultureInfo.InvariantCulture);
            else if (arguments[i].StartsWith("label:", StringComparison.OrdinalIgnoreCase))
                labelFilter = arguments[i][6..];
        }

        var result = await context.Client.ShortestPathAsync(sourceId, targetId, maxDepth, labelFilter, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowTraversalResultAsync("Shortest Path", result);
    }

    // ── nodes ─────────────────────────────────────────────────
    private static async Task ListNodesAsync(ReplCommandContext context, CancellationToken ct)
    {
        var nodes = await context.Client.GetAllNodesAsync(200, ct);
        var presenter = new GraphConsolePresenter(context.Console);
        await presenter.ShowNodeListAsync(nodes);
    }

    // ── delete-node <nodeId> ──────────────────────────────────
    private static async Task DeleteNodeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 2);
        var nodeId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        await context.Client.DeleteNodeAsync(nodeId, ct);
        await context.Console.WriteSuccessAsync($"Deleted node {nodeId} and all its edges.");
    }

    // ── delete-edge <sourceId> <targetId> ─────────────────────
    private static async Task DeleteEdgeAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        Require(arguments, 3);
        var sourceId = long.Parse(arguments[1], CultureInfo.InvariantCulture);
        var targetId = long.Parse(arguments[2], CultureInfo.InvariantCulture);
        await context.Client.DeleteEdgeAsync(sourceId, targetId, ct);
        await context.Console.WriteSuccessAsync($"Deleted edge {sourceId} → {targetId}.");
    }

    private static void Require(IReadOnlyList<string> args, int min)
    {
        if (args.Count < min) throw new InvalidOperationException("Not enough arguments.");
    }

    private static async Task WriteHelpAsync(AnsiConsoleWriter console)
    {
        await console.WriteSectionAsync("graph commands");
        await console.WriteTableAsync(
            ["Action", "Description", "Example"],
            [
                ["add-node", "Add a node.", "add-node \"Alice\" type:person"],
                ["add-edge", "Add a directed edge.", "add-edge 1 2 KNOWS weight:0.9"],
                ["get-node", "Lookup a node by ID.", "get-node 1"],
                ["get-edge", "Lookup an edge.", "get-edge 1 2"],
                ["out", "List outgoing edges.", "out 1 label:KNOWS"],
                ["in", "List incoming edges.", "in 2"],
                ["bfs", "BFS traversal.", "bfs 1 depth:3 dir:both"],
                ["path", "Shortest path (BFS).", "path 1 5"],
                ["nodes", "List all nodes.", "nodes"],
                ["delete-node", "Delete a node + edges.", "delete-node 3"],
                ["delete-edge", "Delete an edge.", "delete-edge 1 2"],
                ["count", "Count nodes and edges.", "count"],
                ["reset", "Delete the database.", "reset"],
            ]);
    }
}

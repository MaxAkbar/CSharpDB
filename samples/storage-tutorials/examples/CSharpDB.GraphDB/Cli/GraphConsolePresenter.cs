using CSharpDB.GraphDB;

internal sealed class GraphConsolePresenter(AnsiConsoleWriter console)
{
    public async Task ShowNodeAsync(GraphNode node)
    {
        await console.WriteKeyValueAsync("ID", node.Id);
        await console.WriteKeyValueAsync("Label", node.Label);
        await console.WriteKeyValueAsync("Type", node.Type ?? "—");
        if (node.Properties is { Count: > 0 })
        {
            foreach (var (k, v) in node.Properties)
                await console.WriteKeyValueAsync($"  {k}", v);
        }
    }

    public async Task ShowEdgeAsync(GraphEdge edge)
    {
        await console.WriteKeyValueAsync("Source", edge.SourceId);
        await console.WriteKeyValueAsync("Target", edge.TargetId);
        await console.WriteKeyValueAsync("Label", edge.Label);
        await console.WriteKeyValueAsync("Weight", edge.Weight);
        if (edge.Properties is { Count: > 0 })
        {
            foreach (var (k, v) in edge.Properties)
                await console.WriteKeyValueAsync($"  {k}", v);
        }
    }

    public async Task ShowNodeListAsync(List<GraphNode> nodes)
    {
        if (nodes.Count == 0) { await console.WriteInfoAsync("No nodes found."); return; }

        await console.WriteSectionAsync($"Nodes ({nodes.Count})");
        var rows = nodes.Select(n => new[]
        {
            n.Id.ToString(),
            n.Label,
            n.Type ?? "—",
            n.Properties is { Count: > 0 } ? string.Join(", ", n.Properties.Select(kv => $"{kv.Key}={kv.Value}")) : "—",
        }).ToArray();

        await console.WriteTableAsync(["ID", "Label", "Type", "Properties"], rows);
    }

    public async Task ShowEdgeListAsync(string title, List<GraphEdge> edges, int scanned)
    {
        await console.WriteSectionAsync(title);
        await console.WriteInfoAsync($"{edges.Count} edges found ({scanned} entries scanned)");

        if (edges.Count == 0) return;

        var rows = edges.Select(e => new[]
        {
            e.SourceId.ToString(),
            e.TargetId.ToString(),
            e.Label,
            e.Weight.ToString("F2"),
        }).ToArray();

        await console.WriteTableAsync(["Source", "Target", "Label", "Weight"], rows);
    }

    public async Task ShowTraversalResultAsync(string title, GraphTraversalResult result)
    {
        await console.WriteSectionAsync(title);

        var stats = result.Statistics;
        await console.WriteKeyValueAsync("Nodes", stats.TotalNodes);
        await console.WriteKeyValueAsync("Edges", stats.TotalEdges);
        await console.WriteKeyValueAsync("Max Depth", stats.MaxDepth);
        await console.WriteKeyValueAsync("Cursor Seeks", stats.CursorSeeks);
        await console.WriteKeyValueAsync("Entries Scanned", stats.EntriesScanned);

        if (result.Nodes.Count == 0)
        {
            await console.WriteInfoAsync("No path found.");
            return;
        }

        await console.WriteBlankLineAsync();

        // Show nodes by depth.
        var rows = result.Nodes.Select(n => new[]
        {
            n.Depth.ToString(),
            n.Node.Id.ToString(),
            n.Node.Label,
            n.Node.Type ?? "—",
        }).ToArray();

        await console.WriteTableAsync(["Depth", "ID", "Label", "Type"], rows);

        // Show ASCII path visualization.
        if (result.Edges.Count > 0 && result.Edges.Count <= 20)
        {
            await console.WriteBlankLineAsync();
            await console.WriteSectionAsync("Path");

            var nodeLabels = result.Nodes.ToDictionary(n => n.Node.Id, n => n.Node.Label);

            foreach (var edge in result.Edges)
            {
                var srcLabel = nodeLabels.GetValueOrDefault(edge.SourceId, $"#{edge.SourceId}");
                var tgtLabel = nodeLabels.GetValueOrDefault(edge.TargetId, $"#{edge.TargetId}");
                var weightInfo = edge.Weight > 0 ? $" ({edge.Weight:F1})" : "";
                await console.WriteInfoAsync($"  [{srcLabel}] --{edge.Label}{weightInfo}--> [{tgtLabel}]");
            }
        }
    }
}

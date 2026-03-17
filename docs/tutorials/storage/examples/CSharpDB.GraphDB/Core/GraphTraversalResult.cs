namespace CSharpDB.GraphDB;

/// <summary>
/// The result of a graph traversal (BFS, shortest path, neighbor lookup).
/// Includes the visited nodes, edges traversed, and efficiency statistics.
/// </summary>
public sealed class GraphTraversalResult
{
    /// <summary>Nodes discovered during the traversal.</summary>
    public required IReadOnlyList<GraphNodeWithDepth> Nodes { get; init; }

    /// <summary>Edges traversed during the traversal.</summary>
    public required IReadOnlyList<GraphEdge> Edges { get; init; }

    /// <summary>Traversal statistics.</summary>
    public required GraphTraversalStatistics Statistics { get; init; }
}

/// <summary>
/// A node annotated with its BFS depth from the starting node.
/// </summary>
public sealed class GraphNodeWithDepth
{
    public required GraphNode Node { get; init; }

    /// <summary>Distance in hops from the start node (0 = start node itself).</summary>
    public int Depth { get; set; }
}

/// <summary>
/// Statistics about a graph traversal's execution.
/// </summary>
public sealed class GraphTraversalStatistics
{
    /// <summary>Total nodes returned.</summary>
    public int TotalNodes { get; set; }

    /// <summary>Total edges returned.</summary>
    public int TotalEdges { get; set; }

    /// <summary>Number of B+tree cursor seek operations performed.</summary>
    public int CursorSeeks { get; set; }

    /// <summary>Total B+tree entries scanned across all seeks.</summary>
    public int EntriesScanned { get; set; }

    /// <summary>Maximum BFS depth reached.</summary>
    public int MaxDepth { get; set; }
}

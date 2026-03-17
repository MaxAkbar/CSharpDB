namespace CSharpDB.GraphDB;

/// <summary>
/// A node in the graph.  Stored in the <c>_nodes</c> B+tree
/// with key = <see cref="Id"/> and value = JSON(GraphNode).
/// </summary>
public sealed class GraphNode
{
    /// <summary>Unique node identifier (the B+tree key).</summary>
    public long Id { get; set; }

    /// <summary>Human-readable label, e.g. "Alice", "New York", "Acme Corp".</summary>
    public string Label { get; set; } = string.Empty;

    /// <summary>Node type / category, e.g. "person", "city", "company".</summary>
    public string? Type { get; set; }

    /// <summary>Arbitrary key-value properties stored with the node.</summary>
    public Dictionary<string, string>? Properties { get; set; }
}

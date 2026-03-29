namespace CSharpDB.GraphDB;

/// <summary>
/// A directed edge in the graph.  Stored in two B+trees:
///
///   Outgoing:  key = (SourceId &lt;&lt; 32) | (TargetId &amp; 0xFFFFFFFF)  →  JSON(GraphEdge)
///   Incoming:  key = (TargetId &lt;&lt; 32) | (SourceId &amp; 0xFFFFFFFF)  →  JSON(GraphEdge)
///
/// This dual-tree design allows efficient cursor range scans in both directions:
///   SeekAsync(nodeId &lt;&lt; 32) lists all outgoing / incoming edges for a node.
/// </summary>
public sealed class GraphEdge
{
    /// <summary>Source (origin) node ID.</summary>
    public long SourceId { get; set; }

    /// <summary>Target (destination) node ID.</summary>
    public long TargetId { get; set; }

    /// <summary>Relationship label, e.g. "KNOWS", "LIVES_IN", "ROAD_TO".</summary>
    public string Label { get; set; } = string.Empty;

    /// <summary>Optional numeric weight (distance, cost, strength, etc.).</summary>
    public double Weight { get; set; }

    /// <summary>Arbitrary key-value properties stored with the edge.</summary>
    public Dictionary<string, string>? Properties { get; set; }
}

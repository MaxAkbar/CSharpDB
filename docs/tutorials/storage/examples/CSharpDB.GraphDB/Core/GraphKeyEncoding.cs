namespace CSharpDB.GraphDB;

// ──────────────────────────────────────────────────────────────
//  Edge key encoding: pack two 32-bit node IDs into one 64-bit B+tree key
// ──────────────────────────────────────────────────────────────

/// <summary>
/// Encodes a directed edge as a single <c>long</c> key for the B+tree.
///
///   key = (highId &lt;&lt; 32) | (lowId &amp; 0xFFFF_FFFF)
///
/// For the <b>outgoing</b> tree:  highId = sourceId, lowId = targetId
/// For the <b>incoming</b> tree:  highId = targetId, lowId = sourceId
///
/// This means all edges from/to a given node share the same upper 32 bits,
/// so <c>BTreeCursor.SeekAsync(nodeId &lt;&lt; 32)</c> positions at the first edge,
/// and <c>MoveNextAsync</c> iterates them in order until the upper bits change.
/// </summary>
public static class GraphKeyEncoding
{
    /// <summary>Maximum node ID that fits in the 32-bit half of an edge key.</summary>
    public const long MaxNodeId = 0xFFFF_FFFF;

    /// <summary>
    /// Encode an edge key for the <b>outgoing</b> edge tree.
    /// </summary>
    public static long EncodeOutgoing(long sourceId, long targetId)
    {
        return (sourceId << 32) | (targetId & MaxNodeId);
    }

    /// <summary>
    /// Encode an edge key for the <b>incoming</b> (reverse) edge tree.
    /// </summary>
    public static long EncodeIncoming(long sourceId, long targetId)
    {
        return (targetId << 32) | (sourceId & MaxNodeId);
    }

    /// <summary>
    /// Compute the start key for a range scan of all edges from/to a node.
    /// </summary>
    public static long RangeStart(long nodeId) => nodeId << 32;

    /// <summary>
    /// Compute the end key (inclusive) for a range scan of all edges from/to a node.
    /// </summary>
    public static long RangeEnd(long nodeId) => (nodeId << 32) | MaxNodeId;

    /// <summary>
    /// Decode the two node IDs from an edge key.
    /// Returns (highId, lowId) — interpret based on which tree it came from.
    /// </summary>
    public static (long HighId, long LowId) Decode(long edgeKey)
    {
        var highId = (edgeKey >> 32) & MaxNodeId;
        var lowId = edgeKey & MaxNodeId;
        return (highId, lowId);
    }
}

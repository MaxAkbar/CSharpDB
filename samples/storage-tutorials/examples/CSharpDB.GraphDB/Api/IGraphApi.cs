using CSharpDB.GraphDB;

internal interface IGraphApi : IAsyncDisposable
{
    Task ResetAsync(CancellationToken ct);

    // Nodes
    Task<GraphNode> AddNodeAsync(string label, string? type, Dictionary<string, string>? properties, CancellationToken ct);
    Task DeleteNodeAsync(long nodeId, CancellationToken ct);
    Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct);
    Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct);
    Task<long> CountNodesAsync(CancellationToken ct);

    // Edges
    Task<GraphEdge> AddEdgeAsync(long sourceId, long targetId, string label, double weight, Dictionary<string, string>? properties, CancellationToken ct);
    Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct);
    Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct);
    Task<(List<GraphEdge> Edges, int Scanned)> GetOutgoingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct);
    Task<(List<GraphEdge> Edges, int Scanned)> GetIncomingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct);
    Task<long> CountEdgesAsync(CancellationToken ct);

    // Traversal
    Task<GraphTraversalResult> TraverseBfsAsync(long startNodeId, int maxDepth, string? edgeLabelFilter, string direction, CancellationToken ct);
    Task<GraphTraversalResult> ShortestPathAsync(long sourceId, long targetId, int maxDepth, string? edgeLabelFilter, CancellationToken ct);
}

internal sealed record AddNodeRequest(string Label, string? Type, Dictionary<string, string>? Properties);

internal sealed record AddEdgeRequest(long SourceId, long TargetId, string Label, double Weight = 0, Dictionary<string, string>? Properties = null);

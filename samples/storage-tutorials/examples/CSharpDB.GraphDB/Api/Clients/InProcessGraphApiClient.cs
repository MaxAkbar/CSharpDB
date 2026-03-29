using CSharpDB.GraphDB;

internal sealed class InProcessGraphApiClient : IGraphApi, IAsyncDisposable
{
    private readonly GraphApiService _service;

    public InProcessGraphApiClient(string databasePath)
    {
        _service = new GraphApiService(databasePath);
    }

    public Task ResetAsync(CancellationToken ct) => _service.ResetAsync(ct);

    public Task<GraphNode> AddNodeAsync(string label, string? type, Dictionary<string, string>? properties, CancellationToken ct)
        => _service.AddNodeAsync(label, type, properties, ct);

    public Task DeleteNodeAsync(long nodeId, CancellationToken ct) => _service.DeleteNodeAsync(nodeId, ct);

    public Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct) => _service.GetNodeAsync(nodeId, ct);

    public Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct) => _service.GetAllNodesAsync(maxResults, ct);

    public Task<long> CountNodesAsync(CancellationToken ct) => _service.CountNodesAsync(ct);

    public Task<GraphEdge> AddEdgeAsync(long sourceId, long targetId, string label, double weight, Dictionary<string, string>? properties, CancellationToken ct)
        => _service.AddEdgeAsync(sourceId, targetId, label, weight, properties, ct);

    public Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct) => _service.DeleteEdgeAsync(sourceId, targetId, ct);

    public Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct) => _service.GetEdgeAsync(sourceId, targetId, ct);

    public Task<(List<GraphEdge> Edges, int Scanned)> GetOutgoingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
        => _service.GetOutgoingEdgesAsync(nodeId, labelFilter, ct);

    public Task<(List<GraphEdge> Edges, int Scanned)> GetIncomingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
        => _service.GetIncomingEdgesAsync(nodeId, labelFilter, ct);

    public Task<long> CountEdgesAsync(CancellationToken ct) => _service.CountEdgesAsync(ct);

    public Task<GraphTraversalResult> TraverseBfsAsync(long startNodeId, int maxDepth, string? edgeLabelFilter, string direction, CancellationToken ct)
        => _service.TraverseBfsAsync(startNodeId, maxDepth, edgeLabelFilter, direction, ct);

    public Task<GraphTraversalResult> ShortestPathAsync(long sourceId, long targetId, int maxDepth, string? edgeLabelFilter, CancellationToken ct)
        => _service.ShortestPathAsync(sourceId, targetId, maxDepth, edgeLabelFilter, ct);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

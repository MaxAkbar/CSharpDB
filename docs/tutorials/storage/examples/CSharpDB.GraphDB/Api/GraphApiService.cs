using CSharpDB.GraphDB;

internal sealed class GraphApiService : IAsyncDisposable
{
    private readonly string _databasePath;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private GraphDatabase? _database;

    public GraphApiService(string databasePath)
    {
        _databasePath = databasePath;
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (_database is not null)
            {
                await _database.DisposeAsync();
                _database = null;
            }

            GraphDatabaseUtility.DeleteDatabaseFiles(_databasePath);
        }
        finally
        {
            _gate.Release();
        }
    }

    // ── Nodes ─────────────────────────────────────────────────

    public Task<GraphNode> AddNodeAsync(string label, string? type, Dictionary<string, string>? properties, CancellationToken ct)
        => ExecuteAsync(db => db.AddNodeAsync(label, type, properties, ct), ct);

    public Task DeleteNodeAsync(long nodeId, CancellationToken ct)
        => ExecuteAsync(async db => { await db.DeleteNodeAsync(nodeId, ct); return true; }, ct);

    public Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct)
        => ExecuteAsync(db => db.GetNodeAsync(nodeId, ct), ct);

    public Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct)
        => ExecuteAsync(db => db.GetAllNodesAsync(maxResults, ct), ct);

    public Task<long> CountNodesAsync(CancellationToken ct)
        => ExecuteAsync(db => db.CountNodesAsync(ct), ct);

    // ── Edges ─────────────────────────────────────────────────

    public Task<GraphEdge> AddEdgeAsync(long sourceId, long targetId, string label, double weight, Dictionary<string, string>? properties, CancellationToken ct)
        => ExecuteAsync(db => db.AddEdgeAsync(sourceId, targetId, label, weight, properties, ct), ct);

    public Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct)
        => ExecuteAsync(async db => { await db.DeleteEdgeAsync(sourceId, targetId, ct); return true; }, ct);

    public Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct)
        => ExecuteAsync(db => db.GetEdgeAsync(sourceId, targetId, ct), ct);

    public Task<(List<GraphEdge> Edges, int Scanned)> GetOutgoingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
        => ExecuteAsync(db => db.GetOutgoingEdgesAsync(nodeId, labelFilter, 10_000, ct), ct);

    public Task<(List<GraphEdge> Edges, int Scanned)> GetIncomingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
        => ExecuteAsync(db => db.GetIncomingEdgesAsync(nodeId, labelFilter, 10_000, ct), ct);

    public Task<long> CountEdgesAsync(CancellationToken ct)
        => ExecuteAsync(db => db.CountEdgesAsync(ct), ct);

    // ── Traversal ─────────────────────────────────────────────

    public Task<GraphTraversalResult> TraverseBfsAsync(long startNodeId, int maxDepth, string? edgeLabelFilter, string direction, CancellationToken ct)
    {
        var dir = direction?.ToLowerInvariant() switch
        {
            "incoming" or "in" => EdgeDirection.Incoming,
            "both" => EdgeDirection.Both,
            _ => EdgeDirection.Outgoing,
        };
        return ExecuteAsync(db => db.TraverseBfsAsync(startNodeId, maxDepth, edgeLabelFilter, dir, ct), ct);
    }

    public Task<GraphTraversalResult> ShortestPathAsync(long sourceId, long targetId, int maxDepth, string? edgeLabelFilter, CancellationToken ct)
        => ExecuteAsync(db => db.ShortestPathAsync(sourceId, targetId, maxDepth, edgeLabelFilter, ct), ct);

    // ── Dispose ───────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_database is not null)
            {
                await _database.DisposeAsync();
                _database = null;
            }
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    // ── Gate helper ───────────────────────────────────────────

    private async Task<T> ExecuteAsync<T>(Func<GraphDatabase, Task<T>> operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await _gate.WaitAsync(ct);
        try
        {
            _database ??= await GraphDatabase.OpenAsync(_databasePath, ct);
            return await operation(_database);
        }
        finally
        {
            _gate.Release();
        }
    }
}

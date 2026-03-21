using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.GraphDB;

// ──────────────────────────────────────────────────────────────
//  Graph database built on three CSharpDB.Storage B+trees
// ──────────────────────────────────────────────────────────────

/// <summary>
/// A graph database using three B+trees:
///
///   <b>Nodes</b>:    nodeId (long) → JSON(GraphNode)
///   <b>Outgoing</b>: (sourceId &lt;&lt; 32 | targetId) → JSON(GraphEdge)
///   <b>Incoming</b>: (targetId &lt;&lt; 32 | sourceId) → JSON(GraphEdge)
///
/// The dual-edge design allows efficient traversal in both directions.
/// <see cref="CSharpDB.Storage.BTrees.BTreeCursor.SeekAsync"/> range scans
/// list all edges from/to a node by seeking to <c>nodeId &lt;&lt; 32</c>.
///
/// Use cases: social networks, knowledge graphs, dependency trees,
/// network topologies, recommendation engines.
/// </summary>
public sealed class GraphDatabase : IAsyncDisposable
{
    private readonly IGraphStore _store;
    private long _nextNodeId;

    private GraphDatabase(IGraphStore store)
    {
        _store = store;
    }

    // ──────────────────────────────────────────────────────────
    //  Factory – open or create
    // ──────────────────────────────────────────────────────────

    public static async Task<GraphDatabase> OpenAsync(string filePath, CancellationToken ct = default)
    {
        var isNew = !File.Exists(filePath);

        var options = new StorageEngineOptionsBuilder()
            .UsePagerOptions(new PagerOptions { MaxCachedPages = 2048 })
            .UseBTreeIndexes()
            .Build();

        var factory = new DefaultStorageEngineFactory();
        var context = await factory.OpenAsync(filePath, options, ct);

        var store = new GraphStore(context.Pager);
        var db = new GraphDatabase(store);

        if (isNew)
        {
            await store.InitializeNewAsync(ct);
            db._nextNodeId = 1;
        }
        else
        {
            await store.LoadAsync(ct);
            // Scan to find the highest existing node ID for auto-increment.
            db._nextNodeId = await db.ComputeNextNodeIdAsync(ct);
        }

        return db;
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – write
    // ──────────────────────────────────────────────────────────

    /// <summary>Add a node with an auto-generated ID.</summary>
    public async Task<GraphNode> AddNodeAsync(
        string label, string? type = null,
        Dictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        var node = new GraphNode
        {
            Id = _nextNodeId++,
            Label = label,
            Type = type,
            Properties = properties,
        };

        await _store.InsertNodeAsync(node, ct);
        return node;
    }

    /// <summary>Add a node with a specific ID.</summary>
    public async Task<GraphNode> AddNodeWithIdAsync(
        long id, string label, string? type = null,
        Dictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        var node = new GraphNode
        {
            Id = id,
            Label = label,
            Type = type,
            Properties = properties,
        };

        await _store.InsertNodeAsync(node, ct);

        if (id >= _nextNodeId)
            _nextNodeId = id + 1;

        return node;
    }

    /// <summary>Add a directed edge between two nodes.</summary>
    public async Task<GraphEdge> AddEdgeAsync(
        long sourceId, long targetId, string label,
        double weight = 0,
        Dictionary<string, string>? properties = null,
        CancellationToken ct = default)
    {
        // Validate that both nodes exist.
        var source = await _store.GetNodeAsync(sourceId, ct)
            ?? throw new KeyNotFoundException($"Source node {sourceId} not found.");
        var target = await _store.GetNodeAsync(targetId, ct)
            ?? throw new KeyNotFoundException($"Target node {targetId} not found.");

        var edge = new GraphEdge
        {
            SourceId = sourceId,
            TargetId = targetId,
            Label = label,
            Weight = weight,
            Properties = properties,
        };

        await _store.InsertEdgeAsync(edge, ct);
        return edge;
    }

    /// <summary>Delete a node and all its edges.</summary>
    public Task DeleteNodeAsync(long nodeId, CancellationToken ct = default)
    {
        return _store.DeleteNodeAsync(nodeId, ct);
    }

    /// <summary>Delete a directed edge.</summary>
    public Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct = default)
    {
        return _store.DeleteEdgeAsync(sourceId, targetId, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Public API – read / query
    // ──────────────────────────────────────────────────────────

    /// <summary>Retrieve a single node by ID.</summary>
    public Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct = default)
    {
        return _store.GetNodeAsync(nodeId, ct);
    }

    /// <summary>Retrieve a single edge.</summary>
    public Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct = default)
    {
        return _store.GetEdgeAsync(sourceId, targetId, ct);
    }

    /// <summary>Retrieve all nodes (limited).</summary>
    public Task<List<GraphNode>> GetAllNodesAsync(int maxResults = 1000, CancellationToken ct = default)
    {
        return _store.GetAllNodesAsync(maxResults, ct);
    }

    /// <summary>
    /// Get all outgoing edges from a node using B+tree cursor range scan.
    /// SeekAsync(nodeId &lt;&lt; 32) → MoveNextAsync until upper bits change.
    /// </summary>
    public Task<(List<GraphEdge> Edges, int Scanned)> GetOutgoingEdgesAsync(
        long nodeId, string? labelFilter = null, int maxResults = 1000,
        CancellationToken ct = default)
    {
        return _store.ScanOutgoingEdgesAsync(nodeId, labelFilter, maxResults, ct);
    }

    /// <summary>
    /// Get all incoming edges to a node using the reverse B+tree cursor range scan.
    /// SeekAsync(nodeId &lt;&lt; 32) → MoveNextAsync until upper bits change.
    /// </summary>
    public Task<(List<GraphEdge> Edges, int Scanned)> GetIncomingEdgesAsync(
        long nodeId, string? labelFilter = null, int maxResults = 1000,
        CancellationToken ct = default)
    {
        return _store.ScanIncomingEdgesAsync(nodeId, labelFilter, maxResults, ct);
    }

    // ──────────────────────────────────────────────────────────
    //  Graph traversal – BFS
    // ──────────────────────────────────────────────────────────

    /// <summary>
    /// Breadth-first traversal from a starting node.
    /// At each level, outgoing edges are discovered via cursor range scans.
    /// </summary>
    public async Task<GraphTraversalResult> TraverseBfsAsync(
        long startNodeId, int maxDepth = 3,
        string? edgeLabelFilter = null,
        EdgeDirection direction = EdgeDirection.Outgoing,
        CancellationToken ct = default)
    {
        var startNode = await _store.GetNodeAsync(startNodeId, ct)
            ?? throw new KeyNotFoundException($"Start node {startNodeId} not found.");

        var visited = new Dictionary<long, GraphNodeWithDepth>();
        var allEdges = new List<GraphEdge>();
        var queue = new Queue<(long NodeId, int Depth)>();
        var totalScanned = 0;
        var seekCount = 0;

        visited[startNodeId] = new GraphNodeWithDepth { Node = startNode, Depth = 0 };
        queue.Enqueue((startNodeId, 0));

        while (queue.Count > 0)
        {
            var (currentId, depth) = queue.Dequeue();
            if (depth >= maxDepth)
                continue;

            // Scan edges from current node using cursor range scan.
            List<GraphEdge> edges;
            int scanned;

            if (direction is EdgeDirection.Outgoing or EdgeDirection.Both)
            {
                (edges, scanned) = await _store.ScanOutgoingEdgesAsync(currentId, edgeLabelFilter, 10_000, ct);
                seekCount++;
                totalScanned += scanned;
                foreach (var edge in edges)
                {
                    allEdges.Add(edge);
                    if (!visited.ContainsKey(edge.TargetId))
                    {
                        var targetNode = await _store.GetNodeAsync(edge.TargetId, ct);
                        if (targetNode is not null)
                        {
                            visited[edge.TargetId] = new GraphNodeWithDepth { Node = targetNode, Depth = depth + 1 };
                            queue.Enqueue((edge.TargetId, depth + 1));
                        }
                    }
                }
            }

            if (direction is EdgeDirection.Incoming or EdgeDirection.Both)
            {
                (edges, scanned) = await _store.ScanIncomingEdgesAsync(currentId, edgeLabelFilter, 10_000, ct);
                seekCount++;
                totalScanned += scanned;
                foreach (var edge in edges)
                {
                    allEdges.Add(edge);
                    if (!visited.ContainsKey(edge.SourceId))
                    {
                        var sourceNode = await _store.GetNodeAsync(edge.SourceId, ct);
                        if (sourceNode is not null)
                        {
                            visited[edge.SourceId] = new GraphNodeWithDepth { Node = sourceNode, Depth = depth + 1 };
                            queue.Enqueue((edge.SourceId, depth + 1));
                        }
                    }
                }
            }
        }

        var maxReachedDepth = visited.Values.Count > 0 ? visited.Values.Max(n => n.Depth) : 0;

        return new GraphTraversalResult
        {
            Nodes = visited.Values.OrderBy(n => n.Depth).ThenBy(n => n.Node.Id).ToList(),
            Edges = allEdges,
            Statistics = new GraphTraversalStatistics
            {
                TotalNodes = visited.Count,
                TotalEdges = allEdges.Count,
                CursorSeeks = seekCount,
                EntriesScanned = totalScanned,
                MaxDepth = maxReachedDepth,
            },
        };
    }

    /// <summary>
    /// Find the shortest path between two nodes using BFS.
    /// Returns only nodes and edges on the shortest path.
    /// </summary>
    public async Task<GraphTraversalResult> ShortestPathAsync(
        long sourceId, long targetId, int maxDepth = 10,
        string? edgeLabelFilter = null,
        CancellationToken ct = default)
    {
        if (sourceId == targetId)
        {
            var node = await _store.GetNodeAsync(sourceId, ct)
                ?? throw new KeyNotFoundException($"Node {sourceId} not found.");
            return new GraphTraversalResult
            {
                Nodes = [new GraphNodeWithDepth { Node = node, Depth = 0 }],
                Edges = [],
                Statistics = new GraphTraversalStatistics { TotalNodes = 1, MaxDepth = 0 },
            };
        }

        // BFS with parent tracking.
        var parent = new Dictionary<long, (long ParentId, GraphEdge Edge)>();
        var visitedNodes = new Dictionary<long, GraphNode>();
        var queue = new Queue<(long NodeId, int Depth)>();
        var totalScanned = 0;
        var seekCount = 0;

        var startNode = await _store.GetNodeAsync(sourceId, ct)
            ?? throw new KeyNotFoundException($"Source node {sourceId} not found.");
        visitedNodes[sourceId] = startNode;
        queue.Enqueue((sourceId, 0));

        var found = false;

        while (queue.Count > 0 && !found)
        {
            var (currentId, depth) = queue.Dequeue();
            if (depth >= maxDepth) continue;

            var (edges, scanned) = await _store.ScanOutgoingEdgesAsync(currentId, edgeLabelFilter, 10_000, ct);
            seekCount++;
            totalScanned += scanned;

            foreach (var edge in edges)
            {
                if (visitedNodes.ContainsKey(edge.TargetId))
                    continue;

                var targetNode = await _store.GetNodeAsync(edge.TargetId, ct);
                if (targetNode is null) continue;

                visitedNodes[edge.TargetId] = targetNode;
                parent[edge.TargetId] = (currentId, edge);
                queue.Enqueue((edge.TargetId, depth + 1));

                if (edge.TargetId == targetId)
                {
                    found = true;
                    break;
                }
            }
        }

        if (!found)
        {
            return new GraphTraversalResult
            {
                Nodes = [],
                Edges = [],
                Statistics = new GraphTraversalStatistics
                {
                    CursorSeeks = seekCount,
                    EntriesScanned = totalScanned,
                },
            };
        }

        // Reconstruct the path.
        var pathNodes = new List<GraphNodeWithDepth>();
        var pathEdges = new List<GraphEdge>();
        var current = targetId;
        var hopCount = 0;

        while (current != sourceId)
        {
            var (parentId, edge) = parent[current];
            pathEdges.Add(edge);
            current = parentId;
            hopCount++;
        }

        pathEdges.Reverse();

        // Build node list in order.
        current = sourceId;
        pathNodes.Add(new GraphNodeWithDepth { Node = visitedNodes[current], Depth = 0 });
        for (var i = 0; i < pathEdges.Count; i++)
        {
            current = pathEdges[i].TargetId;
            pathNodes.Add(new GraphNodeWithDepth { Node = visitedNodes[current], Depth = i + 1 });
        }

        return new GraphTraversalResult
        {
            Nodes = pathNodes,
            Edges = pathEdges,
            Statistics = new GraphTraversalStatistics
            {
                TotalNodes = pathNodes.Count,
                TotalEdges = pathEdges.Count,
                CursorSeeks = seekCount,
                EntriesScanned = totalScanned,
                MaxDepth = hopCount,
            },
        };
    }

    // ──────────────────────────────────────────────────────────
    //  Counts
    // ──────────────────────────────────────────────────────────

    public Task<long> CountNodesAsync(CancellationToken ct = default) => _store.CountNodesAsync(ct);
    public Task<long> CountEdgesAsync(CancellationToken ct = default) => _store.CountEdgesAsync(ct);

    // ──────────────────────────────────────────────────────────
    //  Helpers
    // ──────────────────────────────────────────────────────────

    private async Task<long> ComputeNextNodeIdAsync(CancellationToken ct)
    {
        // Get all nodes and find the max ID.
        var nodes = await _store.GetAllNodesAsync(100_000, ct);
        return nodes.Count > 0 ? nodes.Max(n => n.Id) + 1 : 1;
    }

    // ──────────────────────────────────────────────────────────
    //  Dispose
    // ──────────────────────────────────────────────────────────

    public ValueTask DisposeAsync() => _store.DisposeAsync();
}

/// <summary>
/// Direction for edge traversal.
/// </summary>
public enum EdgeDirection
{
    /// <summary>Follow outgoing edges only.</summary>
    Outgoing,

    /// <summary>Follow incoming edges only.</summary>
    Incoming,

    /// <summary>Follow both outgoing and incoming edges.</summary>
    Both,
}

using System.Buffers.Binary;
using System.Text.Json;
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;

namespace CSharpDB.GraphDB;

// ──────────────────────────────────────────────────────────────
//  Low-level storage layer – owns the three B+trees and the Pager
// ──────────────────────────────────────────────────────────────

internal interface IGraphStore : IAsyncDisposable
{
    Pager Pager { get; }

    Task InitializeNewAsync(CancellationToken ct);
    Task LoadAsync(CancellationToken ct);

    // Nodes
    Task InsertNodeAsync(GraphNode node, CancellationToken ct);
    Task DeleteNodeAsync(long nodeId, CancellationToken ct);
    Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct);
    Task<long> CountNodesAsync(CancellationToken ct);
    Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct);

    // Edges
    Task InsertEdgeAsync(GraphEdge edge, CancellationToken ct);
    Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct);
    Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct);
    Task<long> CountEdgesAsync(CancellationToken ct);

    // Edge traversal (cursor range scans — the core showcase)
    Task<(List<GraphEdge> Edges, int Scanned)> ScanOutgoingEdgesAsync(long nodeId, string? labelFilter, int maxResults, CancellationToken ct);
    Task<(List<GraphEdge> Edges, int Scanned)> ScanIncomingEdgesAsync(long nodeId, string? labelFilter, int maxResults, CancellationToken ct);
}

internal sealed class GraphStore(Pager pager) : IGraphStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    // ── Three B+trees ─────────────────────────────────────────
    //
    //   _nodes:     nodeId (long) → JSON(GraphNode)
    //   _outEdges:  (sourceId << 32 | targetId) → JSON(GraphEdge)
    //   _inEdges:   (targetId << 32 | sourceId) → JSON(GraphEdge)   (reverse index)
    //
    // The superblock (key 0 in _nodes) stores the root page IDs
    // of all three trees in a 12-byte buffer.
    //
    private BTree _nodes = null!;
    private BTree _outEdges = null!;
    private BTree _inEdges = null!;

    public Pager Pager { get; } = pager;

    // ── Initialisation ────────────────────────────────────────

    public async Task InitializeNewAsync(CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var nodesRoot = await BTree.CreateNewAsync(Pager, ct);
            var outRoot = await BTree.CreateNewAsync(Pager, ct);
            var inRoot = await BTree.CreateNewAsync(Pager, ct);

            _nodes = new BTree(Pager, nodesRoot);
            _outEdges = new BTree(Pager, outRoot);
            _inEdges = new BTree(Pager, inRoot);

            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task LoadAsync(CancellationToken ct)
    {
        // Bootstrap: read the superblock from a temporary tree at root page 1.
        _nodes = new BTree(Pager, 1);
        var superblock = await _nodes.FindAsync(0, ct)
            ?? throw new InvalidOperationException("Superblock not found – file is corrupted or not a graph database.");

        var span = superblock.AsSpan();
        var nodesRootPageId = BinaryPrimitives.ReadUInt32LittleEndian(span[..4]);
        var outRootPageId = BinaryPrimitives.ReadUInt32LittleEndian(span[4..8]);
        var inRootPageId = BinaryPrimitives.ReadUInt32LittleEndian(span[8..12]);

        _nodes = new BTree(Pager, nodesRootPageId);
        _outEdges = new BTree(Pager, outRootPageId);
        _inEdges = new BTree(Pager, inRootPageId);
    }

    // ── Superblock persistence ────────────────────────────────

    private async Task PersistSuperblockAsync(CancellationToken ct)
    {
        var buffer = new byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(0), _nodes.RootPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(4), _outEdges.RootPageId);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.AsSpan(8), _inEdges.RootPageId);

        await _nodes.DeleteAsync(0, ct);
        await _nodes.InsertAsync(0, buffer, ct);
    }

    // ── Node operations ───────────────────────────────────────

    public async Task InsertNodeAsync(GraphNode node, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(node, JsonOptions);

        await Pager.BeginTransactionAsync(ct);
        try
        {
            await _nodes.DeleteAsync(node.Id, ct);
            await _nodes.InsertAsync(node.Id, json, ct);
            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task DeleteNodeAsync(long nodeId, CancellationToken ct)
    {
        await Pager.BeginTransactionAsync(ct);
        try
        {
            var deleted = await _nodes.DeleteAsync(nodeId, ct);
            if (!deleted)
            {
                await Pager.RollbackAsync(ct);
                throw new KeyNotFoundException($"No node found with ID {nodeId}.");
            }

            // Also delete all outgoing and incoming edges for this node.
            await DeleteEdgesForNodeAsync(_outEdges, nodeId, ct);
            await DeleteEdgesForNodeAsync(_inEdges, nodeId, ct);

            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch (KeyNotFoundException)
        {
            throw;
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct)
    {
        var data = await _nodes.FindAsync(nodeId, ct);
        return data is null ? null : JsonSerializer.Deserialize<GraphNode>(data, JsonOptions);
    }

    public async Task<long> CountNodesAsync(CancellationToken ct)
    {
        var total = await _nodes.CountEntriesAsync(ct);
        return Math.Max(0, total - 1); // subtract superblock at key 0
    }

    public async Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct)
    {
        var results = new List<GraphNode>();
        var cursor = _nodes.CreateCursor();

        if (!await cursor.SeekAsync(1, ct)) // skip superblock at key 0
            return results;

        do
        {
            if (cursor.CurrentKey == 0) continue;

            var node = JsonSerializer.Deserialize<GraphNode>(cursor.CurrentValue.Span, JsonOptions);
            if (node is not null)
            {
                results.Add(node);
                if (results.Count >= maxResults) break;
            }
        }
        while (await cursor.MoveNextAsync(ct));

        return results;
    }

    // ── Edge operations ───────────────────────────────────────

    public async Task InsertEdgeAsync(GraphEdge edge, CancellationToken ct)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(edge, JsonOptions);
        var outKey = GraphKeyEncoding.EncodeOutgoing(edge.SourceId, edge.TargetId);
        var inKey = GraphKeyEncoding.EncodeIncoming(edge.SourceId, edge.TargetId);

        await Pager.BeginTransactionAsync(ct);
        try
        {
            // Upsert: remove existing then insert.
            await _outEdges.DeleteAsync(outKey, ct);
            await _outEdges.InsertAsync(outKey, json, ct);

            await _inEdges.DeleteAsync(inKey, ct);
            await _inEdges.InsertAsync(inKey, json, ct);

            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct)
    {
        var outKey = GraphKeyEncoding.EncodeOutgoing(sourceId, targetId);
        var inKey = GraphKeyEncoding.EncodeIncoming(sourceId, targetId);

        await Pager.BeginTransactionAsync(ct);
        try
        {
            var deleted = await _outEdges.DeleteAsync(outKey, ct);
            if (!deleted)
            {
                await Pager.RollbackAsync(ct);
                throw new KeyNotFoundException($"No edge found from {sourceId} to {targetId}.");
            }

            await _inEdges.DeleteAsync(inKey, ct);
            await PersistSuperblockAsync(ct);
            await Pager.CommitAsync(ct);
        }
        catch (KeyNotFoundException)
        {
            throw;
        }
        catch
        {
            await Pager.RollbackAsync(ct);
            throw;
        }
    }

    public async Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct)
    {
        var outKey = GraphKeyEncoding.EncodeOutgoing(sourceId, targetId);
        var data = await _outEdges.FindAsync(outKey, ct);
        return data is null ? null : JsonSerializer.Deserialize<GraphEdge>(data, JsonOptions);
    }

    public async Task<long> CountEdgesAsync(CancellationToken ct)
    {
        return await _outEdges.CountEntriesAsync(ct);
    }

    // ── Edge range scans — the core B+tree cursor showcase ────

    /// <summary>
    /// Scan all outgoing edges from a node using BTreeCursor.SeekAsync.
    /// SeekAsync(nodeId &lt;&lt; 32) positions at the first outgoing edge,
    /// MoveNextAsync iterates until the upper 32 bits change.
    /// </summary>
    public async Task<(List<GraphEdge> Edges, int Scanned)> ScanOutgoingEdgesAsync(
        long nodeId, string? labelFilter, int maxResults, CancellationToken ct)
    {
        return await ScanEdgeRangeAsync(_outEdges, nodeId, labelFilter, maxResults, ct);
    }

    /// <summary>
    /// Scan all incoming edges to a node using BTreeCursor.SeekAsync on the reverse tree.
    /// SeekAsync(nodeId &lt;&lt; 32) positions at the first incoming edge,
    /// MoveNextAsync iterates until the upper 32 bits change.
    /// </summary>
    public async Task<(List<GraphEdge> Edges, int Scanned)> ScanIncomingEdgesAsync(
        long nodeId, string? labelFilter, int maxResults, CancellationToken ct)
    {
        return await ScanEdgeRangeAsync(_inEdges, nodeId, labelFilter, maxResults, ct);
    }

    private static async Task<(List<GraphEdge> Edges, int Scanned)> ScanEdgeRangeAsync(
        BTree tree, long nodeId, string? labelFilter, int maxResults, CancellationToken ct)
    {
        var results = new List<GraphEdge>();
        var scanned = 0;
        var startKey = GraphKeyEncoding.RangeStart(nodeId);
        var endKey = GraphKeyEncoding.RangeEnd(nodeId);

        var cursor = tree.CreateCursor();
        if (!await cursor.SeekAsync(startKey, ct))
            return (results, scanned);

        do
        {
            if (cursor.CurrentKey > endKey)
                break;

            scanned++;

            var edge = JsonSerializer.Deserialize<GraphEdge>(cursor.CurrentValue.Span, JsonOptions);
            if (edge is null)
                continue;

            // Optional label filter.
            if (labelFilter is not null
                && !string.Equals(edge.Label, labelFilter, StringComparison.OrdinalIgnoreCase))
                continue;

            results.Add(edge);
            if (results.Count >= maxResults)
                break;
        }
        while (await cursor.MoveNextAsync(ct));

        return (results, scanned);
    }

    // ── Helpers ───────────────────────────────────────────────

    /// <summary>
    /// Delete all edges keyed by a given nodeId in a specific tree.
    /// Used when deleting a node (must remove its outgoing + incoming edges).
    /// </summary>
    private static async Task DeleteEdgesForNodeAsync(BTree tree, long nodeId, CancellationToken ct)
    {
        var startKey = GraphKeyEncoding.RangeStart(nodeId);
        var endKey = GraphKeyEncoding.RangeEnd(nodeId);
        var keysToDelete = new List<long>();

        var cursor = tree.CreateCursor();
        if (!await cursor.SeekAsync(startKey, ct))
            return;

        do
        {
            if (cursor.CurrentKey > endKey) break;
            keysToDelete.Add(cursor.CurrentKey);
        }
        while (await cursor.MoveNextAsync(ct));

        foreach (var key in keysToDelete)
        {
            await tree.DeleteAsync(key, ct);
        }
    }

    // ── Dispose ───────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        return Pager.DisposeAsync();
    }
}

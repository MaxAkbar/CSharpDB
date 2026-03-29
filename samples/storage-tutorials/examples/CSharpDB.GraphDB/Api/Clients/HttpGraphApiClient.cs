using System.Net.Http.Json;
using CSharpDB.GraphDB;

internal sealed class HttpGraphApiClient : IGraphApi, IAsyncDisposable
{
    private readonly HttpClient _httpClient;

    public HttpGraphApiClient(string baseAddress)
    {
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(baseAddress.EndsWith('/') ? baseAddress : $"{baseAddress}/"),
        };
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        using var response = await _httpClient.PostAsync("api/graph/reset", content: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    // ── Nodes ─────────────────────────────────────────────────

    public async Task<GraphNode> AddNodeAsync(string label, string? type, Dictionary<string, string>? properties, CancellationToken ct)
    {
        var request = new AddNodeRequest(label, type, properties);
        using var response = await _httpClient.PostAsJsonAsync("api/graph/nodes", request, ct);
        await EnsureSuccessAsync(response, ct);
        return await response.Content.ReadFromJsonAsync<GraphNode>(ct)
            ?? throw new InvalidOperationException("The API returned no data.");
    }

    public async Task DeleteNodeAsync(long nodeId, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, $"api/graph/nodes/{nodeId}");
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<GraphNode?> GetNodeAsync(long nodeId, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<GraphNode>($"api/graph/nodes/{nodeId}", ct);
    }

    public async Task<List<GraphNode>> GetAllNodesAsync(int maxResults, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<List<GraphNode>>($"api/graph/nodes?maxResults={maxResults}", ct)
            ?? [];
    }

    public async Task<long> CountNodesAsync(CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<long>("api/graph/nodes/count", ct);
    }

    // ── Edges ─────────────────────────────────────────────────

    public async Task<GraphEdge> AddEdgeAsync(long sourceId, long targetId, string label, double weight, Dictionary<string, string>? properties, CancellationToken ct)
    {
        var request = new AddEdgeRequest(sourceId, targetId, label, weight, properties);
        using var response = await _httpClient.PostAsJsonAsync("api/graph/edges", request, ct);
        await EnsureSuccessAsync(response, ct);
        return await response.Content.ReadFromJsonAsync<GraphEdge>(ct)
            ?? throw new InvalidOperationException("The API returned no data.");
    }

    public async Task DeleteEdgeAsync(long sourceId, long targetId, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, $"api/graph/edges?sourceId={sourceId}&targetId={targetId}");
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<GraphEdge?> GetEdgeAsync(long sourceId, long targetId, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<GraphEdge>($"api/graph/edges?sourceId={sourceId}&targetId={targetId}", ct);
    }

    public async Task<(List<GraphEdge> Edges, int Scanned)> GetOutgoingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
    {
        var url = $"api/graph/edges/outgoing/{nodeId}";
        if (labelFilter is not null) url += $"?label={Uri.EscapeDataString(labelFilter)}";
        var result = await _httpClient.GetFromJsonAsync<EdgeScanResult>(url, ct);
        return (result?.Edges ?? [], result?.Scanned ?? 0);
    }

    public async Task<(List<GraphEdge> Edges, int Scanned)> GetIncomingEdgesAsync(long nodeId, string? labelFilter, CancellationToken ct)
    {
        var url = $"api/graph/edges/incoming/{nodeId}";
        if (labelFilter is not null) url += $"?label={Uri.EscapeDataString(labelFilter)}";
        var result = await _httpClient.GetFromJsonAsync<EdgeScanResult>(url, ct);
        return (result?.Edges ?? [], result?.Scanned ?? 0);
    }

    public async Task<long> CountEdgesAsync(CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<long>("api/graph/edges/count", ct);
    }

    // ── Traversal ─────────────────────────────────────────────

    public async Task<GraphTraversalResult> TraverseBfsAsync(long startNodeId, int maxDepth, string? edgeLabelFilter, string direction, CancellationToken ct)
    {
        var url = $"api/graph/traverse/bfs?startNodeId={startNodeId}&maxDepth={maxDepth}&direction={direction}";
        if (edgeLabelFilter is not null) url += $"&edgeLabel={Uri.EscapeDataString(edgeLabelFilter)}";
        return await _httpClient.GetFromJsonAsync<GraphTraversalResult>(url, ct)
            ?? throw new InvalidOperationException("The API returned no result.");
    }

    public async Task<GraphTraversalResult> ShortestPathAsync(long sourceId, long targetId, int maxDepth, string? edgeLabelFilter, CancellationToken ct)
    {
        var url = $"api/graph/traverse/shortest-path?sourceId={sourceId}&targetId={targetId}&maxDepth={maxDepth}";
        if (edgeLabelFilter is not null) url += $"&edgeLabel={Uri.EscapeDataString(edgeLabelFilter)}";
        return await _httpClient.GetFromJsonAsync<GraphTraversalResult>(url, ct)
            ?? throw new InvalidOperationException("The API returned no result.");
    }

    // ── Helpers ───────────────────────────────────────────────

    public ValueTask DisposeAsync() { _httpClient.Dispose(); return ValueTask.CompletedTask; }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode) return;
        var details = await response.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException(string.IsNullOrWhiteSpace(details) ? response.ReasonPhrase ?? "Request failed." : details);
    }

    /// <summary>DTO for deserialising edge scan results from the REST API.</summary>
    private sealed class EdgeScanResult
    {
        public List<GraphEdge> Edges { get; set; } = [];
        public int Scanned { get; set; }
    }
}

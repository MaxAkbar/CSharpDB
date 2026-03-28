using System.Net.Http.Json;
using CSharpDB.SpatialIndex;

internal sealed class HttpSpatialIndexApiClient : ISpatialIndexApi, IAsyncDisposable
{
    private readonly HttpClient _httpClient;

    public HttpSpatialIndexApiClient(string baseAddress)
    {
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(baseAddress.EndsWith('/') ? baseAddress : $"{baseAddress}/"),
        };
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        using var response = await _httpClient.PostAsync("api/spatial/reset", content: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<SpatialPoint> AddAsync(double latitude, double longitude, string name, string? category, string? description, Dictionary<string, string>? tags, CancellationToken ct)
    {
        var request = new AddPointRequest(latitude, longitude, name, category, description, tags);
        using var response = await _httpClient.PostAsJsonAsync("api/spatial/points", request, ct);
        await EnsureSuccessAsync(response, ct);
        return await response.Content.ReadFromJsonAsync<SpatialPoint>(ct)
            ?? throw new InvalidOperationException("The API returned no data.");
    }

    public async Task DeleteAsync(long hilbertKey, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, $"api/spatial/points/{hilbertKey}");
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<SpatialPoint?> GetAsync(long hilbertKey, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<SpatialPoint>($"api/spatial/points/{hilbertKey}", ct);
    }

    public async Task<SpatialQueryResult> QueryNearbyAsync(double latitude, double longitude, double radiusKm, string? category, int maxResults, CancellationToken ct)
    {
        var url = $"api/spatial/nearby?lat={latitude}&lon={longitude}&radiusKm={radiusKm}&maxResults={maxResults}";
        if (category is not null) url += $"&category={Uri.EscapeDataString(category)}";
        return await _httpClient.GetFromJsonAsync<SpatialQueryResult>(url, ct)
            ?? throw new InvalidOperationException("The API returned no result.");
    }

    public async Task<SpatialQueryResult> QueryBoundingBoxAsync(double minLat, double minLon, double maxLat, double maxLon, string? category, int maxResults, CancellationToken ct)
    {
        var url = $"api/spatial/bbox?minLat={minLat}&minLon={minLon}&maxLat={maxLat}&maxLon={maxLon}&maxResults={maxResults}";
        if (category is not null) url += $"&category={Uri.EscapeDataString(category)}";
        return await _httpClient.GetFromJsonAsync<SpatialQueryResult>(url, ct)
            ?? throw new InvalidOperationException("The API returned no result.");
    }

    public async Task<long> CountAsync(CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<long>("api/spatial/count", ct);
    }

    public ValueTask DisposeAsync() { _httpClient.Dispose(); return ValueTask.CompletedTask; }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode) return;
        var details = await response.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException(string.IsNullOrWhiteSpace(details) ? response.ReasonPhrase ?? "Request failed." : details);
    }
}

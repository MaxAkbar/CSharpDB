using System.Net.Http.Json;
using CSharpDB.TimeSeries;

internal sealed class HttpTimeSeriesApiClient : ITimeSeriesApi, IAsyncDisposable
{
    private readonly HttpClient _httpClient;

    public HttpTimeSeriesApiClient(string baseAddress)
    {
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(AppendTrailingSlash(baseAddress)),
        };
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        using var response = await _httpClient.PostAsync("api/timeseries/reset", content: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<TimeSeriesPoint> RecordAsync(
        string metric, double value, string? unit,
        Dictionary<string, string>? tags, DateTime? timestampUtc,
        CancellationToken ct)
    {
        var request = new RecordPointRequest(metric, value, unit, tags, timestampUtc);
        using var response = await _httpClient.PostAsJsonAsync("api/timeseries/points", request, ct);
        await EnsureSuccessAsync(response, ct);
        return await response.Content.ReadFromJsonAsync<TimeSeriesPoint>(ct)
            ?? throw new InvalidOperationException("The API returned no data point.");
    }

    public async Task DeleteAsync(long timestampTicks, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, $"api/timeseries/points/{timestampTicks}");
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<TimeSeriesPoint?> GetPointAsync(long timestampTicks, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<TimeSeriesPoint>($"api/timeseries/points/{timestampTicks}", ct);
    }

    public async Task<TimeSeriesQueryResult> QueryAsync(
        DateTime from, DateTime to, string? metric, int maxResults, CancellationToken ct)
    {
        var url = $"api/timeseries/query?from={from:O}&to={to:O}&maxResults={maxResults}";
        if (metric is not null)
        {
            url += $"&metric={Uri.EscapeDataString(metric)}";
        }

        return await _httpClient.GetFromJsonAsync<TimeSeriesQueryResult>(url, ct)
            ?? throw new InvalidOperationException("The API returned no query result.");
    }

    public async Task<TimeSeriesPoint?> GetLatestAsync(CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<TimeSeriesPoint>("api/timeseries/latest", ct);
    }

    public async Task<long> CountAsync(CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<long>("api/timeseries/count", ct);
    }

    public ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        return ValueTask.CompletedTask;
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var details = await response.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException(string.IsNullOrWhiteSpace(details)
            ? response.ReasonPhrase ?? "Request failed."
            : details);
    }

    private static string AppendTrailingSlash(string baseAddress)
    {
        return baseAddress.EndsWith('/') ? baseAddress : $"{baseAddress}/";
    }
}

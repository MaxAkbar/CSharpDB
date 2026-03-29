using System.Net.Http.Json;
using CSharpDB.VirtualFS;

internal sealed class HttpVirtualFileSystemApiClient : IVirtualFileSystemApi, IAsyncDisposable
{
    private readonly HttpClient _httpClient;

    public HttpVirtualFileSystemApiClient(string baseAddress)
    {
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(AppendTrailingSlash(baseAddress)),
        };
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        using var response = await _httpClient.PostAsync("api/filesystem/reset", content: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<string>> RenderTreeAsync(string path, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<string[]>($"api/filesystem/tree?path={Uri.EscapeDataString(path)}", ct)
            ?? [];
    }

    public async Task<IReadOnlyList<FsEntry>> ListDirectoryAsync(string path, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<FsEntry[]>($"api/filesystem/entries?path={Uri.EscapeDataString(path)}", ct)
            ?? [];
    }

    public async Task CreateDirectoryAsync(string path, CancellationToken ct)
    {
        using var response = await _httpClient.PostAsJsonAsync("api/filesystem/directories", new { path }, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task WriteFileAsync(string path, byte[] content, CancellationToken ct)
    {
        using var response = await _httpClient.PostAsJsonAsync("api/filesystem/files", new VirtualFileWriteRequest(path, content), ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<VirtualFileContentResult> ReadFileAsync(string path, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<VirtualFileContentResult>($"api/filesystem/files/content?path={Uri.EscapeDataString(path)}", ct)
            ?? throw new InvalidOperationException("The API returned no file content.");
    }

    public async Task<FsEntry> GetEntryInfoAsync(string path, CancellationToken ct)
    {
        return await _httpClient.GetFromJsonAsync<FsEntry>($"api/filesystem/entry?path={Uri.EscapeDataString(path)}", ct)
            ?? throw new InvalidOperationException("The API returned no entry info.");
    }

    public async Task CreateShortcutAsync(string shortcutPath, string targetPath, CancellationToken ct)
    {
        using var response = await _httpClient.PostAsJsonAsync(
            "api/filesystem/shortcuts",
            new VirtualFileShortcutRequest(shortcutPath, targetPath),
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DeleteAsync(string path, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, $"api/filesystem/entry?path={Uri.EscapeDataString(path)}");
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessAsync(response, ct);
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

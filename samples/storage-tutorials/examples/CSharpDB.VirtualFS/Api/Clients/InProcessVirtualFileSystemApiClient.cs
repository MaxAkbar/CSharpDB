using CSharpDB.VirtualFS;

internal sealed class InProcessVirtualFileSystemApiClient : IVirtualFileSystemApi, IAsyncDisposable
{
    private readonly VirtualFileSystemApiService _service;

    public InProcessVirtualFileSystemApiClient(string databasePath)
    {
        _service = new VirtualFileSystemApiService(databasePath);
    }

    public Task ResetAsync(CancellationToken ct) => _service.ResetAsync(ct);

    public Task<IReadOnlyList<string>> RenderTreeAsync(string path, CancellationToken ct) => _service.RenderTreeAsync(path, ct);

    public Task<IReadOnlyList<FsEntry>> ListDirectoryAsync(string path, CancellationToken ct) => _service.ListDirectoryAsync(path, ct);

    public Task CreateDirectoryAsync(string path, CancellationToken ct) => _service.CreateDirectoryAsync(path, ct);

    public Task WriteFileAsync(string path, byte[] content, CancellationToken ct) => _service.WriteFileAsync(path, content, ct);

    public Task<VirtualFileContentResult> ReadFileAsync(string path, CancellationToken ct) => _service.ReadFileAsync(path, ct);

    public Task<FsEntry> GetEntryInfoAsync(string path, CancellationToken ct) => _service.GetEntryInfoAsync(path, ct);

    public Task CreateShortcutAsync(string shortcutPath, string targetPath, CancellationToken ct) => _service.CreateShortcutAsync(shortcutPath, targetPath, ct);

    public Task DeleteAsync(string path, CancellationToken ct) => _service.DeleteAsync(path, ct);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

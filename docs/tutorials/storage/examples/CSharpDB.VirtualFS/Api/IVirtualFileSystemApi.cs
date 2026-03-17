using CSharpDB.VirtualFS;

internal interface IVirtualFileSystemApi : IAsyncDisposable
{
    Task ResetAsync(CancellationToken ct);
    Task<IReadOnlyList<string>> RenderTreeAsync(string path, CancellationToken ct);
    Task<IReadOnlyList<FsEntry>> ListDirectoryAsync(string path, CancellationToken ct);
    Task CreateDirectoryAsync(string path, CancellationToken ct);
    Task WriteFileAsync(string path, byte[] content, CancellationToken ct);
    Task<VirtualFileContentResult> ReadFileAsync(string path, CancellationToken ct);
    Task<FsEntry> GetEntryInfoAsync(string path, CancellationToken ct);
    Task CreateShortcutAsync(string shortcutPath, string targetPath, CancellationToken ct);
    Task DeleteAsync(string path, CancellationToken ct);
}

internal sealed record VirtualFileContentResult(string Path, byte[] Content, bool IsText, string DisplayContent);

internal sealed record VirtualFileWriteRequest(string Path, byte[] Content);

internal sealed record VirtualFileShortcutRequest(string ShortcutPath, string TargetPath);

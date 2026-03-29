using System.Text;
using CSharpDB.VirtualFS;

internal sealed class VirtualFileSystemApiService : IAsyncDisposable
{
    private readonly string _databasePath;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private VirtualFileSystem? _fileSystem;

    public VirtualFileSystemApiService(string databasePath)
    {
        _databasePath = databasePath;
    }

    public async Task ResetAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (_fileSystem is not null)
            {
                await _fileSystem.DisposeAsync();
                _fileSystem = null;
            }

            VirtualFileSystemDatabaseUtility.DeleteDatabaseFiles(_databasePath);
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<IReadOnlyList<string>> RenderTreeAsync(string path, CancellationToken ct)
    {
        return ExecuteAsync(fileSystem => fileSystem.RenderTreeAsync(path, ct: ct), ct);
    }

    public Task<IReadOnlyList<FsEntry>> ListDirectoryAsync(string path, CancellationToken ct)
    {
        return ExecuteAsync<IReadOnlyList<FsEntry>>(async fileSystem => await fileSystem.ListDirectoryAsync(path, ct), ct);
    }

    public async Task CreateDirectoryAsync(string path, CancellationToken ct)
    {
        await ExecuteAsync(async fileSystem =>
        {
            await fileSystem.CreateDirectoryAsync(path, ct);
            return true;
        }, ct);
    }

    public async Task WriteFileAsync(string path, byte[] content, CancellationToken ct)
    {
        await ExecuteAsync(async fileSystem =>
        {
            await fileSystem.WriteFileAsync(path, content, ct);
            return true;
        }, ct);
    }

    public async Task<VirtualFileContentResult> ReadFileAsync(string path, CancellationToken ct)
    {
        var content = await ExecuteAsync(fileSystem => fileSystem.ReadFileAsync(path, ct), ct);
        var isText = LooksLikeText(content);
        var displayContent = content.Length == 0
            ? "<empty>"
            : isText
                ? Encoding.UTF8.GetString(content)
                : Convert.ToHexString(content);

        return new VirtualFileContentResult(path, content, isText, displayContent);
    }

    public Task<FsEntry> GetEntryInfoAsync(string path, CancellationToken ct)
    {
        return ExecuteAsync(fileSystem => fileSystem.GetEntryInfoAsync(path, ct), ct);
    }

    public async Task CreateShortcutAsync(string shortcutPath, string targetPath, CancellationToken ct)
    {
        await ExecuteAsync(async fileSystem =>
        {
            await fileSystem.CreateShortcutAsync(shortcutPath, targetPath, ct);
            return true;
        }, ct);
    }

    public async Task DeleteAsync(string path, CancellationToken ct)
    {
        await ExecuteAsync(async fileSystem =>
        {
            await fileSystem.DeleteAsync(path, ct);
            return true;
        }, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try
        {
            if (_fileSystem is not null)
            {
                await _fileSystem.DisposeAsync();
                _fileSystem = null;
            }
        }
        finally
        {
            _gate.Release();
            _gate.Dispose();
        }
    }

    private async Task<T> ExecuteAsync<T>(Func<VirtualFileSystem, Task<T>> operation, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await _gate.WaitAsync(ct);
        try
        {
            _fileSystem ??= await VirtualFileSystem.OpenAsync(_databasePath, ct);
            return await operation(_fileSystem);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static bool LooksLikeText(byte[] bytes)
    {
        foreach (var value in bytes)
        {
            if (value == 9 || value == 10 || value == 13)
            {
                continue;
            }

            if (value < 32)
            {
                return false;
            }
        }

        return true;
    }
}

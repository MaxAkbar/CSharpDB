using CSharpDB.VirtualFS;

internal sealed class VirtualFileSystemConsolePresenter
{
    private readonly AnsiConsoleWriter _console;

    public VirtualFileSystemConsolePresenter(AnsiConsoleWriter console)
    {
        _console = console;
    }

    public async Task ShowTreeAsync(IVirtualFileSystemApi api, string path, CancellationToken ct)
    {
        await _console.WriteTreeAsync(await api.RenderTreeAsync(path, ct));
    }

    public async Task ShowDirectoryListingAsync(IVirtualFileSystemApi api, string path, CancellationToken ct)
    {
        var entries = await api.ListDirectoryAsync(path, ct);
        var rows = entries
            .Select(entry => new[]
            {
                entry.Kind.ToString(),
                entry.Name,
                entry.Kind == EntryKind.File ? $"{entry.Size} bytes" : "—",
            })
            .ToArray();

        await _console.WriteTableAsync(["Type", "Name", "Details"], rows);
    }

    public async Task ShowEntryInfoAsync(IVirtualFileSystemApi api, string path, CancellationToken ct)
    {
        var entry = await api.GetEntryInfoAsync(path, ct);
        await _console.WriteKeyValueAsync("Name", entry.Name);
        await _console.WriteKeyValueAsync("Kind", entry.Kind);
        await _console.WriteKeyValueAsync("Size", $"{entry.Size} bytes");
        await _console.WriteKeyValueAsync("Created", entry.CreatedUtc.ToString("u"));
        await _console.WriteKeyValueAsync("Modified", entry.ModifiedUtc.ToString("u"));
        await _console.WriteKeyValueAsync("ID", entry.Id);
        await _console.WriteKeyValueAsync("Parent ID", entry.ParentId);
    }

    public async Task ShowFileContentAsync(IVirtualFileSystemApi api, string path, CancellationToken ct)
    {
        var content = await api.ReadFileAsync(path, ct);
        await _console.WriteTextBlockAsync(path, content.DisplayContent);
    }
}

using System.Text;

internal sealed class VirtualFileSystemSampleRunner
{
    private readonly VirtualFileSystemConsolePresenter _presenter;
    private readonly AnsiConsoleWriter _console;

    public VirtualFileSystemSampleRunner(AnsiConsoleWriter console, VirtualFileSystemConsolePresenter presenter)
    {
        _console = console;
        _presenter = presenter;
    }

    public async Task RunAsync(IVirtualFileSystemApi api, CancellationToken ct)
    {
        await api.ResetAsync(ct);

        await _console.WriteSectionAsync("Sample scenario");
        await _console.WriteInfoAsync("Resetting database and replaying the demo workflow.");
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Creating directory structure");
        await CreateDirectoryStructureAsync(api, ct);
        await _console.WriteSuccessAsync("Directory structure created.");
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Writing sample files");
        await WriteSampleFilesAsync(api, ct);
        await _console.WriteSuccessAsync("Sample content written.");
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Creating shortcuts");
        await CreateShortcutsAsync(api, ct);
        await _console.WriteSuccessAsync("Shortcuts created.");
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Full tree view");
        await _presenter.ShowTreeAsync(api, "/", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Reading a file");
        await _presenter.ShowFileContentAsync(api, "/documents/work/report.txt", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Reading via shortcut");
        await _presenter.ShowFileContentAsync(api, "/documents/report-shortcut", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Listing /documents");
        await _presenter.ShowDirectoryListingAsync(api, "/documents", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Entry information");
        await _presenter.ShowEntryInfoAsync(api, "/documents/work/report.txt", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Overwriting report.txt");
        await api.WriteFileAsync(
            "/documents/work/report.txt",
            Encoding.UTF8.GetBytes("Q4 Revenue Report v2\n====================\nTotal: $1.5M (revised)"),
            ct);
        await _presenter.ShowFileContentAsync(api, "/documents/work/report.txt", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Deleting todo.txt");
        await api.DeleteAsync("/documents/work/todo.txt", ct);
        var remainingEntries = await api.ListDirectoryAsync("/documents/work", ct);
        await _console.WriteSuccessAsync($"Remaining files in /documents/work: {remainingEntries.Count}");
        foreach (var entry in remainingEntries)
        {
            await _console.WriteInfoAsync(entry.Name);
        }

        await _console.WriteBlankLineAsync();
        await _console.WriteSectionAsync("Final tree");
        await _presenter.ShowTreeAsync(api, "/", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Reopening from disk");
        await _console.WriteSuccessAsync("Persistence verified after reopen.");
        await _presenter.ShowFileContentAsync(api, "/documents/work/report.txt", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSectionAsync("Tree after reopen");
        await _presenter.ShowTreeAsync(api, "/", ct);
        await _console.WriteBlankLineAsync();

        await _console.WriteSuccessAsync("Sample completed.");
    }

    private static async Task CreateDirectoryStructureAsync(IVirtualFileSystemApi api, CancellationToken ct)
    {
        await api.CreateDirectoryAsync("/documents", ct);
        await api.CreateDirectoryAsync("/documents/work", ct);
        await api.CreateDirectoryAsync("/documents/personal", ct);
        await api.CreateDirectoryAsync("/pictures", ct);
        await api.CreateDirectoryAsync("/pictures/vacation", ct);
        await api.CreateDirectoryAsync("/programs", ct);
    }

    private static async Task WriteSampleFilesAsync(IVirtualFileSystemApi api, CancellationToken ct)
    {
        await api.WriteFileAsync(
            "/documents/work/report.txt",
            Encoding.UTF8.GetBytes("Q4 Revenue Report\n==================\nTotal: $1.2M"),
            ct);

        await api.WriteFileAsync(
            "/documents/work/todo.txt",
            Encoding.UTF8.GetBytes("1. Finish report\n2. Send to manager\n3. Update slides"),
            ct);

        await api.WriteFileAsync(
            "/documents/personal/diary.txt",
            Encoding.UTF8.GetBytes("Dear diary, today I built a virtual file system..."),
            ct);

        await api.WriteFileAsync(
            "/pictures/vacation/beach.dat",
            [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            ct);

        await api.WriteFileAsync(
            "/programs/hello.cs",
            Encoding.UTF8.GetBytes("Console.WriteLine(\"Hello from the virtual drive!\");"),
            ct);
    }

    private static async Task CreateShortcutsAsync(IVirtualFileSystemApi api, CancellationToken ct)
    {
        await api.CreateShortcutAsync("/documents/report-shortcut", "/documents/work/report.txt", ct);
        await api.CreateShortcutAsync("/vacation-pics", "/pictures/vacation", ct);
    }
}

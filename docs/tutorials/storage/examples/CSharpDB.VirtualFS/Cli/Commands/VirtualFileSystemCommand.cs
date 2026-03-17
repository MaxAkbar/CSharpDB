using System.Text;
using CSharpDB.VirtualFS;

internal sealed class VirtualFileSystemCommand : IReplCommand
{
    public string Name => "filesystem";

    public string Description => "Inspect and mutate the virtual file system.";

    public string Usage => "filesystem <tree|ls|mkdir|write|read|info|shortcut|rm|reset> [...]";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count == 0 || arguments[0].Equals("help", StringComparison.OrdinalIgnoreCase))
        {
            await WriteHelpAsync(context.Console);
            return;
        }

        var action = arguments[0].ToLowerInvariant();
        switch (action)
        {
            case "reset":
                RequireArgumentCount(arguments, 1);
                await context.Client.ResetAsync(ct);
                context.ResetToRoot();
                await context.Console.WriteSuccessAsync("Database files deleted.");
                break;

            case "tree":
                {
                    var presenter = new VirtualFileSystemConsolePresenter(context.Console);
                    var path = arguments.Count > 1 ? context.ResolvePath(arguments[1]) : context.CurrentDirectory;
                    await presenter.ShowTreeAsync(context.Client, path, ct);
                }
                break;

            case "ls":
                {
                    var presenter = new VirtualFileSystemConsolePresenter(context.Console);
                    var path = arguments.Count > 1 ? context.ResolvePath(arguments[1]) : context.CurrentDirectory;
                    await presenter.ShowDirectoryListingAsync(context.Client, path, ct);
                }
                break;

            case "mkdir":
                RequireArgumentCount(arguments, 2);
                {
                    var path = context.ResolvePath(arguments[1]);
                    await context.Client.CreateDirectoryAsync(path, ct);
                    await context.Console.WriteSuccessAsync($"Directory created: {path}");
                }
                break;

            case "write":
                RequireArgumentCount(arguments, 3);
                {
                    var content = string.Join(' ', arguments.Skip(2));
                    var path = context.ResolvePath(arguments[1]);
                    await context.Client.WriteFileAsync(path, Encoding.UTF8.GetBytes(content), ct);
                    await context.Console.WriteSuccessAsync($"File written: {path}");
                }
                break;

            case "read":
                RequireArgumentCount(arguments, 2);
                {
                    var presenter = new VirtualFileSystemConsolePresenter(context.Console);
                    await presenter.ShowFileContentAsync(context.Client, context.ResolvePath(arguments[1]), ct);
                }
                break;

            case "info":
                RequireArgumentCount(arguments, 2);
                {
                    var presenter = new VirtualFileSystemConsolePresenter(context.Console);
                    await presenter.ShowEntryInfoAsync(context.Client, context.ResolvePath(arguments[1]), ct);
                }
                break;

            case "shortcut":
                RequireArgumentCount(arguments, 3);
                {
                    var shortcutPath = context.ResolvePath(arguments[1]);
                    var targetPath = context.ResolvePath(arguments[2]);
                    await context.Client.CreateShortcutAsync(shortcutPath, targetPath, ct);
                    await context.Console.WriteSuccessAsync($"Shortcut created: {shortcutPath} -> {targetPath}");
                }
                break;

            case "rm":
                RequireArgumentCount(arguments, 2);
                {
                    var path = context.ResolvePath(arguments[1]);
                    await context.Client.DeleteAsync(path, ct);
                    await context.Console.WriteSuccessAsync($"Entry deleted: {path}");
                }
                break;

            default:
                await context.Console.WriteErrorAsync($"Unknown filesystem action '{arguments[0]}'.");
                await WriteHelpAsync(context.Console);
                break;
        }
    }

    private static void RequireArgumentCount(IReadOnlyList<string> arguments, int minimumCount)
    {
        if (arguments.Count < minimumCount)
        {
            throw new InvalidOperationException("Not enough arguments for filesystem command.");
        }
    }

    private static async Task WriteHelpAsync(AnsiConsoleWriter console)
    {
        await console.WriteSectionAsync("filesystem command");
        await console.WriteTableAsync(
            ["Action", "Description", "Example"],
            [
                ["tree", "Render the tree view for the current or specified path.", "tree ./"],
                ["ls", "List the current or specified directory.", "ls ../documents"],
                ["mkdir", "Create a directory.", "mkdir notes"],
                ["write", "Write UTF-8 text to a file.", "write notes/todo.txt \"ship it\""],
                ["read", "Read a file.", "read notes/todo.txt"],
                ["info", "Show entry metadata.", "info notes/todo.txt"],
                ["shortcut", "Create a shortcut.", "shortcut latest ../documents/work/report.txt"],
                ["rm", "Delete a file or empty directory.", "rm notes/todo.txt"],
                ["reset", "Delete the database files and return to /.", "reset"],
            ]);
    }
}

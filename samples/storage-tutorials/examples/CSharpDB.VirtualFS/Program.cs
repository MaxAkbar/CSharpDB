class Program
{
    static async Task Main(string[] args)
    {
        const string databasePath = "virtual_drive.cdb";

        if (args.Length > 0 && args[0].Equals("serve", StringComparison.OrdinalIgnoreCase))
        {
            await VirtualFileSystemWebHost.RunAsync(args.Skip(1).ToArray(), databasePath);
            return;
        }

        var console = new AnsiConsoleWriter();
        await using var client = CreateClient(args, databasePath);
        var context = new ReplCommandContext(console, databasePath, client);
        var filesystemCommand = new VirtualFileSystemCommand();
        var commands = new IReplCommand[]
        {
            new ChangeDirectoryCommand(),
            new PrintWorkingDirectoryCommand(),
            new SampleCommand(),
            new PrefixedReplCommand("tree", "Render the tree view.", "tree [path]", filesystemCommand, "tree"),
            new PrefixedReplCommand("ls", "List a directory.", "ls [path]", filesystemCommand, "ls"),
            new PrefixedReplCommand("mkdir", "Create a directory.", "mkdir <path>", filesystemCommand, "mkdir"),
            new PrefixedReplCommand("write", "Write UTF-8 text to a file.", "write <path> <text>", filesystemCommand, "write"),
            new PrefixedReplCommand("read", "Read a file.", "read <path>", filesystemCommand, "read"),
            new PrefixedReplCommand("info", "Show entry metadata.", "info <path>", filesystemCommand, "info"),
            new PrefixedReplCommand("shortcut", "Create a shortcut.", "shortcut <shortcutPath> <targetPath>", filesystemCommand, "shortcut"),
            new PrefixedReplCommand("rm", "Delete a file or empty directory.", "rm <path>", filesystemCommand, "rm"),
            new PrefixedReplCommand("reset", "Delete the database files.", "reset", filesystemCommand, "reset"),
        };

        var host = new ReplHost(commands, console);
        await host.RunAsync(context, CancellationToken.None);
    }

    private static IVirtualFileSystemApi CreateClient(string[] args, string databasePath)
    {
        var apiIndex = Array.FindIndex(args, arg => arg.Equals("--api", StringComparison.OrdinalIgnoreCase));
        if (apiIndex >= 0)
        {
            if (apiIndex == args.Length - 1)
            {
                throw new InvalidOperationException("Expected a base URL after --api.");
            }

            return new HttpVirtualFileSystemApiClient(args[apiIndex + 1]);
        }

        return new InProcessVirtualFileSystemApiClient(databasePath);
    }
}

using CSharpDB.VirtualFS;

internal sealed class ChangeDirectoryCommand : IReplCommand
{
    public string Name => "cd";

    public string Description => "Change the current working directory.";

    public string Usage => "cd <path>";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        var target = arguments.Count == 0 ? "/" : context.ResolvePath(arguments[0]);

        var entry = await context.Client.GetEntryInfoAsync(target, ct);
        if (entry.Kind != EntryKind.Directory)
        {
            throw new IOException($"'{target}' is not a directory.");
        }

        context.ChangeDirectory(target);
        await context.Console.WriteSuccessAsync($"Current directory: {context.CurrentDirectory}");
    }
}

internal sealed class PrintWorkingDirectoryCommand : IReplCommand
{
    public string Name => "pwd";

    public string Description => "Print the current working directory.";

    public string Usage => "pwd";

    public Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count > 0)
        {
            throw new InvalidOperationException($"Usage: {Usage}");
        }

        return context.Console.WriteInfoAsync(context.CurrentDirectory);
    }
}

internal sealed class SampleCommand : IReplCommand
{
    public string Name => "sample";

    public string Description => "Reset the database and run the original end-to-end demo.";

    public string Usage => "sample";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count > 0)
        {
            await context.Console.WriteErrorAsync($"Usage: {Usage}");
            return;
        }

        var presenter = new VirtualFileSystemConsolePresenter(context.Console);
        var sampleRunner = new VirtualFileSystemSampleRunner(context.Console, presenter);
        await sampleRunner.RunAsync(context.Client, ct);
        context.ResetToRoot();
    }
}

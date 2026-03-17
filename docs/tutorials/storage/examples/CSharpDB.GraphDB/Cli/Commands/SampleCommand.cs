internal sealed class SampleCommand : IReplCommand
{
    public string Name => "sample";
    public string Description => "Reset the database and load a social network sample graph.";
    public string Usage => "sample";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count > 0) { await context.Console.WriteErrorAsync($"Usage: {Usage}"); return; }
        var runner = new GraphSampleRunner(context.Console, new GraphConsolePresenter(context.Console));
        await runner.RunAsync(context.Client, ct);
    }
}

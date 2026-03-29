internal sealed class SampleCommand : IReplCommand
{
    public string Name => "sample";

    public string Description => "Reset the database and run an end-to-end demo with IoT and stock data.";

    public string Usage => "sample";

    public async Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct)
    {
        if (arguments.Count > 0)
        {
            await context.Console.WriteErrorAsync($"Usage: {Usage}");
            return;
        }

        var runner = new TimeSeriesSampleRunner(context.Console, new TimeSeriesConsolePresenter(context.Console));
        await runner.RunAsync(context.Client, ct);
    }
}

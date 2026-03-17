internal interface IReplCommand
{
    string Name { get; }
    string Description { get; }
    string Usage { get; }
    Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct);
}

internal sealed class ReplCommandContext
{
    public ReplCommandContext(AnsiConsoleWriter console, string databasePath, IGraphApi client)
    {
        Console = console;
        DatabasePath = databasePath;
        Client = client;
    }

    public AnsiConsoleWriter Console { get; }
    public string DatabasePath { get; }
    public IGraphApi Client { get; }
}

internal interface IReplCommand
{
    string Name { get; }
    string Description { get; }
    string Usage { get; }

    Task ExecuteAsync(ReplCommandContext context, IReadOnlyList<string> arguments, CancellationToken ct);
}

internal sealed class ReplCommandContext
{
    public ReplCommandContext(AnsiConsoleWriter console, string databasePath, IVirtualFileSystemApi client)
    {
        Console = console;
        DatabasePath = databasePath;
        Client = client;
    }

    public AnsiConsoleWriter Console { get; }

    public string DatabasePath { get; }

    public IVirtualFileSystemApi Client { get; }

    public string CurrentDirectory { get; private set; } = "/";

    public string ResolvePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return CurrentDirectory;
        }

        var segments = new List<string>();
        if (!path.StartsWith('/'))
        {
            segments.AddRange(CurrentDirectory.Split('/', StringSplitOptions.RemoveEmptyEntries));
        }

        foreach (var segment in path.Split('/', StringSplitOptions.RemoveEmptyEntries))
        {
            if (segment == ".")
            {
                continue;
            }

            if (segment == "..")
            {
                if (segments.Count > 0)
                {
                    segments.RemoveAt(segments.Count - 1);
                }

                continue;
            }

            segments.Add(segment);
        }

        return segments.Count == 0 ? "/" : $"/{string.Join('/', segments)}";
    }

    public void ChangeDirectory(string path)
    {
        CurrentDirectory = ResolvePath(path);
    }

    public void ResetToRoot()
    {
        CurrentDirectory = "/";
    }
}

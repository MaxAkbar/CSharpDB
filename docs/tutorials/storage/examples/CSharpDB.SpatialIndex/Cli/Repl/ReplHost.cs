internal sealed class ReplHost
{
    private readonly IReadOnlyDictionary<string, IReplCommand> _commands;
    private readonly AnsiConsoleWriter _console;

    public ReplHost(IEnumerable<IReplCommand> commands, AnsiConsoleWriter console)
    {
        _commands = commands.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);
        _console = console;
    }

    public async Task RunAsync(ReplCommandContext context, CancellationToken ct)
    {
        await _console.WriteBannerAsync("CSharpDB Spatial Index REPL", "Type 'help' for commands, 'exit' to quit");
        await _console.WriteInfoAsync($"Database file: {context.DatabasePath}");
        await _console.WriteBlankLineAsync();

        while (!ct.IsCancellationRequested)
        {
            await _console.WritePromptAsync("spatialdb");
            var input = Console.ReadLine();
            if (input is null) break;

            var tokens = ReplTokenizer.Tokenize(input);
            if (tokens.Count == 0) continue;

            var name = tokens[0];
            if (name.Equals("exit", StringComparison.OrdinalIgnoreCase) || name.Equals("quit", StringComparison.OrdinalIgnoreCase))
            { await _console.WriteInfoAsync("Goodbye."); break; }

            if (name.Equals("help", StringComparison.OrdinalIgnoreCase))
            {
                await WriteHelpAsync();
                await _console.WriteBlankLineAsync();
                continue;
            }

            if (!_commands.TryGetValue(name, out var command))
            {
                await _console.WriteErrorAsync($"Unknown command '{name}'. Type 'help'.");
                await _console.WriteBlankLineAsync();
                continue;
            }

            try { await command.ExecuteAsync(context, tokens.Skip(1).ToArray(), ct); }
            catch (Exception ex) { await _console.WriteErrorAsync(ex.Message); }
            await _console.WriteBlankLineAsync();
        }
    }

    private async Task WriteHelpAsync()
    {
        await _console.WriteSectionAsync("Available commands");
        var rows = _commands.Values
            .OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase)
            .Select(c => new[] { c.Name, c.Description, c.Usage })
            .ToArray();
        await _console.WriteTableAsync(["Command", "Description", "Usage"], rows);
        await _console.WriteInfoAsync("Built-in commands: help, exit, quit");
    }
}

internal static class ReplTokenizer
{
    public static IReadOnlyList<string> Tokenize(string input)
    {
        var tokens = new List<string>();
        var buffer = new System.Text.StringBuilder();
        var quote = '\0'; var escape = false;

        foreach (var ch in input)
        {
            if (escape) { buffer.Append(ch); escape = false; continue; }
            if (ch == '\\') { escape = true; continue; }
            if (quote != '\0') { if (ch == quote) quote = '\0'; else buffer.Append(ch); continue; }
            if (ch is '"' or '\'') { quote = ch; continue; }
            if (char.IsWhiteSpace(ch)) { if (buffer.Length > 0) { tokens.Add(buffer.ToString()); buffer.Clear(); } continue; }
            buffer.Append(ch);
        }

        if (escape) buffer.Append('\\');
        if (buffer.Length > 0) tokens.Add(buffer.ToString());
        return tokens;
    }
}

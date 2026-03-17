internal sealed class ReplHost
{
    private readonly IReadOnlyDictionary<string, IReplCommand> _commands;
    private readonly AnsiConsoleWriter _console;

    public ReplHost(IEnumerable<IReplCommand> commands, AnsiConsoleWriter console)
    {
        _commands = commands.ToDictionary(command => command.Name, StringComparer.OrdinalIgnoreCase);
        _console = console;
    }

    public async Task RunAsync(ReplCommandContext context, CancellationToken ct)
    {
        await _console.WriteBannerAsync("CSharpDB Time-Series Database REPL", "Type 'help' for commands, 'exit' to quit");
        await _console.WriteInfoAsync($"Database file: {context.DatabasePath}");
        await _console.WriteBlankLineAsync();

        while (!ct.IsCancellationRequested)
        {
            await _console.WritePromptAsync("tsdb");
            var input = Console.ReadLine();
            if (input is null)
            {
                break;
            }

            var tokens = ReplTokenizer.Tokenize(input);
            if (tokens.Count == 0)
            {
                continue;
            }

            var commandName = tokens[0];
            if (commandName.Equals("exit", StringComparison.OrdinalIgnoreCase)
                || commandName.Equals("quit", StringComparison.OrdinalIgnoreCase))
            {
                await _console.WriteInfoAsync("Goodbye.");
                break;
            }

            if (commandName.Equals("help", StringComparison.OrdinalIgnoreCase))
            {
                await WriteHelpAsync();
                await _console.WriteBlankLineAsync();
                continue;
            }

            if (!_commands.TryGetValue(commandName, out var command))
            {
                await _console.WriteErrorAsync($"Unknown command '{commandName}'. Type 'help' to see available commands.");
                await _console.WriteBlankLineAsync();
                continue;
            }

            try
            {
                await command.ExecuteAsync(context, tokens.Skip(1).ToArray(), ct);
            }
            catch (Exception ex)
            {
                await _console.WriteErrorAsync(ex.Message);
            }

            await _console.WriteBlankLineAsync();
        }
    }

    private async Task WriteHelpAsync()
    {
        await _console.WriteSectionAsync("Available commands");
        var rows = _commands.Values
            .OrderBy(command => command.Name, StringComparer.OrdinalIgnoreCase)
            .Select(command => new[] { command.Name, command.Description, command.Usage })
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
        var quote = '\0';
        var escape = false;

        foreach (var ch in input)
        {
            if (escape)
            {
                buffer.Append(ch);
                escape = false;
                continue;
            }

            if (ch == '\\')
            {
                escape = true;
                continue;
            }

            if (quote != '\0')
            {
                if (ch == quote)
                {
                    quote = '\0';
                }
                else
                {
                    buffer.Append(ch);
                }

                continue;
            }

            if (ch is '"' or '\'')
            {
                quote = ch;
                continue;
            }

            if (char.IsWhiteSpace(ch))
            {
                if (buffer.Length > 0)
                {
                    tokens.Add(buffer.ToString());
                    buffer.Clear();
                }

                continue;
            }

            buffer.Append(ch);
        }

        if (escape)
        {
            buffer.Append('\\');
        }

        if (buffer.Length > 0)
        {
            tokens.Add(buffer.ToString());
        }

        return tokens;
    }
}

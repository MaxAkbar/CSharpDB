using CSharpDB.VirtualFS;

internal sealed class AnsiConsoleWriter
{
    private const string Reset = "\u001b[0m";
    private const string Bold = "\u001b[1m";
    private const string Dim = "\u001b[2m";
    private const string BrightBlue = "\u001b[94m";
    private const string BrightCyan = "\u001b[96m";
    private const string BrightGreen = "\u001b[92m";
    private const string BrightMagenta = "\u001b[95m";
    private const string BrightRed = "\u001b[91m";
    private const string BrightYellow = "\u001b[93m";
    private const string BrightWhite = "\u001b[97m";
    private const string Gray = "\u001b[90m";

    private readonly TextWriter _writer;
    private readonly bool _useAnsi;

    public AnsiConsoleWriter(TextWriter? writer = null)
    {
        _writer = writer ?? Console.Out;
        _useAnsi = !Console.IsOutputRedirected;
    }

    public async Task WriteBannerAsync(string title, string subtitle)
    {
        var width = Math.Max(title.Length, subtitle.Length) + 4;
        var border = new string('═', width);

        await WriteLineAsync($"╔{border}╗", BrightBlue);
        await WriteLineAsync($"║  {title.PadRight(width - 2)}║", BrightBlue, BrightWhite + Bold);
        await WriteLineAsync($"║  {subtitle.PadRight(width - 2)}║", BrightBlue, Gray);
        await WriteLineAsync($"╚{border}╝", BrightBlue);
        await WriteLineAsync();
    }

    public async Task WriteSectionAsync(string title)
    {
        await WriteLineAsync($"▶ {title}", BrightCyan + Bold);
        await WriteLineAsync(new string('─', title.Length + 2), Gray);
    }

    public Task WriteSuccessAsync(string message)
    {
        return WriteLineAsync($"✓ {message}", BrightGreen);
    }

    public Task WriteInfoAsync(string message)
    {
        return WriteLineAsync($"• {message}", Gray);
    }

    public Task WriteErrorAsync(string message)
    {
        return WriteLineAsync($"✗ {message}", BrightRed + Bold);
    }

    public Task WritePromptAsync(string name)
    {
        return WriteRawAsync($"{Style(name, BrightBlue + Bold)}{Style("> ", Gray)}");
    }

    public async Task WriteKeyValueAsync(string key, object? value)
    {
        var formatted = value?.ToString() ?? string.Empty;
        await WriteRawLineAsync($"  {Style(key.PadRight(10), BrightYellow + Bold)} {formatted}");
    }

    public async Task WriteTextBlockAsync(string title, string content)
    {
        await WriteLineAsync($"┌─ {title}", BrightMagenta + Bold);
        foreach (var line in content.Replace("\r", string.Empty).Split('\n'))
        {
            await WriteRawLineAsync($"│ {line}");
        }
        await WriteLineAsync("└", BrightMagenta);
    }

    public async Task WriteTableAsync(IReadOnlyList<string> headers, IReadOnlyList<string[]> rows)
    {
        var widths = new int[headers.Count];
        for (var i = 0; i < headers.Count; i++)
        {
            widths[i] = headers[i].Length;
        }

        foreach (var row in rows)
        {
            for (var i = 0; i < row.Length; i++)
            {
                widths[i] = Math.Max(widths[i], row[i].Length);
            }
        }

        await WriteRawLineAsync(BuildRow(headers.ToArray(), widths, BrightWhite + Bold));
        await WriteLineAsync(BuildSeparator(widths), Gray);
        foreach (var row in rows)
        {
            await WriteRawLineAsync(BuildRow(row, widths));
        }
    }

    public async Task WriteTreeAsync(IReadOnlyList<string> lines)
    {
        foreach (var line in lines)
        {
            await WriteRawLineAsync(ColorizeTreeLine(line));
        }
    }

    public Task WriteBlankLineAsync()
    {
        return _writer.WriteLineAsync();
    }

    private async Task WriteLineAsync(string value = "", string? color = null, string? contentStyle = null)
    {
        if (!_useAnsi || string.IsNullOrEmpty(color))
        {
            await _writer.WriteLineAsync(value);
            return;
        }

        if (string.IsNullOrEmpty(contentStyle))
        {
            await _writer.WriteLineAsync($"{color}{value}{Reset}");
            return;
        }

        var prefixLength = value.IndexOf('║');
        if (prefixLength >= 0 && value.EndsWith('║'))
        {
            var prefix = value[..(prefixLength + 1)];
            var suffix = "║";
            var content = value[(prefixLength + 1)..^1];
            await _writer.WriteLineAsync($"{color}{prefix}{Reset}{contentStyle}{content}{Reset}{color}{suffix}{Reset}");
            return;
        }

        await _writer.WriteLineAsync($"{color}{value}{Reset}");
    }

    private Task WriteRawLineAsync(string value)
    {
        return _writer.WriteLineAsync(value);
    }

    private Task WriteRawAsync(string value)
    {
        return _writer.WriteAsync(value);
    }

    private string ColorizeTreeLine(string line)
    {
        if (!_useAnsi)
        {
            return line;
        }

        return line
            .Replace("[DIR]", Style("[DIR]", BrightBlue + Bold), StringComparison.Ordinal)
            .Replace("[FILE]", Style("[FILE]", BrightGreen + Bold), StringComparison.Ordinal)
            .Replace("[LNK]", Style("[LNK]", BrightMagenta + Bold), StringComparison.Ordinal)
            .Replace("├──", Style("├──", Gray), StringComparison.Ordinal)
            .Replace("└──", Style("└──", Gray), StringComparison.Ordinal)
            .Replace("│", Style("│", Gray), StringComparison.Ordinal);
    }

    private string BuildRow(string[] columns, int[] widths, string? style = null)
    {
        var cells = new string[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            cells[i] = columns[i].PadRight(widths[i]);
        }

        var row = $"  {string.Join("  ", cells)}";
        return string.IsNullOrEmpty(style) ? row : Style(row, style);
    }

    private static string BuildSeparator(int[] widths)
    {
        var segments = widths.Select(width => new string('─', width)).ToArray();
        return $"  {string.Join("  ", segments)}";
    }

    private string Style(string value, string style)
    {
        return !_useAnsi ? value : $"{style}{value}{Reset}";
    }
}

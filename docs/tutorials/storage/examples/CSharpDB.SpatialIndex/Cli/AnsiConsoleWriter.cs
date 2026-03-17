internal sealed class AnsiConsoleWriter
{
    private const string Reset = "\u001b[0m";
    private const string Bold = "\u001b[1m";
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
        var border = new string('=', width);
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

    public Task WriteSuccessAsync(string message) => WriteLineAsync($"✓ {message}", BrightGreen);
    public Task WriteInfoAsync(string message) => WriteLineAsync($"• {message}", Gray);
    public Task WriteErrorAsync(string message) => WriteLineAsync($"✗ {message}", BrightRed + Bold);
    public Task WritePromptAsync(string name) => WriteRawAsync($"{Style(name, BrightBlue + Bold)}{Style("> ", Gray)}");

    public async Task WriteKeyValueAsync(string key, object? value)
    {
        await WriteRawLineAsync($"  {Style(key.PadRight(14), BrightYellow + Bold)} {value?.ToString() ?? string.Empty}");
    }

    public async Task WriteTableAsync(IReadOnlyList<string> headers, IReadOnlyList<string[]> rows)
    {
        var widths = new int[headers.Count];
        for (var i = 0; i < headers.Count; i++) widths[i] = headers[i].Length;
        foreach (var row in rows)
            for (var i = 0; i < row.Length; i++)
                widths[i] = Math.Max(widths[i], row[i].Length);

        await WriteRawLineAsync(BuildRow(headers.ToArray(), widths, BrightWhite + Bold));
        await WriteLineAsync(BuildSeparator(widths), Gray);
        foreach (var row in rows) await WriteRawLineAsync(BuildRow(row, widths));
    }

    public Task WriteBlankLineAsync() => _writer.WriteLineAsync();

    private async Task WriteLineAsync(string value = "", string? color = null, string? contentStyle = null)
    {
        if (!_useAnsi || string.IsNullOrEmpty(color)) { await _writer.WriteLineAsync(value); return; }
        if (string.IsNullOrEmpty(contentStyle)) { await _writer.WriteLineAsync($"{color}{value}{Reset}"); return; }
        var idx = value.IndexOf('║');
        if (idx >= 0 && value.EndsWith('║'))
        {
            var prefix = value[..(idx + 1)]; var content = value[(idx + 1)..^1];
            await _writer.WriteLineAsync($"{color}{prefix}{Reset}{contentStyle}{content}{Reset}{color}║{Reset}");
            return;
        }
        await _writer.WriteLineAsync($"{color}{value}{Reset}");
    }

    private Task WriteRawLineAsync(string value) => _writer.WriteLineAsync(value);
    private Task WriteRawAsync(string value) => _writer.WriteAsync(value);

    private string BuildRow(string[] columns, int[] widths, string? style = null)
    {
        var cells = new string[columns.Length];
        for (var i = 0; i < columns.Length; i++) cells[i] = columns[i].PadRight(widths[i]);
        var row = $"  {string.Join("  ", cells)}";
        return string.IsNullOrEmpty(style) ? row : Style(row, style);
    }

    private static string BuildSeparator(int[] widths)
    {
        return $"  {string.Join("  ", widths.Select(w => new string('─', w)))}";
    }

    private string Style(string value, string style) => !_useAnsi ? value : $"{style}{value}{Reset}";
}

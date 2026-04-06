using CSharpDB.Client;
using System.Diagnostics;
using System.Text;
using CSharpDB.Primitives;
using CSharpDB.Engine;
using CSharpDB.Sql;
using Spectre.Console;

namespace CSharpDB.Cli;

/// <summary>
/// The Read-Eval-Print Loop. Reads SQL and meta-commands, executes them,
/// and writes formatted output.
/// </summary>
internal sealed class Repl : IDisposable
{
    private readonly TextWriter _output;
    private readonly IAnsiConsole _console;
    private readonly TableFormatter _tableFormatter;
    private readonly Dictionary<string, IMetaCommand> _commands;
    private readonly IReadOnlyList<MetaCommandMenuItem> _menuItems;
    private readonly MetaCommandContext _context;

    public Repl(ICSharpDbClient client, Database? localDatabase, string databasePath, TextWriter output, IReadOnlyList<IMetaCommand> commands)
    {
        _output = output;
        _console = CliConsole.Create(output, interactive: true);
        _tableFormatter = new TableFormatter(_console);
        _context = new MetaCommandContext(client, localDatabase, databasePath, ExecuteSqlAsync);

        _commands = new Dictionary<string, IMetaCommand>(StringComparer.OrdinalIgnoreCase);
        foreach (var cmd in commands)
        {
            foreach (var alias in cmd.Aliases)
                _commands[alias] = cmd;
        }

        _menuItems = BuildMenuItems(commands);
    }

    public async Task RunAsync(CancellationToken ct = default)
    {
        var sqlBuffer = new StringBuilder();
        bool hasPendingSql = false;

        while (true)
        {
            CliConsole.WritePrompt(_console, hasPendingSql);
            string? line = ReadInputLine(hasPendingSql);

            if (line is null) // EOF (Ctrl+D / Ctrl+Z)
                break;

            if (string.IsNullOrWhiteSpace(line) && !hasPendingSql)
                continue;

            // Meta-commands (only when not mid-SQL)
            if (!hasPendingSql && line.TrimStart().StartsWith('.'))
            {
                if (await HandleMetaCommandAsync(line.Trim(), ct))
                    break;

                _console.WriteLine();
                continue;
            }

            sqlBuffer.AppendLine(line);
            hasPendingSql = true;

            string bufferedSql = sqlBuffer.ToString();
            bool splitOk = SqlScriptSplitter.TrySplitCompleteStatements(
                bufferedSql,
                out var statements,
                out var remainder,
                out _);

            if (!splitOk)
            {
                // If user ended with ';', execute anyway to surface parse errors.
                if (bufferedSql.TrimEnd().EndsWith(';'))
                {
                    await ExecuteSqlAsync(bufferedSql.Trim(), ct);
                    sqlBuffer.Clear();
                    hasPendingSql = false;
                }

                continue;
            }

            foreach (var statement in statements)
                await ExecuteSqlAsync(statement, ct);

            sqlBuffer.Clear();
            sqlBuffer.Append(remainder);
            hasPendingSql = !string.IsNullOrWhiteSpace(remainder);
        }

        CliConsole.WriteMuted(_console, "Bye.");
    }

    private string? ReadInputLine(bool hasPendingSql)
    {
        if (Console.IsInputRedirected || Console.In is StringReader || hasPendingSql)
            return Console.ReadLine();

        ConsoleKeyInfo firstKey = Console.ReadKey(intercept: true);

        if (firstKey.Key == ConsoleKey.Enter)
        {
            Console.WriteLine();
            return string.Empty;
        }

        if (firstKey.Key == ConsoleKey.Z && firstKey.Modifiers.HasFlag(ConsoleModifiers.Control))
            return null;

        if (firstKey.KeyChar == '.' && firstKey.Modifiers == 0)
        {
            Console.WriteLine();
            MetaCommandMenuItem? selected = TryShowMetaCommandMenu();
            return selected?.CommandText ?? string.Empty;
        }

        if (!char.IsControl(firstKey.KeyChar))
            Console.Write(firstKey.KeyChar);

        string? remainder = Console.ReadLine();
        if (remainder is null)
            return null;

        return char.IsControl(firstKey.KeyChar)
            ? remainder
            : string.Concat(firstKey.KeyChar, remainder);
    }

    /// <summary>
    /// Executes a SQL statement and prints formatted output.
    /// Returns true on success.
    /// </summary>
    public async ValueTask<bool> ExecuteSqlAsync(string sql, CancellationToken ct = default)
    {
        Stopwatch? sw = _context.ShowTiming ? Stopwatch.StartNew() : null;

        SqlStatementClassification classification;
        try
        {
            classification = SqlStatementClassifier.Classify(sql);
        }
        catch (CSharpDbException ex)
        {
            CliConsole.WriteError(_console, $"[{ex.Code}] {ex.Message}");
            _console.WriteLine();
            return false;
        }

        try
        {
            if (_context.SnapshotEnabled && !classification.IsReadOnly)
            {
                CliConsole.WriteWarning(
                    _console,
                    "Snapshot mode is read-only. Run '.snapshot off' to execute writes.");
                _console.WriteLine();
                return false;
            }

            if (_context.SnapshotEnabled)
            {
                await using var snapshotResult = await _context.ExecuteReadSnapshotAsync(sql, ct);

                sw?.Stop();
                string timingSuffix = _context.ShowTiming && sw is not null
                    ? $" ({sw.ElapsedMilliseconds}ms)"
                    : string.Empty;

                if (snapshotResult.IsQuery)
                {
                    var rows = await snapshotResult.ToListAsync(ct);
                    if (snapshotResult.Schema.Length > 0)
                        _tableFormatter.PrintTable(snapshotResult.Schema, rows);

                    int count = rows.Count;
                    CliConsole.WriteMuted(
                        _console,
                        $"{count} {(count == 1 ? "row" : "rows")}{timingSuffix}");
                }
                else
                {
                    int n = snapshotResult.RowsAffected;
                    CliConsole.WriteMuted(
                        _console,
                        $"{n} {(n == 1 ? "row" : "rows")} affected{timingSuffix}");
                }
            }
            else
            {
                var result = await _context.ExecuteDbSqlAsync(sql, ct);

                sw?.Stop();
                string timingSuffix = _context.ShowTiming && sw is not null
                    ? $" ({sw.ElapsedMilliseconds}ms)"
                    : string.Empty;

                if (!string.IsNullOrWhiteSpace(result.Error))
                {
                    CliConsole.WriteError(_console, result.Error);
                    _console.WriteLine();
                    return false;
                }

                if (result.IsQuery)
                {
                    var rows = result.Rows ?? [];
                    var columns = result.ColumnNames ?? [];
                    if (columns.Length > 0)
                        _tableFormatter.PrintTable(columns, rows);

                    int count = rows.Count;
                    CliConsole.WriteMuted(
                        _console,
                        $"{count} {(count == 1 ? "row" : "rows")}{timingSuffix}");
                }
                else
                {
                    int n = result.RowsAffected;
                    CliConsole.WriteMuted(
                        _console,
                        $"{n} {(n == 1 ? "row" : "rows")} affected{timingSuffix}");
                }
            }

            _console.WriteLine();
            return true;
        }
        catch (CSharpDbException ex)
        {
            CliConsole.WriteError(_console, $"[{ex.Code}] {ex.Message}");
            _console.WriteLine();
            return false;
        }
        catch (Exception ex)
        {
            CliConsole.WriteError(_console, ex.Message);
            _console.WriteLine();
            return false;
        }
    }

    /// <returns>true if the REPL should exit</returns>
    private async ValueTask<bool> HandleMetaCommandAsync(string input, CancellationToken ct)
    {
        if (input == ".")
            return await HandleMetaCommandAsync(".help", ct);

        var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string cmd = parts[0];
        string arg = parts.Length > 1 ? parts[1] : string.Empty;

        if (cmd is ".quit" or ".exit")
            return true;

        if (_commands.TryGetValue(cmd, out var command))
        {
            try
            {
                await command.ExecuteAsync(_context, arg, _output, ct);
            }
            catch (CSharpDbException ex)
            {
                CliConsole.WriteError(_console, $"[{ex.Code}] {ex.Message}");
            }
            catch (Exception ex)
            {
                CliConsole.WriteError(_console, ex.Message);
            }
        }
        else
        {
            CliConsole.WriteError(
                _console,
                $"Unknown command: {cmd}. Type .help for available commands.");
        }

        return false;
    }

    private MetaCommandMenuItem? TryShowMetaCommandMenu()
    {
        int width = GetMenuWidth();
        int requestedTop = GetCursorTop();
        int selectedIndex = 0;
        bool restoreCursorVisibility = false;
        bool originalCursorVisible = false;
        ConsoleColor originalForeground = Console.ForegroundColor;
        ConsoleColor originalBackground = Console.BackgroundColor;

        try
        {
            if (OperatingSystem.IsWindows())
            {
                originalCursorVisible = Console.CursorVisible;
                restoreCursorVisibility = true;
                Console.CursorVisible = false;
            }

            while (true)
            {
                MenuLayout layout = CalculateMenuLayout(
                    requestedTop,
                    selectedIndex,
                    _menuItems.Count,
                    GetConsoleBufferHeight());

                RenderMetaCommandMenu(layout, width, selectedIndex);

                ConsoleKeyInfo key = Console.ReadKey(intercept: true);
                switch (key.Key)
                {
                    case ConsoleKey.UpArrow:
                        selectedIndex = selectedIndex == 0 ? _menuItems.Count - 1 : selectedIndex - 1;
                        break;
                    case ConsoleKey.DownArrow:
                        selectedIndex = selectedIndex == _menuItems.Count - 1 ? 0 : selectedIndex + 1;
                        break;
                    case ConsoleKey.Enter:
                        ClearMetaCommandMenu(layout.Top, width, layout.LineCount, requestedTop);
                        return _menuItems[selectedIndex];
                    case ConsoleKey.Escape:
                        ClearMetaCommandMenu(layout.Top, width, layout.LineCount, requestedTop);
                        return null;
                }
            }
        }
        finally
        {
            if (restoreCursorVisibility && OperatingSystem.IsWindows())
                Console.CursorVisible = originalCursorVisible;
            Console.ForegroundColor = originalForeground;
            Console.BackgroundColor = originalBackground;
        }
    }

    private void RenderMetaCommandMenu(MenuLayout layout, int width, int selectedIndex)
    {
        WriteMenuLine(layout.Top, width, "Dot Commands", isSelected: false, ConsoleColor.Cyan, ConsoleColor.Black);
        WriteMenuLine(layout.Top + 1, width, "Use Up/Down to select, Enter to run, Esc to cancel.", isSelected: false, ConsoleColor.DarkGray, ConsoleColor.Black);
        WriteMenuLine(layout.Top + 2, width, string.Empty, isSelected: false, ConsoleColor.Gray, ConsoleColor.Black);

        for (int i = 0; i < layout.VisibleItemCount; i++)
        {
            int itemIndex = layout.WindowStart + i;
            MetaCommandMenuItem item = _menuItems[itemIndex];
            string text = $"> {item.DisplayText} - {item.Description}";
            WriteMenuLine(
                layout.Top + 3 + i,
                width,
                text,
                isSelected: itemIndex == selectedIndex,
                foreground: itemIndex == selectedIndex ? ConsoleColor.Black : ConsoleColor.Gray,
                background: itemIndex == selectedIndex ? ConsoleColor.Cyan : ConsoleColor.Black);
        }
    }

    private static void ClearMetaCommandMenu(int top, int width, int lineCount, int returnTop)
    {
        for (int i = 0; i < lineCount; i++)
            WriteMenuLine(top + i, width, string.Empty, isSelected: false, ConsoleColor.Gray, ConsoleColor.Black);

        int safeReturnTop = Math.Clamp(returnTop, 0, GetConsoleBufferHeight() - 1);
        Console.SetCursorPosition(0, safeReturnTop);
    }

    private static void WriteMenuLine(
        int row,
        int width,
        string text,
        bool isSelected,
        ConsoleColor foreground,
        ConsoleColor background)
    {
        int bufferHeight = GetConsoleBufferHeight();
        if (row < 0 || row >= bufferHeight)
            return;

        int safeWidth = Math.Max(1, width - 1);
        string rendered = text.Length > safeWidth
            ? safeWidth > 3
                ? text[..(safeWidth - 3)] + "..."
                : text[..safeWidth]
            : text;

        Console.SetCursorPosition(0, row);
        Console.ForegroundColor = foreground;
        Console.BackgroundColor = background;
        Console.Write(rendered.PadRight(safeWidth));
        Console.ResetColor();
    }

    private static int GetMenuWidth()
    {
        try
        {
            return Console.BufferWidth > 0 ? Console.BufferWidth : 100;
        }
        catch
        {
            return 100;
        }
    }

    internal static MenuLayout CalculateMenuLayout(int requestedTop, int selectedIndex, int itemCount, int bufferHeight)
    {
        int safeBufferHeight = Math.Max(4, bufferHeight);
        int safeItemCount = Math.Max(1, itemCount);
        int visibleItemCount = Math.Min(safeItemCount, Math.Max(1, safeBufferHeight - 3));
        int lineCount = visibleItemCount + 3;
        int maxTop = Math.Max(0, safeBufferHeight - lineCount);
        int clampedSelectedIndex = Math.Clamp(selectedIndex, 0, safeItemCount - 1);
        int windowStart = 0;

        if (safeItemCount > visibleItemCount)
        {
            windowStart = clampedSelectedIndex - visibleItemCount + 1;
            windowStart = Math.Clamp(windowStart, 0, safeItemCount - visibleItemCount);
        }

        int top = Math.Clamp(requestedTop, 0, maxTop);
        return new MenuLayout(top, lineCount, visibleItemCount, windowStart);
    }

    private static int GetConsoleBufferHeight()
    {
        try
        {
            return Console.BufferHeight > 0 ? Console.BufferHeight : 40;
        }
        catch
        {
            return 40;
        }
    }

    private static int GetCursorTop()
    {
        try
        {
            return Console.CursorTop;
        }
        catch
        {
            return 0;
        }
    }

    private static IReadOnlyList<MetaCommandMenuItem> BuildMenuItems(IReadOnlyList<IMetaCommand> commands)
    {
        var items = new List<MetaCommandMenuItem>
        {
            new(".help", ".help", "Show this help message"),
            new(".quit", ".quit", "Exit the shell"),
            new(".exit", ".exit", "Alias for .quit"),
        };

        foreach (IMetaCommand command in commands.OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase))
        {
            string commandText = command.Aliases.FirstOrDefault() ?? command.Name;
            if (items.Any(item => item.CommandText.Equals(commandText, StringComparison.OrdinalIgnoreCase)))
                continue;

            items.Add(new MetaCommandMenuItem(commandText, command.Name, command.Description));
        }

        return items;
    }

    public void Dispose()
    {
        _context.Dispose();
    }

    internal readonly record struct MenuLayout(int Top, int LineCount, int VisibleItemCount, int WindowStart);
    private sealed record MetaCommandMenuItem(string CommandText, string DisplayText, string Description);
}

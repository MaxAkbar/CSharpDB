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
            string? line = await ReadInputLineAsync(hasPendingSql, ct);

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

    private ValueTask<string?> ReadInputLineAsync(bool hasPendingSql, CancellationToken ct)
    {
        if (Console.IsInputRedirected || Console.In is StringReader)
            return ValueTask.FromResult<string?>(Console.ReadLine());

        return ReadInteractiveInputLineAsync(hasPendingSql, ct);
    }

    private async ValueTask<string?> ReadInteractiveInputLineAsync(bool hasPendingSql, CancellationToken ct)
    {
        var buffer = new StringBuilder();
        int promptLeft = GetCursorLeft();
        int promptTop = GetCursorTop();
        int renderedLength = 0;

        while (true)
        {
            ConsoleKeyInfo key = Console.ReadKey(intercept: true);

            if (key.Key == ConsoleKey.Enter)
            {
                Console.WriteLine();
                return buffer.ToString();
            }

            if (key.Key == ConsoleKey.Z && key.Modifiers.HasFlag(ConsoleModifiers.Control) && buffer.Length == 0)
                return null;

            if (key.Key == ConsoleKey.Backspace)
            {
                if (buffer.Length == 0)
                    continue;

                buffer.Length--;
                RenderInteractiveInput(promptLeft, promptTop, buffer.ToString(), ref renderedLength);
                continue;
            }

            if (!hasPendingSql && buffer.Length == 0 && key.KeyChar == '.' && key.Modifiers == 0)
            {
                Console.WriteLine();
                MetaCommandMenuItem? selected = TryShowMetaCommandMenu();
                if (selected is null)
                    return ResetPromptAfterMenuCancel(promptLeft, promptTop, ref renderedLength);

                SetCursorPosition(promptLeft, promptTop);
                RenderInteractiveInput(promptLeft, promptTop, string.Empty, ref renderedLength);

                string? resolved = await ResolveMenuSelectionAsync(selected, ct);
                return string.IsNullOrWhiteSpace(resolved)
                    ? ResetPromptAfterMenuCancel(promptLeft, promptTop, ref renderedLength)
                    : ReturnResolvedMenuCommand(resolved);
            }

            if (key.Key is ConsoleKey.LeftArrow or ConsoleKey.RightArrow or ConsoleKey.UpArrow or ConsoleKey.DownArrow)
                continue;

            if (char.IsControl(key.KeyChar))
                continue;

            buffer.Append(key.KeyChar);
            RenderInteractiveInput(promptLeft, promptTop, buffer.ToString(), ref renderedLength);
        }
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
        var choices = _menuItems
            .Select(item => new MenuChoice<MetaCommandMenuItem>(
                $"> {item.DisplayText} - {item.Description}",
                item))
            .ToArray();

        return TryShowMenu(
            "Dot Commands",
            "Use Up/Down to select, Enter to choose, Esc to cancel.",
            choices);
    }

    private async ValueTask<string?> ResolveMenuSelectionAsync(MetaCommandMenuItem selected, CancellationToken ct)
    {
        return selected.CommandText switch
        {
            ".view" => await ResolveNamedCommandAsync(
                commandText: ".view",
                title: "Views",
                subtitle: "Select a view to show its SQL.",
                values: await _context.Client.GetViewNamesAsync(ct),
                emptyFallbackCommand: ".views"),
            ".trigger" => await ResolveNamedCommandAsync(
                commandText: ".trigger",
                title: "Triggers",
                subtitle: "Select a trigger to show its SQL.",
                values: (await _context.Client.GetTriggersAsync(ct)).Select(trigger => trigger.TriggerName),
                emptyFallbackCommand: ".triggers"),
            ".schema" => await ResolveSchemaCommandAsync(ct),
            ".tables" => ResolveSimpleChoiceCommand(
                "Tables",
                "Choose which tables to list.",
                new MenuChoice<string>("> User tables", ".tables"),
                new MenuChoice<string>("> All tables (including internal)", ".tables --all")),
            ".indexes" => await ResolveTableScopedCommandAsync(
                commandText: ".indexes",
                title: "Indexes",
                subtitle: "Choose a table to filter indexes, or list them all.",
                allChoiceLabel: "> All indexes",
                allCommand: ".indexes",
                ct),
            ".triggers" => await ResolveTableScopedCommandAsync(
                commandText: ".triggers",
                title: "Triggers",
                subtitle: "Choose a table to filter triggers, or list them all.",
                allChoiceLabel: "> All triggers",
                allCommand: ".triggers",
                ct),
            ".read" => ResolvePathCommand(".read", "SQL script file"),
            ".backup" => ResolveBackupCommand(),
            ".restore" => ResolveRestoreCommand(),
            ".snapshot" => ResolveToggleCommand(".snapshot", "Snapshot Mode"),
            ".syncpoint" => ResolveToggleCommand(".syncpoint", "Sync Point Mode"),
            ".timing" => ResolveToggleCommand(".timing", "Timing"),
            ".reindex" => await ResolveReindexCommandAsync(ct),
            ".migrate-fks" => ResolveMigrateForeignKeysCommand(),
            _ => selected.CommandText,
        };
    }

    private async ValueTask<string> ResolveNamedCommandAsync(
        string commandText,
        string title,
        string subtitle,
        IEnumerable<string> values,
        string emptyFallbackCommand)
    {
        string[] names = values
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (names.Length == 0)
            return emptyFallbackCommand;

        var choices = names
            .Select(name => new MenuChoice<string>(
                $"> {name}",
                $"{commandText} {QuoteCommandArgument(name)}"))
            .ToArray();

        return TryShowMenu(title, subtitle, choices) ?? string.Empty;
    }

    private async ValueTask<string> ResolveSchemaCommandAsync(CancellationToken ct)
    {
        var tableNames = await MetaCommandHelpers.GetUserTableNamesAsync(_context, ct);
        var choices = new List<MenuChoice<string>>
        {
            new("> All user tables", ".schema"),
            new("> All tables (including internal)", ".schema --all"),
        };

        choices.AddRange(tableNames
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => new MenuChoice<string>(
                $"> {name}",
                $".schema {QuoteCommandArgument(name)}")));

        return TryShowMenu(
                   "Schema",
                   "Choose a table or schema listing scope.",
                   choices)
               ?? string.Empty;
    }

    private async ValueTask<string> ResolveTableScopedCommandAsync(
        string commandText,
        string title,
        string subtitle,
        string allChoiceLabel,
        string allCommand,
        CancellationToken ct)
    {
        var tableNames = await MetaCommandHelpers.GetUserTableNamesAsync(_context, ct);
        var choices = new List<MenuChoice<string>>
        {
            new(allChoiceLabel, allCommand),
        };

        choices.AddRange(tableNames
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => new MenuChoice<string>(
                $"> {name}",
                $"{commandText} {QuoteCommandArgument(name)}")));

        return TryShowMenu(title, subtitle, choices) ?? string.Empty;
    }

    private string ResolvePathCommand(string commandText, string label)
    {
        string? path = PromptForTextInput($"{label}: ");
        return string.IsNullOrWhiteSpace(path)
            ? string.Empty
            : $"{commandText} {QuoteCommandArgument(path)}";
    }

    private string ResolveBackupCommand()
    {
        string? path = PromptForTextInput("Backup file path: ");
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        string baseCommand = $".backup {QuoteCommandArgument(path)}";
        return ResolveSimpleChoiceCommand(
                   "Backup Options",
                   "Choose the backup mode.",
                   new MenuChoice<string>("> Backup only", baseCommand),
                   new MenuChoice<string>("> Backup with manifest", baseCommand + " --with-manifest"))
               ?? string.Empty;
    }

    private string ResolveRestoreCommand()
    {
        string? path = PromptForTextInput("Restore source file: ");
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        string baseCommand = $".restore {QuoteCommandArgument(path)}";
        return ResolveSimpleChoiceCommand(
                   "Restore Options",
                   "Choose validation or full restore.",
                   new MenuChoice<string>("> Restore into current database", baseCommand),
                   new MenuChoice<string>("> Validate only", baseCommand + " --validate-only"))
               ?? string.Empty;
    }

    private string ResolveToggleCommand(string commandText, string title)
    {
        return ResolveSimpleChoiceCommand(
                   title,
                   "Choose a mode.",
                   new MenuChoice<string>("> Status", commandText),
                   new MenuChoice<string>("> Turn on", commandText + " on"),
                   new MenuChoice<string>("> Turn off", commandText + " off"))
               ?? string.Empty;
    }

    private async ValueTask<string> ResolveReindexCommandAsync(CancellationToken ct)
    {
        string? scope = ResolveSimpleChoiceCommand(
            "Reindex",
            "Choose what to rebuild.",
            new MenuChoice<string>("> All indexes", ".reindex --all"),
            new MenuChoice<string>("> One table", "__table__"),
            new MenuChoice<string>("> One index", "__index__"));

        if (string.IsNullOrWhiteSpace(scope))
            return string.Empty;

        if (scope == "__table__")
        {
            var tableNames = await MetaCommandHelpers.GetUserTableNamesAsync(_context, ct);
            var choices = tableNames
                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                .Select(name => new MenuChoice<string>(
                    $"> {name}",
                    $".reindex --table {QuoteCommandArgument(name)}"))
                .ToArray();

            return choices.Length == 0
                ? ".reindex --all"
                : TryShowMenu("Reindex Table", "Select a table to reindex.", choices) ?? string.Empty;
        }

        if (scope == "__index__")
        {
            var indexes = await _context.Client.GetIndexesAsync(ct);
            var choices = indexes
                .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .Select(index => new MenuChoice<string>(
                    $"> {index.IndexName} ({index.TableName})",
                    $".reindex --index {QuoteCommandArgument(index.IndexName)}"))
                .ToArray();

            return choices.Length == 0
                ? ".reindex --all"
                : TryShowMenu("Reindex Index", "Select an index to rebuild.", choices) ?? string.Empty;
        }

        return scope;
    }

    private string ResolveMigrateForeignKeysCommand()
    {
        string? specPath = PromptForTextInput("Foreign key spec file: ");
        if (string.IsNullOrWhiteSpace(specPath))
            return string.Empty;

        string baseCommand = $".migrate-fks {QuoteCommandArgument(specPath)}";
        return ResolveSimpleChoiceCommand(
                   "Foreign Key Migration",
                   "Choose validation or apply.",
                   new MenuChoice<string>("> Apply migration", baseCommand),
                   new MenuChoice<string>("> Validate only", baseCommand + " --validate-only"))
               ?? string.Empty;
    }

    private string? ResolveSimpleChoiceCommand(string title, string subtitle, params MenuChoice<string>[] choices)
        => TryShowMenu(title, subtitle, choices);

    private string? PromptForTextInput(string prompt)
    {
        int row = GetCursorTop();
        int width = GetMenuWidth();
        WriteMenuLine(row, width, prompt, isSelected: false, ConsoleColor.Gray, ConsoleColor.Black);
        SetCursorPosition(Math.Min(prompt.Length, Math.Max(0, width - 1)), row);
        return Console.ReadLine()?.Trim();
    }

    private T? TryShowMenu<T>(string title, string subtitle, IReadOnlyList<MenuChoice<T>> choices)
        where T : class
    {
        if (choices.Count == 0)
            return null;

        int width = GetMenuWidth();
        int requestedTop = GetCursorTop();
        int selectedIndex = 0;
        string searchText = string.Empty;
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
                    choices.Count,
                    GetConsoleBufferHeight());

                RenderChoiceMenu(layout, width, selectedIndex, title, subtitle, searchText, choices);

                ConsoleKeyInfo key = Console.ReadKey(intercept: true);
                switch (key.Key)
                {
                    case ConsoleKey.UpArrow:
                        selectedIndex = selectedIndex == 0 ? choices.Count - 1 : selectedIndex - 1;
                        break;
                    case ConsoleKey.DownArrow:
                        selectedIndex = selectedIndex == choices.Count - 1 ? 0 : selectedIndex + 1;
                        break;
                    case ConsoleKey.Backspace:
                        if (searchText.Length > 0)
                        {
                            searchText = searchText[..^1];
                            selectedIndex = FindMenuMatchIndex(choices, searchText, selectedIndex);
                        }
                        break;
                    case ConsoleKey.Enter:
                        ClearMetaCommandMenu(layout.Top, width, layout.LineCount, requestedTop);
                        return choices[selectedIndex].Value;
                    case ConsoleKey.Escape:
                        ClearMetaCommandMenu(layout.Top, width, layout.LineCount, requestedTop);
                        return null;
                    default:
                        if (!char.IsControl(key.KeyChar))
                        {
                            searchText += key.KeyChar;
                            selectedIndex = FindMenuMatchIndex(choices, searchText, selectedIndex);
                        }
                        break;
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

    private static void RenderChoiceMenu<T>(
        MenuLayout layout,
        int width,
        int selectedIndex,
        string title,
        string subtitle,
        string searchText,
        IReadOnlyList<MenuChoice<T>> choices)
        where T : class
    {
        WriteMenuLine(layout.Top, width, title, isSelected: false, ConsoleColor.Cyan, ConsoleColor.Black);
        string subtitleText = string.IsNullOrWhiteSpace(searchText)
            ? subtitle
            : $"{subtitle}  Filter: {searchText}";
        WriteMenuLine(layout.Top + 1, width, subtitleText, isSelected: false, ConsoleColor.DarkGray, ConsoleColor.Black);
        WriteMenuLine(layout.Top + 2, width, string.Empty, isSelected: false, ConsoleColor.Gray, ConsoleColor.Black);

        for (int i = 0; i < layout.VisibleItemCount; i++)
        {
            int itemIndex = layout.WindowStart + i;
            WriteMenuLine(
                layout.Top + 3 + i,
                width,
                choices[itemIndex].Text,
                isSelected: itemIndex == selectedIndex,
                foreground: itemIndex == selectedIndex ? ConsoleColor.Black : ConsoleColor.Gray,
                background: itemIndex == selectedIndex ? ConsoleColor.Cyan : ConsoleColor.Black);
        }
    }

    internal static int FindMenuMatchIndex<T>(IReadOnlyList<MenuChoice<T>> choices, string searchText, int currentIndex)
        where T : class
    {
        if (choices.Count == 0 || string.IsNullOrWhiteSpace(searchText))
            return Math.Clamp(currentIndex, 0, Math.Max(0, choices.Count - 1));

        string normalizedSearch = searchText.Trim();
        for (int i = 0; i < choices.Count; i++)
        {
            if (NormalizeMenuSearchText(choices[i].Text).StartsWith(normalizedSearch, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        for (int i = 0; i < choices.Count; i++)
        {
            if (NormalizeMenuSearchText(choices[i].Text).Contains(normalizedSearch, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return Math.Clamp(currentIndex, 0, Math.Max(0, choices.Count - 1));
    }

    internal static string NormalizeMenuSearchText(string text)
    {
        string normalized = text.Trim();
        if (normalized.StartsWith("> ", StringComparison.Ordinal))
            normalized = normalized[2..];

        int descriptionSeparator = normalized.IndexOf(" - ", StringComparison.Ordinal);
        if (descriptionSeparator >= 0)
            normalized = normalized[..descriptionSeparator];

        return normalized.Trim();
    }

    private static void RenderInteractiveInput(int promptLeft, int promptTop, string text, ref int renderedLength)
    {
        SetCursorPosition(promptLeft, promptTop);
        int clearLength = Math.Max(renderedLength, text.Length);
        if (clearLength > 0)
            Console.Write(new string(' ', clearLength));

        SetCursorPosition(promptLeft, promptTop);
        if (text.Length > 0)
            Console.Write(text);
        renderedLength = text.Length;
    }

    private static string ResetPromptAfterMenuCancel(int promptLeft, int promptTop, ref int renderedLength)
    {
        SetCursorPosition(promptLeft, promptTop);
        RenderInteractiveInput(promptLeft, promptTop, string.Empty, ref renderedLength);
        Console.WriteLine();
        return string.Empty;
    }

    private static string ReturnResolvedMenuCommand(string resolved)
    {
        if (GetCursorLeft() > 0)
            Console.WriteLine();

        return resolved;
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

    private static int GetCursorLeft()
    {
        try
        {
            return Console.CursorLeft;
        }
        catch
        {
            return 0;
        }
    }

    private static void SetCursorPosition(int left, int top)
    {
        try
        {
            int safeTop = Math.Clamp(top, 0, GetConsoleBufferHeight() - 1);
            int safeLeft = Math.Clamp(left, 0, Math.Max(0, GetMenuWidth() - 1));
            Console.SetCursorPosition(safeLeft, safeTop);
        }
        catch
        {
            // Ignore cursor-position failures and let subsequent console writes proceed normally.
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

    private static string QuoteCommandArgument(string value)
        => value.IndexOfAny([' ', '\t']) >= 0 ? $"\"{value}\"" : value;

    public void Dispose()
    {
        _context.Dispose();
    }

    internal readonly record struct MenuLayout(int Top, int LineCount, int VisibleItemCount, int WindowStart);
    internal sealed record MenuChoice<T>(string Text, T Value) where T : class;
    private sealed record MetaCommandMenuItem(string CommandText, string DisplayText, string Description);
}

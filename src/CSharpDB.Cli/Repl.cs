using System.Diagnostics;
using System.Text;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Sql;

namespace CSharpDB.Cli;

/// <summary>
/// The Read-Eval-Print Loop. Reads SQL and meta-commands, executes them,
/// and writes formatted output.
/// </summary>
internal sealed class Repl : IDisposable
{
    private readonly Database _db;
    private readonly TextWriter _out;
    private readonly TableFormatter _tableFormatter;
    private readonly Dictionary<string, IMetaCommand> _commands;
    private readonly MetaCommandContext _context;

    public Repl(Database db, string databasePath, TextWriter output, IReadOnlyList<IMetaCommand> commands)
    {
        _db = db;
        _out = output;
        _tableFormatter = new TableFormatter(output);
        _context = new MetaCommandContext(db, databasePath, ExecuteSqlAsync);

        _commands = new Dictionary<string, IMetaCommand>(StringComparer.OrdinalIgnoreCase);
        foreach (var cmd in commands)
        {
            foreach (var alias in cmd.Aliases)
                _commands[alias] = cmd;
        }
    }

    public async Task RunAsync(CancellationToken ct = default)
    {
        var sqlBuffer = new StringBuilder();
        bool hasPendingSql = false;

        while (true)
        {
            string prompt = hasPendingSql
                ? Ansi.Colorize("...> ", Ansi.Dim)
                : Ansi.Colorize("csdb", Ansi.Cyan) + Ansi.Colorize("> ", Ansi.Dim);

            _out.Write(prompt);
            string? line = Console.ReadLine();

            if (line is null) // EOF (Ctrl+D / Ctrl+Z)
                break;

            if (string.IsNullOrWhiteSpace(line) && !hasPendingSql)
                continue;

            // Meta-commands (only when not mid-SQL)
            if (!hasPendingSql && line.TrimStart().StartsWith('.'))
            {
                if (await HandleMetaCommandAsync(line.Trim(), ct))
                    break;

                _out.WriteLine();
                continue;
            }

            sqlBuffer.AppendLine(line);
            hasPendingSql = true;

            string bufferedSql = sqlBuffer.ToString();
            bool splitOk = SqlScriptParser.TrySplitCompleteStatements(
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

        _out.WriteLine(Ansi.Colorize("Bye.", Ansi.Dim));
    }

    /// <summary>
    /// Executes a SQL statement and prints formatted output.
    /// Returns true on success.
    /// </summary>
    public async ValueTask<bool> ExecuteSqlAsync(string sql, CancellationToken ct = default)
    {
        Stopwatch? sw = _context.ShowTiming ? Stopwatch.StartNew() : null;

        Statement statement;
        try
        {
            statement = Parser.TryParseSimpleSelect(sql, out var fastStmt)
                ? fastStmt
                : Parser.Parse(sql);
        }
        catch (CSharpDbException ex)
        {
            _out.WriteLine(Ansi.Colorize($"Error [{ex.Code}]: {ex.Message}", Ansi.Red));
            _out.WriteLine();
            return false;
        }

        try
        {
            if (_context.SnapshotEnabled && statement is not SelectStatement)
            {
                _out.WriteLine(Ansi.Colorize(
                    "Snapshot mode is read-only. Run '.snapshot off' to execute writes.",
                    Ansi.Yellow));
                _out.WriteLine();
                return false;
            }

            await using var result = _context.SnapshotEnabled
                ? await _context.ExecuteReadSnapshotAsync(sql, ct)
                : await _db.ExecuteAsync(statement, ct);

            sw?.Stop();
            string timingSuffix = _context.ShowTiming && sw is not null
                ? $" ({sw.ElapsedMilliseconds}ms)"
                : string.Empty;

            if (result.IsQuery)
            {
                var rows = await result.ToListAsync(ct);
                if (result.Schema.Length > 0)
                    _tableFormatter.PrintTable(result.Schema, rows);

                int count = rows.Count;
                _out.WriteLine(Ansi.Colorize(
                    $"{count} {(count == 1 ? "row" : "rows")}{timingSuffix}",
                    Ansi.Dim));
            }
            else
            {
                int n = result.RowsAffected;
                _out.WriteLine(Ansi.Colorize(
                    $"{n} {(n == 1 ? "row" : "rows")} affected{timingSuffix}",
                    Ansi.Dim));
            }

            _out.WriteLine();
            return true;
        }
        catch (CSharpDbException ex)
        {
            _out.WriteLine(Ansi.Colorize($"Error [{ex.Code}]: {ex.Message}", Ansi.Red));
            _out.WriteLine();
            return false;
        }
        catch (Exception ex)
        {
            _out.WriteLine(Ansi.Colorize($"Error: {ex.Message}", Ansi.Red));
            _out.WriteLine();
            return false;
        }
    }

    /// <returns>true if the REPL should exit</returns>
    private async ValueTask<bool> HandleMetaCommandAsync(string input, CancellationToken ct)
    {
        var parts = input.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        string cmd = parts[0];
        string arg = parts.Length > 1 ? parts[1] : string.Empty;

        if (cmd is ".quit" or ".exit")
            return true;

        if (_commands.TryGetValue(cmd, out var command))
        {
            try
            {
                await command.ExecuteAsync(_context, arg, _out, ct);
            }
            catch (CSharpDbException ex)
            {
                _out.WriteLine(Ansi.Colorize($"Error [{ex.Code}]: {ex.Message}", Ansi.Red));
            }
            catch (Exception ex)
            {
                _out.WriteLine(Ansi.Colorize($"Error: {ex.Message}", Ansi.Red));
            }
        }
        else
        {
            _out.WriteLine(Ansi.Colorize(
                $"Unknown command: {cmd}. Type .help for available commands.", Ansi.Red));
        }

        return false;
    }

    public void Dispose()
    {
        _context.Dispose();
    }
}

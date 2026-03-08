using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;

namespace StorageStudyExamples.Core;

/// <summary>
/// Abstract base class for application-pattern examples that support interactive
/// domain-specific commands. Provides database lifecycle management and common
/// helpers. Subclasses define their schema, seed data, scripted demo, and
/// domain-specific command set.
/// </summary>
public abstract class DataStoreBase : IInteractiveExample
{
    private Database? _db;

    /// <summary>The underlying CSharpDB database instance. Available after <see cref="InitializeAsync"/>.</summary>
    protected Database Db => _db ?? throw new InvalidOperationException("Store not initialized. Call InitializeAsync first.");

    public abstract string Name { get; }
    public abstract string CommandName { get; }
    public abstract string Description { get; }

    // ── Lifecycle ──────────────────────────────────────────────────────────

    public async Task InitializeAsync(string workingDirectory)
    {
        var dbPath = Path.Combine(workingDirectory, $"{CommandName}.cdb");
        _db = await Database.OpenAsync(dbPath);
        await CreateSchemaAsync();
        await SeedDataAsync();
    }

    /// <summary>Create tables and indexes. Called once during initialization.</summary>
    protected abstract Task CreateSchemaAsync();

    /// <summary>Populate sample data. Called once after schema creation.</summary>
    protected abstract Task SeedDataAsync();

    public abstract Task RunDemoAsync(TextWriter output);

    // ── Domain-specific commands (implemented by each store) ───────────────

    public abstract IReadOnlyList<CommandInfo> GetCommands();

    public abstract Task<bool> ExecuteCommandAsync(string commandName, string args, TextWriter output);

    // ── Raw SQL ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Execute a raw SQL statement and write the results to the given output.
    /// Used by the REPL's <c>sql</c> command for ad-hoc queries.
    /// </summary>
    public async Task ExecuteRawSqlAsync(string sql, TextWriter output)
    {
        await using var result = await Db.ExecuteAsync(sql);

        if (!result.IsQuery)
        {
            output.WriteLine($"{result.RowsAffected} row(s) affected.");
            return;
        }

        var schema = result.Schema;
        var rows = await result.ToListAsync();

        if (rows.Count == 0)
        {
            output.WriteLine("(no rows)");
            return;
        }

        // Calculate column widths
        var widths = new int[schema.Length];
        for (int i = 0; i < schema.Length; i++)
            widths[i] = schema[i].Name.Length;

        foreach (var row in rows)
        {
            for (int i = 0; i < schema.Length && i < row.Length; i++)
            {
                var val = FormatDbValue(row[i]);
                widths[i] = Math.Max(widths[i], Math.Min(val.Length, 40));
            }
        }

        // Header
        var header = string.Join("  ", schema.Select((col, i) => col.Name.PadRight(widths[i])));
        output.WriteLine($"  {header}");
        var separator = string.Join("  ", widths.Select(w => new string('-', w)));
        output.WriteLine($"  {separator}");

        // Rows
        foreach (var row in rows)
        {
            var values = schema.Select((col, i) =>
            {
                var val = i < row.Length ? FormatDbValue(row[i]) : "";
                if (val.Length > 40) val = val[..38] + "..";
                return val.PadRight(widths[i]);
            });
            output.WriteLine($"  {string.Join("  ", values)}");
        }

        output.WriteLine($"  ({rows.Count} rows)");
    }

    private static string FormatDbValue(DbValue value) => value.Type switch
    {
        DbType.Null => "(null)",
        DbType.Integer => value.AsInteger.ToString(),
        DbType.Real => value.AsReal.ToString(),
        DbType.Text => value.AsText,
        DbType.Blob => $"({value.AsBlob.Length} bytes)",
        _ => "?"
    };

    // ── Disposal ───────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_db != null)
        {
            await _db.DisposeAsync();
            _db = null;
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /// <summary>Escape single quotes for SQL string literals.</summary>
    protected static string Esc(string s) => s.Replace("'", "''");

    /// <summary>Truncate a string to a maximum length for display.</summary>
    protected static string Truncate(string s, int maxLen) =>
        s.Length <= maxLen ? s : s[..(maxLen - 2)] + "..";
}

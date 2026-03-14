using CSharpDB.Client.Models;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using ClientTriggerSchema = CSharpDB.Client.Models.TriggerSchema;

namespace CSharpDB.Cli;

internal sealed class HelpCommand : IMetaCommand
{
    private readonly IReadOnlyList<IMetaCommand> _commands;

    public HelpCommand(IReadOnlyList<IMetaCommand> commands) => _commands = commands;

    public IReadOnlyList<string> Aliases => [".help"];
    public string Name => ".help";
    public string Description => "Show this help message";

    public ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        output.WriteLine($"{Ansi.Bold}Available commands:{Ansi.Reset}");
        output.WriteLine($"  {Ansi.Colorize(".quit", Ansi.Cyan),-24} {Ansi.Colorize("Exit the shell", Ansi.Dim)}");
        output.WriteLine($"  {Ansi.Colorize(".exit", Ansi.Cyan),-24} {Ansi.Colorize("Alias for .quit", Ansi.Dim)}");

        foreach (var cmd in _commands.OrderBy(c => c.Name, StringComparer.OrdinalIgnoreCase))
        {
            string name = Ansi.Colorize(cmd.Name, Ansi.Cyan);
            string desc = Ansi.Colorize(cmd.Description, Ansi.Dim);
            output.WriteLine($"  {name,-24} {desc}");
        }

        output.WriteLine();
        output.WriteLine($"{Ansi.Bold}SQL:{Ansi.Reset}");
        output.WriteLine(Ansi.Colorize("  Enter SQL statements terminated with a semicolon (;).", Ansi.Dim));
        output.WriteLine(Ansi.Colorize("  Trigger bodies are handled as a single statement.", Ansi.Dim));
        output.WriteLine(Ansi.Colorize("  Multi-line input and multi-statement lines are supported.", Ansi.Dim));
        return ValueTask.CompletedTask;
    }
}

internal sealed class InfoCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".info"];
    public string Name => ".info";
    public string Description => "Show database and runtime status";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var info = await context.Client.GetInfoAsync(ct);
        string snapshotStatus = context.SupportsLocalDirectFeatures
            ? (context.SnapshotEnabled ? "on" : "off")
            : "unavailable";
        string syncPointStatus = context.SupportsLocalDirectFeatures
            ? (context.PreferSyncPointLookups ? "on" : "off")
            : "unavailable";

        output.WriteLine($"{Ansi.Bold}Database:{Ansi.Reset} {Ansi.Cyan}{context.DatabasePath}{Ansi.Reset}");
        output.WriteLine($"{Ansi.Bold}Objects:{Ansi.Reset} " +
                         $"tables={info.TableCount}, indexes={info.IndexCount}, views={info.ViewCount}, " +
                         $"triggers={info.TriggerCount}, collections={info.CollectionCount}");
        output.WriteLine($"{Ansi.Bold}Modes:{Ansi.Reset} " +
                         $"timing={(context.ShowTiming ? "on" : "off")}, " +
                         $"snapshot={snapshotStatus}, " +
                         $"syncpoint={syncPointStatus}, " +
                         $"tx={(context.InExplicitTransaction ? "active" : "none")}");
    }
}

internal sealed class TablesCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".tables"];
    public string Name => ".tables [PATTERN|--all]";
    public string Description => "List tables (collection backing tables hidden by default)";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        bool includeInternal = argument.Equals("--all", StringComparison.OrdinalIgnoreCase);
        string? pattern = includeInternal || string.IsNullOrWhiteSpace(argument) ? null : argument.Trim();

        IEnumerable<string> names = includeInternal
            ? await MetaCommandHelpers.GetTableNamesAsync(context, includeInternal, ct)
            : await MetaCommandHelpers.GetUserTableNamesAsync(context, ct);

        if (!string.IsNullOrWhiteSpace(pattern))
            names = names.Where(n => n.Contains(pattern, StringComparison.OrdinalIgnoreCase));

        var ordered = names.OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No tables.", Ansi.Dim));
            return;
        }

        foreach (var name in ordered)
            output.WriteLine($"  {Ansi.Cyan}{name}{Ansi.Reset}");
    }
}

internal sealed class SchemaCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".schema"];
    public string Name => ".schema [TABLE|--all]";
    public string Description => "Show CREATE TABLE schema";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            await PrintAllSchemasAsync(context, output, includeInternal: false, ct);
            return;
        }

        if (argument.Equals("--all", StringComparison.OrdinalIgnoreCase))
        {
            await PrintAllSchemasAsync(context, output, includeInternal: true, ct);
            return;
        }

        var schema = await MetaCommandHelpers.GetTableSchemaAsync(context, argument, ct);
        if (schema is null && !argument.StartsWith(MetaCommandHelpers.CollectionPrefix, StringComparison.Ordinal))
            schema = await MetaCommandHelpers.GetTableSchemaAsync(context, MetaCommandHelpers.CollectionPrefix + argument, ct);

        if (schema is null)
        {
            output.WriteLine(Ansi.Colorize($"Table '{argument}' not found.", Ansi.Red));
            return;
        }

        PrintSingleTableSchema(schema, output);
    }

    private static async ValueTask PrintAllSchemasAsync(
        MetaCommandContext context,
        TextWriter output,
        bool includeInternal,
        CancellationToken ct)
    {
        var names = await MetaCommandHelpers.GetTableNamesAsync(context, includeInternal, ct);
        var ordered = names.OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No tables.", Ansi.Dim));
            return;
        }

        foreach (var name in ordered)
        {
            var schema = await MetaCommandHelpers.GetTableSchemaAsync(context, name, ct);
            if (schema is null)
                continue;

            PrintSingleTableSchema(schema, output);
            output.WriteLine();
        }
    }

    private static void PrintSingleTableSchema(ClientTableSchema schema, TextWriter output)
    {
        output.WriteLine($"{Ansi.Bold}CREATE TABLE{Ansi.Reset} {Ansi.Cyan}{schema.TableName}{Ansi.Reset} (");

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            var col = schema.Columns[i];
            string comma = i < schema.Columns.Count - 1 ? "," : string.Empty;

            string type = Ansi.Colorize(col.Type.ToString().ToUpperInvariant(), Ansi.Yellow);
            string pk = col.IsPrimaryKey ? Ansi.Colorize(" PRIMARY KEY", Ansi.Magenta) : string.Empty;
            string identity = col.IsIdentity ? Ansi.Colorize(" IDENTITY", Ansi.Magenta) : string.Empty;
            string nn = !col.Nullable ? Ansi.Colorize(" NOT NULL", Ansi.Magenta) : string.Empty;

            output.WriteLine($"  {col.Name} {type}{pk}{identity}{nn}{comma}");
        }

        output.WriteLine(");");
    }
}

internal sealed class IndexesCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".indexes"];
    public string Name => ".indexes [TABLE]";
    public string Description => "List indexes";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        IEnumerable<ClientIndexSchema> indexes = await context.Client.GetIndexesAsync(ct);
        if (!string.IsNullOrWhiteSpace(argument))
            indexes = indexes.Where(i => i.TableName.Equals(argument.Trim(), StringComparison.OrdinalIgnoreCase));

        var ordered = indexes.OrderBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No indexes.", Ansi.Dim));
            return;
        }

        foreach (var idx in ordered)
        {
            string unique = idx.IsUnique ? Ansi.Colorize(" UNIQUE", Ansi.Magenta) : string.Empty;
            string cols = string.Join(", ", idx.Columns);
            output.WriteLine($"  {Ansi.Cyan}{idx.IndexName}{Ansi.Reset} ON {idx.TableName} ({cols}){unique}");
        }
    }
}

internal sealed class ViewsCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".views"];
    public string Name => ".views";
    public string Description => "List views";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var views = (await context.Client.GetViewNamesAsync(ct))
            .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (views.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No views.", Ansi.Dim));
            return;
        }

        foreach (var view in views)
            output.WriteLine($"  {Ansi.Cyan}{view}{Ansi.Reset}");
    }
}

internal sealed class ViewCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".view"];
    public string Name => ".view <NAME>";
    public string Description => "Show CREATE VIEW SQL";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            output.WriteLine(Ansi.Colorize("Usage: .view <NAME>", Ansi.Yellow));
            return;
        }

        string name = argument.Trim();
        string? sql = await context.Client.GetViewSqlAsync(name, ct);
        if (sql is null)
        {
            output.WriteLine(Ansi.Colorize($"View '{name}' not found.", Ansi.Red));
            return;
        }

        output.WriteLine($"{Ansi.Bold}CREATE VIEW{Ansi.Reset} {Ansi.Cyan}{name}{Ansi.Reset} AS");
        output.WriteLine(sql.Trim().TrimEnd(';') + ";");
    }
}

internal sealed class TriggersCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".triggers"];
    public string Name => ".triggers [TABLE]";
    public string Description => "List triggers";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        IEnumerable<ClientTriggerSchema> triggers = await context.Client.GetTriggersAsync(ct);
        if (!string.IsNullOrWhiteSpace(argument))
            triggers = triggers.Where(t => t.TableName.Equals(argument.Trim(), StringComparison.OrdinalIgnoreCase));

        var ordered = triggers.OrderBy(t => t.TriggerName, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No triggers.", Ansi.Dim));
            return;
        }

        foreach (var trig in ordered)
        {
            output.WriteLine(
                $"  {Ansi.Cyan}{trig.TriggerName}{Ansi.Reset} " +
                $"{trig.Timing.ToString().ToUpperInvariant()} {trig.Event.ToString().ToUpperInvariant()} ON {trig.TableName}");
        }
    }
}

internal sealed class TriggerCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".trigger"];
    public string Name => ".trigger <NAME>";
    public string Description => "Show CREATE TRIGGER SQL";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            output.WriteLine(Ansi.Colorize("Usage: .trigger <NAME>", Ansi.Yellow));
            return;
        }

        string name = argument.Trim();
        var trigger = (await context.Client.GetTriggersAsync(ct))
            .FirstOrDefault(t => t.TriggerName.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (trigger is null)
        {
            output.WriteLine(Ansi.Colorize($"Trigger '{name}' not found.", Ansi.Red));
            return;
        }

        output.WriteLine(
            $"{Ansi.Bold}CREATE TRIGGER{Ansi.Reset} {Ansi.Cyan}{trigger.TriggerName}{Ansi.Reset} " +
            $"{trigger.Timing.ToString().ToUpperInvariant()} {trigger.Event.ToString().ToUpperInvariant()} ON {trigger.TableName}");
        output.WriteLine("BEGIN");
        output.WriteLine($"  {trigger.BodySql.Trim().TrimEnd(';')};");
        output.WriteLine("END;");
    }
}

internal sealed class CollectionsCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".collections"];
    public string Name => ".collections";
    public string Description => "List document collections";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var names = (await context.Client.GetCollectionNamesAsync(ct))
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (names.Length == 0)
        {
            output.WriteLine(Ansi.Colorize("No collections.", Ansi.Dim));
            return;
        }

        foreach (var name in names)
            output.WriteLine($"  {Ansi.Cyan}{name}{Ansi.Reset}");
    }
}

internal sealed class BeginTransactionCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".begin"];
    public string Name => ".begin";
    public string Description => "Begin explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        await context.BeginTransactionAsync(ct);
        output.WriteLine(Ansi.Colorize("Transaction started.", Ansi.Green));
    }
}

internal sealed class CommitCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".commit"];
    public string Name => ".commit";
    public string Description => "Commit explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        await context.CommitAsync(ct);
        output.WriteLine(Ansi.Colorize("Transaction committed.", Ansi.Green));
    }
}

internal sealed class RollbackCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".rollback"];
    public string Name => ".rollback";
    public string Description => "Rollback explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        await context.RollbackAsync(ct);
        output.WriteLine(Ansi.Colorize("Transaction rolled back.", Ansi.Green));
    }
}

internal sealed class CheckpointCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".checkpoint"];
    public string Name => ".checkpoint";
    public string Description => "Flush WAL pages to main DB file";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        await context.CheckpointAsync(ct);
        output.WriteLine(Ansi.Colorize("Checkpoint completed.", Ansi.Green));
    }
}

internal sealed class ReindexCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".reindex"];
    public string Name => ".reindex [--all|--table <name>|--index <name>]";
    public string Description => "Rebuild indexes for the database, one table, or one index";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (!MetaCommandHelpers.TryParseReindexArgument(argument, out var request, out string? error))
        {
            output.WriteLine(Ansi.Colorize(error ?? "Usage: .reindex [--all|--table <name>|--index <name>]", Ansi.Yellow));
            return;
        }

        var result = await context.ReindexAsync(request, ct);
        string target = result.Scope == ReindexScope.All || string.IsNullOrWhiteSpace(result.Name)
            ? "all indexes"
            : $"{result.Scope.ToString().ToLowerInvariant()} '{result.Name}'";

        output.WriteLine(Ansi.Colorize($"Reindexed {result.RebuiltIndexCount} index(es) for {target}.", Ansi.Green));
    }
}

internal sealed class VacuumCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".vacuum"];
    public string Name => ".vacuum";
    public string Description => "Rewrite the database file to reclaim free pages";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (!string.IsNullOrWhiteSpace(argument))
        {
            output.WriteLine(Ansi.Colorize("Usage: .vacuum", Ansi.Yellow));
            return;
        }

        var result = await context.VacuumAsync(ct);
        output.WriteLine(Ansi.Colorize(
            $"Vacuum complete: bytes {result.DatabaseFileBytesBefore} -> {result.DatabaseFileBytesAfter}, " +
            $"pages {result.PhysicalPageCountBefore} -> {result.PhysicalPageCountAfter}.",
            Ansi.Green));
    }
}

internal sealed class SnapshotCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".snapshot"];
    public string Name => ".snapshot [on|off|status]";
    public string Description => "Toggle read-only snapshot mode for SELECT";

    public ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (!context.SupportsLocalDirectFeatures)
        {
            output.WriteLine(Ansi.Colorize("Snapshot mode requires direct local access.", Ansi.Yellow));
            return ValueTask.CompletedTask;
        }

        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            string status = context.SnapshotEnabled ? "on" : "off";
            output.WriteLine($"Snapshot mode: {Ansi.Colorize(status, context.SnapshotEnabled ? Ansi.Green : Ansi.Dim)}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            if (context.SnapshotEnabled)
            {
                output.WriteLine(Ansi.Colorize("Snapshot mode is already on.", Ansi.Dim));
                return ValueTask.CompletedTask;
            }

            context.EnableSnapshot();
            output.WriteLine(Ansi.Colorize("Snapshot mode enabled (SELECT only).", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            if (!context.SnapshotEnabled)
            {
                output.WriteLine(Ansi.Colorize("Snapshot mode is already off.", Ansi.Dim));
                return ValueTask.CompletedTask;
            }

            context.DisableSnapshot();
            output.WriteLine(Ansi.Colorize("Snapshot mode disabled.", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        output.WriteLine(Ansi.Colorize("Usage: .snapshot [on|off|status]", Ansi.Yellow));
        return ValueTask.CompletedTask;
    }
}

internal sealed class SyncPointCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".syncpoint"];
    public string Name => ".syncpoint [on|off|status]";
    public string Description => "Toggle sync fast path for PK lookups";

    public ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (!context.SupportsLocalDirectFeatures)
        {
            output.WriteLine(Ansi.Colorize("Sync point mode requires direct local access.", Ansi.Yellow));
            return ValueTask.CompletedTask;
        }

        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            string status = context.PreferSyncPointLookups ? "on" : "off";
            output.WriteLine($"Sync point lookup fast path: {Ansi.Colorize(status, context.PreferSyncPointLookups ? Ansi.Green : Ansi.Dim)}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            context.PreferSyncPointLookups = true;
            output.WriteLine(Ansi.Colorize("Sync point lookup fast path enabled.", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            context.PreferSyncPointLookups = false;
            output.WriteLine(Ansi.Colorize("Sync point lookup fast path disabled.", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        output.WriteLine(Ansi.Colorize("Usage: .syncpoint [on|off|status]", Ansi.Yellow));
        return ValueTask.CompletedTask;
    }
}

internal sealed class TimingCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".timing"];
    public string Name => ".timing [on|off|status]";
    public string Description => "Toggle query timing output";

    public ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            string status = context.ShowTiming ? "on" : "off";
            output.WriteLine($"Timing: {Ansi.Colorize(status, context.ShowTiming ? Ansi.Green : Ansi.Dim)}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            context.ShowTiming = true;
            output.WriteLine(Ansi.Colorize("Timing enabled.", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            context.ShowTiming = false;
            output.WriteLine(Ansi.Colorize("Timing disabled.", Ansi.Green));
            return ValueTask.CompletedTask;
        }

        output.WriteLine(Ansi.Colorize("Usage: .timing [on|off|status]", Ansi.Yellow));
        return ValueTask.CompletedTask;
    }
}

internal sealed class ReadCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".read"];
    public string Name => ".read <FILE>";
    public string Description => "Execute SQL statements from file";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            output.WriteLine(Ansi.Colorize("Usage: .read <FILE>", Ansi.Yellow));
            return;
        }

        string path = NormalizePath(argument);
        if (!File.Exists(path))
        {
            output.WriteLine(Ansi.Colorize($"File not found: {path}", Ansi.Red));
            return;
        }

        string sqlText = await File.ReadAllTextAsync(path, ct);
        IReadOnlyList<string> statements;

        try
        {
            if (sqlText.AsSpan().TrimEnd().Length > 0 && sqlText.AsSpan().TrimEnd()[^1] != ';')
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    "SQL script ended with an incomplete statement (missing semicolon).");

            statements = SqlScriptSplitter.SplitExecutableStatements(sqlText);
        }
        catch (CSharpDbException ex)
        {
            output.WriteLine(Ansi.Colorize($"Error: {ex.Message}", Ansi.Red));
            return;
        }

        if (statements.Count == 0)
        {
            output.WriteLine(Ansi.Colorize("No SQL statements found.", Ansi.Dim));
            return;
        }

        output.WriteLine(Ansi.Colorize($"Running {statements.Count} statements from {path}", Ansi.Dim));

        int ok = 0;
        int fail = 0;
        foreach (var statement in statements)
        {
            bool success = await context.ExecuteSqlAsync(statement, ct);
            if (success) ok++;
            else fail++;
        }

        string summaryColor = fail == 0 ? Ansi.Green : Ansi.Yellow;
        output.WriteLine(Ansi.Colorize($"Script complete: {ok} passed, {fail} failed.", summaryColor));
    }

    private static string NormalizePath(string argument)
    {
        string trimmed = argument.Trim();
        if ((trimmed.StartsWith('"') && trimmed.EndsWith('"')) ||
            (trimmed.StartsWith('\'') && trimmed.EndsWith('\'')))
        {
            trimmed = trimmed[1..^1];
        }

        return Path.GetFullPath(trimmed);
    }
}

internal static class MetaCommandHelpers
{
    internal const string CollectionPrefix = "_col_";

    internal static bool TryParseReindexArgument(string argument, out ReindexRequest request, out string? error)
    {
        error = null;
        string[] tokens = argument.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length == 0)
        {
            request = new ReindexRequest { Scope = ReindexScope.All };
            return true;
        }

        if (tokens.Length == 1 && tokens[0].Equals("--all", StringComparison.OrdinalIgnoreCase))
        {
            request = new ReindexRequest { Scope = ReindexScope.All };
            return true;
        }

        if (tokens.Length == 2 && tokens[0].Equals("--table", StringComparison.OrdinalIgnoreCase))
        {
            request = new ReindexRequest { Scope = ReindexScope.Table, Name = tokens[1] };
            return true;
        }

        if (tokens.Length == 2 && tokens[0].Equals("--index", StringComparison.OrdinalIgnoreCase))
        {
            request = new ReindexRequest { Scope = ReindexScope.Index, Name = tokens[1] };
            return true;
        }

        request = new ReindexRequest();
        error = "Usage: .reindex [--all|--table <name>|--index <name>]";
        return false;
    }

    internal static async Task<IReadOnlyList<string>> GetTableNamesAsync(
        MetaCommandContext context,
        bool includeInternal,
        CancellationToken ct)
    {
        if (includeInternal && context.LocalDatabase is not null)
        {
            return context.LocalDatabase.GetTableNames()
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }

        return await context.Client.GetTableNamesAsync(ct);
    }

    internal static async Task<IReadOnlyList<string>> GetUserTableNamesAsync(
        MetaCommandContext context,
        CancellationToken ct)
    {
        return await context.Client.GetTableNamesAsync(ct);
    }

    internal static async Task<ClientTableSchema?> GetTableSchemaAsync(
        MetaCommandContext context,
        string tableName,
        CancellationToken ct)
    {
        var schema = await context.Client.GetTableSchemaAsync(tableName, ct);
        if (schema is not null)
            return schema;

        if (context.LocalDatabase is null)
            return null;

        var localSchema = context.LocalDatabase.GetTableSchema(tableName);
        return localSchema is null ? null : MapTableSchema(localSchema);
    }

    private static ClientTableSchema MapTableSchema(CSharpDB.Primitives.TableSchema schema)
    {
        return new ClientTableSchema
        {
            TableName = schema.TableName,
            Columns = schema.Columns
                .Select(column => new ClientColumnDefinition
                {
                    Name = column.Name,
                    Type = MapDbType(column.Type),
                    Nullable = column.Nullable,
                    IsPrimaryKey = column.IsPrimaryKey,
                    IsIdentity = column.IsIdentity,
                })
                .ToArray(),
        };
    }

    private static CSharpDB.Client.Models.DbType MapDbType(CSharpDB.Primitives.DbType type) => type switch
    {
        CSharpDB.Primitives.DbType.Integer => CSharpDB.Client.Models.DbType.Integer,
        CSharpDB.Primitives.DbType.Real => CSharpDB.Client.Models.DbType.Real,
        CSharpDB.Primitives.DbType.Text => CSharpDB.Client.Models.DbType.Text,
        CSharpDB.Primitives.DbType.Blob => CSharpDB.Client.Models.DbType.Blob,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };
}

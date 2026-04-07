using CSharpDB.Client.Models;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using Spectre.Console;
using System.Text.Json;
using System.Text.Json.Serialization;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
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
        var console = CliConsole.Create(output);

        var groups = new (string Title, (string Command, string Description)[] Items)[]
        {
            ("Shell", [
                (".help", "Show this help message"),
                (".quit / .exit", "Exit the shell"),
                (".timing", "Toggle query timing display"),
                (".read <FILE>", "Execute SQL from a script file"),
            ]),
            ("Inspection", GetCommandGroup([".info", ".tables", ".schema", ".indexes",
                ".views", ".view", ".triggers", ".trigger", ".collections"])),
            ("Transactions", GetCommandGroup([".begin", ".commit", ".rollback"])),
            ("Snapshots", GetCommandGroup([".snapshot", ".syncpoint"])),
            ("Maintenance", GetCommandGroup([".checkpoint", ".backup", ".restore",
                ".reindex", ".vacuum", ".migrate-fks"])),
        };

        foreach (var (title, items) in groups)
        {
            if (items.Length == 0)
                continue;

            console.Write(new Rule($"[bold deepskyblue1]{title}[/]").LeftJustified().RuleStyle("grey42"));

            var table = new Table()
                .Border(TableBorder.None)
                .HideHeaders()
                .AddColumn(new TableColumn(string.Empty).PadRight(2))
                .AddColumn(new TableColumn(string.Empty));

            foreach (var (cmd, desc) in items)
                table.AddRow(
                    new Markup($"  [deepskyblue1]{CliConsole.Escape(cmd)}[/]"),
                    new Markup($"[grey]{CliConsole.Escape(desc)}[/]"));

            console.Write(table);
        }

        console.WriteLine();
        console.Write(new Rule("[bold deepskyblue1]SQL[/]").LeftJustified().RuleStyle("grey42"));
        CliConsole.WriteMuted(console, "  Enter SQL statements terminated with a semicolon (;).");
        CliConsole.WriteMuted(console, "  Multi-line input, multi-statement lines, and trigger bodies are supported.");
        return ValueTask.CompletedTask;
    }

    private (string Command, string Description)[] GetCommandGroup(string[] aliases)
    {
        var items = new List<(string Command, string Description)>();
        foreach (string alias in aliases)
        {
            var cmd = _commands.FirstOrDefault(c => c.Aliases.Contains(alias, StringComparer.OrdinalIgnoreCase));
            if (cmd is not null)
                items.Add((cmd.Name, cmd.Description));
        }

        return items.ToArray();
    }
}

internal sealed class InfoCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".info"];
    public string Name => ".info";
    public string Description => "Show database and runtime status";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        var info = await context.Client.GetInfoAsync(ct);

        console.Write(new Rule("[bold deepskyblue1]Database[/]").LeftJustified().RuleStyle("grey42"));

        var dbTable = CliConsole.CreateKeyValueTable();
        dbTable.AddColumn(new TableColumn(string.Empty).PadRight(2));
        dbTable.AddColumn(new TableColumn(string.Empty));
        dbTable.AddRow(new Markup("  [grey]Path:[/]"), new Markup($"[white]{CliConsole.Escape(context.DatabasePath)}[/]"));
        dbTable.AddRow(new Markup("  [grey]Tables:[/]"), new Markup($"[deepskyblue1]{info.TableCount}[/]"));
        dbTable.AddRow(new Markup("  [grey]Indexes:[/]"), new Markup($"[deepskyblue1]{info.IndexCount}[/]"));
        dbTable.AddRow(new Markup("  [grey]Views:[/]"), new Markup($"[deepskyblue1]{info.ViewCount}[/]"));
        dbTable.AddRow(new Markup("  [grey]Triggers:[/]"), new Markup($"[deepskyblue1]{info.TriggerCount}[/]"));
        dbTable.AddRow(new Markup("  [grey]Collections:[/]"), new Markup($"[deepskyblue1]{info.CollectionCount}[/]"));
        console.Write(dbTable);

        console.Write(new Rule("[bold deepskyblue1]Session[/]").LeftJustified().RuleStyle("grey42"));

        string snapshotStatus = context.SupportsLocalDirectFeatures
            ? (context.SnapshotEnabled ? "on" : "off")
            : "unavailable";
        string syncPointStatus = context.SupportsLocalDirectFeatures
            ? (context.PreferSyncPointLookups ? "on" : "off")
            : "unavailable";

        var modeTable = CliConsole.CreateKeyValueTable();
        modeTable.AddColumn(new TableColumn(string.Empty).PadRight(2));
        modeTable.AddColumn(new TableColumn(string.Empty));
        modeTable.AddRow(new Markup("  [grey]Timing:[/]"), FormatToggle(context.ShowTiming));
        modeTable.AddRow(new Markup("  [grey]Snapshot:[/]"), FormatStatus(snapshotStatus));
        modeTable.AddRow(new Markup("  [grey]Sync point:[/]"), FormatStatus(syncPointStatus));
        modeTable.AddRow(new Markup("  [grey]Transaction:[/]"), context.InExplicitTransaction
            ? new Markup("[bold yellow]active[/]")
            : new Markup("[grey]none[/]"));
        console.Write(modeTable);
    }

    private static Markup FormatToggle(bool value)
        => value ? new Markup("[green]on[/]") : new Markup("[grey]off[/]");

    private static Markup FormatStatus(string status) => status switch
    {
        "on" => new Markup("[green]on[/]"),
        "off" => new Markup("[grey]off[/]"),
        _ => new Markup("[dim grey]unavailable[/]"),
    };
}

internal sealed class TablesCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".tables"];
    public string Name => ".tables [PATTERN|--all]";
    public string Description => "List tables (collection backing tables hidden by default)";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        bool includeInternal = argument.Equals("--all", StringComparison.OrdinalIgnoreCase);
        string? pattern = includeInternal || string.IsNullOrWhiteSpace(argument) ? null : argument.Trim();

        IEnumerable<string> names = includeInternal
            ? await MetaCommandHelpers.GetTableNamesAsync(context, includeInternal, ct)
            : await MetaCommandHelpers.GetUserTableNamesAsync(context, ct);

        if (!string.IsNullOrWhiteSpace(pattern))
            names = names.Where(n => n.Contains(pattern, StringComparison.OrdinalIgnoreCase));

        var ordered = names.OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        CliConsole.WriteNameList(console, ordered, "No tables.");
    }
}

internal sealed class SchemaCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".schema"];
    public string Name => ".schema [TABLE|--all]";
    public string Description => "Show CREATE TABLE schema";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (string.IsNullOrWhiteSpace(argument))
        {
            await PrintAllSchemasAsync(context, console, includeInternal: false, ct);
            return;
        }

        if (argument.Equals("--all", StringComparison.OrdinalIgnoreCase))
        {
            await PrintAllSchemasAsync(context, console, includeInternal: true, ct);
            return;
        }

        var schema = await MetaCommandHelpers.GetTableSchemaAsync(context, argument, ct);
        if (schema is null && !argument.StartsWith(MetaCommandHelpers.CollectionPrefix, StringComparison.Ordinal))
            schema = await MetaCommandHelpers.GetTableSchemaAsync(context, MetaCommandHelpers.CollectionPrefix + argument, ct);

        if (schema is null)
        {
            CliConsole.WriteError(console, $"Table '{argument}' not found.");
            return;
        }

        PrintSingleTableSchema(schema, console);
    }

    private static async ValueTask PrintAllSchemasAsync(
        MetaCommandContext context,
        IAnsiConsole console,
        bool includeInternal,
        CancellationToken ct)
    {
        var names = await MetaCommandHelpers.GetTableNamesAsync(context, includeInternal, ct);
        var ordered = names.OrderBy(n => n, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            CliConsole.WriteMuted(console, "No tables.");
            return;
        }

        foreach (var name in ordered)
        {
            var schema = await MetaCommandHelpers.GetTableSchemaAsync(context, name, ct);
            if (schema is null)
                continue;

            PrintSingleTableSchema(schema, console);
            console.WriteLine();
        }
    }

    private static void PrintSingleTableSchema(ClientTableSchema schema, IAnsiConsole console)
    {
        var sql = new System.Text.StringBuilder();
        sql.AppendLine($"CREATE TABLE {schema.TableName} (");

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            var col = schema.Columns[i];
            bool hasTrailingItems = i < schema.Columns.Count - 1 || schema.ForeignKeys.Count > 0;
            string comma = hasTrailingItems ? "," : string.Empty;

            string type = col.Type.ToString().ToUpperInvariant();
            string pk = col.IsPrimaryKey ? " PRIMARY KEY" : string.Empty;
            string identity = col.IsIdentity ? " IDENTITY" : string.Empty;
            string nn = !col.Nullable ? " NOT NULL" : string.Empty;
            string foreignKey = string.Empty;
            ClientForeignKeyDefinition? columnForeignKey = schema.ForeignKeys.FirstOrDefault(fk =>
                fk.ColumnName.Equals(col.Name, StringComparison.OrdinalIgnoreCase));
            if (columnForeignKey is not null)
            {
                foreignKey =
                    $" REFERENCES {columnForeignKey.ReferencedTableName}({columnForeignKey.ReferencedColumnName})";
                if (columnForeignKey.OnDelete == ClientForeignKeyOnDeleteAction.Cascade)
                    foreignKey += " ON DELETE CASCADE";
            }

            sql.AppendLine($"  {col.Name} {type}{pk}{identity}{nn}{foreignKey}{comma}");
        }

        sql.Append(");");
        CliConsole.WriteSqlPanel(console, schema.TableName, sql.ToString());
    }
}

internal sealed class IndexesCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".indexes"];
    public string Name => ".indexes [TABLE]";
    public string Description => "List indexes";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        IEnumerable<ClientIndexSchema> indexes = await context.Client.GetIndexesAsync(ct);
        if (!string.IsNullOrWhiteSpace(argument))
            indexes = indexes.Where(i => i.TableName.Equals(argument.Trim(), StringComparison.OrdinalIgnoreCase));

        var ordered = indexes.OrderBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            CliConsole.WriteMuted(console, "No indexes.");
            return;
        }

        var table = CliConsole.CreateDataTable();
        table.AddColumn("[bold]Index[/]");
        table.AddColumn("[bold]Table[/]");
        table.AddColumn("[bold]Columns[/]");
        table.AddColumn("[bold]Flags[/]");
        foreach (var idx in ordered)
        {
            table.AddRow(
                new Markup($"[deepskyblue1]{CliConsole.Escape(idx.IndexName)}[/]"),
                new Markup(CliConsole.Escape(idx.TableName)),
                new Markup(CliConsole.Escape(string.Join(", ", idx.Columns))),
                new Markup(idx.IsUnique ? "[fuchsia]UNIQUE[/]" : "[grey]-[/]"));
        }
        console.Write(table);
    }
}

internal sealed class ViewsCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".views"];
    public string Name => ".views";
    public string Description => "List views";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        var views = (await context.Client.GetViewNamesAsync(ct))
            .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        CliConsole.WriteNameList(console, views, "No views.");
    }
}

internal sealed class ViewCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".view"];
    public string Name => ".view <NAME>";
    public string Description => "Show CREATE VIEW SQL";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (string.IsNullOrWhiteSpace(argument))
        {
            CliConsole.WriteWarning(console, "Usage: .view <NAME>");
            return;
        }

        string name = argument.Trim();
        string? sql = await context.Client.GetViewSqlAsync(name, ct);
        if (sql is null)
        {
            CliConsole.WriteError(console, $"View '{name}' not found.");
            return;
        }

        CliConsole.WriteSqlPanel(console, name, $"CREATE VIEW {name} AS{Environment.NewLine}{sql.Trim().TrimEnd(';')};");
    }
}

internal sealed class TriggersCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".triggers"];
    public string Name => ".triggers [TABLE]";
    public string Description => "List triggers";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        IEnumerable<ClientTriggerSchema> triggers = await context.Client.GetTriggersAsync(ct);
        if (!string.IsNullOrWhiteSpace(argument))
            triggers = triggers.Where(t => t.TableName.Equals(argument.Trim(), StringComparison.OrdinalIgnoreCase));

        var ordered = triggers.OrderBy(t => t.TriggerName, StringComparer.OrdinalIgnoreCase).ToArray();
        if (ordered.Length == 0)
        {
            CliConsole.WriteMuted(console, "No triggers.");
            return;
        }

        var table = CliConsole.CreateDataTable();
        table.AddColumn("[bold]Trigger[/]");
        table.AddColumn("[bold]Timing[/]");
        table.AddColumn("[bold]Event[/]");
        table.AddColumn("[bold]Table[/]");
        foreach (var trig in ordered)
        {
            table.AddRow(
                new Markup($"[deepskyblue1]{CliConsole.Escape(trig.TriggerName)}[/]"),
                new Markup(CliConsole.Escape(trig.Timing.ToString().ToUpperInvariant())),
                new Markup(CliConsole.Escape(trig.Event.ToString().ToUpperInvariant())),
                new Markup(CliConsole.Escape(trig.TableName)));
        }
        console.Write(table);
    }
}

internal sealed class TriggerCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".trigger"];
    public string Name => ".trigger <NAME>";
    public string Description => "Show CREATE TRIGGER SQL";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (string.IsNullOrWhiteSpace(argument))
        {
            CliConsole.WriteWarning(console, "Usage: .trigger <NAME>");
            return;
        }

        string name = argument.Trim();
        var trigger = (await context.Client.GetTriggersAsync(ct))
            .FirstOrDefault(t => t.TriggerName.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (trigger is null)
        {
            CliConsole.WriteError(console, $"Trigger '{name}' not found.");
            return;
        }

        CliConsole.WriteSqlPanel(
            console,
            trigger.TriggerName,
            $"CREATE TRIGGER {trigger.TriggerName} {trigger.Timing.ToString().ToUpperInvariant()} {trigger.Event.ToString().ToUpperInvariant()} ON {trigger.TableName}{Environment.NewLine}BEGIN{Environment.NewLine}  {trigger.BodySql.Trim().TrimEnd(';')};{Environment.NewLine}END;");
    }
}

internal sealed class CollectionsCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".collections"];
    public string Name => ".collections";
    public string Description => "List document collections";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        var names = (await context.Client.GetCollectionNamesAsync(ct))
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        CliConsole.WriteNameList(console, names, "No collections.");
    }
}

internal sealed class BeginTransactionCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".begin"];
    public string Name => ".begin";
    public string Description => "Begin explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        await context.BeginTransactionAsync(ct);
        CliConsole.WriteSuccess(console, "Transaction started.");
    }
}

internal sealed class CommitCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".commit"];
    public string Name => ".commit";
    public string Description => "Commit explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        await context.CommitAsync(ct);
        CliConsole.WriteSuccess(console, "Transaction committed.");
    }
}

internal sealed class RollbackCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".rollback"];
    public string Name => ".rollback";
    public string Description => "Rollback explicit transaction";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        await context.RollbackAsync(ct);
        CliConsole.WriteSuccess(console, "Transaction rolled back.");
    }
}

internal sealed class CheckpointCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".checkpoint"];
    public string Name => ".checkpoint";
    public string Description => "Flush WAL pages to main DB file";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        await context.CheckpointAsync(ct);
        CliConsole.WriteSuccess(console, "Checkpoint completed.");
    }
}

internal sealed class BackupCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".backup"];
    public string Name => ".backup <FILE> [--with-manifest]";
    public string Description => "Write a committed snapshot backup to a file";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!MetaCommandHelpers.TryParseBackupArgument(argument, out string? destinationPath, out bool withManifest, out string? error))
        {
            CliConsole.WriteWarning(console, error ?? "Usage: .backup <FILE> [--with-manifest]");
            return;
        }

        var result = await context.BackupAsync(destinationPath!, withManifest, ct);
        CliConsole.WriteSuccess(console, $"Backup saved to {result.DestinationPath}.");
        CliConsole.WriteMuted(console, $"Bytes={result.DatabaseFileBytes}, pages={result.PhysicalPageCount}, changeCounter={result.ChangeCounter}.");
        CliConsole.WriteMuted(console, $"SHA-256={result.Sha256}");

        if (!string.IsNullOrWhiteSpace(result.ManifestPath))
            CliConsole.WriteMuted(console, $"Manifest: {result.ManifestPath}");
    }
}

internal sealed class RestoreCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".restore"];
    public string Name => ".restore <FILE> [--validate-only]";
    public string Description => "Validate or restore a database snapshot into the current database";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!MetaCommandHelpers.TryParseRestoreArgument(argument, out string? sourcePath, out bool validateOnly, out string? error))
        {
            CliConsole.WriteWarning(console, error ?? "Usage: .restore <FILE> [--validate-only]");
            return;
        }

        var result = await context.RestoreAsync(sourcePath!, validateOnly, ct);
        if (result.ValidateOnly)
        {
            CliConsole.WriteSuccess(console, $"Restore source is valid: {result.SourcePath}");
        }
        else
        {
            CliConsole.WriteSuccess(console, $"Restore complete from {result.SourcePath}.");
            CliConsole.WriteMuted(console, $"Target: {result.DestinationPath}");
        }

        CliConsole.WriteMuted(
            console,
            $"Bytes={result.DatabaseFileBytes}, pages={result.PhysicalPageCount}, changeCounter={result.ChangeCounter}, sourceWal={(result.SourceWalExists ? "present" : "absent")}.");
    }
}

internal sealed class MigrateForeignKeysCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".migrate-fks"];
    public string Name => ".migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]";
    public string Description => "Validate or retrofit foreign keys onto existing tables";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!MetaCommandHelpers.TryParseForeignKeyMigrationArgument(argument, out string? specPath, out bool validateOnly, out string? backupPath, out string? error))
        {
            CliConsole.WriteWarning(console, error ?? "Usage: .migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]");
            return;
        }

        var request = await MetaCommandHelpers.LoadForeignKeyMigrationRequestAsync(specPath!, validateOnly, backupPath, ct);
        var result = await context.MigrateForeignKeysAsync(request, ct);
        MetaCommandHelpers.WriteForeignKeyMigrationSummary(result, output);
    }
}

internal sealed class ReindexCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".reindex"];
    public string Name => ".reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]";
    public string Description => "Rebuild indexes for the database, one table, or one index";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!MetaCommandHelpers.TryParseReindexArgument(argument, out var request, out string? error))
        {
            CliConsole.WriteWarning(console, error ?? "Usage: .reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]");
            return;
        }

        var result = await context.ReindexAsync(request, ct);
        string target = result.Scope == ReindexScope.All || string.IsNullOrWhiteSpace(result.Name)
            ? "all indexes"
            : $"{result.Scope.ToString().ToLowerInvariant()} '{result.Name}'";

        CliConsole.WriteSuccess(console, $"Reindexed {result.RebuiltIndexCount} index(es) for {target}.");
        if (result.RecoveredCorruptIndexCount > 0)
        {
            CliConsole.WriteWarning(
                console,
                $"Recovered {result.RecoveredCorruptIndexCount} corrupt index tree(s) without reclaim; run .vacuum to reclaim orphaned pages.");
        }
    }
}

internal sealed class VacuumCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".vacuum"];
    public string Name => ".vacuum";
    public string Description => "Rewrite the database file to reclaim free pages";

    public async ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!string.IsNullOrWhiteSpace(argument))
        {
            CliConsole.WriteWarning(console, "Usage: .vacuum");
            return;
        }

        var result = await context.VacuumAsync(ct);
        CliConsole.WriteSuccess(
            console,
            $"Vacuum complete: bytes {result.DatabaseFileBytesBefore} -> {result.DatabaseFileBytesAfter}, pages {result.PhysicalPageCountBefore} -> {result.PhysicalPageCountAfter}.");
    }
}

internal sealed class SnapshotCommand : IMetaCommand
{
    public IReadOnlyList<string> Aliases => [".snapshot"];
    public string Name => ".snapshot [on|off|status]";
    public string Description => "Toggle read-only snapshot mode for SELECT";

    public ValueTask ExecuteAsync(MetaCommandContext context, string argument, TextWriter output, CancellationToken ct = default)
    {
        var console = CliConsole.Create(output);
        if (!context.SupportsLocalDirectFeatures)
        {
            CliConsole.WriteWarning(console, "Snapshot mode requires direct local access.");
            return ValueTask.CompletedTask;
        }

        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            console.MarkupLine($"Snapshot mode: {(context.SnapshotEnabled ? "[green]on[/]" : "[grey]off[/]")}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            if (context.SnapshotEnabled)
            {
                CliConsole.WriteMuted(console, "Snapshot mode is already on.");
                return ValueTask.CompletedTask;
            }

            context.EnableSnapshot();
            CliConsole.WriteSuccess(console, "Snapshot mode enabled (SELECT only).");
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            if (!context.SnapshotEnabled)
            {
                CliConsole.WriteMuted(console, "Snapshot mode is already off.");
                return ValueTask.CompletedTask;
            }

            context.DisableSnapshot();
            CliConsole.WriteSuccess(console, "Snapshot mode disabled.");
            return ValueTask.CompletedTask;
        }

        CliConsole.WriteWarning(console, "Usage: .snapshot [on|off|status]");
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
        var console = CliConsole.Create(output);
        if (!context.SupportsLocalDirectFeatures)
        {
            CliConsole.WriteWarning(console, "Sync point mode requires direct local access.");
            return ValueTask.CompletedTask;
        }

        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            console.MarkupLine($"Sync point lookup fast path: {(context.PreferSyncPointLookups ? "[green]on[/]" : "[grey]off[/]")}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            context.PreferSyncPointLookups = true;
            CliConsole.WriteSuccess(console, "Sync point lookup fast path enabled.");
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            context.PreferSyncPointLookups = false;
            CliConsole.WriteSuccess(console, "Sync point lookup fast path disabled.");
            return ValueTask.CompletedTask;
        }

        CliConsole.WriteWarning(console, "Usage: .syncpoint [on|off|status]");
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
        var console = CliConsole.Create(output);
        string arg = argument.Trim().ToLowerInvariant();
        if (arg.Length == 0 || arg == "status")
        {
            console.MarkupLine($"Timing: {(context.ShowTiming ? "[green]on[/]" : "[grey]off[/]")}");
            return ValueTask.CompletedTask;
        }

        if (arg is "on" or "enable")
        {
            context.ShowTiming = true;
            CliConsole.WriteSuccess(console, "Timing enabled.");
            return ValueTask.CompletedTask;
        }

        if (arg is "off" or "disable")
        {
            context.ShowTiming = false;
            CliConsole.WriteSuccess(console, "Timing disabled.");
            return ValueTask.CompletedTask;
        }

        CliConsole.WriteWarning(console, "Usage: .timing [on|off|status]");
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
        var console = CliConsole.Create(output);
        if (string.IsNullOrWhiteSpace(argument))
        {
            CliConsole.WriteWarning(console, "Usage: .read <FILE>");
            return;
        }

        string path = MetaCommandHelpers.NormalizePath(argument);
        if (!File.Exists(path))
        {
            CliConsole.WriteError(console, $"File not found: {path}");
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
            CliConsole.WriteError(console, ex.Message);
            return;
        }

        if (statements.Count == 0)
        {
            CliConsole.WriteMuted(console, "No SQL statements found.");
            return;
        }

        CliConsole.WriteMuted(console, $"Running {statements.Count} statements from {path}");

        int ok = 0;
        int fail = 0;
        foreach (var statement in statements)
        {
            bool success = await context.ExecuteSqlAsync(statement, ct);
            if (success) ok++;
            else fail++;
        }

        if (fail == 0)
            CliConsole.WriteSuccess(console, $"Script complete: {ok} passed, {fail} failed.");
        else
            CliConsole.WriteWarning(console, $"Script complete: {ok} passed, {fail} failed.");
    }
}

internal static class MetaCommandHelpers
{
    internal const string CollectionPrefix = "_col_";
    private static readonly JsonSerializerOptions s_foreignKeyMigrationJsonOptions = CreateForeignKeyMigrationJsonOptions();

    internal static bool TryParseReindexArgument(string argument, out ReindexRequest request, out string? error)
    {
        error = null;
        string[] tokens = argument.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var scope = ReindexScope.All;
        string? name = null;
        bool allowCorruptIndexRecovery = false;

        for (int i = 0; i < tokens.Length; i++)
        {
            switch (tokens[i].ToLowerInvariant())
            {
                case "--all":
                    scope = ReindexScope.All;
                    name = null;
                    break;
                case "--table":
                    if (i + 1 >= tokens.Length)
                    {
                        request = new ReindexRequest();
                        error = "Usage: .reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]";
                        return false;
                    }

                    scope = ReindexScope.Table;
                    name = tokens[++i];
                    break;
                case "--index":
                    if (i + 1 >= tokens.Length)
                    {
                        request = new ReindexRequest();
                        error = "Usage: .reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]";
                        return false;
                    }

                    scope = ReindexScope.Index;
                    name = tokens[++i];
                    break;
                case "--force-corrupt-rebuild":
                    allowCorruptIndexRecovery = true;
                    break;
                default:
                    request = new ReindexRequest();
                    error = "Usage: .reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]";
                    return false;
            }
        }

        request = new ReindexRequest
        {
            Scope = scope,
            Name = name,
            AllowCorruptIndexRecovery = allowCorruptIndexRecovery,
        };
        return true;
    }

    internal static bool TryParseBackupArgument(
        string argument,
        out string? destinationPath,
        out bool withManifest,
        out string? error)
    {
        destinationPath = null;
        withManifest = false;
        error = null;

        if (!TryTokenizeArgument(argument, out var tokens, out error))
            return false;

        foreach (string token in tokens)
        {
            if (token.Equals("--with-manifest", StringComparison.OrdinalIgnoreCase))
            {
                withManifest = true;
                continue;
            }

            if (destinationPath is not null)
            {
                error = "Usage: .backup <FILE> [--with-manifest]";
                return false;
            }

            destinationPath = token;
        }

        if (string.IsNullOrWhiteSpace(destinationPath))
        {
            error = "Usage: .backup <FILE> [--with-manifest]";
            return false;
        }

        return true;
    }

    internal static bool TryParseRestoreArgument(
        string argument,
        out string? sourcePath,
        out bool validateOnly,
        out string? error)
    {
        sourcePath = null;
        validateOnly = false;
        error = null;

        if (!TryTokenizeArgument(argument, out var tokens, out error))
            return false;

        foreach (string token in tokens)
        {
            if (token.Equals("--validate-only", StringComparison.OrdinalIgnoreCase))
            {
                validateOnly = true;
                continue;
            }

            if (sourcePath is not null)
            {
                error = "Usage: .restore <FILE> [--validate-only]";
                return false;
            }

            sourcePath = token;
        }

        if (string.IsNullOrWhiteSpace(sourcePath))
        {
            error = "Usage: .restore <FILE> [--validate-only]";
            return false;
        }

        return true;
    }

    internal static bool TryParseForeignKeyMigrationArgument(
        string argument,
        out string? specPath,
        out bool validateOnly,
        out string? backupPath,
        out string? error)
    {
        specPath = null;
        validateOnly = false;
        backupPath = null;
        error = null;

        if (!TryTokenizeArgument(argument, out var tokens, out error))
            return false;

        for (int i = 0; i < tokens.Count; i++)
        {
            string token = tokens[i];
            if (token.Equals("--validate-only", StringComparison.OrdinalIgnoreCase))
            {
                validateOnly = true;
                continue;
            }

            if (token.Equals("--backup", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= tokens.Count)
                {
                    error = "Usage: .migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]";
                    return false;
                }

                backupPath = tokens[++i];
                continue;
            }

            if (specPath is not null)
            {
                error = "Usage: .migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]";
                return false;
            }

            specPath = token;
        }

        if (string.IsNullOrWhiteSpace(specPath))
        {
            error = "Usage: .migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]";
            return false;
        }

        return true;
    }

    internal static string NormalizePath(string argument)
    {
        string trimmed = argument.Trim();
        if ((trimmed.StartsWith('"') && trimmed.EndsWith('"')) ||
            (trimmed.StartsWith('\'') && trimmed.EndsWith('\'')))
        {
            trimmed = trimmed[1..^1];
        }

        return Path.GetFullPath(trimmed);
    }

    internal static bool PathsEqual(string left, string right)
    {
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        return string.Equals(Path.GetFullPath(left), Path.GetFullPath(right), comparison);
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
            ForeignKeys = schema.ForeignKeys
                .Select(foreignKey => new ClientForeignKeyDefinition
                {
                    ConstraintName = foreignKey.ConstraintName,
                    ColumnName = foreignKey.ColumnName,
                    ReferencedTableName = foreignKey.ReferencedTableName,
                    ReferencedColumnName = foreignKey.ReferencedColumnName,
                    OnDelete = foreignKey.OnDelete switch
                    {
                        CSharpDB.Primitives.ForeignKeyOnDeleteAction.Restrict => ClientForeignKeyOnDeleteAction.Restrict,
                        CSharpDB.Primitives.ForeignKeyOnDeleteAction.Cascade => ClientForeignKeyOnDeleteAction.Cascade,
                        _ => throw new ArgumentOutOfRangeException(nameof(foreignKey.OnDelete), foreignKey.OnDelete, null),
                    },
                    SupportingIndexName = foreignKey.SupportingIndexName,
                })
                .ToArray(),
        };
    }

    private static bool TryTokenizeArgument(
        string argument,
        out IReadOnlyList<string> tokens,
        out string? error)
    {
        error = null;
        var result = new List<string>();
        var current = new System.Text.StringBuilder();
        char? quote = null;

        foreach (char ch in argument)
        {
            if (quote.HasValue)
            {
                if (ch == quote.Value)
                {
                    quote = null;
                    continue;
                }

                current.Append(ch);
                continue;
            }

            if (ch is '"' or '\'')
            {
                quote = ch;
                continue;
            }

            if (char.IsWhiteSpace(ch))
            {
                if (current.Length > 0)
                {
                    result.Add(current.ToString());
                    current.Clear();
                }

                continue;
            }

            current.Append(ch);
        }

        if (quote.HasValue)
        {
            tokens = Array.Empty<string>();
            error = "Unterminated quoted argument.";
            return false;
        }

        if (current.Length > 0)
            result.Add(current.ToString());

        tokens = result;
        return true;
    }

    internal static async Task<ForeignKeyMigrationRequest> LoadForeignKeyMigrationRequestAsync(
        string specPath,
        bool validateOnly,
        string? backupPath,
        CancellationToken ct)
    {
        string normalizedSpecPath = NormalizePath(specPath);
        if (!File.Exists(normalizedSpecPath))
            throw new FileNotFoundException($"Foreign key migration spec file not found: {normalizedSpecPath}", normalizedSpecPath);

        string json = await File.ReadAllTextAsync(normalizedSpecPath, ct);
        var request = JsonSerializer.Deserialize<ForeignKeyMigrationRequest>(json, s_foreignKeyMigrationJsonOptions)
            ?? throw new InvalidOperationException($"Foreign key migration spec '{normalizedSpecPath}' did not deserialize.");

        return new ForeignKeyMigrationRequest
        {
            ValidateOnly = validateOnly || request.ValidateOnly,
            BackupDestinationPath = string.IsNullOrWhiteSpace(backupPath) ? request.BackupDestinationPath : NormalizePath(backupPath),
            ViolationSampleLimit = request.ViolationSampleLimit,
            Constraints = request.Constraints,
        };
    }

    internal static void WriteForeignKeyMigrationSummary(ForeignKeyMigrationResult result, TextWriter output)
    {
        var console = CliConsole.Create(output);
        if (result.ValidateOnly)
        {
            if (result.Succeeded)
                CliConsole.WriteSuccess(console, "Foreign key migration validation succeeded.");
            else
                CliConsole.WriteError(console, "Foreign key migration validation failed.");
        }
        else
        {
            if (result.Succeeded)
                CliConsole.WriteSuccess(console, "Foreign key migration completed.");
            else
                CliConsole.WriteError(console, "Foreign key migration failed.");
        }

        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Affected tables:[/]"), new Markup(CliConsole.Escape(result.AffectedTables.ToString())));
        summary.AddRow(new Markup("[bold]Applied foreign keys:[/]"), new Markup(CliConsole.Escape(result.AppliedForeignKeys.ToString())));
        summary.AddRow(new Markup("[bold]Copied rows:[/]"), new Markup(CliConsole.Escape(result.CopiedRows.ToString())));
        summary.AddRow(new Markup("[bold]Violations:[/]"), new Markup(CliConsole.Escape(result.ViolationCount.ToString())));

        if (!string.IsNullOrWhiteSpace(result.BackupDestinationPath))
            summary.AddRow(new Markup("[bold]Backup:[/]"), new Markup(CliConsole.Escape(result.BackupDestinationPath)));

        console.Write(summary);

        if (result.AppliedConstraints.Count > 0)
        {
            console.WriteLine();
            var constraints = CliConsole.CreateDataTable();
            constraints.AddColumn("[bold]Constraint[/]");
            constraints.AddColumn("[bold]Reference[/]");
            constraints.AddColumn("[bold]Options[/]");
            foreach (var applied in result.AppliedConstraints.OrderBy(item => item.TableName, StringComparer.OrdinalIgnoreCase).ThenBy(item => item.ColumnName, StringComparer.OrdinalIgnoreCase))
            {
                constraints.AddRow(
                    new Markup(CliConsole.Escape($"{applied.TableName}.{applied.ColumnName}")),
                    new Markup(CliConsole.Escape($"{applied.ReferencedTableName}({applied.ReferencedColumnName})")),
                    new Markup(CliConsole.Escape($"constraint={applied.ConstraintName}, onDelete={applied.OnDelete}, supportIndex={applied.SupportingIndexName}")));
            }
            console.Write(new Rule("[bold]Applied Constraints[/]").LeftJustified());
            console.Write(constraints);
        }

        if (result.Violations.Count > 0)
        {
            console.WriteLine();
            var violations = CliConsole.CreateDataTable();
            violations.AddColumn("[bold]Child[/]");
            violations.AddColumn("[bold]Value[/]");
            violations.AddColumn("[bold]Reference[/]");
            violations.AddColumn("[bold]Reason[/]");
            foreach (var violation in result.Violations)
            {
                violations.AddRow(
                    new Markup(CliConsole.Escape($"{violation.TableName}.{violation.ColumnName} / {violation.ChildKeyColumnName}={FormatValue(violation.ChildKeyValue)}")),
                    new Markup(CliConsole.Escape(FormatValue(violation.ChildValue))),
                    new Markup(CliConsole.Escape($"{violation.ReferencedTableName}({violation.ReferencedColumnName})")),
                    new Markup(CliConsole.Escape(violation.Reason)));
            }
            console.Write(new Rule("[bold]Violation Sample[/]").LeftJustified());
            console.Write(violations);
        }
    }

    private static JsonSerializerOptions CreateForeignKeyMigrationJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = true,
        };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    private static string FormatValue(object? value)
        => value switch
        {
            null => "NULL",
            string text => $"'{text}'",
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
        };

    private static CSharpDB.Client.Models.DbType MapDbType(CSharpDB.Primitives.DbType type) => type switch
    {
        CSharpDB.Primitives.DbType.Integer => CSharpDB.Client.Models.DbType.Integer,
        CSharpDB.Primitives.DbType.Real => CSharpDB.Client.Models.DbType.Real,
        CSharpDB.Primitives.DbType.Text => CSharpDB.Client.Models.DbType.Text,
        CSharpDB.Primitives.DbType.Blob => CSharpDB.Client.Models.DbType.Blob,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };
}

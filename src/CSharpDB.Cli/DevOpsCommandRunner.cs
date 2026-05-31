using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.DevOps;
using Spectre.Console;

namespace CSharpDB.Cli;

internal static class DevOpsCommandRunner
{
    private static readonly HashSet<string> KnownCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "compare",
        "drift",
    };

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    static DevOpsCommandRunner()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static bool IsKnownCommand(string? arg)
        => !string.IsNullOrWhiteSpace(arg) && KnownCommands.Contains(arg);

    public static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct = default)
    {
        if (args.Length == 0 || !IsKnownCommand(args[0]))
            return InspectorCommandRunner.ExitUsage;

        try
        {
            if (args[0].Equals("drift", StringComparison.OrdinalIgnoreCase))
                return await RunDriftAsync(args, output, error, ct);

            if (args.Length < 2)
            {
                await error.WriteLineAsync(CompareUsage);
                return InspectorCommandRunner.ExitUsage;
            }

            return args[1].ToLowerInvariant() switch
            {
                "schema" => await RunCompareSchemaAsync(args, output, error, ct),
                "data" => await RunCompareDataAsync(args, output, error, ct),
                _ => await WriteUsageAsync(error, CompareUsage),
            };
        }
        catch (FileNotFoundException ex)
        {
            await error.WriteLineAsync($"Error: {ex.Message}");
            return InspectorCommandRunner.ExitError;
        }
        catch (Exception ex)
        {
            await error.WriteLineAsync($"Error: {ex.Message}");
            return InspectorCommandRunner.ExitError;
        }
    }

    private static async ValueTask<int> RunCompareSchemaAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 4)
        {
            await error.WriteLineAsync("Usage: csharpdb compare schema <source> <target> [--json] [--script-out <file>]");
            return InspectorCommandRunner.ExitUsage;
        }

        string sourcePath = Path.GetFullPath(args[2]);
        string targetPath = Path.GetFullPath(args[3]);
        bool asJson = false;
        string? scriptOut = null;

        for (int i = 4; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--script-out":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --script-out.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    scriptOut = Path.GetFullPath(args[++i]);
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return InspectorCommandRunner.ExitUsage;
            }
        }

        await using DevOpsTargetHandle source = CreateTarget(sourcePath);
        await using DevOpsTargetHandle target = CreateTarget(targetPath);

        var service = new SchemaComparisonService();
        SchemaDiffReport report = await service.CompareAsync(source.SchemaTarget, target.SchemaTarget, ct);

        if (scriptOut is not null)
        {
            string script = SchemaScriptRenderer.RenderDeployScript(report);
            string? directory = Path.GetDirectoryName(scriptOut);
            if (!string.IsNullOrWhiteSpace(directory))
                Directory.CreateDirectory(directory);
            await File.WriteAllTextAsync(scriptOut, script, ct);
        }

        if (asJson)
        {
            await output.WriteLineAsync(JsonSerializer.Serialize(report, JsonOptions));
        }
        else
        {
            WriteSchemaCompareSummary(report, output);
            if (scriptOut is not null)
                await output.WriteLineAsync($"Deployment preview written to {scriptOut}");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunCompareDataAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 4)
        {
            await error.WriteLineAsync(DataCompareUsage);
            return InspectorCommandRunner.ExitUsage;
        }

        string sourcePath = Path.GetFullPath(args[2]);
        string targetPath = Path.GetFullPath(args[3]);
        string? tableName = null;
        IReadOnlyList<string> keyColumns = [];
        bool asJson = false;
        string? scriptOut = null;
        int? maxPreviewRows = null;

        for (int i = 4; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--table":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --table.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    tableName = args[++i];
                    break;
                case "--key":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --key.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    keyColumns = ParseCsv(args[++i]);
                    break;
                case "--json":
                    asJson = true;
                    break;
                case "--script-out":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --script-out.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    scriptOut = Path.GetFullPath(args[++i]);
                    break;
                case "--max-preview":
                    if (i + 1 >= args.Length || !int.TryParse(args[++i], out int parsedPreview) || parsedPreview < 0)
                    {
                        await error.WriteLineAsync("Invalid value for --max-preview.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    maxPreviewRows = parsedPreview;
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return InspectorCommandRunner.ExitUsage;
            }
        }

        if (string.IsNullOrWhiteSpace(tableName))
        {
            await error.WriteLineAsync("Missing required option: --table <name>.");
            return InspectorCommandRunner.ExitUsage;
        }

        await using DevOpsTargetHandle source = CreateTarget(sourcePath);
        await using DevOpsTargetHandle target = CreateTarget(targetPath);

        var service = new DataComparisonService();
        var report = await service.CompareAsync(
            source.DataTarget,
            target.DataTarget,
            new DataCompareOptions
            {
                TableName = tableName,
                KeyColumns = keyColumns,
                MaxPreviewRows = maxPreviewRows ?? (scriptOut is null ? 100 : int.MaxValue),
            },
            ct);

        if (scriptOut is not null)
        {
            WriteFileEnsuringDirectory(scriptOut, service.RenderSyncScript(report));
        }

        if (asJson)
        {
            await output.WriteLineAsync(JsonSerializer.Serialize(report, JsonOptions));
        }
        else
        {
            WriteDataCompareSummary(report, output);
            if (scriptOut is not null)
                await output.WriteLineAsync($"Data sync preview written to {scriptOut}");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunDriftAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync(DriftUsage);
            return InspectorCommandRunner.ExitUsage;
        }

        string currentPath = Path.GetFullPath(args[1]);
        string? baselinePath = null;
        string? tableName = null;
        IReadOnlyList<string> keyColumns = [];
        bool asJson = false;

        for (int i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--baseline":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --baseline.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    baselinePath = Path.GetFullPath(args[++i]);
                    break;
                case "--table":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --table.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    tableName = args[++i];
                    break;
                case "--key":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --key.");
                        return InspectorCommandRunner.ExitUsage;
                    }

                    keyColumns = ParseCsv(args[++i]);
                    break;
                case "--json":
                    asJson = true;
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return InspectorCommandRunner.ExitUsage;
            }
        }

        if (string.IsNullOrWhiteSpace(baselinePath))
        {
            await error.WriteLineAsync("Missing required option: --baseline <archive-or-dbfile>.");
            return InspectorCommandRunner.ExitUsage;
        }

        await using DevOpsTargetHandle baseline = CreateTarget(baselinePath);
        await using DevOpsTargetHandle current = CreateTarget(currentPath);

        var options = new DriftReportOptions
        {
            DataTables = string.IsNullOrWhiteSpace(tableName)
                ? []
                :
                [
                    new DataCompareOptions
                    {
                        TableName = tableName,
                        KeyColumns = keyColumns,
                        MaxPreviewRows = 100,
                    },
                ],
        };

        DriftReport report = await new DriftReportService().CreateAsync(
            baseline.SchemaTarget,
            current.SchemaTarget,
            baseline.DataTarget,
            current.DataTarget,
            options,
            ct);

        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(report, JsonOptions));
        else
            WriteDriftSummary(report, output);

        return report.Summary.HasDrift ? InspectorCommandRunner.ExitWarn : InspectorCommandRunner.ExitOk;
    }

    private static DevOpsTargetHandle CreateTarget(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Compare target not found: {path}", path);

        if (path.EndsWith(".csdbtable", StringComparison.OrdinalIgnoreCase))
        {
            return new DevOpsTargetHandle(
                new TableArchiveSchemaCompareTarget(path),
                new TableArchiveDataCompareTarget(path),
                client: null);
        }

        ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
        return new DevOpsTargetHandle(
            new ClientSchemaCompareTarget(client, Path.GetFileName(path)),
            new ClientDataCompareTarget(client, Path.GetFileName(path)),
            client);
    }

    private static void WriteSchemaCompareSummary(SchemaDiffReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        console.Write(new Rule("[bold deepskyblue1]Schema Compare[/]").LeftJustified().RuleStyle("grey42"));

        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Source:[/]"), new Markup(CliConsole.Escape(report.Source.DisplayName)));
        summary.AddRow(new Markup("[bold]Target:[/]"), new Markup(CliConsole.Escape(report.Target.DisplayName)));
        summary.AddRow(new Markup("[bold]Changes:[/]"), new Markup(CliConsole.Escape(report.Summary.TotalChanges.ToString())));
        summary.AddRow(new Markup("[bold]Destructive:[/]"), new Markup(CliConsole.Escape(report.Summary.DestructiveChanges.ToString())));
        console.Write(summary);

        if (report.Changes.Count == 0)
        {
            CliConsole.WriteSuccess(console, "Schemas match.");
            return;
        }

        console.WriteLine();
        var changes = CliConsole.CreateDataTable();
        changes.AddColumn("[bold]Action[/]");
        changes.AddColumn("[bold]Object[/]");
        changes.AddColumn("[bold]Name[/]");
        changes.AddColumn("[bold]Warning[/]");

        foreach (SchemaDiffChange change in report.Changes.Take(100))
        {
            changes.AddRow(
                new Markup(CliConsole.Escape(change.ChangeKind.ToString())),
                new Markup(CliConsole.Escape(change.ObjectKind.ToString())),
                new Markup(CliConsole.Escape(change.Name)),
                new Markup(CliConsole.Escape(change.Warning ?? string.Empty)));
        }

        console.Write(changes);
        if (report.Changes.Count > 100)
            CliConsole.WriteMuted(console, $"Showing 100 of {report.Changes.Count} changes. Use --json for the full report.");
    }

    private static void WriteDataCompareSummary(DataDiffReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        console.Write(new Rule("[bold deepskyblue1]Data Compare[/]").LeftJustified().RuleStyle("grey42"));

        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Source:[/]"), new Markup(CliConsole.Escape(report.Source.DisplayName)));
        summary.AddRow(new Markup("[bold]Target:[/]"), new Markup(CliConsole.Escape(report.Target.DisplayName)));
        summary.AddRow(new Markup("[bold]Table:[/]"), new Markup(CliConsole.Escape(report.TableName)));
        summary.AddRow(new Markup("[bold]Key:[/]"), new Markup(CliConsole.Escape(string.Join(", ", report.KeyColumns))));
        summary.AddRow(new Markup("[bold]Rows:[/]"), new Markup(CliConsole.Escape($"source={report.Summary.SourceRowCount}, target={report.Summary.TargetRowCount}")));
        summary.AddRow(new Markup("[bold]Differences:[/]"), new Markup(CliConsole.Escape($"sourceOnly={report.Summary.SourceOnlyRows}, targetOnly={report.Summary.TargetOnlyRows}, changed={report.Summary.ChangedRows}")));
        console.Write(summary);

        if (!report.Summary.HasDifferences)
        {
            CliConsole.WriteSuccess(console, "Table data matches.");
            return;
        }

        console.WriteLine();
        var rows = CliConsole.CreateDataTable();
        rows.AddColumn("[bold]Action[/]");
        rows.AddColumn("[bold]Key[/]");
        rows.AddColumn("[bold]Changed Columns[/]");

        foreach (DataDiffRow row in report.Rows.Take(100))
        {
            rows.AddRow(
                new Markup(CliConsole.Escape(row.ChangeKind.ToString())),
                new Markup(CliConsole.Escape(string.Join(", ", row.Key.Select(kvp => $"{kvp.Key}={kvp.Value ?? "NULL"}")))),
                new Markup(CliConsole.Escape(string.Join(", ", row.ChangedColumns))));
        }

        console.Write(rows);
        if (report.Rows.Count > 100)
            CliConsole.WriteMuted(console, $"Showing 100 of {report.Rows.Count} preview rows. Use --json for the full report.");
    }

    private static void WriteDriftSummary(DriftReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        console.Write(new Rule("[bold deepskyblue1]Drift Report[/]").LeftJustified().RuleStyle("grey42"));

        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Baseline:[/]"), new Markup(CliConsole.Escape(report.Baseline.DisplayName)));
        summary.AddRow(new Markup("[bold]Current:[/]"), new Markup(CliConsole.Escape(report.Current.DisplayName)));
        summary.AddRow(new Markup("[bold]Schema changes:[/]"), new Markup(CliConsole.Escape(report.Summary.SchemaChanges.ToString())));
        summary.AddRow(new Markup("[bold]Destructive schema changes:[/]"), new Markup(CliConsole.Escape(report.Summary.DestructiveSchemaChanges.ToString())));
        summary.AddRow(new Markup("[bold]Data tables compared:[/]"), new Markup(CliConsole.Escape(report.Summary.DataTablesCompared.ToString())));
        summary.AddRow(new Markup("[bold]Data rows different:[/]"), new Markup(CliConsole.Escape(report.Summary.DataRowsDifferent.ToString())));
        console.Write(summary);

        if (report.Summary.HasDrift)
            CliConsole.WriteWarning(console, "Drift detected.");
        else
            CliConsole.WriteSuccess(console, "No drift detected.");
    }

    private static IReadOnlyList<string> ParseCsv(string value)
        => value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static void WriteFileEnsuringDirectory(string path, string contents)
    {
        string? directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);
        File.WriteAllText(path, contents);
    }

    private static async ValueTask<int> WriteUsageAsync(TextWriter error, string usage)
    {
        await error.WriteLineAsync(usage);
        return InspectorCommandRunner.ExitUsage;
    }

    private const string CompareUsage =
        "Usage: csharpdb compare schema <source> <target> [--json] [--script-out <file>] OR csharpdb compare data <source> <target> --table <name> [--key <columns>] [--json] [--script-out <file>] [--max-preview <n>]";

    private const string DataCompareUsage =
        "Usage: csharpdb compare data <source> <target> --table <name> [--key <columns>] [--json] [--script-out <file>] [--max-preview <n>]";

    private const string DriftUsage =
        "Usage: csharpdb drift <dbfile> --baseline <archive-or-dbfile> [--table <name>] [--key <columns>] [--json]";

    private sealed class DevOpsTargetHandle : IAsyncDisposable
    {
        private readonly ICSharpDbClient? _client;

        public DevOpsTargetHandle(ISchemaCompareTarget schemaTarget, IDataCompareTarget dataTarget, ICSharpDbClient? client)
        {
            SchemaTarget = schemaTarget;
            DataTarget = dataTarget;
            _client = client;
        }

        public ISchemaCompareTarget SchemaTarget { get; }
        public IDataCompareTarget DataTarget { get; }

        public async ValueTask DisposeAsync()
        {
            if (_client is not null)
                await _client.DisposeAsync();
        }
    }
}

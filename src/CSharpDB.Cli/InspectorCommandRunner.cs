using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Storage.Diagnostics;
using Spectre.Console;

namespace CSharpDB.Cli;

internal static class InspectorCommandRunner
{
    internal const int ExitOk = 0;
    internal const int ExitWarn = 1;
    internal const int ExitError = 2;
    internal const int ExitUsage = 64;

    private static readonly HashSet<string> KnownCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "inspect",
        "inspect-page",
        "check-wal",
        "check-indexes",
    };

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    static InspectorCommandRunner()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter());
    }

    public static bool IsKnownCommand(string? arg)
    {
        return !string.IsNullOrWhiteSpace(arg) && KnownCommands.Contains(arg);
    }

    public static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct = default)
    {
        if (args.Length == 0 || !IsKnownCommand(args[0]))
            return ExitUsage;

        try
        {
            return args[0].ToLowerInvariant() switch
            {
                "inspect" => await RunInspectAsync(args, output, error, ct),
                "inspect-page" => await RunInspectPageAsync(args, output, error, ct),
                "check-wal" => await RunCheckWalAsync(args, output, error, ct),
                "check-indexes" => await RunCheckIndexesAsync(args, output, error, ct),
                _ => ExitUsage,
            };
        }
        catch (FileNotFoundException ex)
        {
            await error.WriteLineAsync($"Error: {ex.Message}");
            return ExitError;
        }
        catch (Exception ex)
        {
            await error.WriteLineAsync($"Error: {ex.Message}");
            return ExitError;
        }
    }

    private static async ValueTask<int> RunInspectAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb inspect <dbfile> [--json] [--out <file>] [--include-pages]");
            return ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        bool asJson = false;
        bool includePages = false;
        string? outFile = null;

        for (int i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--include-pages":
                    includePages = true;
                    break;
                case "--out":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --out.");
                        return ExitUsage;
                    }
                    outFile = Path.GetFullPath(args[++i]);
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return ExitUsage;
            }
        }

        var report = await DatabaseInspector.InspectAsync(
            dbPath,
            new DatabaseInspectOptions { IncludePages = includePages },
            ct);
        int exitCode = ExitCodeFromIssues(report.Issues);

        if (asJson)
        {
            string json = Serialize(report);
            if (outFile is null)
            {
                await output.WriteLineAsync(json);
            }
            else
            {
                await File.WriteAllTextAsync(outFile, json, ct);
                await output.WriteLineAsync($"Status: {StatusLabel(exitCode)} (report written to {outFile})");
            }
        }
        else
        {
            WriteInspectSummary(report, output);
            if (outFile is not null)
            {
                await File.WriteAllTextAsync(outFile, Serialize(report), ct);
                await output.WriteLineAsync($"JSON report written to {outFile}");
            }
        }

        return exitCode;
    }

    private static async ValueTask<int> RunInspectPageAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 3)
        {
            await error.WriteLineAsync("Usage: csharpdb inspect-page <dbfile> <pageId> [--json] [--hex]");
            return ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        if (!uint.TryParse(args[2], out uint pageId))
        {
            await error.WriteLineAsync($"Invalid pageId: {args[2]}");
            return ExitUsage;
        }

        bool asJson = false;
        bool includeHex = false;

        for (int i = 3; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--hex":
                    includeHex = true;
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return ExitUsage;
            }
        }

        var report = await DatabaseInspector.InspectPageAsync(dbPath, pageId, includeHex, ct);
        int exitCode = ExitCodeFromIssues(report.Issues);

        if (asJson)
        {
            await output.WriteLineAsync(Serialize(report));
        }
        else
        {
            WriteInspectPageSummary(report, output);
        }

        return exitCode;
    }

    private static async ValueTask<int> RunCheckWalAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb check-wal <dbfile> [--json]");
            return ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        bool asJson = false;

        for (int i = 2; i < args.Length; i++)
        {
            if (args[i] == "--json")
            {
                asJson = true;
            }
            else
            {
                await error.WriteLineAsync($"Unknown option: {args[i]}");
                return ExitUsage;
            }
        }

        var report = await WalInspector.InspectAsync(dbPath, options: null, ct);
        int exitCode = ExitCodeFromIssues(report.Issues);

        if (asJson)
        {
            await output.WriteLineAsync(Serialize(report));
        }
        else
        {
            WriteWalSummary(report, output);
        }

        return exitCode;
    }

    private static async ValueTask<int> RunCheckIndexesAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb check-indexes <dbfile> [--index <name>] [--sample <n>] [--json]");
            return ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        string? indexName = null;
        int? sampleSize = null;
        bool asJson = false;

        for (int i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--index":
                    if (i + 1 >= args.Length)
                    {
                        await error.WriteLineAsync("Missing value for --index.");
                        return ExitUsage;
                    }
                    indexName = args[++i];
                    break;
                case "--sample":
                    if (i + 1 >= args.Length || !int.TryParse(args[++i], out int parsed))
                    {
                        await error.WriteLineAsync("Invalid value for --sample.");
                        return ExitUsage;
                    }
                    sampleSize = parsed;
                    break;
                default:
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return ExitUsage;
            }
        }

        var report = await IndexInspector.CheckAsync(dbPath, indexName, sampleSize, ct);
        int exitCode = ExitCodeFromIssues(report.Issues);

        if (asJson)
        {
            await output.WriteLineAsync(Serialize(report));
        }
        else
        {
            WriteIndexSummary(report, output);
        }

        return exitCode;
    }

    private static int ExitCodeFromIssues(IEnumerable<IntegrityIssue> issues)
    {
        bool hasError = false;
        bool hasWarning = false;

        foreach (var issue in issues)
        {
            if (issue.Severity == InspectSeverity.Error)
                hasError = true;
            else if (issue.Severity == InspectSeverity.Warning)
                hasWarning = true;
        }

        if (hasError) return ExitError;
        if (hasWarning) return ExitWarn;
        return ExitOk;
    }

    private static string StatusLabel(int exitCode) => exitCode switch
    {
        ExitOk => "OK",
        ExitWarn => "WARN",
        ExitError => "ERROR",
        _ => "UNKNOWN",
    };

    private static string Serialize<T>(T report)
    {
        return JsonSerializer.Serialize(report, JsonOptions);
    }

    private static void WriteInspectSummary(DatabaseInspectReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Database:[/]"), new Markup(CliConsole.Escape(report.DatabasePath)));
        summary.AddRow(new Markup("[bold]File:[/]"), new Markup(CliConsole.Escape($"bytes={report.Header.FileLengthBytes}, pages={report.Header.PhysicalPageCount}")));
        summary.AddRow(
            new Markup("[bold]Header:[/]"),
            new Markup(CliConsole.Escape(
                $"magic={report.Header.Magic} ({Flag(report.Header.MagicValid)}), version={report.Header.Version} ({Flag(report.Header.VersionValid)}), pageSize={report.Header.PageSize} ({Flag(report.Header.PageSizeValid)})")));
        summary.AddRow(
            new Markup("[bold]Declared pages:[/]"),
            new Markup(CliConsole.Escape($"{report.Header.DeclaredPageCount} ({Flag(report.Header.DeclaredPageCountMatchesPhysical)})")));
        if (report.Pages is not null)
            summary.AddRow(new Markup("[bold]Detailed pages:[/]"), new Markup(CliConsole.Escape(report.Pages.Count.ToString())));
        console.Write(summary);
        console.WriteLine();

        var pageTypes = CliConsole.CreateDataTable();
        pageTypes.AddColumn("[bold]Page Type[/]");
        pageTypes.AddColumn("[bold]Count[/]");
        foreach (var kvp in report.PageTypeHistogram.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
            pageTypes.AddRow(new Markup(CliConsole.Escape(kvp.Key)), new Markup(CliConsole.Escape(kvp.Value.ToString())));
        console.Write(pageTypes);
        WriteIssues(report.Issues, output);
    }

    private static void WriteInspectPageSummary(PageInspectReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Database:[/]"), new Markup(CliConsole.Escape(report.DatabasePath)));
        summary.AddRow(new Markup("[bold]Page:[/]"), new Markup(CliConsole.Escape(report.PageId.ToString())));
        summary.AddRow(new Markup("[bold]Exists:[/]"), new Markup(CliConsole.Escape(report.Exists.ToString())));
        if (report.Page is not null)
        {
            summary.AddRow(
                new Markup("[bold]Type:[/]"),
                new Markup(CliConsole.Escape($"{report.Page.PageTypeName} ({report.Page.PageTypeCode}), cells={report.Page.CellCount}, freeSpace={report.Page.FreeSpaceBytes}")));
            summary.AddRow(new Markup("[bold]Cell content start:[/]"), new Markup(CliConsole.Escape(report.Page.CellContentStart.ToString())));
            summary.AddRow(new Markup("[bold]Right child / next leaf:[/]"), new Markup(CliConsole.Escape(report.Page.RightChildOrNextLeaf.ToString())));

            if (report.Page.LeafCells is not null)
                summary.AddRow(new Markup("[bold]Leaf cells:[/]"), new Markup(CliConsole.Escape(report.Page.LeafCells.Count.ToString())));
            if (report.Page.InteriorCells is not null)
                summary.AddRow(new Markup("[bold]Interior cells:[/]"), new Markup(CliConsole.Escape(report.Page.InteriorCells.Count.ToString())));
        }
        console.Write(summary);

        if (!string.IsNullOrWhiteSpace(report.HexDump))
        {
            console.WriteLine();
            CliConsole.WriteSqlPanel(console, $"Page {report.PageId} Hex Dump", report.HexDump);
        }

        WriteIssues(report.Issues, output);
    }

    private static void WriteWalSummary(WalInspectReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Database:[/]"), new Markup(CliConsole.Escape(report.DatabasePath)));
        summary.AddRow(new Markup("[bold]WAL:[/]"), new Markup(CliConsole.Escape(report.WalPath)));
        summary.AddRow(new Markup("[bold]Exists:[/]"), new Markup(CliConsole.Escape(report.Exists.ToString())));

        if (!report.Exists)
        {
            console.Write(summary);
            CliConsole.WriteMuted(console, "No WAL file present.");
            return;
        }

        summary.AddRow(new Markup("[bold]File bytes:[/]"), new Markup(CliConsole.Escape(report.FileLengthBytes.ToString())));
        summary.AddRow(
            new Markup("[bold]Header:[/]"),
            new Markup(CliConsole.Escape(
                $"magic={report.Magic} ({Flag(report.MagicValid)}), version={report.Version} ({Flag(report.VersionValid)}), pageSize={report.PageSize} ({Flag(report.PageSizeValid)})")));
        summary.AddRow(new Markup("[bold]Salts:[/]"), new Markup(CliConsole.Escape($"{report.Salt1}, {report.Salt2}")));
        summary.AddRow(
            new Markup("[bold]Frames:[/]"),
            new Markup(CliConsole.Escape($"full={report.FullFrameCount}, commits={report.CommitFrameCount}, trailingBytes={report.TrailingBytes}")));
        console.Write(summary);

        WriteIssues(report.Issues, output);
    }

    private static void WriteIndexSummary(IndexInspectReport report, TextWriter output)
    {
        var console = CliConsole.Create(output);
        var summary = CliConsole.CreateKeyValueTable();
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddColumn(new TableColumn(string.Empty));
        summary.AddRow(new Markup("[bold]Database:[/]"), new Markup(CliConsole.Escape(report.DatabasePath)));
        summary.AddRow(new Markup("[bold]Indexes checked:[/]"), new Markup(CliConsole.Escape(report.Indexes.Count.ToString())));
        if (!string.IsNullOrWhiteSpace(report.RequestedIndexName))
            summary.AddRow(new Markup("[bold]Requested index:[/]"), new Markup(CliConsole.Escape(report.RequestedIndexName)));
        summary.AddRow(new Markup("[bold]Sample size:[/]"), new Markup(CliConsole.Escape(report.SampleSize.ToString())));
        console.Write(summary);

        if (report.Indexes.Count > 0)
        {
            console.WriteLine();
            var table = CliConsole.CreateDataTable();
            table.AddColumn("[bold]Index[/]");
            table.AddColumn("[bold]Table[/]");
            table.AddColumn("[bold]Root[/]");
            table.AddColumn("[bold]Root OK[/]");
            table.AddColumn("[bold]Table OK[/]");
            table.AddColumn("[bold]Cols OK[/]");
            table.AddColumn("[bold]Reachable[/]");
            foreach (var item in report.Indexes)
            {
                table.AddRow(
                    new Markup(CliConsole.Escape(Trim(item.IndexName, 34))),
                    new Markup(CliConsole.Escape(Trim(item.TableName, 25))),
                    new Markup(CliConsole.Escape(item.RootPage.ToString())),
                    new Markup(CliConsole.Escape(Bool(item.RootPageValid))),
                    new Markup(CliConsole.Escape(Bool(item.TableExists))),
                    new Markup(CliConsole.Escape(Bool(item.ColumnsExistInTable))),
                    new Markup(CliConsole.Escape(Bool(item.RootTreeReachable))));
            }
            console.Write(table);
        }

        WriteIssues(report.Issues, output);
    }

    private static void WriteIssues(IReadOnlyList<IntegrityIssue> issues, TextWriter output)
    {
        var console = CliConsole.Create(output);
        int errors = issues.Count(i => i.Severity == InspectSeverity.Error);
        int warnings = issues.Count(i => i.Severity == InspectSeverity.Warning);
        int info = issues.Count(i => i.Severity == InspectSeverity.Info);

        console.WriteLine();
        console.MarkupLine($"[bold]Issues:[/] errors={errors}, warnings={warnings}, info={info}");

        if (issues.Count == 0)
            return;

        var table = CliConsole.CreateDataTable();
        table.AddColumn("[bold]Severity[/]");
        table.AddColumn("[bold]Code[/]");
        table.AddColumn("[bold]Location[/]");
        table.AddColumn("[bold]Message[/]");
        foreach (var issue in issues.OrderByDescending(i => i.Severity).ThenBy(i => i.Code, StringComparer.Ordinal))
        {
            string location = string.Empty;
            if (issue.PageId.HasValue)
                location += $" page={issue.PageId.Value}";
            if (issue.Offset.HasValue)
                location += $" offset={issue.Offset.Value}";
            table.AddRow(
                new Markup(CliConsole.Escape(issue.Severity.ToString())),
                new Markup(CliConsole.Escape(issue.Code)),
                new Markup(CliConsole.Escape(location.Trim())),
                new Markup(CliConsole.Escape(issue.Message)));
        }
        console.Write(table);
    }

    private static string Bool(bool value) => value ? "yes" : "no";
    private static string Flag(bool value) => value ? "ok" : "bad";

    private static string Trim(string value, int max)
    {
        if (value.Length <= max)
            return value;
        return value[..(max - 1)] + "…";
    }
}

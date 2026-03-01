using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Storage.Diagnostics;

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
        output.WriteLine($"Database: {report.DatabasePath}");
        output.WriteLine($"File bytes: {report.Header.FileLengthBytes}, pages: {report.Header.PhysicalPageCount}");
        output.WriteLine(
            $"Header: magic={report.Header.Magic} ({Flag(report.Header.MagicValid)}), " +
            $"version={report.Header.Version} ({Flag(report.Header.VersionValid)}), " +
            $"pageSize={report.Header.PageSize} ({Flag(report.Header.PageSizeValid)})");
        output.WriteLine(
            $"Declared pages: {report.Header.DeclaredPageCount} " +
            $"({Flag(report.Header.DeclaredPageCountMatchesPhysical)})");
        output.WriteLine("Page types:");
        foreach (var kvp in report.PageTypeHistogram.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
            output.WriteLine($"  {kvp.Key,-10} {kvp.Value}");

        if (report.Pages is not null)
            output.WriteLine($"Detailed pages included: {report.Pages.Count}");

        WriteIssues(report.Issues, output);
    }

    private static void WriteInspectPageSummary(PageInspectReport report, TextWriter output)
    {
        output.WriteLine($"Database: {report.DatabasePath}");
        output.WriteLine($"Page: {report.PageId}");
        output.WriteLine($"Exists: {report.Exists}");

        if (report.Page is not null)
        {
            output.WriteLine(
                $"Type: {report.Page.PageTypeName} ({report.Page.PageTypeCode}), " +
                $"cells={report.Page.CellCount}, freeSpace={report.Page.FreeSpaceBytes}");
            output.WriteLine($"Cell content start: {report.Page.CellContentStart}");
            output.WriteLine($"Right child / next leaf: {report.Page.RightChildOrNextLeaf}");

            if (report.Page.LeafCells is not null)
                output.WriteLine($"Leaf cells: {report.Page.LeafCells.Count}");
            if (report.Page.InteriorCells is not null)
                output.WriteLine($"Interior cells: {report.Page.InteriorCells.Count}");
        }

        if (!string.IsNullOrWhiteSpace(report.HexDump))
        {
            output.WriteLine();
            output.WriteLine(report.HexDump);
        }

        WriteIssues(report.Issues, output);
    }

    private static void WriteWalSummary(WalInspectReport report, TextWriter output)
    {
        output.WriteLine($"Database: {report.DatabasePath}");
        output.WriteLine($"WAL: {report.WalPath}");
        output.WriteLine($"Exists: {report.Exists}");

        if (!report.Exists)
        {
            output.WriteLine("No WAL file present.");
            return;
        }

        output.WriteLine($"File bytes: {report.FileLengthBytes}");
        output.WriteLine(
            $"Header: magic={report.Magic} ({Flag(report.MagicValid)}), " +
            $"version={report.Version} ({Flag(report.VersionValid)}), " +
            $"pageSize={report.PageSize} ({Flag(report.PageSizeValid)})");
        output.WriteLine($"Salts: {report.Salt1}, {report.Salt2}");
        output.WriteLine($"Frames: full={report.FullFrameCount}, commits={report.CommitFrameCount}, trailingBytes={report.TrailingBytes}");

        WriteIssues(report.Issues, output);
    }

    private static void WriteIndexSummary(IndexInspectReport report, TextWriter output)
    {
        output.WriteLine($"Database: {report.DatabasePath}");
        output.WriteLine($"Indexes checked: {report.Indexes.Count}");
        if (!string.IsNullOrWhiteSpace(report.RequestedIndexName))
            output.WriteLine($"Requested index: {report.RequestedIndexName}");
        output.WriteLine($"Sample size: {report.SampleSize}");

        if (report.Indexes.Count > 0)
        {
            output.WriteLine();
            output.WriteLine("Index                              Table                     Root  RootOK TableOK ColsOK Reachable");
            output.WriteLine("-----------------------------------------------------------------------------------------------------");
            foreach (var item in report.Indexes)
            {
                output.WriteLine(
                    $"{Trim(item.IndexName, 34),-34} " +
                    $"{Trim(item.TableName, 25),-25} " +
                    $"{item.RootPage,5} " +
                    $"{Bool(item.RootPageValid),6} " +
                    $"{Bool(item.TableExists),7} " +
                    $"{Bool(item.ColumnsExistInTable),6} " +
                    $"{Bool(item.RootTreeReachable),9}");
            }
        }

        WriteIssues(report.Issues, output);
    }

    private static void WriteIssues(IReadOnlyList<IntegrityIssue> issues, TextWriter output)
    {
        int errors = issues.Count(i => i.Severity == InspectSeverity.Error);
        int warnings = issues.Count(i => i.Severity == InspectSeverity.Warning);
        int info = issues.Count(i => i.Severity == InspectSeverity.Info);

        output.WriteLine();
        output.WriteLine($"Issues: errors={errors}, warnings={warnings}, info={info}");

        if (issues.Count == 0)
            return;

        foreach (var issue in issues.OrderByDescending(i => i.Severity).ThenBy(i => i.Code, StringComparer.Ordinal))
        {
            string location = string.Empty;
            if (issue.PageId.HasValue)
                location += $" page={issue.PageId.Value}";
            if (issue.Offset.HasValue)
                location += $" offset={issue.Offset.Value}";
            output.WriteLine($"  [{issue.Severity}] {issue.Code}:{location} {issue.Message}");
        }
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

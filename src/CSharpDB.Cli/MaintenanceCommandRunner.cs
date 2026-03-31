using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Cli;

internal static class MaintenanceCommandRunner
{
    private static readonly HashSet<string> KnownCommands = new(StringComparer.OrdinalIgnoreCase)
    {
        "maintenance-report",
        "migrate-foreign-keys",
        "reindex",
        "vacuum",
    };

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    static MaintenanceCommandRunner()
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
            return args[0].ToLowerInvariant() switch
            {
                "maintenance-report" => await RunMaintenanceReportAsync(args, output, error, ct),
                "migrate-foreign-keys" => await RunMigrateForeignKeysAsync(args, output, error, ct),
                "reindex" => await RunReindexAsync(args, output, error, ct),
                "vacuum" => await RunVacuumAsync(args, output, error, ct),
                _ => InspectorCommandRunner.ExitUsage,
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

    private static async ValueTask<int> RunMaintenanceReportAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb maintenance-report <dbfile> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        bool asJson = ExpectOnlyJsonFlag(args, 2, error);
        if (args.Length > 2 && !asJson)
            return InspectorCommandRunner.ExitUsage;

        await using var client = CreateClient(dbPath);
        var report = await client.GetMaintenanceReportAsync(ct);

        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(report, JsonOptions));
        else
            WriteMaintenanceReportSummary(report, output);

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunMigrateForeignKeysAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 4)
        {
            await error.WriteLineAsync("Usage: csharpdb migrate-foreign-keys <dbfile> --spec <json-file> [--validate-only] [--backup <file>] [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        if (!TryParseForeignKeyMigrationOptions(args, 2, error, out string? specPath, out bool validateOnly, out string? backupPath, out bool asJson))
            return InspectorCommandRunner.ExitUsage;

        var request = await MetaCommandHelpers.LoadForeignKeyMigrationRequestAsync(specPath!, validateOnly, backupPath, ct);

        await using var client = CreateClient(dbPath);
        var result = await client.MigrateForeignKeysAsync(request, ct);

        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
        else
            MetaCommandHelpers.WriteForeignKeyMigrationSummary(result, output);

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunReindexAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb reindex <dbfile> [--all|--table <name>|--index <name>] [--force-corrupt-rebuild] [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        if (!TryParseReindexOptions(args, 2, error, out var request, out bool asJson))
            return InspectorCommandRunner.ExitUsage;

        await using var client = CreateClient(dbPath);
        var result = await client.ReindexAsync(request, ct);

        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
        else
            WriteReindexSummary(result, output);

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunVacuumAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct)
    {
        if (args.Length < 2)
        {
            await error.WriteLineAsync("Usage: csharpdb vacuum <dbfile> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[1]);
        bool asJson = ExpectOnlyJsonFlag(args, 2, error);
        if (args.Length > 2 && !asJson)
            return InspectorCommandRunner.ExitUsage;

        await using var client = CreateClient(dbPath);
        var result = await client.VacuumAsync(ct);

        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
        else
            WriteVacuumSummary(result, output);

        return InspectorCommandRunner.ExitOk;
    }

    private static ICSharpDbClient CreateClient(string dbPath)
        => CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });

    private static bool ExpectOnlyJsonFlag(string[] args, int startIndex, TextWriter error)
    {
        if (args.Length == startIndex)
            return false;

        if (args.Length == startIndex + 1 && string.Equals(args[startIndex], "--json", StringComparison.OrdinalIgnoreCase))
            return true;

        _ = error.WriteLineAsync($"Unknown option: {args[startIndex]}");
        return false;
    }

    private static bool TryParseReindexOptions(
        string[] args,
        int startIndex,
        TextWriter error,
        out ReindexRequest request,
        out bool asJson)
    {
        var scope = ReindexScope.All;
        string? name = null;
        bool allowCorruptIndexRecovery = false;
        asJson = false;

        for (int i = startIndex; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--force-corrupt-rebuild":
                    allowCorruptIndexRecovery = true;
                    break;
                case "--all":
                    scope = ReindexScope.All;
                    name = null;
                    break;
                case "--table":
                    if (i + 1 >= args.Length)
                    {
                        _ = error.WriteLineAsync("Missing value for --table.");
                        request = new ReindexRequest();
                        return false;
                    }

                    scope = ReindexScope.Table;
                    name = args[++i];
                    break;
                case "--index":
                    if (i + 1 >= args.Length)
                    {
                        _ = error.WriteLineAsync("Missing value for --index.");
                        request = new ReindexRequest();
                        return false;
                    }

                    scope = ReindexScope.Index;
                    name = args[++i];
                    break;
                default:
                    _ = error.WriteLineAsync($"Unknown option: {args[i]}");
                    request = new ReindexRequest();
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

    private static bool TryParseForeignKeyMigrationOptions(
        string[] args,
        int startIndex,
        TextWriter error,
        out string? specPath,
        out bool validateOnly,
        out string? backupPath,
        out bool asJson)
    {
        specPath = null;
        validateOnly = false;
        backupPath = null;
        asJson = false;

        for (int i = startIndex; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--json":
                    asJson = true;
                    break;
                case "--validate-only":
                    validateOnly = true;
                    break;
                case "--spec":
                    if (i + 1 >= args.Length)
                    {
                        _ = error.WriteLineAsync("Missing value for --spec.");
                        return false;
                    }

                    specPath = args[++i];
                    break;
                case "--backup":
                    if (i + 1 >= args.Length)
                    {
                        _ = error.WriteLineAsync("Missing value for --backup.");
                        return false;
                    }

                    backupPath = args[++i];
                    break;
                default:
                    _ = error.WriteLineAsync($"Unknown option: {args[i]}");
                    return false;
            }
        }

        if (string.IsNullOrWhiteSpace(specPath))
        {
            _ = error.WriteLineAsync("Missing required option: --spec <json-file>.");
            return false;
        }

        return true;
    }

    private static void WriteMaintenanceReportSummary(DatabaseMaintenanceReport report, TextWriter output)
    {
        output.WriteLine($"Database: {report.DatabasePath}");
        output.WriteLine($"File bytes: {report.SpaceUsage.DatabaseFileBytes}");
        output.WriteLine($"WAL bytes: {report.SpaceUsage.WalFileBytes}");
        output.WriteLine($"Page size: {report.SpaceUsage.PageSizeBytes}");
        output.WriteLine($"Pages: physical={report.SpaceUsage.PhysicalPageCount}, declared={report.SpaceUsage.DeclaredPageCount}, freelist={report.SpaceUsage.FreelistPageCount}");
        output.WriteLine($"Fragmentation: btreeFree={report.Fragmentation.BTreeFreeBytes}, pagesWithFreeSpace={report.Fragmentation.PagesWithFreeSpace}, tailFreelist={report.Fragmentation.TailFreelistPageCount}");
    }

    private static void WriteReindexSummary(ReindexResult result, TextWriter output)
    {
        string target = result.Scope == ReindexScope.All || string.IsNullOrWhiteSpace(result.Name)
            ? result.Scope.ToString().ToLowerInvariant()
            : $"{result.Scope.ToString().ToLowerInvariant()}:{result.Name}";
        output.WriteLine($"Reindexed {result.RebuiltIndexCount} index(es) for {target}.");
        if (result.RecoveredCorruptIndexCount > 0)
            output.WriteLine($"Recovered {result.RecoveredCorruptIndexCount} corrupt index tree(s) without reclaim; run vacuum to reclaim orphaned pages.");
    }

    private static void WriteVacuumSummary(VacuumResult result, TextWriter output)
    {
        output.WriteLine($"Database bytes: {result.DatabaseFileBytesBefore} -> {result.DatabaseFileBytesAfter}");
        output.WriteLine($"Physical pages: {result.PhysicalPageCountBefore} -> {result.PhysicalPageCountAfter}");
    }
}

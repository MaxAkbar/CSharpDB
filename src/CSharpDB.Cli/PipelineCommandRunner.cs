using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client;
using CSharpDB.Client.Pipelines;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Cli;

internal static class PipelineCommandRunner
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    static PipelineCommandRunner()
    {
        JsonOptions.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
    }

    public static bool IsKnownCommand(string? arg)
        => string.Equals(arg, "etl", StringComparison.OrdinalIgnoreCase);

    public static async ValueTask<int> RunAsync(
        string[] args,
        TextWriter output,
        TextWriter error,
        CancellationToken ct = default)
    {
        if (args.Length < 2 || !IsKnownCommand(args[0]))
        {
            await error.WriteLineAsync("Usage: csharpdb etl <validate|dry-run|run> <dbfile> <packagefile> [--json]");
            await error.WriteLineAsync("       csharpdb etl <list> <dbfile> [--json]");
            await error.WriteLineAsync("       csharpdb etl <status|run-package|rejects|resume> <dbfile> <runId> [--json]");
            await error.WriteLineAsync("       csharpdb etl <pipelines|revisions|import|export|export-revision|delete|run-stored> ...");
            return InspectorCommandRunner.ExitUsage;
        }

        string verb = args[1].ToLowerInvariant();
        if (verb is "pipelines" or "revisions" or "import" or "export" or "export-revision" or "delete" or "run-stored")
            return await RunPipelineDefinitionCommandAsync(args, output, error, ct);

        if (verb is "list")
            return await RunListAsync(args, output, error, ct);

        if (verb is "status" or "run-package" or "rejects" or "resume")
            return await RunRunCatalogCommandAsync(args, output, error, ct);

        if (verb is not ("validate" or "dry-run" or "run"))
        {
            await error.WriteLineAsync($"Unsupported etl command '{args[1]}'.");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[2]);
        string packagePath = Path.GetFullPath(args[3]);
        bool asJson = args.Length > 4 && string.Equals(args[4], "--json", StringComparison.OrdinalIgnoreCase);
        if (args.Length > 5 || (args.Length == 5 && !asJson))
        {
            await error.WriteLineAsync("Usage: csharpdb etl <validate|dry-run|run> <dbfile> <packagefile> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        var mode = verb switch
        {
            "validate" => PipelineExecutionMode.Validate,
            "dry-run" => PipelineExecutionMode.DryRun,
            _ => PipelineExecutionMode.Run,
        };

        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = dbPath,
        });
        var runner = new CSharpDbPipelineRunner(client);
        PipelineRunResult result = await runner.RunPackageFileAsync(packagePath, mode, ct);

        if (asJson)
        {
            await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
        }
        else
        {
            await output.WriteLineAsync($"Run ID: {result.RunId}");
            await output.WriteLineAsync($"Pipeline: {result.PipelineName}");
            await output.WriteLineAsync($"Mode: {result.Mode}");
            await output.WriteLineAsync($"Status: {result.Status}");
            await output.WriteLineAsync($"Rows read: {result.Metrics.RowsRead}");
            await output.WriteLineAsync($"Rows written: {result.Metrics.RowsWritten}");
            await output.WriteLineAsync($"Batches completed: {result.Metrics.BatchesCompleted}");
            if (!string.IsNullOrWhiteSpace(result.ErrorSummary))
                await output.WriteLineAsync($"Error: {result.ErrorSummary}");
        }

        return result.Status == PipelineRunStatus.Succeeded
            ? InspectorCommandRunner.ExitOk
            : InspectorCommandRunner.ExitError;
    }

    private static async ValueTask<int> RunListAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        if (args.Length < 3)
        {
            await error.WriteLineAsync("Usage: csharpdb etl list <dbfile> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string dbPath = Path.GetFullPath(args[2]);
        bool asJson = args.Length > 3 && string.Equals(args[3], "--json", StringComparison.OrdinalIgnoreCase);

        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
        var catalog = new CSharpDbPipelineCatalogClient(client);
        var runs = await catalog.ListRunsAsync(ct: ct);

        if (asJson)
        {
            await output.WriteLineAsync(JsonSerializer.Serialize(runs, JsonOptions));
        }
        else
        {
            foreach (var run in runs)
                await output.WriteLineAsync($"{run.RunId}  {run.Status,-10} {run.Mode,-7} {run.PipelineName}");
        }

        return InspectorCommandRunner.ExitOk;
    }

    private static async ValueTask<int> RunRunCatalogCommandAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        if (args.Length < 4)
        {
            await error.WriteLineAsync("Usage: csharpdb etl <status|run-package|rejects|resume> <dbfile> <runId> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        string verb = args[1].ToLowerInvariant();
        string dbPath = Path.GetFullPath(args[2]);
        string runId = args[3];
        bool asJson = args.Length > 4 && string.Equals(args[4], "--json", StringComparison.OrdinalIgnoreCase);

        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
        var catalog = new CSharpDbPipelineCatalogClient(client);

        if (verb == "status")
        {
            var run = await catalog.GetRunAsync(runId, ct);
            if (run is null)
            {
                await error.WriteLineAsync($"Run '{runId}' not found.");
                return InspectorCommandRunner.ExitError;
            }

            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(run, JsonOptions));
            else
                await output.WriteLineAsync($"{run.RunId}  {run.Status}  {run.Mode}  {run.PipelineName}  read={run.Metrics.RowsRead} written={run.Metrics.RowsWritten}");

            return run.Status == PipelineRunStatus.Succeeded ? InspectorCommandRunner.ExitOk : InspectorCommandRunner.ExitWarn;
        }

        if (verb == "rejects")
        {
            var rejects = await catalog.GetRejectsAsync(runId, ct);
            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(rejects, JsonOptions));
            else
                foreach (var reject in rejects)
                    await output.WriteLineAsync($"row={reject.RowNumber}  reason={reject.Reason}");

            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "run-package")
        {
            var package = await catalog.GetRunPackageAsync(runId, ct);
            if (package is null)
            {
                await error.WriteLineAsync($"Run '{runId}' does not have a stored package definition.");
                return InspectorCommandRunner.ExitError;
            }

            if (asJson)
            {
                await output.WriteLineAsync(JsonSerializer.Serialize(package, JsonOptions));
            }
            else
            {
                await output.WriteLineAsync($"Pipeline: {package.Name}");
                await output.WriteLineAsync($"Version: {package.Version}");
                await output.WriteLineAsync($"Source: {package.Source.Kind}");
                await output.WriteLineAsync($"Destination: {package.Destination.Kind}");
                await output.WriteLineAsync($"Transforms: {package.Transforms.Count}");
            }

            return InspectorCommandRunner.ExitOk;
        }

        PipelineRunResult resumed = await catalog.ResumeAsync(runId, ct);
        if (asJson)
            await output.WriteLineAsync(JsonSerializer.Serialize(resumed, JsonOptions));
        else
            await output.WriteLineAsync($"Resumed {resumed.RunId}: {resumed.Status} ({resumed.Metrics.RowsRead} read / {resumed.Metrics.RowsWritten} written)");

        return resumed.Status == PipelineRunStatus.Succeeded ? InspectorCommandRunner.ExitOk : InspectorCommandRunner.ExitError;
    }

    private static async ValueTask<int> RunPipelineDefinitionCommandAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        string verb = args[1].ToLowerInvariant();

        if (verb == "pipelines")
        {
            if (args.Length < 3)
            {
                await error.WriteLineAsync("Usage: csharpdb etl pipelines <dbfile> [--json]");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            bool asJson = args.Length > 3 && string.Equals(args[3], "--json", StringComparison.OrdinalIgnoreCase);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var pipelines = await catalog.ListPipelinesAsync(ct: ct);

            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(pipelines, JsonOptions));
            else
                foreach (var pipeline in pipelines)
                    await output.WriteLineAsync($"{pipeline.Name}  v{pipeline.Version}  rev={pipeline.Revision}  {pipeline.Description}");

            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "revisions")
        {
            if (args.Length < 4)
            {
                await error.WriteLineAsync("Usage: csharpdb etl revisions <dbfile> <name> [--json]");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            string name = args[3];
            bool asJson = args.Length > 4 && string.Equals(args[4], "--json", StringComparison.OrdinalIgnoreCase);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var revisions = await catalog.ListPipelineRevisionsAsync(name, ct: ct);

            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(revisions, JsonOptions));
            else
                foreach (var revision in revisions)
                    await output.WriteLineAsync($"{revision.Name}  rev={revision.Revision}  v{revision.Version}  {revision.CreatedUtc.LocalDateTime}");

            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "import")
        {
            if (args.Length < 4)
            {
                await error.WriteLineAsync("Usage: csharpdb etl import <dbfile> <packagefile> [--name <name>] [--json]");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            string packagePath = Path.GetFullPath(args[3]);
            string? nameOverride = null;
            bool asJson = false;
            for (int i = 4; i < args.Length; i++)
            {
                if (string.Equals(args[i], "--json", StringComparison.OrdinalIgnoreCase))
                {
                    asJson = true;
                }
                else if (string.Equals(args[i], "--name", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                {
                    nameOverride = args[++i];
                }
                else
                {
                    await error.WriteLineAsync($"Unknown option: {args[i]}");
                    return InspectorCommandRunner.ExitUsage;
                }
            }

            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var package = await CSharpDB.Pipelines.Serialization.PipelinePackageSerializer.LoadFromFileAsync(packagePath, ct);
            var summary = await catalog.SavePipelineAsync(package, nameOverride, ct);
            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(summary, JsonOptions));
            else
                await output.WriteLineAsync($"Imported pipeline {summary.Name} revision {summary.Revision}.");

            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "export")
        {
            if (args.Length < 5)
            {
                await error.WriteLineAsync("Usage: csharpdb etl export <dbfile> <name> <packagefile>");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            string name = args[3];
            string packagePath = Path.GetFullPath(args[4]);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var package = await catalog.GetPipelineAsync(name, ct);
            if (package is null)
            {
                await error.WriteLineAsync($"Pipeline '{name}' not found.");
                return InspectorCommandRunner.ExitError;
            }

            await CSharpDB.Pipelines.Serialization.PipelinePackageSerializer.SaveToFileAsync(package, packagePath, ct);
            await output.WriteLineAsync($"Exported pipeline {name} to {packagePath}");
            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "export-revision")
        {
            if (args.Length < 6)
            {
                await error.WriteLineAsync("Usage: csharpdb etl export-revision <dbfile> <name> <revision> <packagefile>");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            string name = args[3];
            if (!int.TryParse(args[4], out int revision))
            {
                await error.WriteLineAsync($"Invalid revision '{args[4]}'.");
                return InspectorCommandRunner.ExitUsage;
            }

            string packagePath = Path.GetFullPath(args[5]);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var package = await catalog.GetPipelineRevisionAsync(name, revision, ct);
            if (package is null)
            {
                await error.WriteLineAsync($"Pipeline '{name}' revision {revision} not found.");
                return InspectorCommandRunner.ExitError;
            }

            await CSharpDB.Pipelines.Serialization.PipelinePackageSerializer.SaveToFileAsync(package, packagePath, ct);
            await output.WriteLineAsync($"Exported pipeline {name} revision {revision} to {packagePath}");
            return InspectorCommandRunner.ExitOk;
        }

        if (verb == "delete")
        {
            if (args.Length < 4)
            {
                await error.WriteLineAsync("Usage: csharpdb etl delete <dbfile> <name>");
                return InspectorCommandRunner.ExitUsage;
            }

            string dbPath = Path.GetFullPath(args[2]);
            string name = args[3];
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            await catalog.DeletePipelineAsync(name, ct);
            await output.WriteLineAsync($"Deleted pipeline {name}");
            return InspectorCommandRunner.ExitOk;
        }

        if (args.Length < 4)
        {
            await error.WriteLineAsync("Usage: csharpdb etl run-stored <dbfile> <name> [--json]");
            return InspectorCommandRunner.ExitUsage;
        }

        {
            string dbPath = Path.GetFullPath(args[2]);
            string name = args[3];
            bool asJson = args.Length > 4 && string.Equals(args[4], "--json", StringComparison.OrdinalIgnoreCase);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = dbPath });
            var catalog = new CSharpDbPipelineCatalogClient(client);
            var result = await catalog.RunStoredPipelineAsync(name, PipelineExecutionMode.Run, ct);
            if (asJson)
                await output.WriteLineAsync(JsonSerializer.Serialize(result, JsonOptions));
            else
                await output.WriteLineAsync($"Ran stored pipeline {name}: {result.Status}");

            return result.Status == PipelineRunStatus.Succeeded ? InspectorCommandRunner.ExitOk : InspectorCommandRunner.ExitError;
        }
    }
}

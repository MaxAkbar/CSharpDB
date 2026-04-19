using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Sql;
using ClientModels = CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class SampleSmokeTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private ICSharpDbClient _client = null!;

    public SampleSmokeTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_sample_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = _dbPath,
        });
        _ = await _client.GetInfoAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ProcurementAnalyticsSample_LoadsAndWorkbookExecutes()
    {
        string sampleDir = GetRepoPath("samples", "procurement-analytics");
        string schemaPath = Path.Combine(sampleDir, "schema.sql");
        string queriesPath = Path.Combine(sampleDir, "queries.sql");
        string proceduresPath = Path.Combine(sampleDir, "procedures.json");

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(await File.ReadAllTextAsync(schemaPath, Ct)))
        {
            var result = await _client.ExecuteSqlAsync(statement, Ct);
            Assert.Null(result.Error);
        }

        foreach (var definition in LoadProcedures(proceduresPath))
            await _client.CreateProcedureAsync(definition, Ct);

        var loadedProcedures = await _client.GetProceduresAsync(ct: Ct);
        Assert.Contains(loadedProcedures, procedure => procedure.Name == "RefreshProcurementStats");
        Assert.Contains(loadedProcedures, procedure => procedure.Name == "GetSupplierActionQueue");
        Assert.Contains(loadedProcedures, procedure => procedure.Name == "GetWarehouseReorderWatch");

        var statsRefresh = await _client.ExecuteProcedureAsync("RefreshProcurementStats", new Dictionary<string, object?>(), Ct);
        Assert.True(statsRefresh.Succeeded);
        Assert.NotEmpty(statsRefresh.Statements);
        Assert.NotNull(statsRefresh.Statements[^1].Rows);
        Assert.NotEmpty(statsRefresh.Statements[^1].Rows!);

        var actionQueue = await _client.ExecuteProcedureAsync("GetSupplierActionQueue", new Dictionary<string, object?>(), Ct);
        Assert.True(actionQueue.Succeeded);
        Assert.NotNull(actionQueue.Statements[^1].Rows);
        Assert.NotEmpty(actionQueue.Statements[^1].Rows!);

        var reorderWatch = await _client.ExecuteProcedureAsync(
            "GetWarehouseReorderWatch",
            new Dictionary<string, object?> { ["warehouseId"] = 3L },
            Ct);
        Assert.True(reorderWatch.Succeeded);
        Assert.NotNull(reorderWatch.Statements[^1].Rows);
        Assert.NotEmpty(reorderWatch.Statements[^1].Rows!);

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(await File.ReadAllTextAsync(queriesPath, Ct)))
        {
            var result = await _client.ExecuteSqlAsync(statement, Ct);
            Assert.Null(result.Error);
        }
    }

    [Fact]
    public async Task CsvBulkImportSample_RunsAndImportsBundledRows()
    {
        string repoRoot = GetRepoPath();
        string projectPath = GetRepoPath("samples", "csv-bulk-import", "CsvBulkImportSample.csproj");
        string csvPath = GetRepoPath("samples", "csv-bulk-import", "events.csv");
        string outputDir = Path.Combine(Path.GetTempPath(), $"csharpdb_csv_sample_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(outputDir, "imported-events.db");
        int expectedRowCount = File.ReadLines(csvPath)
            .Skip(1)
            .Count(line => !string.IsNullOrWhiteSpace(line));

        Directory.CreateDirectory(outputDir);

        try
        {
            ProcessResult result = await RunDotNetAsync(
                [
                    "run",
                    "--project",
                    projectPath,
                    "--verbosity",
                    "quiet",
                    "--",
                    "--database-path",
                    dbPath,
                ],
                repoRoot,
                Ct);

            Assert.Equal(0, result.ExitCode);
            Assert.DoesNotContain("Import failed:", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Import failed:", result.StdErr, StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(dbPath), "The sample did not produce the expected database file.");

            await using var db = await Database.OpenAsync(dbPath, Ct);

            await using (var countQuery = await db.ExecuteAsync("SELECT COUNT(*) FROM events;", Ct))
            {
                var rows = await countQuery.ToListAsync(Ct);
                Assert.Equal(expectedRowCount, rows[0][0].AsInteger);
            }

            await using (var warnQuery = await db.ExecuteAsync(
                             "SELECT COUNT(*) FROM events WHERE severity = 'warn';",
                             Ct))
            {
                var rows = await warnQuery.ToListAsync(Ct);
                Assert.True(rows[0][0].AsInteger > 0);
            }
        }
        finally
        {
            DeleteDirectoryIfExists(outputDir);
        }
    }

    [Fact]
    public async Task EfCoreProviderSample_RunsAndCreatesExpectedTables()
    {
        string repoRoot = GetRepoPath();
        string projectPath = GetRepoPath("samples", "efcore-provider", "EfCoreProviderSample.csproj");
        string outputDir = Path.Combine(Path.GetTempPath(), $"csharpdb_efcore_sample_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(outputDir, "efcore-provider.db");

        Directory.CreateDirectory(outputDir);

        try
        {
            ProcessResult result = await RunDotNetAsync(
                [
                    "run",
                    "--project",
                    projectPath,
                    "--verbosity",
                    "quiet",
                    "--",
                    "--database-path",
                    dbPath,
                ],
                repoRoot,
                Ct);

            Assert.Equal(0, result.ExitCode);
            Assert.True(File.Exists(dbPath), "The EF Core sample did not produce the expected database file.");
            Assert.Contains("Blogs: 2", result.StdOut, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Posts: 3", result.StdOut, StringComparison.OrdinalIgnoreCase);

            await using var db = await Database.OpenAsync(dbPath, Ct);

            await using (var blogCountQuery = await db.ExecuteAsync("SELECT COUNT(*) FROM Blogs;", Ct))
            {
                var rows = await blogCountQuery.ToListAsync(Ct);
                Assert.Equal(2L, rows[0][0].AsInteger);
            }

            await using (var postCountQuery = await db.ExecuteAsync("SELECT COUNT(*) FROM Posts;", Ct))
            {
                var rows = await postCountQuery.ToListAsync(Ct);
                Assert.Equal(3L, rows[0][0].AsInteger);
            }
        }
        finally
        {
            DeleteDirectoryIfExists(outputDir);
        }
    }

    private static IReadOnlyList<ClientModels.ProcedureDefinition> LoadProcedures(string proceduresPath)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
        };

        var payload = JsonSerializer.Deserialize<List<SampleProcedureDefinition>>(File.ReadAllText(proceduresPath), options)
            ?? [];

        return payload.Select(definition => new ClientModels.ProcedureDefinition
        {
            Name = definition.Name,
            BodySql = definition.BodySql,
            Parameters = definition.Parameters.Select(parameter => new ClientModels.ProcedureParameterDefinition
            {
                Name = parameter.Name,
                Type = Enum.Parse<ClientModels.DbType>(parameter.Type, ignoreCase: true),
                Required = parameter.Required,
                Default = parameter.Default,
                Description = parameter.Description,
            }).ToList(),
            Description = definition.Description,
            IsEnabled = definition.IsEnabled,
        }).ToList();
    }

    private static string GetRepoPath(params string[] segments)
    {
        string root = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
        return Path.Combine([root, .. segments]);
    }

    private static async Task<ProcessResult> RunDotNetAsync(
        IReadOnlyList<string> args,
        string workingDirectory,
        CancellationToken ct)
    {
        var startInfo = new ProcessStartInfo("dotnet")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        foreach (string arg in args)
            startInfo.ArgumentList.Add(arg);

        using var process = new Process { StartInfo = startInfo };
        if (!process.Start())
            throw new InvalidOperationException("Failed to start the sample process.");

        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        Task<string> stderrTask = process.StandardError.ReadToEndAsync(ct);

        await process.WaitForExitAsync(ct);

        return new ProcessResult(
            process.ExitCode,
            await stdoutTask,
            await stderrTask);
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }

    private sealed class SampleProcedureDefinition
    {
        public string Name { get; set; } = string.Empty;
        public string BodySql { get; set; } = string.Empty;
        public List<SampleProcedureParameter> Parameters { get; set; } = [];
        public string? Description { get; set; }
        public bool IsEnabled { get; set; } = true;
    }

    private sealed class SampleProcedureParameter
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = "TEXT";
        public bool Required { get; set; } = true;
        public object? Default { get; set; }
        public string? Description { get; set; }
    }

    private sealed record ProcessResult(int ExitCode, string StdOut, string StdErr);
}

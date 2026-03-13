using System.Text.Json;
using CSharpDB.Client;
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
}

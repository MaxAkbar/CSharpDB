using System.Net;
using System.Net.Http.Json;
using CSharpDB.Api.Dtos;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Api.Tests;

public sealed class ProcedureApiTests : IAsyncLifetime
{
    private string _dbPath = null!;
    private TestApiFactory _factory = null!;
    private HttpClient _client = null!;

    public ValueTask InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_api_proc_{Guid.NewGuid():N}.db");
        _factory = new TestApiFactory(_dbPath);
        _client = _factory.CreateClient();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _client.Dispose();
        await _factory.DisposeAsync();
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public async Task ProcedureCrudAndExecute_HappyPath()
    {
        var create = new CreateProcedureRequest(
            Name: "ApiProc",
            BodySql: """
                CREATE TABLE IF NOT EXISTS api_proc_data (id INTEGER PRIMARY KEY, name TEXT);
                INSERT INTO api_proc_data VALUES (@id, @name);
                SELECT id, name FROM api_proc_data WHERE id = @id;
                """,
            Parameters:
            [
                new ProcedureParameterRequest("id", "INTEGER", true),
                new ProcedureParameterRequest("name", "TEXT", false, "fallback"),
            ],
            Description: "API test",
            IsEnabled: true);

        var createResp = await _client.PostAsJsonAsync("/api/procedures", create);
        Assert.Equal(HttpStatusCode.Created, createResp.StatusCode);

        var listResp = await _client.GetFromJsonAsync<List<ProcedureSummaryResponse>>("/api/procedures");
        Assert.NotNull(listResp);
        Assert.Contains(listResp, p => p.Name == "ApiProc");

        var execResp = await _client.PostAsJsonAsync("/api/procedures/ApiProc/execute",
            new ExecuteProcedureRequest(new Dictionary<string, object?> { ["id"] = 10L }));
        Assert.Equal(HttpStatusCode.OK, execResp.StatusCode);

        var execution = await execResp.Content.ReadFromJsonAsync<ProcedureExecutionResponse>();
        Assert.NotNull(execution);
        Assert.True(execution.Succeeded);
        Assert.NotEmpty(execution.Statements);
    }

    [Fact]
    public async Task ExecuteProcedure_ValidationError_ReturnsBadRequestWithStructuredPayload()
    {
        var create = new CreateProcedureRequest(
            Name: "TypeProc",
            BodySql: "SELECT @id;",
            Parameters: [new ProcedureParameterRequest("id", "INTEGER", true)]);
        var createResp = await _client.PostAsJsonAsync("/api/procedures", create);
        Assert.Equal(HttpStatusCode.Created, createResp.StatusCode);

        var execResp = await _client.PostAsJsonAsync("/api/procedures/TypeProc/execute",
            new ExecuteProcedureRequest(new Dictionary<string, object?> { ["id"] = "bad" }));

        Assert.Equal(HttpStatusCode.BadRequest, execResp.StatusCode);
        var payload = await execResp.Content.ReadFromJsonAsync<ProcedureExecutionResponse>();
        Assert.NotNull(payload);
        Assert.False(payload.Succeeded);
        Assert.Contains("expects INTEGER", payload.Error ?? string.Empty);
    }

    [Fact]
    public async Task MissingProcedure_ReturnsNotFound()
    {
        var getResp = await _client.GetAsync("/api/procedures/Nope");
        Assert.Equal(HttpStatusCode.NotFound, getResp.StatusCode);

        var execResp = await _client.PostAsJsonAsync("/api/procedures/Nope/execute",
            new ExecuteProcedureRequest(new Dictionary<string, object?>()));
        Assert.Equal(HttpStatusCode.NotFound, execResp.StatusCode);
    }

    [Fact]
    public async Task TablesEndpoint_HidesProcedureCatalogTable()
    {
        var resp = await _client.GetAsync("/api/tables");
        resp.EnsureSuccessStatusCode();
        var tables = await resp.Content.ReadFromJsonAsync<List<string>>();
        Assert.NotNull(tables);
        Assert.DoesNotContain("__procedures", tables, StringComparer.OrdinalIgnoreCase);
    }

    private sealed class TestApiFactory(string dbPath) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                });
            });
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
            // Ignore transient file locks in test cleanup.
        }
    }
}

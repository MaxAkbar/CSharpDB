using System.Net;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcClientTests : IAsyncLifetime
{
    private string _dbPath = null!;
    private TestDaemonFactory _factory = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_grpc_{Guid.NewGuid():N}.db");
        _factory = new TestDaemonFactory(_dbPath);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _factory.DisposeAsync();
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public async Task GrpcClient_RowCrud_RoundTripsPrimitiveValues()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        DatabaseInfo info = await client.GetInfoAsync(Ct);
        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            "CREATE TABLE grpc_users (id INTEGER PRIMARY KEY, name TEXT, score REAL)",
            Ct);
        Assert.Null(createResult.Error);

        int inserted = await client.InsertRowAsync(
            "grpc_users",
            new Dictionary<string, object?>
            {
                ["id"] = 7,
                ["name"] = "seven",
                ["score"] = 12.5,
            },
            Ct);
        Assert.Equal(1, inserted);

        Dictionary<string, object?>? row = await client.GetRowByPkAsync("grpc_users", "id", 7, Ct);
        Assert.NotNull(row);
        Assert.Equal(7L, Assert.IsType<long>(row["id"]));
        Assert.Equal("seven", Assert.IsType<string>(row["name"]));
        Assert.Equal(12.5, Assert.IsType<double>(row["score"]));

        TableBrowseResult browse = await client.BrowseTableAsync("grpc_users", ct: Ct);
        Assert.Single(browse.Rows);
        Assert.Equal(7L, Assert.IsType<long>(browse.Rows[0][0]));
        Assert.Equal("seven", Assert.IsType<string>(browse.Rows[0][1]));
        Assert.Equal(12.5, Assert.IsType<double>(browse.Rows[0][2]));
    }

    [Fact]
    public async Task GrpcClient_ProcedureCrudAndValidation_WorkThroughTransport()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        await client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "GrpcProc",
                BodySql = """
                    CREATE TABLE IF NOT EXISTS grpc_proc_data (id INTEGER PRIMARY KEY, name TEXT);
                    INSERT INTO grpc_proc_data VALUES (@id, @name);
                    SELECT id, name FROM grpc_proc_data WHERE id = @id;
                    """,
                Parameters =
                [
                    new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
                    new ProcedureParameterDefinition { Name = "name", Type = DbType.Text, Required = false, Default = "fallback" },
                ],
                Description = "gRPC test",
                IsEnabled = true,
            },
            Ct);

        IReadOnlyList<ProcedureDefinition> procedures = await client.GetProceduresAsync(ct: Ct);
        Assert.Contains(procedures, p => p.Name == "GrpcProc");

        ProcedureExecutionResult execution = await client.ExecuteProcedureAsync(
            "GrpcProc",
            new Dictionary<string, object?> { ["id"] = 10L },
            Ct);

        Assert.True(execution.Succeeded);
        Assert.NotEmpty(execution.Statements);
        Assert.Equal(10L, Assert.IsType<long>(execution.Statements[^1].Rows![0][0]));
        Assert.Equal("fallback", Assert.IsType<string>(execution.Statements[^1].Rows![0][1]));

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "BrokenProc",
                BodySql = "SELECT @missing;",
                Parameters = [],
            },
            Ct));

        Assert.Contains("missing from params metadata", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private ICSharpDbClient CreateGrpcClient(HttpClient transportClient)
        => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private HttpClient CreateGrpcHttpClient()
    {
        return new HttpClient(_factory.Server.CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
    }

    private sealed class TestDaemonFactory(string dbPath) : WebApplicationFactory<Program>
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

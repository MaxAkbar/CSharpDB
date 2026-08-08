using System.Net;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Daemon.Tests;

public sealed class PhysicalExplainGrpcTransportTests
{
    private static readonly string[] PlanColumnNames =
    [
        "node_id",
        "parent_node_id",
        "operator_type",
        "estimated_rows",
        "estimated_cost",
        "actual_rows",
        "actual_loops",
        "elapsed_microseconds",
        "access_path",
        "object_name",
        "index_name",
        "join_type",
        "predicate",
        "status",
        "diagnostic_code",
    ];

    private static readonly string[] PlanColumnTypes =
    [
        "BIGINT",
        "BIGINT",
        "TEXT",
        "BIGINT",
        "DOUBLE PRECISION",
        "BIGINT",
        "BIGINT",
        "BIGINT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
        "TEXT",
    ];

    private static readonly bool[] PlanColumnNullability =
    [
        false,
        true,
        false,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        false,
        true,
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExplainAndAnalyze_PhysicalRowsAndNullableTypes_RoundTripOverGrpc()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_grpc_physical_explain_{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestDaemonFactory(dbPath);
            using HttpClient transportClient = CreateGrpcHttpClient(factory);
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    Transport = CSharpDbTransport.Grpc,
                    Endpoint = "http://localhost",
                    HttpClient = transportClient,
                });

            SqlExecutionResult seed = await client.ExecuteSqlAsync(
                """
                CREATE TABLE grpc_plan (
                    id INTEGER PRIMARY KEY,
                    payload TEXT
                );
                INSERT INTO grpc_plan VALUES (1, 'alpha'), (2, 'beta');
                """,
                Ct);
            Assert.Null(seed.Error);

            SqlExecutionResult planned = await client.ExecuteSqlAsync(
                "EXPLAIN SELECT payload FROM grpc_plan WHERE id = 2;",
                Ct);
            AssertPlanContract(planned);

            int parentNodeId = Ordinal(planned, "parent_node_id");
            int actualRows = Ordinal(planned, "actual_rows");
            int actualLoops = Ordinal(planned, "actual_loops");
            int elapsed = Ordinal(planned, "elapsed_microseconds");
            Assert.Contains(planned.Rows!, row => row[parentNodeId] is null);
            Assert.All(
                planned.Rows!,
                row =>
                {
                    Assert.Null(row[actualRows]);
                    Assert.Null(row[actualLoops]);
                    Assert.Null(row[elapsed]);
                });

            SqlExecutionResult analyzed = await client.ExecuteSqlAsync(
                "EXPLAIN ANALYZE SELECT payload FROM grpc_plan WHERE id = 2;",
                Ct);
            AssertPlanContract(analyzed);

            int operatorType = Ordinal(analyzed, "operator_type");
            int accessPath = Ordinal(analyzed, "access_path");
            actualRows = Ordinal(analyzed, "actual_rows");
            actualLoops = Ordinal(analyzed, "actual_loops");
            elapsed = Ordinal(analyzed, "elapsed_microseconds");

            object?[] lookup = Assert.Single(
                analyzed.Rows!,
                row => Equals(row[operatorType], "primary_key_lookup"));
            Assert.Equal("primary_key", lookup[accessPath]);
            Assert.Equal(1L, lookup[actualRows]);
            Assert.Equal(1L, lookup[actualLoops]);
            Assert.IsType<long>(lookup[elapsed]);
        }
        finally
        {
            DeleteIfPresent(dbPath);
            DeleteIfPresent(dbPath + ".wal");
            DeleteIfPresent(dbPath + ".shm");
        }
    }

    private static void AssertPlanContract(SqlExecutionResult result)
    {
        Assert.Null(result.Error);
        Assert.True(result.IsQuery);
        Assert.Equal(PlanColumnNames, result.ColumnNames);
        Assert.Equal(PlanColumnTypes, result.ColumnTypes);
        Assert.Equal(PlanColumnNullability, result.ColumnNullability);
        Assert.NotNull(result.Rows);
        Assert.NotEmpty(result.Rows);

        int nodeId = Ordinal(result, "node_id");
        Assert.All(result.Rows, row => Assert.IsType<long>(row[nodeId]));
    }

    private static int Ordinal(SqlExecutionResult result, string columnName)
        => Array.FindIndex(
            result.ColumnNames!,
            name => string.Equals(name, columnName, StringComparison.Ordinal));

    private static HttpClient CreateGrpcHttpClient(TestDaemonFactory factory)
        => new(factory.Server.CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };

    private sealed class TestDaemonFactory(string dbPath) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureServices(services =>
                services.AddHostedService<TestDaemonClientShutdown>());
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                });
            });
        }
    }

    private sealed class TestDaemonClientShutdown(ICSharpDbClient client) : IHostedService
    {
        private int _stopped;

        public Task StartAsync(CancellationToken _) => Task.CompletedTask;

        public async Task StopAsync(CancellationToken _)
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 0)
                await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void DeleteIfPresent(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Ignore transient test cleanup file locks.
        }
    }
}

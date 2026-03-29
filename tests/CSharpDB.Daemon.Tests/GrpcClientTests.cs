using System.Net;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;
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
    public async Task GrpcClient_Collections_RoundTripNestedDocuments()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        using JsonDocument document = JsonDocument.Parse("""
            {
              "name": "typed",
              "tags": ["grpc", "proto"],
              "meta": {
                "score": 9,
                "active": true
              }
            }
            """);

        await client.PutDocumentAsync("grpc_docs", "doc-1", document.RootElement, Ct);

        JsonElement? fetched = await client.GetDocumentAsync("grpc_docs", "doc-1", Ct);
        Assert.True(fetched.HasValue);
        Assert.Equal("typed", fetched.Value.GetProperty("name").GetString());
        Assert.Equal(9, fetched.Value.GetProperty("meta").GetProperty("score").GetInt32());

        CollectionBrowseResult browse = await client.BrowseCollectionAsync("grpc_docs", ct: Ct);
        Assert.Single(browse.Documents);
        Assert.Equal("doc-1", browse.Documents[0].Key);
        Assert.Equal("proto", browse.Documents[0].Document.GetProperty("tags")[1].GetString());
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

    [Fact]
    public async Task GrpcClient_BackupAndRestore_WorkThroughTransport()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_daemon_backup_{Guid.NewGuid():N}.db");
        string manifestPath = backupPath + ".manifest.json";

        try
        {
            SqlExecutionResult createResult = await client.ExecuteSqlAsync(
                "CREATE TABLE grpc_restore (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO grpc_restore VALUES (1, 'before');",
                Ct);
            Assert.Null(createResult.Error);

            BackupResult backup = await client.BackupAsync(new BackupRequest
            {
                DestinationPath = backupPath,
                WithManifest = true,
            }, Ct);

            Assert.Equal(Path.GetFullPath(backupPath), backup.DestinationPath);
            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(manifestPath));

            SqlExecutionResult mutateResult = await client.ExecuteSqlAsync(
                "INSERT INTO grpc_restore VALUES (2, 'after');",
                Ct);
            Assert.Null(mutateResult.Error);

            RestoreResult validate = await client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
                ValidateOnly = true,
            }, Ct);
            Assert.True(validate.ValidateOnly);

            RestoreResult restore = await client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
            }, Ct);
            Assert.False(restore.ValidateOnly);

            SqlExecutionResult rows = await client.ExecuteSqlAsync("SELECT id, value FROM grpc_restore ORDER BY id;", Ct);
            Assert.Null(rows.Error);
            Assert.NotNull(rows.Rows);
            var row = Assert.Single(rows.Rows);
            Assert.Equal(1L, row[0]);
            Assert.Equal("before", row[1]);
        }
        finally
        {
            TryDelete(backupPath);
            TryDelete(backupPath + ".wal");
            TryDelete(manifestPath);
        }
    }

    [Fact]
    public async Task GrpcClient_MutatingSchemaEndpoints_AcceptCollationMetadata()
    {
        using var transportClient = CreateGrpcHttpClient();
        await using var client = CreateGrpcClient(transportClient);

        SqlExecutionResult createResult = await client.ExecuteSqlAsync(
            "CREATE TABLE grpc_mutation_collation (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(createResult.Error);

        await client.AddColumnAsync("grpc_mutation_collation", "name", DbType.Text, notNull: false, collation: "NOCASE", ct: Ct);
        await client.CreateIndexAsync("idx_grpc_mutation_collation_name_binary", "grpc_mutation_collation", "name", isUnique: false, collation: "BINARY", ct: Ct);
        await client.UpdateIndexAsync("idx_grpc_mutation_collation_name_binary", "idx_grpc_mutation_collation_name_nocase", "grpc_mutation_collation", "name", isUnique: false, collation: "NOCASE", ct: Ct);

        TableSchema? schema = await client.GetTableSchemaAsync("grpc_mutation_collation", Ct);
        Assert.NotNull(schema);
        Assert.Equal("NOCASE", Assert.Single(schema!.Columns, column => column.Name == "name").Collation);

        IReadOnlyList<IndexSchema> indexes = await client.GetIndexesAsync(Ct);
        IndexSchema index = Assert.Single(indexes, item => item.IndexName == "idx_grpc_mutation_collation_name_nocase");
        Assert.Equal(["name"], index.Columns);
        Assert.Equal(["NOCASE"], index.ColumnCollations);
    }

    [Fact]
    public async Task GrpcContract_ExposesExplicitRpcMethods()
    {
        using var transportClient = CreateGrpcHttpClient();
        using var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpClient = transportClient,
            DisposeHttpClient = false,
        });

        var rpcClient = new CSharpDbRpc.CSharpDbRpcClient(channel);

        SqlExecutionResultMessage createResponse = await rpcClient.ExecuteSqlAsync(
            new SqlRequest
            {
                Sql = "CREATE TABLE grpc_contract (id INTEGER PRIMARY KEY, name TEXT)",
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.Null(createResponse.Error);

        await rpcClient.InsertRowAsync(
            new InsertRowRequest
            {
                TableName = "grpc_contract",
                Values = GrpcValueMapper.ToObject(new Dictionary<string, object?>
                {
                    ["id"] = 11L,
                    ["name"] = "typed",
                }),
            },
            cancellationToken: Ct).ResponseAsync;

        DatabaseInfoMessage infoResponse = await rpcClient.GetInfoAsync(new Empty(), cancellationToken: Ct).ResponseAsync;
        DatabaseInfo info = GrpcModelMapper.ToModel(infoResponse);
        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);

        StringList namesResponse = await rpcClient.GetTableNamesAsync(new Empty(), cancellationToken: Ct).ResponseAsync;
        IReadOnlyList<string> tableNames = GrpcModelMapper.ToStringList(namesResponse);
        Assert.Contains("grpc_contract", tableNames);

        OptionalVariantObjectResponse rowResponse = await rpcClient.GetRowByPkAsync(
            new GetRowByPkRequest
            {
                TableName = "grpc_contract",
                PkColumn = "id",
                PkValue = GrpcValueMapper.ToMessage(11),
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.NotNull(rowResponse.Value);
        Assert.Equal("typed", rowResponse.Value.Fields["name"].StringValue);

        await rpcClient.PutDocumentAsync(
            new PutDocumentRequest
            {
                CollectionName = "grpc_contract_docs",
                Key = "doc-1",
                Document = GrpcValueMapper.ToMessage(JsonDocument.Parse("""
                    {
                      "nested": {
                        "count": 3
                      },
                      "tags": ["typed", "contract"]
                    }
                    """).RootElement),
            },
            cancellationToken: Ct).ResponseAsync;

        OptionalVariantValueResponse documentResponse = await rpcClient.GetDocumentAsync(
            new GetDocumentRequest
            {
                CollectionName = "grpc_contract_docs",
                Key = "doc-1",
            },
            cancellationToken: Ct).ResponseAsync;

        Assert.NotNull(documentResponse.Value);
        Assert.Equal(3L, documentResponse.Value.ObjectValue.Fields["nested"].ObjectValue.Fields["count"].Int64Value);
        Assert.Equal("contract", documentResponse.Value.ObjectValue.Fields["tags"].ArrayValue.Items[1].StringValue);
    }

    private ICSharpDbClient CreateGrpcClient(HttpClient transportClient)
        => CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://localhost",
            HttpClient = transportClient,
        });

    private HttpClient CreateGrpcHttpClient()
        => CreateGrpcHttpClient(_factory);

    private static HttpClient CreateGrpcHttpClient(TestDaemonFactory factory)
    {
        return new HttpClient(factory.Server.CreateHandler())
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestVersion = HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
    }

    private sealed class TestDaemonFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.ConfigureAppConfiguration((_, config) =>
            {
                var values = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                };

                if (extraConfig is not null)
                {
                    foreach (var pair in extraConfig)
                        values[pair.Key] = pair.Value;
                }

                config.AddInMemoryCollection(values);
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

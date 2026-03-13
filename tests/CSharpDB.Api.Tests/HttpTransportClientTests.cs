using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Api.Tests;

public sealed class HttpTransportClientTests : IAsyncLifetime
{
    private string _dbPath = null!;
    private TestApiFactory _factory = null!;
    private HttpClient _httpClient = null!;
    private ICSharpDbClient _client = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_api_http_{Guid.NewGuid():N}.db");
        _factory = new TestApiFactory(_dbPath);
        _httpClient = _factory.CreateClient();
        _client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http,
            Endpoint = _httpClient.BaseAddress!.ToString(),
            HttpClient = _httpClient,
        });

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        _httpClient.Dispose();
        await _factory.DisposeAsync();
        TryDelete(_dbPath);
        TryDelete(_dbPath + ".wal");
    }

    [Fact]
    public async Task HttpTransport_SupportsTransactionsCollectionsSavedQueriesAndCheckpoint()
    {
        var createTable = await _client.ExecuteSqlAsync(
            "CREATE TABLE http_items (id INTEGER PRIMARY KEY, name TEXT);",
            Ct);
        Assert.Null(createTable.Error);

        int inserted = await _client.InsertRowAsync(
            "http_items",
            new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Ada" },
            Ct);
        Assert.Equal(1, inserted);

        var tx = await _client.BeginTransactionAsync(Ct);
        var txInsert = await _client.ExecuteInTransactionAsync(
            tx.TransactionId,
            "INSERT INTO http_items (id, name) VALUES (2, 'Grace');",
            Ct);
        Assert.Null(txInsert.Error);
        await _client.CommitTransactionAsync(tx.TransactionId, Ct);

        var rollbackTx = await _client.BeginTransactionAsync(Ct);
        var rollbackInsert = await _client.ExecuteInTransactionAsync(
            rollbackTx.TransactionId,
            "INSERT INTO http_items (id, name) VALUES (3, 'Rolled Back');",
            Ct);
        Assert.Null(rollbackInsert.Error);
        await _client.RollbackTransactionAsync(rollbackTx.TransactionId, Ct);

        int count = await _client.GetRowCountAsync("http_items", Ct);
        Assert.Equal(2, count);
        Assert.Null(await _client.GetRowByPkAsync("http_items", "id", 3L, Ct));

        var schema = await _client.GetTableSchemaAsync("http_items", Ct);
        Assert.NotNull(schema);
        Assert.Contains(schema!.Columns, column => column.Name == "id" && column.IsPrimaryKey);

        var saved = await _client.UpsertSavedQueryAsync("All Items", "SELECT * FROM http_items;", Ct);
        Assert.Equal("All Items", saved.Name);
        var savedAgain = await _client.GetSavedQueryAsync("All Items", Ct);
        Assert.NotNull(savedAgain);
        Assert.Contains("http_items", savedAgain!.SqlText, StringComparison.OrdinalIgnoreCase);
        var savedQueries = await _client.GetSavedQueriesAsync(Ct);
        Assert.Contains(savedQueries, query => query.Name == "All Items");

        JsonElement document;
        using (var json = JsonDocument.Parse("""{"name":"Ada","active":true}"""))
            document = json.RootElement.Clone();

        await _client.PutDocumentAsync("profiles", "user-1", document, Ct);
        var loadedDocument = await _client.GetDocumentAsync("profiles", "user-1", Ct);
        Assert.NotNull(loadedDocument);
        Assert.Equal("Ada", loadedDocument!.Value.GetProperty("name").GetString());

        var collections = await _client.GetCollectionNamesAsync(Ct);
        Assert.Contains("profiles", collections);
        Assert.Equal(1, await _client.GetCollectionCountAsync("profiles", Ct));

        var collectionBrowse = await _client.BrowseCollectionAsync("profiles", page: 1, pageSize: 10, ct: Ct);
        var collectionDoc = Assert.Single(collectionBrowse.Documents);
        Assert.Equal("user-1", collectionDoc.Key);

        Assert.True(await _client.DeleteDocumentAsync("profiles", "user-1", Ct));
        Assert.Null(await _client.GetDocumentAsync("profiles", "user-1", Ct));
        Assert.False(await _client.DeleteDocumentAsync("profiles", "missing", Ct));

        await _client.CheckpointAsync(Ct);

        var info = await _client.GetInfoAsync(Ct);
        Assert.True(info.TableCount >= 1);
        Assert.True(info.SavedQueryCount >= 1);
        Assert.True(info.CollectionCount >= 1);
    }

    [Fact]
    public async Task HttpTransport_MapsProcedureDetailsAndStructuredFailures()
    {
        await _client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "HttpProc",
                BodySql = """
                    CREATE TABLE IF NOT EXISTS http_proc_data (id INTEGER PRIMARY KEY, name TEXT);
                    INSERT INTO http_proc_data VALUES (@id, 'ok');
                    SELECT id, name FROM http_proc_data WHERE id = @id;
                    """,
                Parameters =
                [
                    new ProcedureParameterDefinition
                    {
                        Name = "id",
                        Type = DbType.Integer,
                        Required = true,
                    },
                ],
                Description = "HTTP transport test",
                IsEnabled = true,
            },
            Ct);

        var procedure = await _client.GetProcedureAsync("HttpProc", Ct);
        Assert.NotNull(procedure);
        Assert.Equal("HttpProc", procedure!.Name);
        Assert.Contains("http_proc_data", procedure.BodySql, StringComparison.Ordinal);

        var procedures = await _client.GetProceduresAsync(ct: Ct);
        var listed = Assert.Single(procedures, item => item.Name == "HttpProc");
        var parameter = Assert.Single(listed.Parameters);
        Assert.Equal("id", parameter.Name);
        Assert.Equal(DbType.Integer, parameter.Type);

        var execution = await _client.ExecuteProcedureAsync(
            "HttpProc",
            new Dictionary<string, object?> { ["id"] = 12L },
            Ct);
        Assert.True(execution.Succeeded);
        Assert.Equal(3, execution.Statements.Count);
        Assert.False(execution.Statements[0].IsQuery);
        Assert.Null(execution.Statements[0].Rows);
        Assert.False(execution.Statements[1].IsQuery);
        Assert.Null(execution.Statements[1].Rows);
        Assert.True(execution.Statements[2].IsQuery);
        var row = Assert.Single(Assert.IsAssignableFrom<IReadOnlyList<object?[]>>(execution.Statements[2].Rows));
        Assert.Equal(12L, row[0]);
        Assert.Equal("ok", row[1]);

        var failedExecution = await _client.ExecuteProcedureAsync(
            "HttpProc",
            new Dictionary<string, object?> { ["id"] = "bad" },
            Ct);
        Assert.False(failedExecution.Succeeded);
        Assert.Contains("expects INTEGER", failedExecution.Error ?? string.Empty);

        var sqlError = await _client.ExecuteSqlAsync("SELECT FROM", Ct);
        Assert.NotNull(sqlError.Error);
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
        }
    }
}

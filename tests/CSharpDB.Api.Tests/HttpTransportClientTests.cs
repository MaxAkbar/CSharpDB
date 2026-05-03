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
        await DeleteIfExistsAsync(_dbPath);
        await DeleteIfExistsAsync(_dbPath + ".wal");
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
        await _client.DropCollectionAsync("profiles", Ct);
        collections = await _client.GetCollectionNamesAsync(Ct);
        Assert.DoesNotContain("profiles", collections);

        await _client.CheckpointAsync(Ct);

        var info = await _client.GetInfoAsync(Ct);
        Assert.True(info.TableCount >= 1);
        Assert.True(info.SavedQueryCount >= 1);
        Assert.Equal(0, info.CollectionCount);
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

    [Fact]
    public async Task HttpTransport_MapsCollationMetadata()
    {
        var createTable = await _client.ExecuteSqlAsync(
            "CREATE TABLE http_collation_items (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE);",
            Ct);
        Assert.Null(createTable.Error);

        var createIndex = await _client.ExecuteSqlAsync(
            "CREATE INDEX idx_http_collation_items_name_binary ON http_collation_items(name COLLATE BINARY);",
            Ct);
        Assert.Null(createIndex.Error);

        var schema = await _client.GetTableSchemaAsync("http_collation_items", Ct);
        Assert.NotNull(schema);
        Assert.Equal("NOCASE", Assert.Single(schema!.Columns, column => column.Name == "name").Collation);

        var indexes = await _client.GetIndexesAsync(Ct);
        var index = Assert.Single(indexes, item => item.IndexName == "idx_http_collation_items_name_binary");
        Assert.Equal(["name"], index.Columns);
        Assert.Equal(["BINARY"], index.ColumnCollations);
    }

    [Fact]
    public async Task HttpTransport_MapsForeignKeyMetadata()
    {
        var create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE http_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES http_parents(id) ON DELETE CASCADE
            );
            """,
            Ct);
        Assert.Null(create.Error);

        var schema = await _client.GetTableSchemaAsync("http_children", Ct);
        Assert.NotNull(schema);
        var foreignKey = Assert.Single(schema!.ForeignKeys);
        Assert.Equal("parent_id", foreignKey.ColumnName);
        Assert.Equal("http_parents", foreignKey.ReferencedTableName);
        Assert.Equal("id", foreignKey.ReferencedColumnName);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
        Assert.StartsWith("__fk_http_children_parent_id_", foreignKey.SupportingIndexName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_MigrateForeignKeys_RoundTripsValidationAndApply()
    {
        var create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_migrate_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE http_migrate_children (id INTEGER PRIMARY KEY, parent_id INTEGER);
            INSERT INTO http_migrate_parents VALUES (1);
            INSERT INTO http_migrate_children VALUES (10, 1);
            """,
            Ct);
        Assert.Null(create.Error);

        var validate = await _client.MigrateForeignKeysAsync(
            new ForeignKeyMigrationRequest
            {
                ValidateOnly = true,
                Constraints =
                [
                    new ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "http_migrate_children",
                        ColumnName = "parent_id",
                        ReferencedTableName = "http_migrate_parents",
                        ReferencedColumnName = "id",
                    },
                ],
            },
            Ct);

        Assert.True(validate.ValidateOnly);
        Assert.True(validate.Succeeded);
        Assert.Equal(1, validate.AppliedForeignKeys);
        Assert.Empty(validate.Violations);

        var apply = await _client.MigrateForeignKeysAsync(
            new ForeignKeyMigrationRequest
            {
                Constraints =
                [
                    new ForeignKeyMigrationConstraintSpec
                    {
                        TableName = "http_migrate_children",
                        ColumnName = "parent_id",
                        ReferencedTableName = "http_migrate_parents",
                        ReferencedColumnName = "id",
                        OnDelete = ForeignKeyOnDeleteAction.Cascade,
                    },
                ],
            },
            Ct);

        Assert.False(apply.ValidateOnly);
        Assert.True(apply.Succeeded);
        Assert.Equal(1, apply.CopiedRows);

        var schema = await _client.GetTableSchemaAsync("http_migrate_children", Ct);
        Assert.NotNull(schema);
        var foreignKey = Assert.Single(schema!.ForeignKeys);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
    }

    [Fact]
    public async Task HttpTransport_MutatingSchemaEndpoints_AcceptCollationMetadata()
    {
        var createTable = await _client.ExecuteSqlAsync(
            "CREATE TABLE http_mutation_collation (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(createTable.Error);

        await _client.AddColumnAsync("http_mutation_collation", "name", DbType.Text, notNull: false, collation: "NOCASE", ct: Ct);
        await _client.CreateIndexAsync("idx_http_mutation_collation_name_binary", "http_mutation_collation", "name", isUnique: false, collation: "BINARY", ct: Ct);
        await _client.UpdateIndexAsync("idx_http_mutation_collation_name_binary", "idx_http_mutation_collation_name_nocase", "http_mutation_collation", "name", isUnique: false, collation: "NOCASE", ct: Ct);

        var schema = await _client.GetTableSchemaAsync("http_mutation_collation", Ct);
        Assert.NotNull(schema);
        Assert.Equal("NOCASE", Assert.Single(schema!.Columns, column => column.Name == "name").Collation);

        var indexes = await _client.GetIndexesAsync(Ct);
        var index = Assert.Single(indexes, item => item.IndexName == "idx_http_mutation_collation_name_nocase");
        Assert.Equal(["name"], index.Columns);
        Assert.Equal(["NOCASE"], index.ColumnCollations);
    }

    [Fact]
    public async Task HttpTransport_BackupAndRestore_WorkThroughApi()
    {
        string backupPath = Path.Combine(Path.GetTempPath(), $"csharpdb_api_backup_{Guid.NewGuid():N}.db");
        string manifestPath = backupPath + ".manifest.json";

        try
        {
            var create = await _client.ExecuteSqlAsync(
                "CREATE TABLE http_restore (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO http_restore VALUES (1, 'before');",
                Ct);
            Assert.Null(create.Error);

            var backup = await _client.BackupAsync(new BackupRequest
            {
                DestinationPath = backupPath,
                WithManifest = true,
            }, Ct);

            Assert.Equal(Path.GetFullPath(backupPath), backup.DestinationPath);
            Assert.True(File.Exists(backupPath));
            Assert.True(File.Exists(manifestPath));

            var mutate = await _client.ExecuteSqlAsync("INSERT INTO http_restore VALUES (2, 'after');", Ct);
            Assert.Null(mutate.Error);

            var validate = await _client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
                ValidateOnly = true,
            }, Ct);
            Assert.True(validate.ValidateOnly);

            var restore = await _client.RestoreAsync(new RestoreRequest
            {
                SourcePath = backupPath,
            }, Ct);
            Assert.False(restore.ValidateOnly);

            var rows = await _client.ExecuteSqlAsync("SELECT id, value FROM http_restore ORDER BY id;", Ct);
            Assert.Null(rows.Error);
            Assert.NotNull(rows.Rows);
            var row = Assert.Single(rows.Rows);
            Assert.Equal(1L, row[0]);
            Assert.Equal("before", row[1]);
        }
        finally
        {
            await DeleteIfExistsAsync(backupPath);
            await DeleteIfExistsAsync(backupPath + ".wal");
            await DeleteIfExistsAsync(manifestPath);
        }
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

    private static async ValueTask DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastException = null;
        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex) when (sw.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex) when (sw.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;

            if (sw.Elapsed >= TimeSpan.FromSeconds(2))
                break;

            await Task.Delay(25);
        }

        throw new IOException($"Failed to delete temporary database file '{path}' within the cleanup timeout.", lastException);
    }
}

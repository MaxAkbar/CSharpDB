using System.Net;
using System.Net.Http.Json;
using System.Reflection;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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
    public async Task ExecuteSql_ResourceLimitExceeded_ReturnsRequestEntityTooLarge()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_api_window_limit_{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestApiFactory(
                dbPath,
                directDatabaseOptions: new CSharpDB.Engine.DatabaseOptions
                {
                    WindowExecution = new CSharpDB.Primitives.WindowExecutionOptions
                    {
                        MaxPartitionRows = 2,
                        MaxBufferedRows = 4,
                    },
                });
            using HttpClient httpClient = factory.CreateClient();

            using HttpResponseMessage seed = await httpClient.PostAsJsonAsync(
                "/api/sql/execute",
                new
                {
                    Sql = """
                        CREATE TABLE api_window_limit_rows (id INTEGER PRIMARY KEY, group_id INTEGER);
                        INSERT INTO api_window_limit_rows VALUES (1, 1), (2, 1), (3, 1);
                        """,
                },
                Ct);
            Assert.Equal(HttpStatusCode.OK, seed.StatusCode);

            using HttpResponseMessage response = await httpClient.PostAsJsonAsync(
                "/api/sql/execute",
                new
                {
                    Sql = """
                        SELECT ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id)
                        FROM api_window_limit_rows;
                        """,
                },
                Ct);

            Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
            using JsonDocument problem = JsonDocument.Parse(
                await response.Content.ReadAsStringAsync(Ct));
            Assert.Contains(
                "partition",
                problem.RootElement.GetProperty("detail").GetString(),
                StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            await DeleteIfExistsAsync(dbPath);
            await DeleteIfExistsAsync(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task SqlAndTransactionEndpoints_ForwardRequestCancellationTokens()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_api_cancellation_{Guid.NewGuid():N}.db");
        ICSharpDbClient captureClient =
            DispatchProxy.Create<ICSharpDbClient, CancellationCaptureClientProxy>();
        var capture = (CancellationCaptureClientProxy)captureClient;

        try
        {
            await using var factory = new TestApiFactory(
                dbPath,
                clientOverride: captureClient);
            using HttpClient httpClient = factory.CreateClient();
            using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);

            using HttpResponseMessage execute = await httpClient.PostAsJsonAsync(
                "/api/sql/execute",
                new { Sql = "SELECT 1;" },
                requestCancellation.Token);
            using HttpResponseMessage begin = await httpClient.PostAsJsonAsync(
                "/api/transactions",
                new { },
                requestCancellation.Token);
            using HttpResponseMessage transactionalExecute = await httpClient.PostAsJsonAsync(
                "/api/transactions/tx-cancellation/execute",
                new { Sql = "SELECT 1;" },
                requestCancellation.Token);
            using HttpResponseMessage commit = await httpClient.PostAsJsonAsync(
                "/api/transactions/tx-cancellation/commit",
                new { },
                requestCancellation.Token);
            using HttpResponseMessage rollback = await httpClient.PostAsJsonAsync(
                "/api/transactions/tx-cancellation/rollback",
                new { },
                requestCancellation.Token);

            Assert.True(execute.IsSuccessStatusCode);
            Assert.True(begin.IsSuccessStatusCode);
            Assert.True(transactionalExecute.IsSuccessStatusCode);
            Assert.True(commit.IsSuccessStatusCode);
            Assert.True(rollback.IsSuccessStatusCode);
            Assert.True(capture.ExecuteSqlCancellationToken.CanBeCanceled);
            Assert.True(capture.BeginTransactionCancellationToken.CanBeCanceled);
            Assert.True(capture.ExecuteInTransactionCancellationToken.CanBeCanceled);
            Assert.True(capture.CommitTransactionCancellationToken.CanBeCanceled);
            Assert.True(capture.RollbackTransactionCancellationToken.CanBeCanceled);
        }
        finally
        {
            await captureClient.DisposeAsync();
            await DeleteIfExistsAsync(dbPath);
            await DeleteIfExistsAsync(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task HttpTransport_GetTableSchemaAsync_PreservesRowVersionMetadata()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            "CREATE TABLE http_versions (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL);",
            Ct);
        Assert.Null(create.Error);

        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync("http_versions", Ct));
        ColumnDefinition version = Assert.Single(schema.Columns, column => column.Name == "version");

        Assert.Equal(DbType.Blob, version.Type);
        Assert.False(version.Nullable);
        Assert.True(version.IsRowVersion);
    }

    [Fact]
    public async Task HttpTransport_PreservesLogicalTypesAndExactDecimalValues()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_logical_types (
                id INTEGER PRIMARY KEY,
                amount DECIMAL(18,6),
                token UUID,
                payload BINARY(3)
            );
            INSERT INTO http_logical_types VALUES (
                1,
                123456789012.345678,
                '00112233-4455-6677-8899-aabbccddeeff',
                X'010203'
            );
            """,
            Ct);
        Assert.Null(create.Error);

        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync("http_logical_types", Ct));
        ColumnDefinition amount = Assert.Single(schema.Columns, column => column.Name == "amount");
        ColumnDefinition token = Assert.Single(schema.Columns, column => column.Name == "token");
        ColumnDefinition payload = Assert.Single(schema.Columns, column => column.Name == "payload");
        Assert.Equal(DbType.Decimal, amount.Type);
        Assert.Equal("DECIMAL(18,6)", amount.EffectiveType.ToSql());
        Assert.Equal(DbType.Blob, token.Type);
        Assert.Equal("UUID", token.EffectiveType.ToSql());
        Assert.Equal("BINARY(3)", payload.EffectiveType.ToSql());

        SqlExecutionResult query = await _client.ExecuteSqlAsync(
            "SELECT amount, token, payload FROM http_logical_types;",
            Ct);
        Assert.Null(query.Error);
        Assert.Equal(
            ["DECIMAL(18,6)", "UUID", "BINARY(3)"],
            Assert.IsType<string[]>(query.ColumnTypes));
        object?[] row = Assert.Single(query.Rows!);
        Assert.Equal(123456789012.345678m, Assert.IsType<decimal>(row[0]));
        Assert.Equal(
            Convert.FromHexString("00112233445566778899AABBCCDDEEFF"),
            Assert.IsType<byte[]>(row[1]));
        Assert.Equal([1, 2, 3], Assert.IsType<byte[]>(row[2]));
    }

    [Fact]
    public async Task HttpTransport_GetTableSchemaAsync_SupportsDotSegmentTableName()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            "CREATE TABLE \".\" (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(create.Error);

        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync(".", Ct));

        Assert.Equal(".", schema.TableName);
        Assert.NotEqual(Guid.Empty, schema.SchemaId);
    }

    [Fact]
    public async Task HttpTransport_TableAndRowOperations_SupportPathSyntaxInIdentifiers()
    {
        const string OriginalTableName = "http/items";
        const string RenamedTableName = "http/items-renamed";
        const string PrimaryKeyName = "key/id";
        const string ValueColumnName = "select";
        const string AddedColumnName = "extra/value";
        const string RenamedColumnName = "renamed/value";

        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE "http/items" (
                "key/id" INTEGER PRIMARY KEY,
                "select" TEXT
            );
            """,
            Ct);
        Assert.Null(create.Error);

        Assert.Equal(
            1,
            await _client.InsertRowAsync(
                OriginalTableName,
                new Dictionary<string, object?>
                {
                    [PrimaryKeyName] = 1L,
                    [ValueColumnName] = "Ada",
                },
                Ct));
        Assert.Equal(1, await _client.GetRowCountAsync(OriginalTableName, Ct));

        TableBrowseResult browse = await _client.BrowseTableAsync(
            OriginalTableName,
            ct: Ct);
        Assert.Single(browse.Rows);

        Dictionary<string, object?> row = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync(
                OriginalTableName,
                PrimaryKeyName,
                1L,
                Ct));
        Assert.Equal("Ada", row[ValueColumnName]);

        Assert.Equal(
            1,
            await _client.UpdateRowAsync(
                OriginalTableName,
                PrimaryKeyName,
                1L,
                new Dictionary<string, object?>
                {
                    [ValueColumnName] = "Grace",
                },
                Ct));

        await _client.AddColumnAsync(
            OriginalTableName,
            AddedColumnName,
            DbType.Text,
            notNull: false,
            Ct);
        await _client.RenameColumnAsync(
            OriginalTableName,
            AddedColumnName,
            RenamedColumnName,
            Ct);
        await _client.DropColumnAsync(
            OriginalTableName,
            RenamedColumnName,
            Ct);
        await _client.RenameTableAsync(
            OriginalTableName,
            RenamedTableName,
            Ct);

        Assert.Equal(
            1,
            await _client.DeleteRowAsync(
                RenamedTableName,
                PrimaryKeyName,
                1L,
                Ct));
        await _client.DropTableAsync(RenamedTableName, Ct);
        Assert.Null(await _client.GetTableSchemaAsync(RenamedTableName, Ct));
    }

    [Fact]
    public async Task HttpTransport_QueryRoutes_DoNotShadowLegacyDropTableNames()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE "row" (id INTEGER PRIMARY KEY);
            CREATE TABLE "columns" (id INTEGER PRIMARY KEY);
            """,
            Ct);
        Assert.Null(create.Error);

        using HttpResponseMessage dropRow = await _httpClient.DeleteAsync(
            "/api/tables/row",
            Ct);
        using HttpResponseMessage dropColumns = await _httpClient.DeleteAsync(
            "/api/tables/columns",
            Ct);

        Assert.Equal(HttpStatusCode.NoContent, dropRow.StatusCode);
        Assert.Equal(HttpStatusCode.NoContent, dropColumns.StatusCode);
        Assert.Null(await _client.GetTableSchemaAsync("row", Ct));
        Assert.Null(await _client.GetTableSchemaAsync("columns", Ct));
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
    public async Task HttpTransport_ExecutesPublicPlannerDiagnostics()
    {
        Assert.Null((await _client.ExecuteSqlAsync(
            "CREATE TABLE http_planner_diag (id INTEGER PRIMARY KEY, value INTEGER);",
            Ct)).Error);
        Assert.Null((await _client.ExecuteSqlAsync(
            "INSERT INTO http_planner_diag VALUES (1, 4), (2, 4), (3, 9);",
            Ct)).Error);
        Assert.Null((await _client.ExecuteSqlAsync("ANALYZE http_planner_diag;", Ct)).Error);

        SqlExecutionResult catalog = await _client.ExecuteSqlAsync(
            "SELECT COUNT(*) FROM sys.planner_histograms WHERE table_name = 'http_planner_diag';",
            Ct);
        Assert.Null(catalog.Error);
        Assert.NotNull(catalog.Rows);
        Assert.True(Convert.ToInt64(Assert.Single(catalog.Rows)[0]) > 0);

        SqlExecutionResult explain = await _client.ExecuteSqlAsync(
            "EXPLAIN ESTIMATE FOR SELECT * FROM http_planner_diag WHERE value = 4;",
            Ct);
        Assert.Null(explain.Error);
        Assert.NotNull(explain.Rows);
        Assert.Contains(explain.Rows, row => string.Equals(Convert.ToString(row[4]), "heavy-hitter", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HttpTransport_RejectsStatelessTempCommandsButSupportsTransactionTempWorkflow()
    {
        SqlExecutionResult rejected = await _client.ExecuteSqlAsync(
            "CREATE TEMP TABLE http_temp (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.NotNull(rejected.Error);
        Assert.Contains("transaction session", rejected.Error, StringComparison.OrdinalIgnoreCase);

        var tx = await _client.BeginTransactionAsync(Ct);
        SqlExecutionResult create = await _client.ExecuteInTransactionAsync(
            tx.TransactionId,
            "CREATE TEMP TABLE http_temp (id INTEGER PRIMARY KEY);",
            Ct);
        Assert.Null(create.Error);

        SqlExecutionResult insert = await _client.ExecuteInTransactionAsync(
            tx.TransactionId,
            "INSERT INTO http_temp VALUES (1);",
            Ct);
        Assert.Null(insert.Error);

        SqlExecutionResult count = await _client.ExecuteInTransactionAsync(
            tx.TransactionId,
            "SELECT COUNT(*) FROM http_temp;",
            Ct);
        Assert.Null(count.Error);
        Assert.Equal(1L, Convert.ToInt64(Assert.Single(count.Rows!)[0]));

        await _client.CommitTransactionAsync(tx.TransactionId, Ct);
    }

    [Fact]
    public async Task RestApi_DefaultNoAuthModeStillWorks()
    {
        DatabaseInfo info = await _client.GetInfoAsync(Ct);

        Assert.Equal(Path.GetFullPath(_dbPath), info.DataSource);
    }

    [Fact]
    public async Task RestApi_ApiKeyModeRejectsMissingAndWrongKeysWithoutEchoingSecret()
    {
        const string secret = "api-secret-value";
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_api_auth_{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestApiFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:Api:Security:Mode"] = "ApiKey",
                    ["CSharpDB:Api:Security:ApiKey"] = secret,
                });

            using var httpClient = factory.CreateClient();

            using HttpResponseMessage missingResponse = await httpClient.GetAsync("/api/info", Ct);
            Assert.Equal(HttpStatusCode.Unauthorized, missingResponse.StatusCode);
            string missingPayload = await missingResponse.Content.ReadAsStringAsync(Ct);
            Assert.DoesNotContain(secret, missingPayload, StringComparison.Ordinal);

            using var wrongRequest = new HttpRequestMessage(HttpMethod.Get, "/api/info");
            wrongRequest.Headers.TryAddWithoutValidation("X-CSharpDB-Api-Key", "wrong-secret");
            using HttpResponseMessage wrongResponse = await httpClient.SendAsync(wrongRequest, Ct);
            Assert.Equal(HttpStatusCode.Unauthorized, wrongResponse.StatusCode);
            string wrongPayload = await wrongResponse.Content.ReadAsStringAsync(Ct);
            Assert.DoesNotContain(secret, wrongPayload, StringComparison.Ordinal);
        }
        finally
        {
            await DeleteIfExistsAsync(dbPath);
            await DeleteIfExistsAsync(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task RestApi_ApiKeyModeAcceptsCorrectKeyAndClientOption()
    {
        const string secret = "api-client-secret";
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_api_auth_client_{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestApiFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:Api:Security:Mode"] = "ApiKey",
                    ["CSharpDB:Api:Security:ApiKey"] = secret,
                });

            using var httpClient = factory.CreateClient();
            using var request = new HttpRequestMessage(HttpMethod.Get, "/api/info");
            request.Headers.TryAddWithoutValidation("X-CSharpDB-Api-Key", secret);
            using HttpResponseMessage response = await httpClient.SendAsync(request, Ct);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress!.ToString(),
                HttpClient = httpClient,
                ApiKey = secret,
            });

            DatabaseInfo info = await client.GetInfoAsync(Ct);
            Assert.Equal(Path.GetFullPath(dbPath), info.DataSource);
        }
        finally
        {
            await DeleteIfExistsAsync(dbPath);
            await DeleteIfExistsAsync(dbPath + ".wal");
        }
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
    public async Task HttpTransport_PreservesBlobValuesAcrossTypedResultPaths()
    {
        byte[] storedPayload = [0x00, 0x01, 0xFE, 0xFF];
        byte[] defaultPayload = [0x10, 0x20, 0x30];

        SqlExecutionResult seed = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_blob_paths (
                id INTEGER PRIMARY KEY,
                payload BLOB
            );
            INSERT INTO http_blob_paths VALUES (1, X'0001FEFF');
            CREATE VIEW http_blob_view AS
                SELECT id, payload FROM http_blob_paths;
            """,
            Ct);
        Assert.Null(seed.Error);

        TableBrowseResult table = await _client.BrowseTableAsync(
            "http_blob_paths",
            ct: Ct);
        object?[] tableRow = Assert.Single(table.Rows);
        Assert.Equal(storedPayload, Assert.IsType<byte[]>(tableRow[1]));

        Dictionary<string, object?> row = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync(
                "http_blob_paths",
                "id",
                1L,
                Ct));
        Assert.Equal(storedPayload, Assert.IsType<byte[]>(row["payload"]));

        ViewBrowseResult view = await _client.BrowseViewAsync(
            "http_blob_view",
            ct: Ct);
        Assert.Equal(
            ["INTEGER", "BLOB"],
            Assert.IsType<string[]>(view.ColumnTypes));
        object?[] viewRow = Assert.Single(view.Rows);
        Assert.Equal(storedPayload, Assert.IsType<byte[]>(viewRow[1]));

        await _client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "HttpBlobDefault",
                BodySql = "SELECT @payload AS payload;",
                Parameters =
                [
                    new ProcedureParameterDefinition
                    {
                        Name = "payload",
                        Type = DbType.Blob,
                        Required = false,
                        Default = defaultPayload,
                    },
                ],
            },
            Ct);

        ProcedureDefinition detail = Assert.IsType<ProcedureDefinition>(
            await _client.GetProcedureAsync("HttpBlobDefault", Ct));
        Assert.Equal(
            defaultPayload,
            Assert.IsType<byte[]>(Assert.Single(detail.Parameters).Default));

        ProcedureDefinition listed = Assert.Single(
            await _client.GetProceduresAsync(ct: Ct),
            procedure => procedure.Name == "HttpBlobDefault");
        Assert.Equal(
            defaultPayload,
            Assert.IsType<byte[]>(Assert.Single(listed.Parameters).Default));

        ProcedureExecutionResult execution = await _client.ExecuteProcedureAsync(
            "HttpBlobDefault",
            new Dictionary<string, object?>(),
            Ct);
        Assert.True(execution.Succeeded);
        ProcedureStatementExecutionResult statement = Assert.Single(execution.Statements);
        Assert.Equal(["BLOB"], Assert.IsType<string[]>(statement.ColumnTypes));
        object?[] procedureRow = Assert.Single(statement.Rows!);
        Assert.Equal(defaultPayload, Assert.IsType<byte[]>(Assert.Single(procedureRow)));
    }

    [Fact]
    public async Task HttpTransport_BinaryBackedRowsAndKeysRoundTripThroughCrud()
    {
        byte[] key = [0x00, 0x01, 0xFE, 0xFF];
        byte[] uuid = Convert.FromHexString("00112233445566778899AABBCCDDEEFF");
        const string Base64LookingText = "AQID";

        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_binary_crud (
                key_blob BLOB PRIMARY KEY,
                fixed_bytes BINARY(4),
                varying_bytes VARBINARY(6),
                token UUID,
                flags BIT(8),
                note TEXT
            );
            CREATE TABLE http_text_key_crud (
                key_text TEXT PRIMARY KEY,
                payload BLOB,
                note TEXT
            );
            CREATE TABLE http_fixed_binary_key_crud (
                key_bytes BINARY(4) PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE http_varying_binary_key_crud (
                key_bytes VARBINARY(4) PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE http_bit_key_crud (
                key_bits BIT(8) PRIMARY KEY,
                note TEXT
            );
            """,
            Ct);
        Assert.Null(create.Error);

        Assert.Equal(
            1,
            await _client.InsertRowAsync(
                "http_binary_crud",
                new Dictionary<string, object?>
                {
                    ["key_blob"] = key,
                    ["fixed_bytes"] = new byte[] { 0x10, 0x20 },
                    ["varying_bytes"] = new byte[] { 0x30, 0x40, 0x50 },
                    ["token"] = uuid,
                    ["flags"] = new byte[] { 0xA5 },
                    ["note"] = Base64LookingText,
                },
                Ct));

        Dictionary<string, object?> inserted = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync(
                "http_binary_crud",
                "key_blob",
                key,
                Ct));
        byte[] returnedKey = Assert.IsType<byte[]>(inserted["key_blob"]);
        Assert.Equal(key, returnedKey);
        Assert.Equal([0x10, 0x20, 0x00, 0x00], Assert.IsType<byte[]>(inserted["fixed_bytes"]));
        Assert.Equal([0x30, 0x40, 0x50], Assert.IsType<byte[]>(inserted["varying_bytes"]));
        Assert.Equal(uuid, Assert.IsType<byte[]>(inserted["token"]));
        SqlBitString insertedFlags = Assert.IsType<SqlBitString>(inserted["flags"]);
        Assert.Equal(8, insertedFlags.BitLength);
        Assert.Equal([0xA5], insertedFlags.PackedBytes.ToArray());
        Assert.Equal(Base64LookingText, inserted["note"]);

        byte[] updatedUuid = Convert.FromHexString("FFEEDDCCBBAA99887766554433221100");
        Assert.Equal(
            1,
            await _client.UpdateRowAsync(
                "http_binary_crud",
                "key_blob",
                returnedKey,
                new Dictionary<string, object?>
                {
                    ["fixed_bytes"] = new byte[] { 0x01, 0x02, 0x03, 0x04 },
                    ["varying_bytes"] = new byte[] { 0x05 },
                    ["token"] = updatedUuid,
                    ["flags"] = new byte[] { 0x5A },
                    ["note"] = "AAE=",
                },
                Ct));

        Dictionary<string, object?> updated = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync(
                "http_binary_crud",
                "key_blob",
                returnedKey,
                Ct));
        Assert.Equal([0x01, 0x02, 0x03, 0x04], Assert.IsType<byte[]>(updated["fixed_bytes"]));
        Assert.Equal([0x05], Assert.IsType<byte[]>(updated["varying_bytes"]));
        Assert.Equal(updatedUuid, Assert.IsType<byte[]>(updated["token"]));
        SqlBitString updatedFlags = Assert.IsType<SqlBitString>(updated["flags"]);
        Assert.Equal(8, updatedFlags.BitLength);
        Assert.Equal([0x5A], updatedFlags.PackedBytes.ToArray());
        Assert.Equal("AAE=", updated["note"]);

        Assert.Equal(
            1,
            await _client.DeleteRowAsync(
                "http_binary_crud",
                "key_blob",
                returnedKey,
                Ct));
        Assert.Null(await _client.GetRowByPkAsync(
            "http_binary_crud",
            "key_blob",
            returnedKey,
            Ct));

        Assert.Equal(
            1,
            await _client.InsertRowAsync(
                "http_text_key_crud",
                new Dictionary<string, object?>
                {
                    ["key_text"] = Base64LookingText,
                    ["payload"] = new byte[] { 0xDE, 0xAD },
                    ["note"] = "AA==",
                },
                Ct));
        Dictionary<string, object?> textKeyRow = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync(
                "http_text_key_crud",
                "key_text",
                Base64LookingText,
                Ct));
        Assert.Equal(Base64LookingText, textKeyRow["key_text"]);
        Assert.Equal([0xDE, 0xAD], Assert.IsType<byte[]>(textKeyRow["payload"]));
        Assert.Equal("AA==", textKeyRow["note"]);
        Assert.Equal(
            1,
            await _client.DeleteRowAsync(
                "http_text_key_crud",
                "key_text",
                Base64LookingText,
                Ct));

        await AssertBinaryKeyCrudAsync(
            "http_fixed_binary_key_crud",
            "key_bytes",
            [0x10, 0x20, 0x30, 0x40]);
        await AssertBinaryKeyCrudAsync(
            "http_varying_binary_key_crud",
            "key_bytes",
            [0x50, 0x60]);
        await AssertBinaryKeyCrudAsync(
            "http_bit_key_crud",
            "key_bits",
            [0xA5]);

        async Task AssertBinaryKeyCrudAsync(
            string tableName,
            string columnName,
            byte[] binaryKey)
        {
            Assert.Equal(
                1,
                await _client.InsertRowAsync(
                    tableName,
                    new Dictionary<string, object?>
                    {
                        [columnName] = binaryKey,
                        ["note"] = "initial",
                    },
                    Ct));
            Assert.NotNull(await _client.GetRowByPkAsync(
                tableName,
                columnName,
                binaryKey,
                Ct));
            Assert.Equal(
                1,
                await _client.UpdateRowAsync(
                    tableName,
                    columnName,
                    binaryKey,
                    new Dictionary<string, object?> { ["note"] = "updated" },
                    Ct));
            Assert.Equal(
                "updated",
                (await _client.GetRowByPkAsync(
                    tableName,
                    columnName,
                    binaryKey,
                    Ct))!["note"]);
            Assert.Equal(
                1,
                await _client.DeleteRowAsync(
                    tableName,
                    columnName,
                    binaryKey,
                    Ct));
        }
    }

    [Fact]
    public async Task HttpTransport_BitStringsPreserveLengthAndRoundTripThroughCrud()
    {
        Assert.Null((await _client.ExecuteSqlAsync(
            "CREATE TABLE http_bits (" +
            "id INTEGER PRIMARY KEY, fixed_bits BIT(3), " +
            "varying_bits VARBIT(8), payload BLOB); " +
            "INSERT INTO http_bits VALUES (1, '1', '1', X'80');",
            Ct)).Error);

        Dictionary<string, object?> first = Assert.IsType<Dictionary<string, object?>>(
            await _client.GetRowByPkAsync("http_bits", "id", 1, Ct));
        SqlBitString fixedBits = Assert.IsType<SqlBitString>(first["fixed_bits"]);
        SqlBitString varyingBits = Assert.IsType<SqlBitString>(first["varying_bits"]);
        Assert.Equal(3, fixedBits.BitLength);
        Assert.Equal("100", fixedBits.ToBitString());
        Assert.Equal(1, varyingBits.BitLength);
        Assert.Equal("1", varyingBits.ToBitString());
        Assert.Equal(new byte[] { 0x80 }, fixedBits.PackedBytes.ToArray());
        Assert.Equal(new byte[] { 0x80 }, varyingBits.PackedBytes.ToArray());
        Assert.Equal(new byte[] { 0x80 }, Assert.IsType<byte[]>(first["payload"]));

        Assert.Equal(1, await _client.InsertRowAsync(
            "http_bits",
            new Dictionary<string, object?>
            {
                ["id"] = 2,
                ["fixed_bits"] = fixedBits,
                ["varying_bits"] = varyingBits,
                ["payload"] = first["payload"],
            },
            Ct));

        SqlExecutionResult copied = await _client.ExecuteSqlAsync(
            "SELECT fixed_bits, varying_bits, payload FROM http_bits WHERE id = 2;",
            Ct);
        Assert.Null(copied.Error);
        object?[] copiedRow = Assert.Single(copied.Rows!);
        Assert.Equal(fixedBits, Assert.IsType<SqlBitString>(copiedRow[0]));
        Assert.Equal(varyingBits, Assert.IsType<SqlBitString>(copiedRow[1]));
        Assert.Equal(new byte[] { 0x80 }, Assert.IsType<byte[]>(copiedRow[2]));

        await _client.CreateProcedureAsync(
            new ProcedureDefinition
            {
                Name = "HttpBitDefault",
                BodySql = "SELECT CAST(@bits AS VARBIT(8)) AS bits;",
                Parameters =
                [
                    new ProcedureParameterDefinition
                    {
                        Name = "bits",
                        Type = DbType.Blob,
                        Required = false,
                        Default = varyingBits,
                    },
                ],
            },
            Ct);

        ProcedureDefinition detail = Assert.IsType<ProcedureDefinition>(
            await _client.GetProcedureAsync("HttpBitDefault", Ct));
        Assert.Equal(
            varyingBits,
            Assert.IsType<SqlBitString>(Assert.Single(detail.Parameters).Default));

        ProcedureExecutionResult defaultExecution = await _client.ExecuteProcedureAsync(
            "HttpBitDefault",
            new Dictionary<string, object?>(),
            Ct);
        Assert.True(defaultExecution.Succeeded, defaultExecution.Error);
        Assert.Equal(
            varyingBits,
            Assert.IsType<SqlBitString>(
                Assert.Single(Assert.Single(defaultExecution.Statements).Rows!)[0]));

        ProcedureExecutionResult argumentExecution = await _client.ExecuteProcedureAsync(
            "HttpBitDefault",
            new Dictionary<string, object?> { ["bits"] = fixedBits },
            Ct);
        Assert.True(argumentExecution.Succeeded, argumentExecution.Error);
        Assert.Equal(
            fixedBits,
            Assert.IsType<SqlBitString>(
                Assert.Single(Assert.Single(argumentExecution.Statements).Rows!)[0]));
    }

    [Fact]
    public async Task HttpTransport_TypedScalarKeysRoundTripThroughCrud()
    {
        const decimal DecimalKey = 12345678901234.5678m;
        const float RealKey = 1.25f;
        const double DoubleKey = 1.23456789012345d;
        DateOnly dateKey = new(2026, 8, 5);
        TimeOnly timeKey = new TimeOnly(14, 30, 15)
            .Add(TimeSpan.FromTicks(1_234_567));
        DateTime timestampKey = new DateTime(
                2026, 8, 5, 14, 30, 15, DateTimeKind.Unspecified)
            .AddTicks(1_234_567);
        DateTimeOffset zonedKey = new(timestampKey, TimeSpan.FromHours(-7));
        TimeSpan intervalKey = TimeSpan.FromDays(1) +
                               new TimeSpan(2, 3, 4) +
                               TimeSpan.FromTicks(5_000_000);
        Guid uuidKey = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");

        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_boolean_key (key_value BOOLEAN PRIMARY KEY, note TEXT);
            CREATE TABLE http_decimal_key (key_value DECIMAL(18,4) PRIMARY KEY, note TEXT);
            CREATE TABLE http_real_key (key_value REAL PRIMARY KEY, note TEXT);
            CREATE TABLE http_double_key (key_value DOUBLE PRECISION PRIMARY KEY, note TEXT);
            CREATE TABLE http_date_key (key_value DATE PRIMARY KEY, note TEXT);
            CREATE TABLE http_time_key (key_value TIME(7) PRIMARY KEY, note TEXT);
            CREATE TABLE http_timestamp_key (key_value TIMESTAMP(7) PRIMARY KEY, note TEXT);
            CREATE TABLE http_zoned_key (
                key_value TIMESTAMP(7) WITH TIME ZONE PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE http_interval_key (
                key_value INTERVAL DAY TO SECOND(7) PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE http_year_month_key (
                key_value INTERVAL YEAR TO MONTH PRIMARY KEY,
                note TEXT
            );
            CREATE TABLE http_uuid_key (key_value UUID PRIMARY KEY, note TEXT);
            CREATE TABLE http_typed_looking_text_key (key_value TEXT PRIMARY KEY, note TEXT);

            INSERT INTO http_boolean_key VALUES (1, 'initial');
            INSERT INTO http_decimal_key VALUES (12345678901234.5678, 'initial');
            INSERT INTO http_real_key VALUES (1.25, 'initial');
            INSERT INTO http_double_key VALUES (1.23456789012345, 'initial');
            INSERT INTO http_date_key VALUES ('2026-08-05', 'initial');
            INSERT INTO http_time_key VALUES ('14:30:15.1234567', 'initial');
            INSERT INTO http_timestamp_key VALUES (
                '2026-08-05 14:30:15.1234567',
                'initial'
            );
            INSERT INTO http_zoned_key VALUES (
                '2026-08-05 14:30:15.1234567-07:00',
                'initial'
            );
            INSERT INTO http_interval_key VALUES ('1.02:03:04.5', 'initial');
            INSERT INTO http_year_month_key VALUES ('2-03', 'initial');
            INSERT INTO http_uuid_key VALUES (
                '01234567-89ab-cdef-0123-456789abcdef',
                'initial'
            );
            INSERT INTO http_typed_looking_text_key VALUES ('123.4500', 'initial');
            INSERT INTO http_typed_looking_text_key VALUES ('2026-08-05', 'initial');
            """,
            Ct);
        Assert.Null(create.Error);

        await AssertCrudAsync("http_boolean_key", true);
        await AssertCrudAsync("http_decimal_key", DecimalKey);
        await AssertCrudAsync("http_real_key", RealKey);
        await AssertCrudAsync("http_double_key", DoubleKey);
        await AssertCrudAsync("http_date_key", dateKey);
        await AssertCrudAsync("http_time_key", timeKey);
        await AssertCrudAsync("http_timestamp_key", timestampKey);
        await AssertCrudAsync("http_zoned_key", zonedKey);
        await AssertCrudAsync("http_interval_key", intervalKey);
        await AssertCrudAsync("http_year_month_key", "2-03");
        await AssertCrudAsync("http_uuid_key", uuidKey);

        // The declared schema wins over the CLR marker. Numeric- and
        // date-looking TEXT keys must remain text all the way to the engine.
        await AssertCrudAsync("http_typed_looking_text_key", 123.4500m);
        await AssertCrudAsync("http_typed_looking_text_key", dateKey);

        async Task AssertCrudAsync(string tableName, object key)
        {
            Dictionary<string, object?> initial = Assert.IsType<Dictionary<string, object?>>(
                await _client.GetRowByPkAsync(tableName, "key_value", key, Ct));
            Assert.Equal("initial", initial["note"]);

            Assert.Equal(
                1,
                await _client.UpdateRowAsync(
                    tableName,
                    "key_value",
                    key,
                    new Dictionary<string, object?> { ["note"] = "updated" },
                    Ct));
            Dictionary<string, object?> updated = Assert.IsType<Dictionary<string, object?>>(
                await _client.GetRowByPkAsync(tableName, "key_value", key, Ct));
            Assert.Equal("updated", updated["note"]);

            Assert.Equal(
                1,
                await _client.DeleteRowAsync(tableName, "key_value", key, Ct));
            Assert.Null(await _client.GetRowByPkAsync(
                tableName,
                "key_value",
                key,
                Ct));
        }
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
    public async Task HttpTransport_MapsDefaultsChecksAndLogicalKeys()
    {
        var create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_schema_metadata (
                id INTEGER PRIMARY KEY,
                tenant TEXT NOT NULL,
                code TEXT DEFAULT 'new',
                score INTEGER,
                CONSTRAINT ck_http_schema_score CHECK (score >= 0),
                CONSTRAINT uq_http_schema_tenant_code UNIQUE (tenant, code)
            );
            """,
            Ct);
        Assert.Null(create.Error);

        TableSchema? schema = await _client.GetTableSchemaAsync("http_schema_metadata", Ct);
        Assert.NotNull(schema);
        Assert.NotEqual(Guid.Empty, schema!.SchemaId);
        Assert.All(
            schema.Columns,
            column => Assert.NotEqual(Guid.Empty, column.SchemaId));
        Assert.Equal("'new'", Assert.Single(schema.Columns, column => column.Name == "code").DefaultSql);
        CheckConstraintDefinition check = Assert.Single(schema.CheckConstraints);
        Assert.NotEqual(Guid.Empty, check.SchemaId);
        Assert.Equal("ck_http_schema_score", check.ConstraintName);
        Assert.Contains("score", check.ExpressionSql, StringComparison.OrdinalIgnoreCase);
        KeyConstraintDefinition unique = Assert.Single(
            schema.KeyConstraints,
            key => key.Kind == KeyConstraintKind.Unique);
        Assert.NotEqual(Guid.Empty, unique.SchemaId);
        Assert.Equal(["tenant", "code"], unique.Columns);
    }

    [Fact]
    public async Task HttpTransport_OlderSchemaPayloadWithoutAdditiveLists_UsesSafeDefaults()
    {
        const string payload =
            """
            {
              "tableName": "legacy_http",
              "columns": [
                {
                  "name": "id",
                  "type": "Integer",
                  "nullable": false,
                  "isPrimaryKey": true,
                  "isIdentity": false,
                  "collation": null
                }
              ],
              "foreignKeys": [
                {
                  "constraintName": "fk_legacy_http_parent",
                  "columnName": "id",
                  "referencedTableName": "legacy_parent",
                  "referencedColumnName": "id",
                  "onDelete": "Restrict",
                  "supportingIndexName": "__fk_legacy_http_parent"
                }
              ]
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://legacy-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http,
            Endpoint = httpClient.BaseAddress.ToString(),
            HttpClient = httpClient,
        });

        TableSchema? schema = await client.GetTableSchemaAsync("legacy_http", Ct);

        Assert.NotNull(schema);
        Assert.Equal(Guid.Empty, schema!.SchemaId);
        ColumnDefinition legacyColumn = Assert.Single(schema.Columns);
        Assert.Equal(Guid.Empty, legacyColumn.SchemaId);
        Assert.Null(legacyColumn.DefaultSql);
        Assert.Empty(schema.CheckConstraints);
        Assert.Empty(schema.KeyConstraints);
        ForeignKeyDefinition legacyForeignKey = Assert.Single(schema.ForeignKeys);
        Assert.Equal(Guid.Empty, legacyForeignKey.SchemaId);
        Assert.Equal(["id"], legacyForeignKey.ColumnNames);
        Assert.Equal(["id"], legacyForeignKey.ReferencedColumnNames);
    }

    [Fact]
    public async Task HttpTransport_RejectsPartialOwnedSchemaIdentities()
    {
        string payload =
            $$"""
            {
              "schemaId": "{{Guid.NewGuid():D}}",
              "tableName": "partial_http",
              "columns": [
                {
                  "name": "id",
                  "type": "Integer",
                  "nullable": false,
                  "isPrimaryKey": true,
                  "isIdentity": false
                }
              ],
              "foreignKeys": [],
              "keyConstraints": [],
              "checkConstraints": [],
              "nextRowId": 1
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://partial-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetTableSchemaAsync("partial_http", Ct));
        Assert.Contains(
            "no stable identity",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HttpTransport_RejectsPartialForeignKeyBindings()
    {
        Guid tableId = Guid.NewGuid();
        Guid columnId = Guid.NewGuid();
        Guid foreignKeyId = Guid.NewGuid();
        string payload =
            $$"""
            {
              "schemaId": "{{tableId:D}}",
              "tableName": "partial_fk_http",
              "columns": [
                {
                  "schemaId": "{{columnId:D}}",
                  "name": "parent_id",
                  "type": "Integer",
                  "nullable": true,
                  "isPrimaryKey": false,
                  "isIdentity": false
                }
              ],
              "foreignKeys": [
                {
                  "schemaId": "{{foreignKeyId:D}}",
                  "constraintName": "fk_partial_http",
                  "columnName": "parent_id",
                  "columnNames": ["parent_id"],
                  "columnSchemaIds": ["{{columnId:D}}"],
                  "referencedTableName": "parents",
                  "referencedColumnName": "id",
                  "referencedColumnNames": ["id"],
                  "onDelete": "Restrict",
                  "supportingIndexName": "__fk_partial_http"
                }
              ],
              "keyConstraints": [],
              "checkConstraints": [],
              "nextRowId": 1
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://partial-fk-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetTableSchemaAsync(
                        "partial_fk_http",
                        Ct));
        Assert.Contains(
            "partial stable bindings",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HttpTransport_RejectsInconsistentLegacyForeignKeyColumns()
    {
        const string payload =
            """
            {
              "tableName": "legacy_inconsistent_fk",
              "columns": [
                {
                  "name": "first_id",
                  "type": "Integer",
                  "nullable": true,
                  "isPrimaryKey": false,
                  "isIdentity": false
                },
                {
                  "name": "second_id",
                  "type": "Integer",
                  "nullable": true,
                  "isPrimaryKey": false,
                  "isIdentity": false
                }
              ],
              "foreignKeys": [
                {
                  "constraintName": "fk_legacy_inconsistent",
                  "columnName": "first_id",
                  "columnNames": ["first_id", "second_id"],
                  "referencedTableName": "parents",
                  "referencedColumnName": "id",
                  "referencedColumnNames": ["id"],
                  "onDelete": "Restrict",
                  "supportingIndexName": "__fk_legacy_inconsistent"
                }
              ],
              "keyConstraints": [],
              "checkConstraints": [],
              "nextRowId": 1
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://legacy-inconsistent-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetTableSchemaAsync(
                        "legacy_inconsistent_fk",
                        Ct));
        Assert.Contains(
            "inconsistent ordered columns",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task HttpTransport_RejectsUndefinedNumericColumnType()
    {
        const string payload =
            """
            {
              "tableName": "invalid_column_type",
              "columns": [
                {
                  "name": "id",
                  "type": "255",
                  "nullable": false,
                  "isPrimaryKey": true,
                  "isIdentity": false
                }
              ],
              "foreignKeys": [],
              "keyConstraints": [],
              "checkConstraints": [],
              "nextRowId": 1
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://invalid-column-type-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetTableSchemaAsync(
                        "invalid_column_type",
                        Ct));

        Assert.Contains(
            "Unsupported column type '255'",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_RejectsNumericForeignKeyAction()
    {
        const string payload =
            """
            {
              "tableName": "invalid_fk_action",
              "columns": [
                {
                  "name": "parent_id",
                  "type": "Integer",
                  "nullable": true,
                  "isPrimaryKey": false,
                  "isIdentity": false
                }
              ],
              "foreignKeys": [
                {
                  "constraintName": "fk_invalid_action",
                  "columnName": "parent_id",
                  "columnNames": ["parent_id"],
                  "referencedTableName": "parents",
                  "referencedColumnName": "id",
                  "referencedColumnNames": ["id"],
                  "onDelete": "2",
                  "supportingIndexName": "__fk_invalid_action"
                }
              ],
              "keyConstraints": [],
              "checkConstraints": [],
              "nextRowId": 1
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://invalid-fk-action-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetTableSchemaAsync(
                        "invalid_fk_action",
                        Ct));

        Assert.Contains(
            "Unsupported foreign key ON DELETE action '2'",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_MigrateForeignKeys_RejectsNumericResultAction()
    {
        const string Payload =
            """
            {
              "validateOnly": false,
              "succeeded": true,
              "affectedTables": 1,
              "appliedForeignKeys": 1,
              "copiedRows": 0,
              "violationCount": 0,
              "violations": [],
              "appliedConstraints": [
                {
                  "tableName": "children",
                  "columnName": "parent_id",
                  "referencedTableName": "parents",
                  "referencedColumnName": "id",
                  "constraintName": "fk_children_parent",
                  "supportingIndexName": "__fk_children_parent",
                  "onDelete": "restrict",
                  "onUpdate": 99
                }
              ]
            }
            """;
        using var httpClient = new HttpClient(
            new StaticJsonHandler(Payload))
        {
            BaseAddress =
                new Uri("http://invalid-fk-migration-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.MigrateForeignKeysAsync(
                    new ForeignKeyMigrationRequest(),
                    Ct));

        Assert.Contains(
            "invalid foreign-key migration payload",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("99", "Insert", "timing")]
    [InlineData("After", "99", "event")]
    public async Task HttpTransport_RejectsUndefinedNumericTriggerEnums(
        string timing,
        string triggerEvent,
        string enumKind)
    {
        string payload =
            $$"""
            [
              {
                "triggerName": "invalid_trigger",
                "tableName": "items",
                "timing": "{{timing}}",
                "event": "{{triggerEvent}}",
                "bodySql": "SELECT 1"
              }
            ]
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri("http://invalid-trigger-enum-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () => await client.GetTriggersAsync(Ct));

        Assert.Contains(
            $"Unsupported trigger {enumKind} '99'",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_RejectsUndefinedNumericProcedureParameterType()
    {
        const string payload =
            """
            {
              "name": "invalid_procedure",
              "bodySql": "SELECT @value",
              "parameters": [
                {
                  "name": "value",
                  "type": "255",
                  "required": true,
                  "default": "unused",
                  "description": "invalid type"
                }
              ],
              "description": "invalid parameter type",
              "isEnabled": true,
              "createdUtc": "1970-01-01T00:00:00Z",
              "updatedUtc": "1970-01-01T00:00:00Z"
            }
            """;
        using var httpClient = new HttpClient(new StaticJsonHandler(payload))
        {
            BaseAddress = new Uri(
                "http://invalid-procedure-type-server/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = CSharpDbTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error =
            await Assert.ThrowsAsync<CSharpDbClientException>(
                async () =>
                    await client.GetProcedureAsync(
                        "invalid_procedure",
                        Ct));

        Assert.Contains(
            "Unsupported procedure parameter type '255'",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_MapsForeignKeyMetadata()
    {
        var create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE http_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES http_parents(id)
                    ON DELETE SET NULL
                    ON UPDATE NO ACTION
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
        Assert.Equal(ForeignKeyOnDeleteAction.SetNull, foreignKey.OnDelete);
        Assert.Equal(ForeignKeyOnDeleteAction.NoAction, foreignKey.OnUpdate);
        Assert.Single(foreignKey.ColumnSchemaIds);
        Assert.NotEqual(Guid.Empty, foreignKey.ReferencedTableSchemaId);
        Assert.Single(foreignKey.ReferencedColumnSchemaIds);
        Assert.NotEqual(Guid.Empty, foreignKey.ReferencedKeySchemaId);
        Assert.StartsWith("__fk_http_children_parent_id_", foreignKey.SupportingIndexName, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HttpTransport_MapsFullImmediateForeignKeyActionMatrix()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_action_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE http_action_children (
                id INTEGER PRIMARY KEY,
                restrict_id INTEGER REFERENCES http_action_parents(id)
                    ON DELETE RESTRICT ON UPDATE RESTRICT,
                no_action_id INTEGER REFERENCES http_action_parents(id)
                    ON DELETE NO ACTION ON UPDATE NO ACTION,
                cascade_id INTEGER REFERENCES http_action_parents(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
                set_null_id INTEGER REFERENCES http_action_parents(id)
                    ON DELETE SET NULL ON UPDATE SET NULL,
                set_default_id INTEGER DEFAULT 1
                    REFERENCES http_action_parents(id)
                    ON DELETE SET DEFAULT ON UPDATE SET DEFAULT
            );
            """,
            Ct);
        Assert.Null(create.Error);

        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync("http_action_children", Ct));
        Dictionary<string, ForeignKeyDefinition> byColumn =
            schema.ForeignKeys.ToDictionary(
                foreignKey => foreignKey.ColumnName,
                StringComparer.Ordinal);
        Assert.Equal(5, byColumn.Count);

        foreach ((string columnName, ForeignKeyOnDeleteAction action) in
                 new[]
                 {
                     ("restrict_id", ForeignKeyOnDeleteAction.Restrict),
                     ("no_action_id", ForeignKeyOnDeleteAction.NoAction),
                     ("cascade_id", ForeignKeyOnDeleteAction.Cascade),
                     ("set_null_id", ForeignKeyOnDeleteAction.SetNull),
                     ("set_default_id", ForeignKeyOnDeleteAction.SetDefault),
                 })
        {
            Assert.Equal(action, byColumn[columnName].OnDelete);
            Assert.Equal(action, byColumn[columnName].OnUpdate);
        }
    }

    [Fact]
    public async Task HttpTransport_MapsOrderedCompositeForeignKeyMetadata()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_composite_parents (
                tenant_id INTEGER,
                code TEXT,
                PRIMARY KEY (tenant_id, code)
            );
            CREATE TABLE http_composite_children (
                id INTEGER PRIMARY KEY,
                tenant_id INTEGER,
                parent_code TEXT,
                CONSTRAINT fk_http_composite_parent
                    FOREIGN KEY (tenant_id, parent_code)
                    REFERENCES http_composite_parents (tenant_id, code)
                    ON DELETE CASCADE
            );
            """,
            Ct);
        Assert.Null(create.Error);

        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync("http_composite_children", Ct));
        ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);

        Assert.Equal("tenant_id", foreignKey.ColumnName);
        Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);
        Assert.Equal(["tenant_id", "parent_code"], foreignKey.ColumnNames);
        Assert.Equal(["tenant_id", "code"], foreignKey.ReferencedColumnNames);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
    }

    [Fact]
    public async Task HttpTransport_MigrateForeignKeys_RoundTripsValidationAndApply()
    {
        var create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE http_migrate_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE http_migrate_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 1
            );
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
                        OnDelete = ForeignKeyOnDeleteAction.SetDefault,
                        OnUpdate = ForeignKeyOnDeleteAction.Cascade,
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
        Assert.Equal(ForeignKeyOnDeleteAction.SetDefault, foreignKey.OnDelete);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnUpdate);
    }

    [Fact]
    public async Task RestApi_MigrateForeignKeys_AcceptsDefinedNumericReferentialActions()
    {
        SqlExecutionResult create = await _client.ExecuteSqlAsync(
            """
            CREATE TABLE numeric_parents (id INTEGER PRIMARY KEY);
            CREATE TABLE numeric_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 1
            );
            """,
            Ct);
        Assert.Null(create.Error);

        using var content = new StringContent(
            """
            {
              "validateOnly": true,
              "constraints": [
                {
                  "tableName": "numeric_children",
                  "columnName": "parent_id",
                  "referencedTableName": "numeric_parents",
                  "referencedColumnName": "id",
                  "onDelete": 4,
                  "onUpdate": 2
                }
              ]
            }
            """,
            Encoding.UTF8,
            "application/json");

        using HttpResponseMessage response = await _httpClient.PostAsync(
            "/api/maintenance/migrate-foreign-keys",
            content,
            Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task RestApi_MigrateForeignKeys_RejectsUndefinedNumericReferentialActions()
    {
        using var content = new StringContent(
            """
            {
              "validateOnly": true,
              "constraints": [
                {
                  "tableName": "undefined_numeric_children",
                  "columnName": "parent_id",
                  "referencedTableName": "undefined_numeric_parents",
                  "referencedColumnName": "id",
                  "onDelete": 99,
                  "onUpdate": "restrict"
                }
              ]
            }
            """,
            Encoding.UTF8,
            "application/json");

        using HttpResponseMessage response = await _httpClient.PostAsync(
            "/api/maintenance/migrate-foreign-keys",
            content,
            Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
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

    private sealed class TestApiFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null,
        CSharpDB.Engine.DatabaseOptions? directDatabaseOptions = null,
        ICSharpDbClient? clientOverride = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            if (directDatabaseOptions is not null || clientOverride is not null)
            {
                builder.ConfigureServices(services =>
                {
                    if (directDatabaseOptions is not null)
                    {
                        services.AddSingleton(new CSharpDbClientOptions
                        {
                            ConnectionString = $"Data Source={dbPath}",
                            DirectDatabaseOptions = directDatabaseOptions,
                        });
                    }

                    if (clientOverride is not null)
                    {
                        services.RemoveAll<ICSharpDbClient>();
                        services.AddSingleton(clientOverride);
                    }
                });
            }
            builder.ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={dbPath}",
                });
                if (extraConfig is not null)
                {
                    config.AddInMemoryCollection(extraConfig);
                }
            });
        }
    }

    public class CancellationCaptureClientProxy : DispatchProxy
    {
        public CancellationToken ExecuteSqlCancellationToken { get; private set; }
        public CancellationToken BeginTransactionCancellationToken { get; private set; }
        public CancellationToken ExecuteInTransactionCancellationToken { get; private set; }
        public CancellationToken CommitTransactionCancellationToken { get; private set; }
        public CancellationToken RollbackTransactionCancellationToken { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            => targetMethod?.Name switch
            {
                "get_DataSource" => "cancellation-capture",
                "GetInfoAsync" => Task.FromResult(new DatabaseInfo
                {
                    DataSource = "cancellation-capture",
                }),
                "ExecuteSqlAsync" => CaptureExecuteSql((CancellationToken)args![1]!),
                "BeginTransactionAsync" => CaptureBeginTransaction((CancellationToken)args![0]!),
                "ExecuteInTransactionAsync" =>
                    CaptureExecuteInTransaction((CancellationToken)args![2]!),
                "CommitTransactionAsync" =>
                    CaptureCommitTransaction((CancellationToken)args![1]!),
                "RollbackTransactionAsync" =>
                    CaptureRollbackTransaction((CancellationToken)args![1]!),
                "DisposeAsync" => ValueTask.CompletedTask,
                _ => throw new NotSupportedException(targetMethod?.Name),
            };

        private Task<SqlExecutionResult> CaptureExecuteSql(CancellationToken cancellationToken)
        {
            ExecuteSqlCancellationToken = cancellationToken;
            return Task.FromResult(new SqlExecutionResult { RowsAffected = 1 });
        }

        private Task<TransactionSessionInfo> CaptureBeginTransaction(
            CancellationToken cancellationToken)
        {
            BeginTransactionCancellationToken = cancellationToken;
            return Task.FromResult(new TransactionSessionInfo
            {
                TransactionId = "tx-cancellation",
                ExpiresAtUtc = DateTime.UtcNow.AddMinutes(1),
            });
        }

        private Task<SqlExecutionResult> CaptureExecuteInTransaction(
            CancellationToken cancellationToken)
        {
            ExecuteInTransactionCancellationToken = cancellationToken;
            return Task.FromResult(new SqlExecutionResult { RowsAffected = 1 });
        }

        private Task CaptureCommitTransaction(CancellationToken cancellationToken)
        {
            CommitTransactionCancellationToken = cancellationToken;
            return Task.CompletedTask;
        }

        private Task CaptureRollbackTransaction(CancellationToken cancellationToken)
        {
            RollbackTransactionCancellationToken = cancellationToken;
            return Task.CompletedTask;
        }
    }

    private sealed class StaticJsonHandler(string payload) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = request,
                Content = new StringContent(payload, Encoding.UTF8, "application/json"),
            });
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

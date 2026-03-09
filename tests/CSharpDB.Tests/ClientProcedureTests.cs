using System.Globalization;
using System.Text.Json;
using CSharpDB.Client;
using ClientModels = CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class ClientProcedureTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private ICSharpDbClient _client = null!;

    public ClientProcedureTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_procedure_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = _dbPath,
        });
        _ = await _client.GetInfoAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    private CancellationToken Ct => TestContext.Current.CancellationToken;

    private Task<IReadOnlyList<string>> GetTableNamesAsync()
        => _client.GetTableNamesAsync(Ct);

    private Task<ClientModels.SqlExecutionResult> ExecuteSqlAsync(string sql)
        => _client.ExecuteSqlAsync(sql, Ct);

    private Task<IReadOnlyList<ClientModels.SavedQueryDefinition>> GetSavedQueriesAsync()
        => _client.GetSavedQueriesAsync(Ct);

    private Task<ClientModels.SavedQueryDefinition?> GetSavedQueryAsync(string name)
        => _client.GetSavedQueryAsync(name, Ct);

    private Task<ClientModels.SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText)
        => _client.UpsertSavedQueryAsync(name, sqlText, Ct);

    private Task DeleteSavedQueryAsync(string name)
        => _client.DeleteSavedQueryAsync(name, Ct);

    private Task<IReadOnlyList<ClientModels.ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true)
        => _client.GetProceduresAsync(includeDisabled, Ct);

    private Task<ClientModels.ProcedureDefinition?> GetProcedureAsync(string name)
        => _client.GetProcedureAsync(name, Ct);

    private Task CreateProcedureAsync(ClientModels.ProcedureDefinition definition)
        => _client.CreateProcedureAsync(definition, Ct);

    private Task UpdateProcedureAsync(string existingName, ClientModels.ProcedureDefinition definition)
        => _client.UpdateProcedureAsync(existingName, definition, Ct);

    private Task DeleteProcedureAsync(string name)
        => _client.DeleteProcedureAsync(name, Ct);

    private Task<ClientModels.ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args)
        => _client.ExecuteProcedureAsync(name, args, Ct);

    [Fact]
    public async Task CatalogAccess_AutoCreatesCatalog_AndHidesInternalTable()
    {
        var procedures = await GetProceduresAsync();
        Assert.Empty(procedures);

        var tables = await GetTableNamesAsync();
        Assert.DoesNotContain("__procedures", tables, StringComparer.OrdinalIgnoreCase);
        Assert.DoesNotContain("__saved_queries", tables, StringComparer.OrdinalIgnoreCase);

        var savedQueriesCount = await ExecuteSqlAsync("SELECT COUNT(*) FROM __saved_queries;");
        Assert.Null(savedQueriesCount.Error);
        Assert.NotNull(savedQueriesCount.Rows);
        Assert.Equal(0L, Convert.ToInt64(Assert.Single(savedQueriesCount.Rows)[0], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task SavedQueryCrud_Lifecycle_Works()
    {
        var created = await UpsertSavedQueryAsync("recent_users", "SELECT * FROM users ORDER BY id DESC;");
        Assert.True(created.Id > 0);
        Assert.Equal("recent_users", created.Name);
        Assert.Equal("SELECT * FROM users ORDER BY id DESC", created.SqlText);

        var loaded = await GetSavedQueryAsync("recent_users");
        Assert.NotNull(loaded);
        Assert.Equal(created.Id, loaded!.Id);
        Assert.Equal(created.SqlText, loaded.SqlText);

        var updated = await UpsertSavedQueryAsync("recent_users", "SELECT id, name FROM users ORDER BY id DESC;");
        Assert.Equal(created.Id, updated.Id);
        Assert.Equal("SELECT id, name FROM users ORDER BY id DESC", updated.SqlText);
        Assert.True(updated.UpdatedUtc >= loaded.UpdatedUtc);

        var all = await GetSavedQueriesAsync();
        var item = Assert.Single(all);
        Assert.Equal("recent_users", item.Name);

        await DeleteSavedQueryAsync("recent_users");
        Assert.Null(await GetSavedQueryAsync("recent_users"));
        Assert.Empty(await GetSavedQueriesAsync());
    }

    [Fact]
    public async Task ProcedureCrud_Lifecycle_Works()
    {
        var create = new ClientModels.ProcedureDefinition
        {
            Name = "GetUserById",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition
                {
                    Name = "id",
                    Type = ClientModels.DbType.Integer,
                    Required = true,
                }
            ],
            Description = "test",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await CreateProcedureAsync(create);
        var loaded = await GetProcedureAsync("GetUserById");
        Assert.NotNull(loaded);
        Assert.Equal("GetUserById", loaded.Name);
        Assert.Single(loaded.Parameters);

        var update = new ClientModels.ProcedureDefinition
        {
            Name = "GetUserById2",
            BodySql = "SELECT @id, @includeInactive;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition
                {
                    Name = "id",
                    Type = ClientModels.DbType.Integer,
                    Required = true,
                },
                new ClientModels.ProcedureParameterDefinition
                {
                    Name = "includeInactive",
                    Type = ClientModels.DbType.Integer,
                    Required = false,
                    Default = 0L,
                }
            ],
            Description = "updated",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await UpdateProcedureAsync("GetUserById", update);
        Assert.Null(await GetProcedureAsync("GetUserById"));
        Assert.NotNull(await GetProcedureAsync("GetUserById2"));

        await DeleteProcedureAsync("GetUserById2");
        Assert.Null(await GetProcedureAsync("GetUserById2"));
    }

    [Fact]
    public async Task CreateProcedure_DuplicateName_Throws()
    {
        var definition = new ClientModels.ProcedureDefinition
        {
            Name = "DupProc",
            BodySql = "SELECT 1;",
            Parameters = [],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await CreateProcedureAsync(definition);
        await Assert.ThrowsAsync<ArgumentException>(() => CreateProcedureAsync(definition));
    }

    [Fact]
    public async Task ExecuteProcedure_ValidArgsAndDefault_Succeeds()
    {
        await ExecuteSqlAsync("CREATE TABLE proc_exec (id INTEGER PRIMARY KEY, name TEXT);");

        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "InsertAndRead",
            BodySql = """
                INSERT INTO proc_exec VALUES (@id, @name);
                SELECT id, name FROM proc_exec WHERE id = @id;
                """,
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
                new ClientModels.ProcedureParameterDefinition { Name = "name", Type = ClientModels.DbType.Text, Required = false, Default = "unknown" },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("InsertAndRead", new Dictionary<string, object?>
        {
            ["id"] = 1L,
        });

        Assert.True(result.Succeeded);
        Assert.Equal(2, result.Statements.Count);
        Assert.True(result.Statements[1].IsQuery);
        Assert.NotNull(result.Statements[1].Rows);
        var row = Assert.Single(result.Statements[1].Rows!);
        Assert.Equal("unknown", row[1]);
    }

    [Fact]
    public async Task ExecuteProcedure_UnknownArg_FailsValidation()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "OnlyId",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("OnlyId", new Dictionary<string, object?>
        {
            ["id"] = 5L,
            ["extra"] = 1L,
        });

        Assert.False(result.Succeeded);
        Assert.Contains("Unknown argument", result.Error ?? string.Empty);
    }

    [Fact]
    public async Task ExecuteProcedure_TypeMismatch_FailsValidation()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "TypeCheck",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("TypeCheck", new Dictionary<string, object?>
        {
            ["id"] = "not-a-number",
        });

        Assert.False(result.Succeeded);
        Assert.Contains("expects INTEGER", result.Error ?? string.Empty);
    }

    [Fact]
    public async Task ExecuteProcedure_BlobParameterBindingFailure_ReturnsStructuredFailure()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "BlobProc",
            BodySql = "SELECT @payload;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "payload", Type = ClientModels.DbType.Blob, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("BlobProc", new Dictionary<string, object?>
        {
            ["payload"] = Convert.FromBase64String("AQID"),
        });

        Assert.False(result.Succeeded);
        Assert.Equal(0, result.FailedStatementIndex);
        Assert.Contains("Blob parameters are not supported", result.Error ?? string.Empty);
    }

    [Fact]
    public async Task ExecuteProcedure_IntegerArg_FromJsonNumber_Succeeds()
    {
        await ExecuteSqlAsync("CREATE TABLE int_args_json (id INTEGER PRIMARY KEY); INSERT INTO int_args_json VALUES (1);");

        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "JsonNumberInt",
            BodySql = "SELECT id FROM int_args_json WHERE id = @id;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var args = JsonSerializer.Deserialize<Dictionary<string, object?>>("{\"id\":1}")
            ?? new Dictionary<string, object?>();

        var result = await ExecuteProcedureAsync("JsonNumberInt", args);

        Assert.True(result.Succeeded, result.Error);
        Assert.Single(result.Statements);
    }

    [Fact]
    public async Task ExecuteProcedure_IntegerArg_FromJsonDecimalNumber_SucceedsWhenWhole()
    {
        await ExecuteSqlAsync("CREATE TABLE int_args_decimal (id INTEGER PRIMARY KEY); INSERT INTO int_args_decimal VALUES (1);");

        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "JsonDecimalInt",
            BodySql = "SELECT id FROM int_args_decimal WHERE id = @id;",
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var args = JsonSerializer.Deserialize<Dictionary<string, object?>>("{\"id\":1.0}")
            ?? new Dictionary<string, object?>();

        var result = await ExecuteProcedureAsync("JsonDecimalInt", args);

        Assert.True(result.Succeeded, result.Error);
        Assert.Single(result.Statements);
    }

    [Fact]
    public async Task ExecuteProcedure_MultiStatementFailure_RollsBack()
    {
        await ExecuteSqlAsync("CREATE TABLE proc_rb (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);");
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "FailingProc",
            BodySql = """
                INSERT INTO proc_rb VALUES (@id, @n);
                INSERT INTO missing_table VALUES (1);
                """,
            Parameters =
            [
                new ClientModels.ProcedureParameterDefinition { Name = "id", Type = ClientModels.DbType.Integer, Required = true },
                new ClientModels.ProcedureParameterDefinition { Name = "n", Type = ClientModels.DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("FailingProc", new Dictionary<string, object?>
        {
            ["id"] = 1L,
            ["n"] = 2L,
        });

        Assert.False(result.Succeeded);
        Assert.Equal(1, result.FailedStatementIndex);

        var count = await ExecuteSqlAsync("SELECT COUNT(*) FROM proc_rb;");
        var countRow = Assert.Single(count.Rows!);
        Assert.Equal(0L, Convert.ToInt64(countRow[0]));
    }

    [Fact]
    public async Task ExecuteProcedure_SchemaMutation_IsVisibleToSubsequentClientReads()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "CreateProcTable",
            BodySql = "CREATE TABLE proc_created (id INTEGER PRIMARY KEY);",
            Parameters = [],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("CreateProcTable", new Dictionary<string, object?>());

        Assert.True(result.Succeeded, result.Error);
        Assert.Contains("proc_created", await GetTableNamesAsync(), StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExecuteProcedure_ProcedureCatalogMutation_IsVisibleToSubsequentClientReads()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "ManagedProc",
            BodySql = "SELECT 1;",
            Parameters = [],
            Description = "before",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "MutateProcedures",
            BodySql = "UPDATE __procedures SET description = 'after' WHERE name = 'ManagedProc';",
            Parameters = [],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("MutateProcedures", new Dictionary<string, object?>());

        Assert.True(result.Succeeded, result.Error);
        Assert.Equal("after", (await GetProcedureAsync("ManagedProc"))?.Description);
    }

    [Fact]
    public async Task ExecuteProcedure_Disabled_RejectsExecution()
    {
        await CreateProcedureAsync(new ClientModels.ProcedureDefinition
        {
            Name = "DisabledProc",
            BodySql = "SELECT 1;",
            Parameters = [],
            IsEnabled = false,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await ExecuteProcedureAsync("DisabledProc", new Dictionary<string, object?>());
        Assert.False(result.Succeeded);
        Assert.Contains("disabled", result.Error ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }
}

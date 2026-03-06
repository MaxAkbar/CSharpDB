using CSharpDB.Core;
using CSharpDB.Service;
using CSharpDB.Service.Models;
using Microsoft.Extensions.Configuration;
using System.Text.Json;

namespace CSharpDB.Tests;

public sealed class ServiceProcedureTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private CSharpDbService _service = null!;

    public ServiceProcedureTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_procedure_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:CSharpDB"] = $"Data Source={_dbPath}",
            })
            .Build();

        _service = new CSharpDbService(configuration);
        await _service.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _service.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task InitializeAsync_AutoCreatesCatalog_AndHidesInternalTable()
    {
        var procedures = await _service.GetProceduresAsync();
        Assert.Empty(procedures);

        var tables = await _service.GetTableNamesAsync();
        Assert.DoesNotContain("__procedures", tables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ProcedureCrud_Lifecycle_Works()
    {
        var create = new ProcedureDefinition
        {
            Name = "GetUserById",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ProcedureParameterDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Required = true,
                }
            ],
            Description = "test",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await _service.CreateProcedureAsync(create);
        var loaded = await _service.GetProcedureAsync("GetUserById");
        Assert.NotNull(loaded);
        Assert.Equal("GetUserById", loaded.Name);
        Assert.Single(loaded.Parameters);

        var update = new ProcedureDefinition
        {
            Name = "GetUserById2",
            BodySql = "SELECT @id, @includeInactive;",
            Parameters =
            [
                new ProcedureParameterDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Required = true,
                },
                new ProcedureParameterDefinition
                {
                    Name = "includeInactive",
                    Type = DbType.Integer,
                    Required = false,
                    Default = 0L,
                }
            ],
            Description = "updated",
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await _service.UpdateProcedureAsync("GetUserById", update);
        Assert.Null(await _service.GetProcedureAsync("GetUserById"));
        Assert.NotNull(await _service.GetProcedureAsync("GetUserById2"));

        await _service.DeleteProcedureAsync("GetUserById2");
        Assert.Null(await _service.GetProcedureAsync("GetUserById2"));
    }

    [Fact]
    public async Task CreateProcedure_DuplicateName_Throws()
    {
        var definition = new ProcedureDefinition
        {
            Name = "DupProc",
            BodySql = "SELECT 1;",
            Parameters = [],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        };

        await _service.CreateProcedureAsync(definition);
        await Assert.ThrowsAsync<ArgumentException>(() => _service.CreateProcedureAsync(definition));
    }

    [Fact]
    public async Task ExecuteProcedure_ValidArgsAndDefault_Succeeds()
    {
        await _service.ExecuteSqlAsync("CREATE TABLE proc_exec (id INTEGER PRIMARY KEY, name TEXT);");

        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "InsertAndRead",
            BodySql = """
                INSERT INTO proc_exec VALUES (@id, @name);
                SELECT id, name FROM proc_exec WHERE id = @id;
                """,
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
                new ProcedureParameterDefinition { Name = "name", Type = DbType.Text, Required = false, Default = "unknown" },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("InsertAndRead", new Dictionary<string, object?>
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
        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "OnlyId",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("OnlyId", new Dictionary<string, object?>
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
        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "TypeCheck",
            BodySql = "SELECT @id;",
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("TypeCheck", new Dictionary<string, object?>
        {
            ["id"] = "not-a-number",
        });

        Assert.False(result.Succeeded);
        Assert.Contains("expects INTEGER", result.Error ?? string.Empty);
    }

    [Fact]
    public async Task ExecuteProcedure_BlobParameterBindingFailure_ReturnsStructuredFailure()
    {
        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "BlobProc",
            BodySql = "SELECT @payload;",
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "payload", Type = DbType.Blob, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("BlobProc", new Dictionary<string, object?>
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
        await _service.ExecuteSqlAsync("CREATE TABLE int_args_json (id INTEGER PRIMARY KEY); INSERT INTO int_args_json VALUES (1);");

        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "JsonNumberInt",
            BodySql = "SELECT id FROM int_args_json WHERE id = @id;",
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var args = JsonSerializer.Deserialize<Dictionary<string, object?>>("{\"id\":1}")
            ?? new Dictionary<string, object?>();

        var result = await _service.ExecuteProcedureAsync("JsonNumberInt", args);

        Assert.True(result.Succeeded, result.Error);
        Assert.Single(result.Statements);
    }

    [Fact]
    public async Task ExecuteProcedure_IntegerArg_FromJsonDecimalNumber_SucceedsWhenWhole()
    {
        await _service.ExecuteSqlAsync("CREATE TABLE int_args_decimal (id INTEGER PRIMARY KEY); INSERT INTO int_args_decimal VALUES (1);");

        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "JsonDecimalInt",
            BodySql = "SELECT id FROM int_args_decimal WHERE id = @id;",
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var args = JsonSerializer.Deserialize<Dictionary<string, object?>>("{\"id\":1.0}")
            ?? new Dictionary<string, object?>();

        var result = await _service.ExecuteProcedureAsync("JsonDecimalInt", args);

        Assert.True(result.Succeeded, result.Error);
        Assert.Single(result.Statements);
    }

    [Fact]
    public async Task ExecuteProcedure_MultiStatementFailure_RollsBack()
    {
        await _service.ExecuteSqlAsync("CREATE TABLE proc_rb (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);");
        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "FailingProc",
            BodySql = """
                INSERT INTO proc_rb VALUES (@id, @n);
                INSERT INTO missing_table VALUES (1);
                """,
            Parameters =
            [
                new ProcedureParameterDefinition { Name = "id", Type = DbType.Integer, Required = true },
                new ProcedureParameterDefinition { Name = "n", Type = DbType.Integer, Required = true },
            ],
            IsEnabled = true,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("FailingProc", new Dictionary<string, object?>
        {
            ["id"] = 1L,
            ["n"] = 2L,
        });

        Assert.False(result.Succeeded);
        Assert.Equal(1, result.FailedStatementIndex);

        var count = await _service.ExecuteSqlAsync("SELECT COUNT(*) FROM proc_rb;");
        var countRow = Assert.Single(count.Rows!);
        Assert.Equal(0L, Convert.ToInt64(countRow[0]));
    }

    [Fact]
    public async Task ExecuteProcedure_Disabled_RejectsExecution()
    {
        await _service.CreateProcedureAsync(new ProcedureDefinition
        {
            Name = "DisabledProc",
            BodySql = "SELECT 1;",
            Parameters = [],
            IsEnabled = false,
            CreatedUtc = DateTime.UtcNow,
            UpdatedUtc = DateTime.UtcNow,
        });

        var result = await _service.ExecuteProcedureAsync("DisabledProc", new Dictionary<string, object?>());
        Assert.False(result.Succeeded);
        Assert.Contains("disabled", result.Error ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }
}

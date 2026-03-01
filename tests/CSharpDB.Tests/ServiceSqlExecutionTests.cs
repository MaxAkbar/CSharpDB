using CSharpDB.Service;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Tests;

public sealed class ServiceSqlExecutionTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private CSharpDbService _service = null!;

    public ServiceSqlExecutionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_service_test_{Guid.NewGuid():N}.db");
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
    public async Task ExecuteSqlAsync_ScriptExecutesAllStatementsIncludingTriggerBody()
    {
        string sql = """
            CREATE TABLE orders (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL);
            CREATE TABLE order_audit (order_id INTEGER NOT NULL);
            CREATE TRIGGER trg_order_audit AFTER INSERT ON orders
            BEGIN
                INSERT INTO order_audit VALUES (NEW.id);
            END;
            INSERT INTO orders VALUES (1, 3);
            """;

        var result = await _service.ExecuteSqlAsync(sql);
        Assert.Null(result.Error);

        var tables = await _service.GetTableNamesAsync();
        Assert.Contains("orders", tables);
        Assert.Contains("order_audit", tables);

        var triggers = await _service.GetTriggersAsync();
        Assert.Contains(triggers, t => t.TriggerName.Equals("trg_order_audit", StringComparison.OrdinalIgnoreCase));

        var auditCount = await _service.ExecuteSqlAsync("SELECT COUNT(*) FROM order_audit;");
        Assert.Null(auditCount.Error);
        Assert.True(auditCount.IsQuery);
        Assert.NotNull(auditCount.Rows);
        var row = Assert.Single(auditCount.Rows);
        Assert.Equal(1L, Convert.ToInt64(row[0]));
    }

    [Fact]
    public async Task ExecuteSqlAsync_SingleStatementWithoutSemicolon_Executes()
    {
        var result = await _service.ExecuteSqlAsync("CREATE TABLE semicolon_optional (id INTEGER PRIMARY KEY)");
        Assert.Null(result.Error);

        var tables = await _service.GetTableNamesAsync();
        Assert.Contains("semicolon_optional", tables);
    }
}

using CSharpDB.Client;

namespace CSharpDB.Tests;

public sealed class ClientStorageInspectionTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private ICSharpDbClient _client = null!;

    public ClientStorageInspectionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_service_test_{Guid.NewGuid():N}.db");
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

    private Task<CSharpDB.Client.Models.SqlExecutionResult> ExecuteSqlAsync(string sql)
        => _client.ExecuteSqlAsync(sql, Ct);

    private Task<IReadOnlyList<string>> GetTableNamesAsync()
        => _client.GetTableNamesAsync(Ct);

    private Task<IReadOnlyList<CSharpDB.Client.Models.TriggerSchema>> GetTriggersAsync()
        => _client.GetTriggersAsync(Ct);

    private Task<CSharpDB.Storage.Diagnostics.DatabaseInspectReport> InspectStorageAsync(bool includePages = false)
        => _client.InspectStorageAsync(includePages: includePages, ct: Ct);

    private Task<CSharpDB.Storage.Diagnostics.PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false)
        => _client.InspectPageAsync(pageId, includeHex, ct: Ct);

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

        var result = await ExecuteSqlAsync(sql);
        Assert.Null(result.Error);

        var tables = await GetTableNamesAsync();
        Assert.Contains("orders", tables);
        Assert.Contains("order_audit", tables);

        var triggers = await GetTriggersAsync();
        Assert.Contains(triggers, t => t.TriggerName.Equals("trg_order_audit", StringComparison.OrdinalIgnoreCase));

        var auditCount = await ExecuteSqlAsync("SELECT COUNT(*) FROM order_audit;");
        Assert.Null(auditCount.Error);
        Assert.True(auditCount.IsQuery);
        Assert.NotNull(auditCount.Rows);
        var row = Assert.Single(auditCount.Rows);
        Assert.Equal(1L, Convert.ToInt64(row[0]));
    }

    [Fact]
    public async Task ExecuteSqlAsync_SingleStatementWithoutSemicolon_Executes()
    {
        var result = await ExecuteSqlAsync("CREATE TABLE semicolon_optional (id INTEGER PRIMARY KEY)");
        Assert.Null(result.Error);

        var tables = await GetTableNamesAsync();
        Assert.Contains("semicolon_optional", tables);
    }

    [Fact]
    public async Task InspectStorageAsync_ReturnsHeaderAndHistogram()
    {
        await ExecuteSqlAsync("CREATE TABLE inspect_service (id INTEGER PRIMARY KEY, n INTEGER);");
        await ExecuteSqlAsync("INSERT INTO inspect_service VALUES (1, 10);");

        var report = await InspectStorageAsync(includePages: false);

        Assert.Equal("1.0", report.SchemaVersion);
        Assert.True(report.Header.FileLengthBytes > 0);
        Assert.NotEmpty(report.PageTypeHistogram);
    }

    [Fact]
    public async Task InspectPageAsync_ReturnsPageReport()
    {
        await ExecuteSqlAsync("CREATE TABLE inspect_page_service (id INTEGER PRIMARY KEY, n INTEGER);");

        var report = await InspectPageAsync(pageId: 0, includeHex: false);

        Assert.Equal("1.0", report.SchemaVersion);
        Assert.True(report.Exists);
        Assert.NotNull(report.Page);
    }
}

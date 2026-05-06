using System.Globalization;
using CSharpDB.Client;

namespace CSharpDB.Tests;

public sealed class ClientSqlExecutionTests
{
    [Fact]
    public async Task ExecuteSqlAsync_PublicPlannerDiagnostics_WorkThroughDirectClient()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE planner_client (id INTEGER PRIMARY KEY, value INTEGER);", ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("INSERT INTO planner_client VALUES (1, 7), (2, 7), (3, 8);", ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("ANALYZE planner_client;", ct)).Error);

            var catalog = await client.ExecuteSqlAsync(
                "SELECT COUNT(*) FROM sys.planner_heavy_hitters WHERE table_name = 'planner_client' AND column_name = 'value';",
                ct);
            Assert.Null(catalog.Error);
            Assert.True(catalog.IsQuery);
            Assert.NotNull(catalog.Rows);
            Assert.True(Convert.ToInt64(Assert.Single(catalog.Rows)[0], CultureInfo.InvariantCulture) > 0);

            var explain = await client.ExecuteSqlAsync(
                "EXPLAIN ESTIMATE FOR SELECT * FROM planner_client WHERE value = 7;",
                ct);
            Assert.Null(explain.Error);
            Assert.True(explain.IsQuery);
            Assert.NotNull(explain.ColumnNames);
            Assert.Contains("decision", explain.ColumnNames);
            Assert.NotNull(explain.Rows);
            Assert.Contains(explain.Rows, row => string.Equals(Convert.ToString(row[4], CultureInfo.InvariantCulture), "heavy-hitter", StringComparison.Ordinal));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_HandlesTriggerBodyAndFinalStatementWithoutSemicolon()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            string sql = """
                CREATE TABLE orders (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL);
                CREATE TABLE order_audit (order_id INTEGER NOT NULL);
                CREATE TRIGGER trg_order_audit AFTER INSERT ON orders
                BEGIN
                INSERT INTO order_audit VALUES (NEW.id);
                END;
                INSERT INTO orders VALUES (1, 3)
                """;

            var result = await client.ExecuteSqlAsync(sql, ct);

            Assert.Null(result.Error);

            var auditCount = await client.ExecuteSqlAsync("SELECT COUNT(*) FROM order_audit;", ct);
            Assert.Null(auditCount.Error);
            Assert.True(auditCount.IsQuery);
            Assert.NotNull(auditCount.Rows);
            var row = Assert.Single(auditCount.Rows);
            Assert.Equal(1L, Convert.ToInt64(row[0]));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_SelectDateWithoutFrom_ReturnsDateText()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            var result = await client.ExecuteSqlAsync("SELECT Date();", ct);

            Assert.Null(result.Error);
            Assert.True(result.IsQuery);
            Assert.NotNull(result.Rows);
            var row = Assert.Single(result.Rows);
            string value = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty;
            Assert.True(
                DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _),
                $"Expected yyyy-MM-dd date text, got '{value}'.");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_TablelessBuiltInScalarFunctions_ReturnValues()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            var result = await client.ExecuteSqlAsync(
                "SELECT abs(1123.34), Len('abc'), UCase('abc'), DateSerial(2024, 2, 29);",
                ct);

            Assert.Null(result.Error);
            Assert.True(result.IsQuery);
            Assert.NotNull(result.Rows);
            var row = Assert.Single(result.Rows);
            Assert.Equal(1123.34, Convert.ToDouble(row[0], CultureInfo.InvariantCulture), precision: 5);
            Assert.Equal(3L, Convert.ToInt64(row[1], CultureInfo.InvariantCulture));
            Assert.Equal("ABC", Convert.ToString(row[2], CultureInfo.InvariantCulture));
            Assert.Equal("2024-02-29", Convert.ToString(row[3], CultureInfo.InvariantCulture));
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetInfoAsync_UsesSingleDirectClientHandle()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            var createResult = await client.ExecuteSqlAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);", ct);
            Assert.Null(createResult.Error);

            var info = await client.GetInfoAsync(ct);

            Assert.Equal(dbPath, info.DataSource);
            Assert.Equal(1, info.TableCount);
            Assert.Equal(0, info.IndexCount);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetInfoAsync_CanceledWarmup_DoesNotPoisonClient()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_client_test_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = dbPath,
            });

            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.GetInfoAsync(cts.Token));

            var result = await client.ExecuteSqlAsync("CREATE TABLE warmup_ok (id INTEGER PRIMARY KEY);", ct);
            Assert.Null(result.Error);

            var info = await client.GetInfoAsync(ct);
            Assert.Equal(1, info.TableCount);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}

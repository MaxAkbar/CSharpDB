using CSharpDB.Client;

namespace CSharpDB.Tests;

public sealed class ClientSqlExecutionTests
{
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

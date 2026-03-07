using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class QueryPlanCacheDiagnosticsTests
{
    [Fact]
    public async Task RepeatedSelect_RecordsMissThenHit()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using var db = await Database.OpenAsync(dbPath, ct);
            await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (3, 30)", ct);

            db.ResetSelectPlanCacheDiagnostics();

            const string sql = "SELECT id, value FROM t WHERE value >= 10 ORDER BY value LIMIT 2";
            await ExecuteAndDrainAsync(db, sql, ct);
            await ExecuteAndDrainAsync(db, sql, ct);

            var stats = db.GetSelectPlanCacheDiagnostics();
            Assert.Equal(1, stats.MissCount);
            Assert.Equal(1, stats.HitCount);
            Assert.Equal(0, stats.ReclassificationCount);
            Assert.True(stats.StoreCount >= 1);
            Assert.True(stats.EntryCount >= 1);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task SchemaChange_ClearsSelectPlanCacheEntries()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using var db = await Database.OpenAsync(dbPath, ct);
            await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (3, 30)", ct);

            db.ResetSelectPlanCacheDiagnostics();

            const string sql = "SELECT id, value FROM t WHERE value >= 10 ORDER BY value LIMIT 2";
            await ExecuteAndDrainAsync(db, sql, ct);
            var afterFirstSelect = db.GetSelectPlanCacheDiagnostics();
            Assert.Equal(1, afterFirstSelect.EntryCount);

            await db.ExecuteAsync("CREATE INDEX idx_t_value ON t(value)", ct);

            await ExecuteAndDrainAsync(db, sql, ct);
            var finalStats = db.GetSelectPlanCacheDiagnostics();
            Assert.Equal(2, finalStats.MissCount);
            Assert.Equal(0, finalStats.HitCount);
            Assert.Equal(0, finalStats.ReclassificationCount);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static async Task ExecuteAndDrainAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        await result.ToListAsync(ct);
    }

    private static string NewTempDbPath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_plan_diag_{Guid.NewGuid():N}.db");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}

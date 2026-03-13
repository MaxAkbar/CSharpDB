using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class PlannerStatisticsTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public PlannerStatisticsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_planner_stats_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task FreshColumnStats_PreferMoreSelectiveLookupTerm()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        var (op, remaining) = InvokeTryBuildIndexScan(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE low_group = 1 AND code = 42");

        Assert.NotNull(op);
        var residual = Assert.IsType<BinaryExpression>(remaining);
        var residualColumn = Assert.IsType<ColumnRefExpression>(residual.Left);
        Assert.Equal("low_group", residualColumn.ColumnName);
    }

    [Fact]
    public async Task StaleColumnStats_AreIgnoredForLookupSuppression()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        var (freshOp, freshRemaining) = InvokeTryBuildIndexScan(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE low_group = 1");
        Assert.Null(freshOp);
        Assert.NotNull(freshRemaining);

        await _db.ExecuteAsync("INSERT INTO planner_stats VALUES (1001, 1, 1001)", ct);

        var (staleOp, staleRemaining) = InvokeTryBuildIndexScan(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE low_group = 1");
        Assert.NotNull(staleOp);
        Assert.Null(staleRemaining);
    }

    private async ValueTask SetupSelectivityTableAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE planner_stats (id INTEGER PRIMARY KEY, low_group INTEGER, code INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_planner_stats_low_group ON planner_stats(low_group)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_planner_stats_code ON planner_stats(code)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 1000; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_stats VALUES ({i}, {i % 2}, {i})",
                ct);
        }
        await _db.CommitAsync(ct);
    }

    private (object? Op, Expression? Remaining) InvokeTryBuildIndexScan(string tableName, string sql)
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(plannerField);
        object? planner = plannerField.GetValue(_db);
        Assert.NotNull(planner);

        var method = planner.GetType().GetMethod("TryBuildIndexScan", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var select = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        Assert.NotNull(select.Where);
        var schema = _db.GetTableSchema(tableName);
        Assert.NotNull(schema);

        object?[] args = [tableName, select.Where, schema, null];
        object? op = method.Invoke(planner, args);
        return (op, (Expression?)args[3]);
    }
}

using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
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

    [Fact]
    public async Task FreshColumnStats_NonUniqueJoin_PrefersIndexLookupWhenExpectedMatchesAreLow()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectiveJoinTablesAsync(ct);

        await using var preAnalyzeResult = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM planner_join_left l JOIN planner_join_right r ON l.code = r.code",
            ct);
        Assert.IsType<HashJoinOperator>(GetRootOperator(preAnalyzeResult));

        await _db.ExecuteAsync("ANALYZE planner_join_left", ct);
        await _db.ExecuteAsync("ANALYZE planner_join_right", ct);

        await using var postAnalyzeResult = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM planner_join_left l JOIN planner_join_right r ON l.code = r.code",
            ct);
        Assert.IsType<IndexNestedLoopJoinOperator>(GetRootOperator(postAnalyzeResult));

        var rows = await postAnalyzeResult.ToListAsync(ct);
        Assert.Equal(6000, rows.Count);
    }

    [Fact]
    public async Task StaleColumnStats_NonUniqueJoin_FallsBackToHashJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectiveJoinTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_join_left", ct);
        await _db.ExecuteAsync("ANALYZE planner_join_right", ct);

        await _db.ExecuteAsync("INSERT INTO planner_join_right VALUES (10001, 5001, 123456)", ct);

        await using var staleResult = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM planner_join_left l JOIN planner_join_right r ON l.code = r.code",
            ct);
        Assert.IsType<HashJoinOperator>(GetRootOperator(staleResult));
    }

    [Fact]
    public async Task HashJoin_BuildsSmallerEstimatedSide_WhenInputsAreClose()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupHashBuildSideTablesAsync(ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM planner_hash_left l JOIN planner_hash_right r ON l.code = r.code",
            ct);

        var rootOperator = Assert.IsType<HashJoinOperator>(GetRootOperator(result));
        bool buildRightSide = GetPrivateField<bool>(rootOperator, "_buildRightSide");
        Assert.False(buildRightSide);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersToSmallestConnectedTables()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_big b JOIN planner_reorder_mid m ON b.code = m.code JOIN planner_reorder_small s ON m.code = s.code");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["s", "m", "b"], order);
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

    private TableRef InvokeTryReorderInnerJoinChain(string sql)
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(plannerField);
        object? planner = plannerField.GetValue(_db);
        Assert.NotNull(planner);

        var method = planner.GetType().GetMethod("TryReorderInnerJoinChain", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var select = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        var join = Assert.IsType<JoinTableRef>(select.From);

        object?[] args = [join, null!];
        bool reordered = (bool)(method.Invoke(planner, args) ?? false);
        Assert.True(reordered);
        return Assert.IsAssignableFrom<TableRef>(args[1]);
    }

    private async ValueTask SetupSelectiveJoinTablesAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE planner_join_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_join_right (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_planner_join_right_code ON planner_join_right(code)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 3000; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_join_left VALUES ({i}, {i})",
                ct);
        }

        for (int i = 1; i <= 10000; i++)
        {
            int code = ((i - 1) % 5000) + 1;
            await _db.ExecuteAsync(
                $"INSERT INTO planner_join_right VALUES ({i}, {code}, {i * 10})",
                ct);
        }
        await _db.CommitAsync(ct);
    }

    private static IOperator GetRootOperator(QueryResult result)
    {
        var storedOperator = GetStoredOperator(result);
        return storedOperator is BatchToRowOperatorAdapter batchAdapter
            ? batchAdapter.BatchSource as IOperator
                ?? throw new InvalidOperationException("Batch adapter did not expose an operator root.")
            : storedOperator;
    }

    private static IOperator GetStoredOperator(QueryResult result)
    {
        var operatorField = typeof(QueryResult).GetField("_operator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult operator field not found.");
        var storedOperator = (IOperator?)operatorField.GetValue(result);
        if (storedOperator != null)
            return storedOperator;

        var batchOperatorField = typeof(QueryResult).GetField("_batchOperator", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("QueryResult batch operator field not found.");
        return (IOperator?)batchOperatorField.GetValue(result)
            ?? throw new InvalidOperationException("QueryResult did not contain an operator.");
    }

    private static T GetPrivateField<T>(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' not found on {target.GetType().Name}.");
        return (T)field.GetValue(target)!;
    }

    private async ValueTask SetupHashBuildSideTablesAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE planner_hash_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_hash_right (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 800; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_hash_left VALUES ({i}, {i})",
                ct);
        }

        for (int i = 1; i <= 1000; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_hash_right VALUES ({i}, {i}, {i * 7})",
                ct);
        }
        await _db.CommitAsync(ct);
    }

    private async ValueTask SetupReorderableJoinChainTablesAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_big (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_mid (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, marker INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_small (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, flag INTEGER NOT NULL)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 5000; i++)
        {
            int code = ((i - 1) % 200) + 1;
            await _db.ExecuteAsync(
                $"INSERT INTO planner_reorder_big VALUES ({i}, {code}, {i * 3})",
                ct);
        }

        for (int i = 1; i <= 200; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_reorder_mid VALUES ({i}, {i}, {i * 5})",
                ct);
        }

        for (int i = 1; i <= 10; i++)
        {
            await _db.ExecuteAsync(
                $"INSERT INTO planner_reorder_small VALUES ({i}, {i}, {i * 7})",
                ct);
        }
        await _db.CommitAsync(ct);
    }

    private static IEnumerable<string> FlattenJoinOrder(TableRef tableRef)
    {
        if (tableRef is SimpleTableRef simple)
        {
            yield return simple.Alias ?? simple.TableName;
            yield break;
        }

        var join = Assert.IsType<JoinTableRef>(tableRef);
        foreach (string name in FlattenJoinOrder(join.Left))
            yield return name;
        foreach (string name in FlattenJoinOrder(join.Right))
            yield return name;
    }
}

using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
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

        await _db.ExecuteAsync("INSERT INTO planner_stats VALUES (1001, 1, 1001, 1001)", ct);

        var (staleOp, staleRemaining) = InvokeTryBuildIndexScan(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE low_group = 1");
        Assert.NotNull(staleOp);
        Assert.Null(staleRemaining);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesNotEqualDistinctCount()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE low_group <> 1");

        Assert.Equal(500, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesOrEqualityUnion()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE code = 1 OR code = 2 OR code = 3");

        Assert.Equal(3, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesNotInDistinctCount()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE code NOT IN (1, 2, 3)");

        Assert.Equal(997, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesOrRangeUnion()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE code BETWEEN 1 AND 2 OR code BETWEEN 10 AND 11");

        Assert.Equal(4, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesQualifiedOrRangeUnion()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_reorder_big",
            "b",
            "SELECT * FROM planner_reorder_big b WHERE b.id BETWEEN 1 AND 2 OR b.id BETWEEN 10 AND 11");

        Assert.Equal(4, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesMixedDiscreteAndRangeUnion()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE code IN (1, 2, 3) OR code BETWEEN 10 AND 11");

        Assert.Equal(5, estimatedRows);
    }

    [Fact]
    public async Task FilteredRowEstimate_UsesNullAndDiscreteUnion()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectivityTableAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_stats", ct);

        long estimatedRows = InvokeEstimateFilteredRows(
            "planner_stats",
            "SELECT * FROM planner_stats WHERE nullable_code IS NULL OR nullable_code = 42");

        Assert.Equal(7, estimatedRows);
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
    public async Task NonUniqueJoin_WithSelectiveTopLevelLeafPredicate_PushesLeafLookupAndPrefersIndexJoin()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupSelectiveJoinTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_join_left", ct);
        await _db.ExecuteAsync("ANALYZE planner_join_right", ct);

        await using var result = await _db.ExecuteAsync(
            "SELECT l.id, r.payload FROM planner_join_left l JOIN planner_join_right r ON l.code = r.code WHERE l.id = 42",
            ct);

        var joinOperator = FindOperatorInUnaryChain<IndexNestedLoopJoinOperator>(GetRootOperator(result));
        var outer = GetPrivateField<IOperator>(joinOperator, "_outer");
        Assert.IsType<PrimaryKeyLookupOperator>(outer);

        var rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(42L, row[0].AsInteger));
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

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveLeafPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code AND b.id = 42");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelWherePredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id = 42");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelRangePredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id BETWEEN 1 AND 5");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelInPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id IN (1, 2, 3)");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelIsNullPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.nullable_tag IS NULL");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelOrPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id = 1 OR b.id = 2 OR b.id = 3");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelOrRangePredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id BETWEEN 1 AND 2 OR b.id BETWEEN 10 AND 11");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_GreedyChooser_UsesSelectiveTopLevelOrRangePredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var leafRows = InvokeGetReorderableLeafRows(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id BETWEEN 1 AND 2 OR b.id BETWEEN 10 AND 11");
        Assert.Equal(4, leafRows["b"]);

        var order = InvokeChooseGreedyInnerJoinOrder(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id BETWEEN 1 AND 2 OR b.id BETWEEN 10 AND 11");
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelMixedUnionPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.id IN (1, 2, 3) OR b.id BETWEEN 10 AND 11");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    [Fact]
    public async Task InnerJoinChain_ReordersUsingSelectiveTopLevelNullOrPredicate()
    {
        var ct = TestContext.Current.CancellationToken;
        await SetupReorderableJoinChainTablesAsync(ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_big", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_mid", ct);
        await _db.ExecuteAsync("ANALYZE planner_reorder_small", ct);

        var reordered = InvokeTryReorderInnerJoinChain(
            "SELECT b.id, s.flag FROM planner_reorder_small s JOIN planner_reorder_mid m ON m.code = s.code JOIN planner_reorder_big b ON b.code = m.code WHERE b.nullable_tag IS NULL OR b.nullable_tag = 42");

        var order = FlattenJoinOrder(reordered).ToArray();
        Assert.Equal(["b", "m", "s"], order);
    }

    private async ValueTask SetupSelectivityTableAsync(CancellationToken ct)
    {
        await _db.ExecuteAsync("CREATE TABLE planner_stats (id INTEGER PRIMARY KEY, low_group INTEGER, code INTEGER, nullable_code INTEGER)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_planner_stats_low_group ON planner_stats(low_group)", ct);
        await _db.ExecuteAsync("CREATE INDEX idx_planner_stats_code ON planner_stats(code)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 1000; i++)
        {
            string nullableCode = i <= 5 ? "NULL" : i.ToString();
            await _db.ExecuteAsync(
                $"INSERT INTO planner_stats VALUES ({i}, {i % 2}, {i}, {nullableCode})",
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

        object?[] args = [join, select.Where, null!];
        bool reordered = (bool)(method.Invoke(planner, args) ?? false);
        Assert.True(reordered);
        return Assert.IsAssignableFrom<TableRef>(args[2]);
    }

    private Dictionary<string, long> InvokeGetReorderableLeafRows(string sql)
    {
        var (planner, leaves, predicates, where) = CollectReorderableJoinState(sql);

        var applyMethod = planner.GetType().GetMethod("ApplyLocalPredicateRowEstimates", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(applyMethod);
        applyMethod.Invoke(planner, [leaves, predicates, where]);

        return ReadLeafRows(leaves);
    }

    private string[] InvokeChooseGreedyInnerJoinOrder(string sql)
    {
        var (planner, leaves, predicates, where) = CollectReorderableJoinState(sql);

        var applyMethod = planner.GetType().GetMethod("ApplyLocalPredicateRowEstimates", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(applyMethod);
        applyMethod.Invoke(planner, [leaves, predicates, where]);

        Type leafType = leaves.GetType().GetGenericArguments()[0];
        object orderedLeaves = Activator.CreateInstance(typeof(List<>).MakeGenericType(leafType))!;

        var chooseMethod = planner.GetType().GetMethod("TryChooseGreedyInnerJoinOrder", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(chooseMethod);
        object?[] args = [leaves, predicates, orderedLeaves];
        bool chose = (bool)(chooseMethod.Invoke(planner, args) ?? false);
        Assert.True(chose);

        return ((System.Collections.IEnumerable)args[2]!)
            .Cast<object>()
            .Select(leaf => (string)(leaf.GetType().GetProperty("Identifier")!.GetValue(leaf)!))
            .ToArray();
    }

    private long InvokeEstimateFilteredRows(string tableName, string sql)
        => InvokeEstimateFilteredRows(tableName, alias: null, sql);

    private long InvokeEstimateFilteredRows(string tableName, string? alias, string sql)
    {
        var estimatorType = typeof(QueryPlanner).Assembly.GetType("CSharpDB.Execution.CardinalityEstimator");
        Assert.NotNull(estimatorType);

        var method = estimatorType.GetMethod(
            "TryEstimateFilteredRowCount",
            BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);

        var catalogField = typeof(Database).GetField("_catalog", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(catalogField);
        object? catalog = catalogField.GetValue(_db);
        Assert.NotNull(catalog);

        var select = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        Assert.NotNull(select.Where);

        var schema = _db.GetTableSchema(tableName);
        Assert.NotNull(schema);
        TableSchema estimateSchema = schema;

        if (!string.IsNullOrWhiteSpace(alias))
        {
            var qualifiedMappings = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < schema.Columns.Count; i++)
                qualifiedMappings[$"{alias}.{schema.Columns[i].Name}"] = i;

            estimateSchema = new TableSchema
            {
                TableName = schema.TableName,
                Columns = schema.Columns,
                QualifiedMappings = qualifiedMappings,
            };
        }

        var rowCountMethod = catalog.GetType().GetMethod("TryGetTableRowCount", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        Assert.NotNull(rowCountMethod);

        object?[] rowCountArgs = [tableName, 0L];
        bool foundRowCount = (bool)(rowCountMethod.Invoke(catalog, rowCountArgs) ?? false);
        Assert.True(foundRowCount);

        long rowCount = (long)rowCountArgs[1]!;
        object?[] args =
        [
            catalog,
            estimateSchema,
            rowCount,
            new List<Expression> { select.Where },
            0L,
        ];

        bool estimated = (bool)(method.Invoke(null, args) ?? false);
        Assert.True(estimated);
        return (long)args[4]!;
    }

    private (object Planner, object Leaves, object Predicates, Expression? Where) CollectReorderableJoinState(string sql)
    {
        var plannerField = typeof(Database).GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(plannerField);
        object? planner = plannerField.GetValue(_db);
        Assert.NotNull(planner);

        Type plannerType = planner.GetType();
        Type leafType = plannerType.GetNestedType("ReorderableJoinLeaf", BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ReorderableJoinLeaf type not found.");
        Type predicateType = plannerType.GetNestedType("ReorderableJoinPredicate", BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("ReorderableJoinPredicate type not found.");

        object leaves = Activator.CreateInstance(typeof(List<>).MakeGenericType(leafType))!;
        object predicates = Activator.CreateInstance(typeof(List<>).MakeGenericType(predicateType))!;

        var collectMethod = plannerType.GetMethod("TryCollectReorderableInnerJoinChain", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(collectMethod);

        var select = Assert.IsType<SelectStatement>(Parser.Parse(sql));
        var join = Assert.IsType<JoinTableRef>(select.From);

        object?[] args = [join, leaves, predicates, 0, 0];
        bool collected = (bool)(collectMethod.Invoke(planner, args) ?? false);
        Assert.True(collected);

        return (planner, leaves, predicates, select.Where);
    }

    private static Dictionary<string, long> ReadLeafRows(object leaves)
    {
        return ((System.Collections.IEnumerable)leaves)
            .Cast<object>()
            .ToDictionary(
                leaf => (string)(leaf.GetType().GetProperty("Identifier")!.GetValue(leaf)!),
                leaf => (long)(leaf.GetType().GetProperty("RowCount")!.GetValue(leaf)!),
                StringComparer.OrdinalIgnoreCase);
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

    private static TOperator FindOperatorInUnaryChain<TOperator>(IOperator? start)
        where TOperator : class, IOperator
    {
        for (var current = start; current != null; current = current is IUnaryOperatorSource unary ? unary.Source : null)
        {
            if (current is TOperator typed)
                return typed;
        }

        throw new Xunit.Sdk.XunitException($"Expected to find {typeof(TOperator).Name} in unary operator chain.");
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
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_big (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL, nullable_tag INTEGER)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_mid (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, marker INTEGER NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE TABLE planner_reorder_small (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, flag INTEGER NOT NULL)", ct);

        await _db.BeginTransactionAsync(ct);
        for (int i = 1; i <= 5000; i++)
        {
            int code = ((i - 1) % 200) + 1;
            string nullableTag = i <= 5 ? "NULL" : i.ToString();
            await _db.ExecuteAsync(
                $"INSERT INTO planner_reorder_big VALUES ({i}, {code}, {i * 3}, {nullableTag})",
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

using System.Collections;
using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class QueryPlanRuntimeObserverTests
{
    [Fact]
    public async Task PlannerObserver_ReportsCacheAccessPathEstimateAndReclassification()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (3, 30)", ct);

        QueryPlanner planner = GetPlanner(db);
        var observer = new RecordingQueryPlanObserver();
        planner.PlanRuntimeObserver = observer;

        var statement = Assert.IsType<SelectStatement>(Parser.Parse(
            "SELECT id, value FROM t WHERE value >= 10 ORDER BY value LIMIT 2"));

        Assert.Equal(2, (await ExecuteAndDrainAsync(planner, statement, ct)).Count);
        Assert.Equal(2, (await ExecuteAndDrainAsync(planner, statement, ct)).Count);

        Assert.Equal([false, true], observer.CacheLookups);
        Assert.Equal(2, observer.Selections.Count);
        Assert.All(
            observer.Selections,
            selection => Assert.Equal(QueryPlanAccessPathCategory.TableScan, selection.AccessPath));
        Assert.All(observer.Selections, selection => Assert.NotNull(selection.EstimatedRows));

        ForceCachedPlanKind(planner, statement, "FastPrimaryKeyLookup");
        Assert.Equal(2, (await ExecuteAndDrainAsync(planner, statement, ct)).Count);

        Assert.Equal([false, true, true], observer.CacheLookups);
        Assert.Contains(QueryPlanChangeKind.CachedPlanReclassified, observer.Changes);
        Assert.Equal(QueryPlanAccessPathCategory.TableScan, observer.Selections[^1].AccessPath);
    }

    [Fact]
    public async Task PlannerObserver_MapsPrimaryKeyAndIndexSeekWithoutReplanning()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await db.ExecuteAsync("CREATE UNIQUE INDEX idx_t_value ON t(value)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (2, 20)", ct);

        QueryPlanner planner = GetPlanner(db);
        var observer = new RecordingQueryPlanObserver();
        planner.PlanRuntimeObserver = observer;

        await using QueryResult primaryKeyResult = await db.ExecuteAsync(
            "SELECT value FROM t WHERE id = 2",
            ct);
        List<DbValue[]> primaryKeyRows = await primaryKeyResult.ToListAsync(ct);
        await using QueryResult indexResult = await db.ExecuteAsync(
            "SELECT id FROM t WHERE value = 20",
            ct);
        List<DbValue[]> indexRows = await indexResult.ToListAsync(ct);

        Assert.Single(primaryKeyRows);
        Assert.Equal(20, primaryKeyRows[0][0].AsInteger);
        Assert.Single(indexRows);
        Assert.Equal(2, indexRows[0][0].AsInteger);
        Assert.Equal(QueryPlanAccessPathCategory.PrimaryKeyLookup, observer.Selections[0].AccessPath);
        Assert.Equal(1, observer.Selections[0].EstimatedRows);
        Assert.Equal(QueryPlanAccessPathCategory.IndexSeek, observer.Selections[1].AccessPath);
    }

    [Fact]
    public async Task ExplicitSimpleReadObserver_IsExecutionLocalAndRejectsComplexPlans()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var db = await Database.OpenInMemoryAsync(ct);
        await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
        await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);

        QueryPlanner planner = GetPlanner(db);
        var plannerWideObserver = new RecordingQueryPlanObserver();
        var explicitObserver = new RecordingQueryPlanObserver();
        planner.PlanRuntimeObserver = plannerWideObserver;

        var prepared = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT value FROM t WHERE id = 1"));
        Assert.True(planner.CanExecuteSimpleReadWithExplicitObserver(prepared));
        Assert.True(planner.CanExecuteSimpleReadWithExplicitObserver(
            Assert.IsType<SelectStatement>(Parser.Parse("SELECT * FROM t LIMIT 128"))));
        Assert.False(planner.CanExecuteSimpleReadWithExplicitObserver(
            Assert.IsType<SelectStatement>(Parser.Parse(
                "SELECT value FROM t ORDER BY value"))));
        Assert.False(planner.CanExecuteSimpleReadWithExplicitObserver(
            Assert.IsType<SelectStatement>(Parser.Parse(
                "SELECT COUNT(*) FROM t"))));

        await using (QueryResult first =
                     await planner.ExecuteSimpleReadAsync(prepared, explicitObserver, ct))
        {
            Assert.Single(await first.ToListAsync(ct));
        }
        await using (QueryResult cached =
                     await planner.ExecuteSimpleReadAsync(prepared, explicitObserver, ct))
        {
            Assert.Single(await cached.ToListAsync(ct));
        }

        Assert.Equal([false, true], explicitObserver.CacheLookups);
        Assert.Equal(2, explicitObserver.Selections.Count);
        Assert.All(
            explicitObserver.Selections,
            selection => Assert.Equal(
                QueryPlanAccessPathCategory.PrimaryKeyLookup,
                selection.AccessPath));
        Assert.Empty(plannerWideObserver.CacheLookups);
        Assert.Empty(plannerWideObserver.Selections);

        Assert.True(Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT value FROM t WHERE id = 1",
            out SimplePrimaryKeyLookupSql directLookup));
        await using QueryResult direct = Assert.IsType<QueryResult>(
            await planner.TryExecuteSimplePrimaryKeyLookupDirectAsync(
                directLookup,
                ct,
                explicitObserver,
                cachedOnly: true));
        Assert.Single(await direct.ToListAsync(ct));
        Assert.Equal(
            QueryPlanAccessPathCategory.PrimaryKeyLookup,
            explicitObserver.Selections[^1].AccessPath);
        Assert.Empty(plannerWideObserver.Selections);
    }

    [Fact]
    public async Task AdaptiveObserver_ReportsReclassificationAndSuccessfulReoptimization()
    {
        var observer = new RecordingQueryPlanObserver();
        AdaptiveIndexNestedLoopJoinOperator op = CreateDivergingAdaptiveOperator(observer);

        List<DbValue[]> rows = await ReadAllRowsAsync(op, TestContext.Current.CancellationToken);

        Assert.Equal([1L, 2L, 3L, 4L, 5L], rows.Select(row => row[0].AsInteger).ToArray());
        Assert.Equal(
            [
                QueryPlanChangeKind.AdaptiveReoptimizationAttempted,
                QueryPlanChangeKind.AdaptiveCardinalityReclassified,
                QueryPlanChangeKind.AdaptiveReoptimized,
            ],
            observer.Changes);
    }

    [Fact]
    public async Task ThrowingObserver_CannotAffectPlanningOrAdaptiveExecution()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var observer = new ThrowingQueryPlanObserver();

        await using (var db = await Database.OpenInMemoryAsync(ct))
        {
            await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY, value INTEGER)", ct);
            await db.ExecuteAsync("INSERT INTO t VALUES (1, 10)", ct);

            QueryPlanner planner = GetPlanner(db);
            planner.PlanRuntimeObserver = observer;
            var statement = Assert.IsType<SelectStatement>(
                Parser.Parse("SELECT id, value FROM t WHERE value >= 10"));

            List<DbValue[]> first = await ExecuteAndDrainAsync(planner, statement, ct);
            List<DbValue[]> second = await ExecuteAndDrainAsync(planner, statement, ct);

            Assert.Single(first);
            Assert.Single(second);
            Assert.Equal(1, first[0][0].AsInteger);
            Assert.Equal(1, second[0][0].AsInteger);
        }

        AdaptiveIndexNestedLoopJoinOperator adaptive = CreateDivergingAdaptiveOperator(observer);
        List<DbValue[]> adaptiveRows = await ReadAllRowsAsync(adaptive, ct);
        Assert.Equal([1L, 2L, 3L, 4L, 5L], adaptiveRows.Select(row => row[0].AsInteger).ToArray());
    }

    private static QueryPlanner GetPlanner(Database db)
    {
        FieldInfo field = typeof(Database).GetField(
            "_planner",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return Assert.IsType<QueryPlanner>(field.GetValue(db));
    }

    private static void ForceCachedPlanKind(
        QueryPlanner planner,
        SelectStatement statement,
        string planKindName)
    {
        FieldInfo cacheField = typeof(QueryPlanner).GetField(
            "_selectPlanCache",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        var cache = Assert.IsAssignableFrom<IDictionary>(cacheField.GetValue(planner));
        Type planKindType = typeof(QueryPlanner).GetNestedType(
            "SelectPlanKind",
            BindingFlags.NonPublic)!;
        cache[statement] = Enum.Parse(planKindType, planKindName);
    }

    private static async Task<List<DbValue[]>> ExecuteAndDrainAsync(
        QueryPlanner planner,
        SelectStatement statement,
        CancellationToken ct)
    {
        await using QueryResult result = await planner.ExecuteAsync(statement, ct);
        return await result.ToListAsync(ct);
    }

    private static AdaptiveIndexNestedLoopJoinOperator CreateDivergingAdaptiveOperator(
        IQueryPlanRuntimeObserver observer)
    {
        ColumnDefinition[] schema =
        [
            new() { Name = "id", Type = DbType.Integer },
        ];
        var rows = Enumerable.Range(1, 5)
            .Select(value => new[] { DbValue.FromInteger(value) })
            .ToList();
        var diagnostics = new AdaptiveQueryReoptimizationRuntimeDiagnostics(
            static () => { },
            static () => { },
            static _ => { },
            static () => { },
            static _ => { })
        {
            RuntimeObserver = observer,
        };
        var lease = new AdaptiveQueryExecutionLease(new AdaptiveQueryReoptimizationOptions
        {
            Enabled = true,
            DivergenceFactor = 2,
            MinimumObservedRows = 1,
            MaxBufferedRows = 16,
            MaxReoptimizationsPerQuery = 1,
        });

        return new AdaptiveIndexNestedLoopJoinOperator(
            new MaterializedOperator(rows, schema),
            new MaterializedOperator([], schema),
            schema,
            static source => source,
            static source => source,
            lease,
            diagnostics,
            estimatedOuterRows: 1,
            estimatedRowCount: null);
    }

    private static async Task<List<DbValue[]>> ReadAllRowsAsync(
        IOperator op,
        CancellationToken ct)
    {
        var rows = new List<DbValue[]>();
        await op.OpenAsync(ct);
        try
        {
            while (await op.MoveNextAsync(ct))
                rows.Add((DbValue[])op.Current.Clone());
        }
        finally
        {
            await op.DisposeAsync();
        }

        return rows;
    }

    private sealed class RecordingQueryPlanObserver : IQueryPlanRuntimeObserver
    {
        public List<bool> CacheLookups { get; } = [];
        public List<QueryPlanRuntimeSelection> Selections { get; } = [];
        public List<QueryPlanChangeKind> Changes { get; } = [];

        public void OnPlanCacheLookup(bool hit) => CacheLookups.Add(hit);

        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection) =>
            Selections.Add(selection);

        public void OnPlanChanged(QueryPlanChangeKind change) => Changes.Add(change);
    }

    private sealed class ThrowingQueryPlanObserver : IQueryPlanRuntimeObserver
    {
        public void OnPlanCacheLookup(bool hit) =>
            throw new InvalidOperationException("observer failure");

        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection) =>
            throw new InvalidOperationException("observer failure");

        public void OnPlanChanged(QueryPlanChangeKind change) =>
            throw new InvalidOperationException("observer failure");
    }
}

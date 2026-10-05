using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class QueryPlannerLazyStateTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private const string JoinSql =
        "SELECT l.id, r.payload FROM adaptive_left l JOIN adaptive_right r ON l.code = r.code";

    [Fact]
    public async Task ObserverInstalledBeforeFirstAdaptiveQuery_ReceivesPlanningAndRuntimeEvents()
    {
        await using var db = await CreateAdaptiveDatabaseAsync();
        QueryPlanner planner = GetPlanner(db);
        var observer = new RecordingObserver();
        planner.PlanRuntimeObserver = observer;
        db.ResetSelectPlanCacheDiagnostics();
        db.ResetAdaptiveQueryReoptimizationDiagnostics();
        Statement statement = Parser.Parse(JoinSql);

        for (int execution = 0; execution < 2; execution++)
        {
            await using QueryResult result = await planner.ExecuteAsync(statement, Ct);
            AssertJoinRows(await result.ToListAsync(Ct));
        }

        Assert.Equal(new[] { false, true }, observer.CacheLookups);
        Assert.Equal(2, observer.Selections.Count);
        Assert.Equal(2, observer.Changes.Count(change =>
            change == QueryPlanChangeKind.AdaptiveReoptimizationAttempted));
        var plans = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(1, plans.MissCount);
        Assert.Equal(1, plans.HitCount);
        var adaptive = db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot();
        Assert.Equal(2, adaptive.EligibleQueryCount);
        Assert.Equal(2, adaptive.AttemptCount);
        Assert.True(adaptive.BufferedRowCount > 0);
    }

    [Fact]
    public async Task ObserverChangedAfterPlanning_UpdatesExistingAdaptiveExecution()
    {
        await using var db = await CreateAdaptiveDatabaseAsync();
        QueryPlanner planner = GetPlanner(db);
        var planningObserver = new RecordingObserver();
        var executionObserver = new RecordingObserver();
        planner.PlanRuntimeObserver = planningObserver;
        Statement statement = Parser.Parse(JoinSql);

        await using (QueryResult result = await planner.ExecuteAsync(statement, Ct))
        {
            Assert.Equal(new[] { false }, planningObserver.CacheLookups);
            Assert.Single(planningObserver.Selections);
            Assert.Empty(planningObserver.Changes);

            // The operator has captured its diagnostics object, but execution has not begun.
            planner.PlanRuntimeObserver = executionObserver;
            AssertJoinRows(await result.ToListAsync(Ct));
        }

        Assert.Empty(planningObserver.Changes);
        Assert.Empty(executionObserver.CacheLookups);
        Assert.Empty(executionObserver.Selections);
        Assert.Equal(1, executionObserver.Changes.Count(change =>
            change == QueryPlanChangeKind.AdaptiveReoptimizationAttempted));
        int observedChangeCount = executionObserver.Changes.Count;

        await using (QueryResult result = await planner.ExecuteAsync(statement, Ct))
        {
            Assert.Equal(new[] { true }, executionObserver.CacheLookups);
            Assert.Single(executionObserver.Selections);
            planner.PlanRuntimeObserver = null;
            AssertJoinRows(await result.ToListAsync(Ct));
        }

        Assert.Equal(observedChangeCount, executionObserver.Changes.Count);
        Assert.Equal(2, db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot().AttemptCount);
    }

    [Fact]
    public async Task SchemaChangesBeforeAndAfterQueries_RefreshExpressionsAndPreparedMetadata()
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        db.PreferSyncPointLookups = false;
        var emptyPlans = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(0, emptyPlans.EntryCount);
        db.ResetSelectPlanCacheDiagnostics();
        db.ResetAdaptiveQueryReoptimizationDiagnostics();
        Assert.Equal(emptyPlans, db.GetSelectPlanCacheDiagnostics());
        Assert.Equal(0, db.GetAdaptiveQueryReoptimizationDiagnosticsSnapshot().AttemptCount);

        await db.ExecuteAsync("CREATE TABLE unused (id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("DROP TABLE unused", Ct);
        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (1, 10)", Ct);
        Statement expression = Parser.Parse(
            "SELECT value - id AS delta FROM items WHERE value >= 10 ORDER BY id");
        var prepared = Assert.IsType<PreparedSelectTemplate>(PreparedSelectTemplate.TryCreate(
            Parser.Parse("SELECT value FROM items WHERE id = @id"), ["id"]));

        for (int execution = 0; execution < 2; execution++)
        {
            Assert.Equal(new long[] { 9 }, await ReadFirstColumnAsync(db, expression));
            Assert.Equal(new long[] { 10 }, await ReadFirstColumnAsync(db, prepared.Bind([1])));
        }
        var populatedPlans = db.GetSelectPlanCacheDiagnostics();
        Assert.Equal(2, populatedPlans.MissCount);
        Assert.Equal(2, populatedPlans.HitCount);
        Assert.Equal(2, populatedPlans.EntryCount);

        await db.BeginTransactionAsync(Ct);
        await db.ExecuteAsync("DROP TABLE items", Ct);
        await db.ExecuteAsync("CREATE TABLE items (value BIGINT, id INTEGER PRIMARY KEY)", Ct);
        await db.ExecuteAsync("INSERT INTO items VALUES (100, 2)", Ct);
        Assert.Equal(new long[] { 98 }, await ReadFirstColumnAsync(db, expression));
        Assert.Equal(new long[] { 100 }, await ReadFirstColumnAsync(db, prepared.Bind([2])));
        Assert.Empty(await ReadFirstColumnAsync(db, prepared.Bind([1])));

        await db.RollbackAsync(Ct);
        Assert.Equal(new long[] { 9 }, await ReadFirstColumnAsync(db, expression));
        Assert.Equal(new long[] { 10 }, await ReadFirstColumnAsync(db, prepared.Bind([1])));
        Assert.Empty(await ReadFirstColumnAsync(db, prepared.Bind([2])));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReplacedTrigger_UsesNewBodyAndPreservesGeneratedRowIds(bool parsedInsert)
    {
        await using var db = await Database.OpenInMemoryAsync(Ct);
        async Task InsertAsync(string sql)
        {
            await using QueryResult insert = parsedInsert
                ? await db.ExecuteAsync(Assert.IsType<InsertStatement>(Parser.Parse(sql)), Ct)
                : await db.ExecuteAsync(sql, Ct);
            Assert.Equal(1, insert.RowsAffected);
        }

        await db.ExecuteAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync("CREATE TABLE audit (id INTEGER PRIMARY KEY, value BIGINT)", Ct);
        await db.ExecuteAsync(
            "CREATE TRIGGER items_ai AFTER INSERT ON items " +
            "BEGIN INSERT INTO audit (value) VALUES (NEW.value + 1); END", Ct);
        await InsertAsync("INSERT INTO items VALUES (1, 10)");
        await InsertAsync("INSERT INTO items VALUES (2, 20)");

        await db.ExecuteAsync("DROP TRIGGER items_ai", Ct);
        await db.ExecuteAsync(
            "CREATE TRIGGER ITEMS_AI AFTER INSERT ON items " +
            "BEGIN INSERT INTO audit (value) VALUES (NEW.value + 100); END", Ct);
        await InsertAsync("INSERT INTO items VALUES (3, 30)");
        await db.ExecuteAsync("DROP TRIGGER items_ai", Ct);
        await InsertAsync("INSERT INTO items VALUES (4, 40)");

        await using QueryResult result = await db.ExecuteAsync("SELECT id, value FROM audit ORDER BY id", Ct);
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        Assert.Equal(new long[] { 1, 2, 3 }, rows.Select(row => row[0].AsInteger));
        Assert.Equal(new long[] { 11, 21, 130 }, rows.Select(row => row[1].AsInteger));
    }

    private static async Task<Database> CreateAdaptiveDatabaseAsync()
    {
        Database db = await Database.OpenInMemoryAsync(
            new DatabaseOptions().EnableAdaptiveQueryReoptimization(builder => builder
                .WithDivergenceFactor(2)
                .WithMinimumObservedRows(1)
                .WithMaxBufferedRows(64)), Ct);
        await db.ExecuteAsync(
            "CREATE TABLE adaptive_left (id INTEGER PRIMARY KEY, code INTEGER NOT NULL)", Ct);
        await db.ExecuteAsync(
            "CREATE TABLE adaptive_right (id INTEGER PRIMARY KEY, code INTEGER NOT NULL, payload INTEGER NOT NULL)", Ct);
        await db.ExecuteAsync("INSERT INTO adaptive_left VALUES (1, 1), (2, 2), (3, 3)", Ct);
        await db.ExecuteAsync("INSERT INTO adaptive_right VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)", Ct);
        return db;
    }

    private static QueryPlanner GetPlanner(Database db)
        => Assert.IsType<QueryPlanner>(typeof(Database)
            .GetField("_planner", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(db));

    private static void AssertJoinRows(List<DbValue[]> rows)
    {
        DbValue[][] ordered = rows.OrderBy(row => row[0].AsInteger).ToArray();
        Assert.Equal(new long[] { 1, 2, 3 }, ordered.Select(row => row[0].AsInteger));
        Assert.Equal(new long[] { 10, 20, 30 }, ordered.Select(row => row[1].AsInteger));
    }

    private static async Task<long[]> ReadFirstColumnAsync(Database db, Statement statement)
    {
        await using QueryResult result = await db.ExecuteAsync(statement, Ct);
        return (await result.ToListAsync(Ct)).Select(row => row[0].AsInteger).ToArray();
    }

    private sealed class RecordingObserver : IQueryPlanRuntimeObserver
    {
        internal List<bool> CacheLookups { get; } = [];
        internal List<QueryPlanRuntimeSelection> Selections { get; } = [];
        internal List<QueryPlanChangeKind> Changes { get; } = [];

        public void OnPlanCacheLookup(bool hit) => CacheLookups.Add(hit);
        public void OnAccessPathSelected(in QueryPlanRuntimeSelection selection) => Selections.Add(selection);
        public void OnPlanChanged(QueryPlanChangeKind change) => Changes.Add(change);
    }
}

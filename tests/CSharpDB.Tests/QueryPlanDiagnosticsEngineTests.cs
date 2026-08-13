using System.Collections;
using System.Reflection;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class QueryPlanDiagnosticsEngineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DirectPreparedFastCacheAndReclassification_ProjectSafePlanState()
    {
        await using Database database = await CreatePopulatedDatabaseAsync();

        const string scanSql =
            "SELECT id, value FROM plan_items WHERE value >= 10 ORDER BY value";
        QueryPlanDiagnosticsSnapshot first = await ExecutePlanAsync(database, scanSql);
        QueryPlanDiagnosticsSnapshot cached = await ExecutePlanAsync(database, scanSql);
        Assert.Equal(QueryAccessPathCategory.TableScan, first.AccessPath);
        Assert.False(first.PlanCacheHit);
        Assert.True(cached.PlanCacheHit);
        Assert.Equal(3, cached.ActualRows);
        Assert.NotNull(cached.EstimatedRows);

        Statement prepared = Parser.Parse(
            "SELECT id FROM plan_items WHERE value >= 20 ORDER BY id");
        QueryPlanDiagnosticsSnapshot preparedPlan = await ExecutePlanAsync(
            database,
            prepared);
        Assert.Equal(QueryAccessPathCategory.TableScan, preparedPlan.AccessPath);
        Assert.False(preparedPlan.PlanCacheHit);
        Assert.Equal(2, preparedPlan.ActualRows);

        QueryPlanDiagnosticsSnapshot fast = await ExecutePlanAsync(
            database,
            "SELECT value FROM plan_items WHERE id = 2");
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, fast.AccessPath);
        Assert.Equal(1, fast.EstimatedRows);
        Assert.Equal(1, fast.ActualRows);

        SelectStatement reclassifiedStatement = Assert.IsType<SelectStatement>(
            Parser.Parse(
                "SELECT id, value FROM plan_items WHERE value >= 10 ORDER BY value LIMIT 2"));
        _ = await ExecutePlanAsync(database, reclassifiedStatement);
        ForceCachedPlanKind(database, reclassifiedStatement, "FastPrimaryKeyLookup");
        QueryPlanDiagnosticsSnapshot reclassified = await ExecutePlanAsync(
            database,
            reclassifiedStatement);
        Assert.True(reclassified.PlanCacheHit);
        Assert.True(reclassified.CachedPlanReclassified);
        Assert.False(reclassified.AdaptiveReclassified);
        Assert.False(reclassified.Reclassified);
        Assert.False(reclassified.Reoptimized);

        string json = JsonSerializer.Serialize(
            reclassified,
            CSharpDbObservabilityJsonContext.Default.QueryPlanDiagnosticsSnapshot);
        QueryPlanDiagnosticsSnapshot roundTrip = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.QueryPlanDiagnosticsSnapshot));
        Assert.Equal(reclassified, roundTrip);
        Assert.Equal("plan-engine-tests", roundTrip.Metadata.DatabaseAlias);
    }

    [Fact]
    public async Task DirectHistorySimpleReads_PreserveExactPlanAndTerminalAttribution()
    {
        await using Database database = await CreatePopulatedDatabaseAsync();

        SelectStatement prepared = Assert.IsType<SelectStatement>(
            Parser.Parse("SELECT value FROM plan_items WHERE id = 2"));
        QueryPlanDiagnosticsSnapshot firstPrepared = await ExecutePlanAsync(
            database,
            prepared);
        QueryPlanDiagnosticsSnapshot cachedPrepared = await ExecutePlanAsync(
            database,
            prepared);
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, firstPrepared.AccessPath);
        Assert.False(firstPrepared.PlanCacheHit);
        Assert.True(cachedPrepared.PlanCacheHit);
        Assert.Equal(1, cachedPrepared.ActualRows);

        QueryPlanDiagnosticsSnapshot sqlLookup = await ExecutePlanAsync(
            database,
            "SELECT value FROM plan_items WHERE id = 2");
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, sqlLookup.AccessPath);
        Assert.Equal(1, sqlLookup.EstimatedRows);
        Assert.Equal(1, sqlLookup.ActualRows);

        const string streamSql = "SELECT * FROM plan_items LIMIT 3";
        _ = await ExecutePlanAsync(database, streamSql);
        QueryPlanDiagnosticsSnapshot cachedStream = await ExecutePlanAsync(
            database,
            streamSql);
        Assert.Equal(QueryAccessPathCategory.TableScan, cachedStream.AccessPath);
        Assert.True(cachedStream.PlanCacheHit);
        Assert.Equal(3, cachedStream.ActualRows);

        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Empty(database.GetActiveQueryDiagnosticsSnapshot(10)!.Records);
        Assert.All(
            database.GetRecentQueryDiagnosticsSnapshot(100)!.Records.Take(5),
            static recent => Assert.Equal(
                CSharpDbOperationOutcome.Succeeded,
                recent.Outcome));
    }

    [Fact]
    public async Task ScopeFreeEligibilityFailures_DoNotLeakActiveOperations()
    {
        await using Database database = await CreatePopulatedDatabaseAsync();

        _ = await Assert.ThrowsAnyAsync<Exception>(
            () => database.ExecuteAsync((string)null!, Ct).AsTask());
        Assert.Empty(database.GetActiveQueryDiagnosticsSnapshot(10)!.Records);

        var malformed = new SelectStatement
        {
            Columns = null!,
            From = new SimpleTableRef { TableName = "plan_items" },
        };
        _ = await Assert.ThrowsAnyAsync<Exception>(
            () => database.ExecuteAsync(malformed, Ct).AsTask());

        Assert.Empty(database.GetActiveQueryDiagnosticsSnapshot(10)!.Records);
        RecentQuerySnapshot[] failures = database
            .GetRecentQueryDiagnosticsSnapshot(10)!
            .Records
            .Where(static recent =>
                recent.Outcome == CSharpDbOperationOutcome.Failed)
            .Take(2)
            .ToArray();
        Assert.Equal(2, failures.Length);
        Assert.All(failures, static recent => Assert.NotNull(recent.Error));
    }

    [Fact]
    public async Task TransactionAndReaderSessionPlanners_UseDatabaseLifetimeAdapter()
    {
        await using Database database = await CreatePopulatedDatabaseAsync();
        string serverInstanceId = database.GetQueryDiagnosticsSummary()!.Metadata.ServerInstanceId;

        QueryPlanDiagnosticsSnapshot transactionPlan;
        await using (WriteTransaction transaction =
                     await database.BeginWriteTransactionAsync(Ct))
        {
            await using QueryResult result = await transaction.ExecuteAsync(
                "SELECT id FROM plan_items WHERE value >= 20 ORDER BY id",
                Ct);
            Assert.Equal(2, (await result.ToListAsync(Ct)).Count);
            RecentQuerySnapshot recent = database
                .GetRecentQueryDiagnosticsSnapshot(100)!
                .Records[0];
            transactionPlan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
                database.GetQueryPlanDiagnosticsSnapshot(recent.OperationId));
            await transaction.RollbackAsync(Ct);
        }

        using Database.ReaderSession reader = database.CreateReaderSession();
        await using (QueryResult count = await reader.ExecuteReadAsync(
                         "SELECT COUNT(*) FROM plan_items",
                         Ct))
        {
            Assert.Equal(3, (await count.ToListAsync(Ct))[0][0].AsInteger);
        }
        RecentQuerySnapshot countRecent = database
            .GetRecentQueryDiagnosticsSnapshot(100)!
            .Records[0];
        QueryPlanDiagnosticsSnapshot countPlan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            database.GetQueryPlanDiagnosticsSnapshot(countRecent.OperationId));

        Statement readerPrepared = Parser.Parse(
            "SELECT value FROM plan_items WHERE id = 1");
        await using (QueryResult lookup = await reader.ExecuteReadAsync(
                         readerPrepared,
                         Ct))
        {
            Assert.Single(await lookup.ToListAsync(Ct));
        }
        RecentQuerySnapshot lookupRecent = database
            .GetRecentQueryDiagnosticsSnapshot(100)!
            .Records[0];
        QueryPlanDiagnosticsSnapshot lookupPlan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            database.GetQueryPlanDiagnosticsSnapshot(lookupRecent.OperationId));

        Assert.Equal(QueryAccessPathCategory.TableScan, transactionPlan.AccessPath);
        Assert.Equal(QueryAccessPathCategory.TableScan, countPlan.AccessPath);
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, lookupPlan.AccessPath);
        Assert.All(
            new[] { transactionPlan, countPlan, lookupPlan },
            plan => Assert.Equal(serverInstanceId, plan.Metadata.ServerInstanceId));
    }

    [Fact]
    public async Task StreamedCallbacks_RestoreExactScopeAndAggregateAdaptiveState()
    {
        using var diagnostics = new QueryObservability(
            CreateObservabilityOptions(),
            startLongRunningSweepTimer: false);
        QueryOperation operation = Assert.IsType<QueryOperation>(
            diagnostics.Start(sql: null));
        OpaqueDiagnosticsId operationId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        var scopeChecks = new ScopeChecks(operationId);
        var planOperator = new StreamingPlanOperator(
            diagnostics.PlanRuntimeObserver,
            operationId);
        var result = new QueryResult(planOperator);
        result.RequireRuntimeExecutionScope();
        result.SetExecutionScopeFactory(scopeChecks.EnterStorageScope);
        operation.MarkExecuting();
        operation.Observe(result);

        Assert.True(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();

        QueryPlanDiagnosticsSnapshot plan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            diagnostics.GetPlanSnapshot(operationId));
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, plan.AccessPath);
        Assert.False(plan.PlanCacheHit);
        Assert.Equal(1, plan.EstimatedRows);
        Assert.Equal(1, plan.ActualRows);
        Assert.True(plan.CachedPlanReclassified);
        Assert.True(plan.AdaptiveReclassified);
        Assert.True(plan.Reclassified);
        Assert.True(plan.AdaptiveReoptimizationAttempted);
        Assert.True(plan.Reoptimized);
        Assert.True(plan.AdaptiveReoptimizationRejected);
        Assert.True(scopeChecks.EnterCount >= 3);
        Assert.Equal(scopeChecks.EnterCount, scopeChecks.DisposeCount);
        Assert.Equal(0, scopeChecks.FailureCount);
        Assert.Equal(0, planOperator.ScopeFailureCount);
    }

    [Fact]
    public async Task ConcurrentStreamedResults_KeepPlanAttributionIndependent()
    {
        using var diagnostics = new QueryObservability(
            CreateObservabilityOptions(),
            startLongRunningSweepTimer: false);
        QueryOperation firstOperation = Assert.IsType<QueryOperation>(
            diagnostics.Start(sql: null));
        OpaqueDiagnosticsId firstId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        var firstResult = new QueryResult(
            new SingleRowPlanOperator(
                diagnostics.PlanRuntimeObserver,
                QueryPlanAccessPathCategory.TableScan,
                estimatedRows: 11));
        firstResult.RequireRuntimeExecutionScope();
        firstOperation.Observe(firstResult);

        QueryOperation secondOperation = Assert.IsType<QueryOperation>(
            diagnostics.Start(sql: null));
        OpaqueDiagnosticsId secondId = diagnostics.GetActiveSnapshot(10).Records
            .Select(static record => record.OperationId)
            .Single(id => id != firstId);
        var secondResult = new QueryResult(
            new SingleRowPlanOperator(
                diagnostics.PlanRuntimeObserver,
                QueryPlanAccessPathCategory.IndexSeek,
                estimatedRows: 1));
        secondResult.RequireRuntimeExecutionScope();
        secondOperation.Observe(secondResult);

        await Task.WhenAll(
            firstResult.ToListAsync(Ct).AsTask(),
            secondResult.ToListAsync(Ct).AsTask());
        await firstResult.DisposeAsync();
        await secondResult.DisposeAsync();

        QueryPlanDiagnosticsSnapshot firstPlan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            diagnostics.GetPlanSnapshot(firstId));
        QueryPlanDiagnosticsSnapshot secondPlan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            diagnostics.GetPlanSnapshot(secondId));
        Assert.Equal(QueryAccessPathCategory.TableScan, firstPlan.AccessPath);
        Assert.Equal(11, firstPlan.EstimatedRows);
        Assert.Equal(QueryAccessPathCategory.IndexSeek, secondPlan.AccessPath);
        Assert.Equal(1, secondPlan.EstimatedRows);
        Assert.Equal(1, firstPlan.ActualRows);
        Assert.Equal(1, secondPlan.ActualRows);
    }

    [Fact]
    public async Task DatabaseDisposal_AbandonsNeverOpenedAndStreamingResults()
    {
        Database database = await CreatePopulatedDatabaseAsync();
        QueryResult? neverOpened = null;
        QueryResult? streaming = null;
        bool databaseDisposed = false;
        try
        {
            neverOpened = await database.ExecuteAsync(
                "SELECT id FROM plan_items ORDER BY id",
                Ct);
            streaming = await database.ExecuteAsync(
                "SELECT value FROM plan_items ORDER BY id",
                Ct);
            Assert.True(await streaming.MoveNextAsync(Ct));
            OpaqueDiagnosticsId[] abandonedIds = database
                .GetActiveQueryDiagnosticsSnapshot(10)!
                .Records
                .Select(static record => record.OperationId)
                .ToArray();
            Assert.Equal(2, abandonedIds.Length);

            await database.DisposeAsync();
            databaseDisposed = true;
            Assert.Empty(database.GetActiveQueryDiagnosticsSnapshot(10)!.Records);
            await neverOpened.DisposeAsync();
            await streaming.DisposeAsync();
            OpaqueDiagnosticsId[] recentIds = database
                .GetRecentQueryDiagnosticsSnapshot(100)!
                .Records
                .Select(static record => record.OperationId)
                .ToArray();
            Assert.DoesNotContain(abandonedIds[0], recentIds);
            Assert.DoesNotContain(abandonedIds[1], recentIds);
        }
        finally
        {
            if (neverOpened is not null)
                await neverOpened.DisposeAsync();
            if (streaming is not null)
                await streaming.DisposeAsync();
            if (!databaseDisposed)
                await database.DisposeAsync();
        }
    }

    [Fact]
    public async Task ContendedWriteAdmission_ReportsWaitingThenRestoresExecution()
    {
        await using Database database = await CreatePopulatedDatabaseAsync();
        SemaphoreSlim gate = GetWriteOperationGate(database);
        await gate.WaitAsync(Ct);
        Task<QueryResult>? pending = null;
        try
        {
            pending = database.ExecuteAsync(
                "INSERT INTO plan_items VALUES (4, 40)",
                Ct).AsTask();
            await WaitUntilAsync(
                () => database.GetActiveQueryDiagnosticsSnapshot(10)!
                    .Records
                    .Any(static record => record.Phase == QueryExecutionPhase.Waiting));
        }
        finally
        {
            gate.Release();
        }

        await using QueryResult completed = await pending!;
        RecentQuerySnapshot recent = database
            .GetRecentQueryDiagnosticsSnapshot(100)!
            .Records[0];
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
        Assert.Equal(1, recent.RowsAffected);
    }

    private static async ValueTask<QueryPlanDiagnosticsSnapshot> ExecutePlanAsync(
        Database database,
        string sql)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, Ct);
        _ = await result.ToListAsync(Ct);
        return GetLatestPlan(database);
    }

    private static async ValueTask<QueryPlanDiagnosticsSnapshot> ExecutePlanAsync(
        Database database,
        Statement statement)
    {
        await using QueryResult result = await database.ExecuteAsync(statement, Ct);
        _ = await result.ToListAsync(Ct);
        return GetLatestPlan(database);
    }

    private static QueryPlanDiagnosticsSnapshot GetLatestPlan(Database database)
    {
        RecentQuerySnapshot recent = database
            .GetRecentQueryDiagnosticsSnapshot(100)!
            .Records[0];
        return Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            database.GetQueryPlanDiagnosticsSnapshot(recent.OperationId));
    }

    private static void ForceCachedPlanKind(
        Database database,
        SelectStatement statement,
        string planKindName)
    {
        QueryPlanner planner = Assert.IsType<QueryPlanner>(
            typeof(Database).GetField(
                "_planner",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(database));
        var cache = Assert.IsAssignableFrom<IDictionary>(
            typeof(QueryPlanner).GetField(
                "_selectPlanCache",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(planner));
        Type planKindType = typeof(QueryPlanner).GetNestedType(
            "SelectPlanKind",
            BindingFlags.NonPublic)!;
        cache[statement] = Enum.Parse(planKindType, planKindName);
    }

    private static SemaphoreSlim GetWriteOperationGate(Database database)
        => Assert.IsType<SemaphoreSlim>(
            typeof(Database).GetField(
                "_writeOperationGate",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(database));

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddSeconds(5);
        while (!predicate())
        {
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException("The expected diagnostics state was not observed.");
            await Task.Delay(10, Ct);
        }
    }

    private static CSharpDbObservabilityOptions CreateObservabilityOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "plan-engine-tests",
            Logging = new CSharpDbLoggingOptions { Enabled = false },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 100,
                RecentQueryCapacity = 100,
                RecentOperationCapacity = 16,
                Retention = TimeSpan.FromMinutes(5),
            },
        };

    private static DatabaseOptions CreateDatabaseOptions()
        => new() { ObservabilityOptions = CreateObservabilityOptions() };

    private static async ValueTask<Database> CreatePopulatedDatabaseAsync()
    {
        Database database = await Database.OpenInMemoryAsync(
            CreateDatabaseOptions(),
            Ct);
        try
        {
            await using (QueryResult create = await database.ExecuteAsync(
                             "CREATE TABLE plan_items (id INTEGER PRIMARY KEY, value INTEGER)",
                             Ct))
            {
            }
            await using (QueryResult insert = await database.ExecuteAsync(
                             "INSERT INTO plan_items VALUES (1, 10), (2, 20), (3, 30)",
                             Ct))
            {
            }
            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    private sealed class ScopeChecks(OpaqueDiagnosticsId expectedOperationId)
    {
        private int _enterCount;
        private int _disposeCount;
        private int _failureCount;

        internal int EnterCount => Volatile.Read(ref _enterCount);
        internal int DisposeCount => Volatile.Read(ref _disposeCount);
        internal int FailureCount => Volatile.Read(ref _failureCount);

        internal IDisposable EnterStorageScope()
        {
            Interlocked.Increment(ref _enterCount);
            CheckCurrent();
            return new CallbackScope(() =>
            {
                // Composite scope disposal is LIFO: the inner storage scope
                // is released while the outer query scope is still current.
                CheckCurrent();
                Interlocked.Increment(ref _disposeCount);
            });
        }

        private void CheckCurrent()
        {
            if (CSharpDbOperationScope.Current?.OperationId != expectedOperationId)
                Interlocked.Increment(ref _failureCount);
        }
    }

    private sealed class StreamingPlanOperator(
        IQueryPlanRuntimeObserver observer,
        OpaqueDiagnosticsId expectedOperationId) : IOperator
    {
        private int _moved;
        private int _scopeFailureCount;

        internal int ScopeFailureCount => Volatile.Read(ref _scopeFailureCount);
        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "value", Type = DbType.Integer },
        ];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; } = [DbValue.FromInteger(1)];

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            CheckScope();
            Parallel.Invoke(
                () => Select(QueryPlanAccessPathCategory.TableScan, 7),
                () => Select(QueryPlanAccessPathCategory.IndexSeek, 3),
                () => Select(QueryPlanAccessPathCategory.PrimaryKeyLookup, 1));
            QueryPlanRuntimeObserver.PlanCacheLookup(observer, hit: true);
            QueryPlanRuntimeObserver.PlanCacheLookup(observer, hit: false);
            QueryPlanRuntimeObserver.PlanChanged(
                observer,
                QueryPlanChangeKind.AdaptiveReoptimizationAttempted);
            QueryPlanRuntimeObserver.PlanChanged(
                observer,
                QueryPlanChangeKind.AdaptiveCardinalityReclassified);
            QueryPlanRuntimeObserver.PlanChanged(
                observer,
                QueryPlanChangeKind.AdaptiveReoptimized);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            CheckScope();
            return ValueTask.FromResult(Interlocked.Exchange(ref _moved, 1) == 0);
        }

        public ValueTask DisposeAsync()
        {
            CheckScope();
            QueryPlanRuntimeObserver.PlanChanged(
                observer,
                QueryPlanChangeKind.CachedPlanReclassified);
            QueryPlanRuntimeObserver.PlanChanged(
                observer,
                QueryPlanChangeKind.AdaptiveReoptimizationRejected);
            return ValueTask.CompletedTask;
        }

        private void Select(QueryPlanAccessPathCategory accessPath, long estimate)
        {
            CheckScope();
            var selection = new QueryPlanRuntimeSelection(accessPath, estimate);
            QueryPlanRuntimeObserver.AccessPathSelected(observer, in selection);
        }

        private void CheckScope()
        {
            if (CSharpDbOperationScope.Current?.OperationId != expectedOperationId)
                Interlocked.Increment(ref _scopeFailureCount);
        }
    }

    private sealed class SingleRowPlanOperator(
        IQueryPlanRuntimeObserver observer,
        QueryPlanAccessPathCategory accessPath,
        long estimatedRows) : IOperator
    {
        private int _moved;
        public ColumnDefinition[] OutputSchema { get; } =
        [
            new() { Name = "value", Type = DbType.Integer },
        ];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current { get; } = [DbValue.FromInteger(1)];

        public ValueTask OpenAsync(CancellationToken ct = default)
        {
            var selection = new QueryPlanRuntimeSelection(
                accessPath,
                estimatedRows);
            QueryPlanRuntimeObserver.AccessPathSelected(observer, in selection);
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
            => ValueTask.FromResult(Interlocked.Exchange(ref _moved, 1) == 0);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CallbackScope(Action dispose) : IDisposable
    {
        private Action? _dispose = dispose;

        public void Dispose()
            => Interlocked.Exchange(ref _dispose, null)?.Invoke();
    }
}

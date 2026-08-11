using System.Collections.Concurrent;
using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class QueryRuntimeDiagnosticsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Lifecycle_UsesDeterministicClockAndTransfersToRecentExactlyOnce()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 0, 0, TimeSpan.Zero));
        var diagnostics = new QueryObservability(CreateOptions(), clock);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));

        ActiveQuerySnapshot planning = Assert.Single(
            diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        Assert.Equal(QueryExecutionPhase.Planning, planning.Phase);
        Assert.Equal(TimeSpan.Zero, planning.Elapsed);
        Assert.Equal(clock.GetUtcNow(), planning.StartedAtUtc);

        clock.Advance(TimeSpan.FromSeconds(2));
        operation.MarkExecuting();
        ActiveQuerySnapshot executing = Assert.Single(
            diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        Assert.Equal(QueryExecutionPhase.Executing, executing.Phase);
        Assert.Equal(TimeSpan.FromSeconds(2), executing.Elapsed);
        Assert.Equal(planning.OperationId, executing.OperationId);
        Assert.Equal(planning.Metadata.ServerInstanceId, executing.Metadata.ServerInstanceId);

        clock.Advance(TimeSpan.FromSeconds(1));
        operation.OnFirstRowProduced();
        clock.Advance(TimeSpan.FromSeconds(2));
        operation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 7));
        operation.Fail(new InvalidOperationException("must-not-complete-twice"));

        Assert.Empty(diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(maximumRecords: 10).Records);
        Assert.Equal(planning.OperationId, recent.OperationId);
        Assert.Equal(planning.StartedAtUtc, recent.StartedAtUtc);
        Assert.Equal(clock.GetUtcNow(), recent.CompletedAtUtc);
        Assert.Equal(TimeSpan.FromSeconds(5), recent.Duration);
        Assert.Equal(TimeSpan.FromSeconds(3), recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.ResultConsumptionDuration);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
        Assert.Equal(7, recent.RowsProduced);
        Assert.Equal(planning.Metadata.ServerInstanceId, recent.Metadata.ServerInstanceId);

        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(1, summary.RequestCount);
        Assert.Equal(1, summary.StatementExecutionCount);
        Assert.Equal(1, summary.SucceededCount);
        Assert.Equal(0, summary.FailedCount);
        Assert.Equal(1, summary.SlowCount);
        Assert.Equal(7, summary.RowsProduced);
        Assert.Equal(0, summary.ActiveCount);
    }

    [Fact]
    public async Task DirectHistoryExecution_ReusesRuntimeLeaseAndCapturesOneTerminalTimestamp()
    {
        var clock = new CountingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 30, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);

        IQueryExecutionObservation observation = Assert.IsType<
            QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                diagnostics.StartExecution(sql: null));
        clock.ResetCallCounts();

        observation.MarkExecuting();
        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup(
                [DbValue.FromInteger(1)],
                []));

        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.True(await result.MoveNextAsync(Ct));
        clock.Advance(TimeSpan.FromSeconds(2));
        await result.DisposeAsync();
        observation.Fail(new InvalidOperationException("must-not-complete-twice"));

        Assert.Equal(2, clock.TimestampCallCount);
        Assert.Equal(1, clock.UtcNowCallCount);
        Assert.Empty(diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(maximumRecords: 10).Records);
        Assert.Equal(TimeSpan.FromSeconds(4), recent.Duration);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.ResultConsumptionDuration);
        Assert.Equal(1, recent.RowsProduced);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
    }

    [Fact]
    public void DirectHistoryExecution_ClampsFirstResultWhenMonotonicClockRegresses()
    {
        var clock = new RegressingUtcTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 45, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsType<
            QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                diagnostics.StartExecution(sql: null));

        clock.AdvanceTimestamp(TimeSpan.FromSeconds(5));
        observation.OnFirstRowProduced();
        clock.AdvanceTimestamp(TimeSpan.FromSeconds(-3));
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(maximumRecords: 10).Records);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.Duration);
        Assert.Equal(recent.Duration, recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.Zero, recent.ResultConsumptionDuration);
    }

    [Fact]
    public async Task LeanHistoryExecution_PreservesActivePlanTerminalAndClockSemantics()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 50, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        Assert.Equal("LeanQueryExecutionObservation", observation.GetType().Name);

        ActiveQuerySnapshot active = Assert.Single(
            diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        OpaqueDiagnosticsId operationId = active.OperationId;
        Assert.Equal(QueryExecutionPhase.Planning, active.Phase);

        observation.MarkExecuting();
        IQueryPlanRuntimeObserver planObserver = Assert.IsAssignableFrom<
            IQueryPlanRuntimeObserver>(observation.ExplicitPlanObserver);
        planObserver.OnPlanCacheLookup(hit: true);
        var selection = new QueryPlanRuntimeSelection(
            QueryPlanAccessPathCategory.PrimaryKeyLookup,
            EstimatedRows: 1);
        planObserver.OnAccessPathSelected(in selection);

        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup(
                [DbValue.FromInteger(7)],
                []));
        Assert.Same(observation, GetExecutionFeatures(result));
        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.True(await result.MoveNextAsync(Ct));
        clock.Advance(TimeSpan.FromSeconds(3));
        await result.DisposeAsync();

        Assert.Empty(diagnostics.GetActiveSnapshot(maximumRecords: 10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(maximumRecords: 10).Records);
        Assert.Equal(operationId, recent.OperationId);
        Assert.Equal(TimeSpan.FromSeconds(5), recent.Duration);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(3), recent.ResultConsumptionDuration);
        Assert.Equal(1, recent.RowsProduced);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);

        QueryPlanDiagnosticsSnapshot plan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            diagnostics.GetPlanSnapshot(operationId));
        Assert.True(plan.PlanCacheHit);
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, plan.AccessPath);
        Assert.Equal(1, plan.EstimatedRows);
        Assert.Equal(1, plan.ActualRows);
    }

    [Fact]
    public void LeanHistoryExecution_ClampsRegressingFirstResultToDuration()
    {
        var clock = new RegressingUtcTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));

        clock.AdvanceTimestamp(TimeSpan.FromSeconds(5));
        observation.OnFirstRowProduced();
        clock.AdvanceTimestamp(TimeSpan.FromSeconds(-3));
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(maximumRecords: 10).Records);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.Duration);
        Assert.Equal(recent.Duration, recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.Zero, recent.ResultConsumptionDuration);
    }

    [Fact]
    public async Task LeanHistoryExecution_AcceptsMinimumTimestampAsFirstResult()
    {
        var clock = new MinimumTimestampTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 30, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));

        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup([DbValue.FromInteger(1)], []));
        Assert.True(await result.MoveNextAsync(Ct));
        clock.Advance(TimeSpan.FromSeconds(2));
        await result.DisposeAsync();

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.Duration);
        Assert.Equal(TimeSpan.Zero, recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(2), recent.ResultConsumptionDuration);
    }

    [Fact]
    public async Task LeanDirectLifecycle_FirstRowClockFailureRetainsRowsAndTerminalTiming()
    {
        var clock = new ThrowingFirstRowTimestampTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 35, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup([DbValue.FromInteger(2)], []));

        Assert.True(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(1, recent.RowsProduced);
        Assert.Null(recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(10), recent.Duration);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
    }

    [Fact]
    public async Task LeanHistoryExecution_CompletionDefersUntilFirstResultTimestampPublication()
    {
        var clock = new BlockingFirstRowTimestampTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 45, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        var terminalStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        Task firstRow = Task.Run(observation.OnFirstRowProduced, Ct);
        await clock.FirstRowTimestampStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            Ct);
        Task terminal = Task.Run(() =>
        {
            terminalStarted.SetResult();
            observation.OnCompleted(new QueryResultCompletion(
                QueryResultCompletionReason.Exhausted,
                RowsProduced: 1));
        }, Ct);
        try
        {
            await terminalStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            await terminal.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);
        }
        finally
        {
            clock.ReleaseFirstRowTimestamp();
        }

        await Task.WhenAll(firstRow, terminal).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(10), recent.Duration);
        Assert.Equal(TimeSpan.FromSeconds(5), recent.TimeToFirstResult);
        Assert.Equal(TimeSpan.FromSeconds(5), recent.ResultConsumptionDuration);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LeanDirectLifecycle_ReentrantTerminalFromFirstRowClockDoesNotDeadlock(
        bool fail)
    {
        var clock = new ReentrantFirstRowTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 50, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        var expectedFailure = new InvalidOperationException("reentrant terminal");
        clock.OnFirstRowTimestamp = fail
            ? () => observation.Fail(expectedFailure)
            : () => observation.OnCompleted(new QueryResultCompletion(
                QueryResultCompletionReason.Exhausted,
                RowsProduced: 1));
        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup([DbValue.FromInteger(5)], []));

        Assert.True(await result.MoveNextAsync(Ct).AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            Ct));
        await result.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(1, recent.RowsProduced);
        Assert.Equal(
            fail
                ? CSharpDbOperationOutcome.Failed
                : CSharpDbOperationOutcome.Succeeded,
            recent.Outcome);
        if (fail)
        {
            Assert.NotNull(recent.Error);
            Assert.Equal("unexpected", recent.Error.ErrorType);
        }
        else
        {
            Assert.Null(recent.Error);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LeanDirectLifecycle_TerminalWaitsForBothFirstRowAndDisposingCallbacks(
        bool releaseFirstRowFirst)
    {
        var clock = new BlockingFirstRowTimestampTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 55, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup([DbValue.FromInteger(8)], []));
        // A resource callback deliberately keeps this canary on the ordinary
        // disposal handshake. The strict no-resource sync lane fuses disposal
        // and therefore has no DisposeInProgress callback to coordinate.
        int disposeCallbacks = 0;
        result.SetDisposeCallback(() =>
        {
            Interlocked.Increment(ref disposeCallbacks);
            return ValueTask.CompletedTask;
        });
        object slot = GetLeanSlot(observation);
        FieldInfo mutationState = GetInstanceField(slot, "MutationState");

        Task<bool> firstRow = Task.Run(
            async () => await result.MoveNextAsync(Ct),
            Ct);
        await clock.FirstRowTimestampStarted.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            Ct);
        mutationState.SetValue(slot, 1); // LeanMutationHeld
        Task disposal = Task.Run(
            async () => await result.DisposeAsync(),
            Ct);
        try
        {
            await WaitForInt64FlagAsync(
                observation,
                "_resultLifecycleState",
                1L << 60);
            observation.OnCompleted(new QueryResultCompletion(
                QueryResultCompletionReason.Exhausted,
                RowsProduced: 1));

            Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);

            if (releaseFirstRowFirst)
            {
                clock.ReleaseFirstRowTimestamp();
                Assert.True(await firstRow.WaitAsync(TimeSpan.FromSeconds(5), Ct));
                Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);

                mutationState.SetValue(slot, 0); // LeanMutationAvailable
                await disposal.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            }
            else
            {
                mutationState.SetValue(slot, 0); // LeanMutationAvailable
                await disposal.WaitAsync(TimeSpan.FromSeconds(5), Ct);
                Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);

                clock.ReleaseFirstRowTimestamp();
                Assert.True(await firstRow.WaitAsync(TimeSpan.FromSeconds(5), Ct));
            }
        }
        finally
        {
            clock.ReleaseFirstRowTimestamp();
            mutationState.SetValue(slot, 0); // LeanMutationAvailable
        }

        await Task.WhenAll(firstRow, disposal).WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
        Assert.Equal(1, recent.RowsProduced);
        Assert.Equal(1, disposeCallbacks);
    }

    [Fact]
    public async Task LeanDirectLifecycle_SynchronousEmptyLookupCompletesOnFirstMove()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 57, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        observation.MarkExecuting();
        QueryResult result = observation.Observe(
            QueryResult.FromSyncLookup(null, []));

        Assert.False(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
        Assert.Equal(0, recent.RowsProduced);
        Assert.Null(recent.TimeToFirstResult);
        Assert.Equal(1, diagnostics.GetSummary().SucceededCount);
    }

    [Fact]
    public async Task LeanDirectLifecycle_SynchronousScalarUsesFusedTerminal()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 55, 58, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        observation.MarkExecuting();
        QueryResult result = observation.Observe(
            QueryResult.FromSyncScalar(DbValue.FromInteger(42), []));

        Assert.True(await result.MoveNextAsync(Ct));
        Assert.Equal(42, result.Current[0].AsInteger);
        await result.DisposeAsync();

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
        Assert.Equal(1, recent.RowsProduced);
        Assert.NotNull(recent.TimeToFirstResult);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LeanDirectLifecycle_ScopeRequirementsPromoteToGenericObservation(
        bool requireRuntimeScope)
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        observation.MarkExecuting();
        int scopeEntries = 0;
        QueryResult result = QueryResult.FromMaterializedRows(
            [],
            [[DbValue.FromInteger(13)]]);
        if (requireRuntimeScope)
        {
            result.RequireRuntimeExecutionScope();
        }
        else
        {
            result.SetExecutionScopeFactory(
                () => new CountingScope(() => Interlocked.Increment(ref scopeEntries)));
        }

        Assert.Same(result, observation.Observe(result));
        Assert.NotSame(observation, GetExecutionFeatures(result));
        Assert.NotNull(GetInstanceField(observation, "_promoted").GetValue(observation));
        Assert.True(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Equal(1, Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records).RowsProduced);
        if (!requireRuntimeScope)
            Assert.True(scopeEntries >= 2);
    }

    [Fact]
    public async Task LeanDirectLifecycle_ExistingObserverAbandonsWithoutChangingQuery()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 5, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        var existingObserver = new CountingResultObserver();
        QueryResult result = QueryResult.FromSyncLookup(
            [DbValue.FromInteger(21)],
            []);
        result.SetObserver(existingObserver);

        Assert.Same(result, observation.Observe(result));
        Assert.True(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();

        Assert.Equal(1, existingObserver.FirstRows);
        Assert.Equal(1, existingObserver.Terminals);
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(0, diagnostics.GetSummary().RequestCount);
    }

    [Fact]
    public async Task LeanDirectLifecycle_ConcurrentFailAndDisposePublishesExactlyOnce()
    {
        const int operationCount = 32;
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 8, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1, recentCapacity: operationCount),
            clock,
            startLongRunningSweepTimer: false);

        for (int index = 0; index < operationCount; index++)
        {
            IQueryExecutionObservation observation = Assert.IsAssignableFrom<
                IQueryExecutionObservation>(
                    diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
            QueryResult result = observation.Observe(
                QueryResult.FromSyncLookup([DbValue.FromInteger(index)], []));
            Assert.True(await result.MoveNextAsync(Ct));
            var failure = new InvalidOperationException($"failure-{index}");

            await Task.WhenAll(
                Task.Run(() => observation.Fail(failure), Ct),
                Task.Run(async () => await result.DisposeAsync(), Ct))
                .WaitAsync(TimeSpan.FromSeconds(5), Ct);

            RecentQuerySnapshot[] recent = diagnostics
                .GetRecentSnapshot(operationCount)
                .Records
                .ToArray();
            Assert.Equal(index + 1, recent.Length);
            Assert.Equal(1, recent[0].RowsProduced);
            Assert.Contains(
                recent[0].Outcome,
                new[]
                {
                    CSharpDbOperationOutcome.Succeeded,
                    CSharpDbOperationOutcome.Failed,
                });
            if (recent[0].Outcome == CSharpDbOperationOutcome.Failed)
                Assert.NotNull(recent[0].Error);
        }

        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(operationCount, summary.RequestCount);
        Assert.Equal(operationCount, summary.SucceededCount + summary.FailedCount);
        Assert.Equal(operationCount, summary.RowsProduced);
        Assert.Equal(0, summary.ActiveCount);
    }

    [Fact]
    public async Task LeanDirectLifecycle_LateAttachAbandonsWithoutChangingQuery()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 10, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        var queryOperator = new BlockingOpenOperator();
        var result = new QueryResult(queryOperator);
        Task<bool> move = Task.Run(
            async () => await result.MoveNextAsync(Ct),
            Ct);
        await queryOperator.OpenStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        try
        {
            Assert.Same(result, observation.Observe(result));
            Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
            Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);
        }
        finally
        {
            queryOperator.ReleaseOpen();
        }

        Assert.False(await move.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        await result.DisposeAsync();
        Assert.Equal(0, diagnostics.GetSummary().RequestCount);
    }

    [Fact]
    public void LeanHistoryExecution_UtcFailurePreservesDurationSlowAndRetentionTimestamp()
    {
        var clock = new SwitchableFailureTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                retention: TimeSpan.FromSeconds(10),
                slowThreshold: TimeSpan.FromSeconds(4)),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        DateTimeOffset startedAtUtc = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).StartedAtUtc;

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FailNextUtcNow();
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(30), recent.Duration);
        Assert.Equal(startedAtUtc, recent.CompletedAtUtc);
        Assert.Equal(1, diagnostics.GetSummary().SlowCount);
    }

    [Fact]
    public void LeanHistoryExecution_TtfrFailurePreservesTerminalTimingSlowAndRetention()
    {
        var clock = new SwitchableFailureTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 30, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                retention: TimeSpan.FromSeconds(10),
                slowThreshold: TimeSpan.FromSeconds(4)),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));

        clock.Advance(TimeSpan.FromSeconds(5));
        observation.OnFirstRowProduced();
        clock.Advance(TimeSpan.FromSeconds(25));
        clock.FailElapsedCalculation(callNumber: 2);
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(30), recent.Duration);
        Assert.Null(recent.TimeToFirstResult);
        Assert.Null(recent.ResultConsumptionDuration);
        Assert.Equal(clock.GetUtcNow(), recent.CompletedAtUtc);
        Assert.Equal(1, diagnostics.GetSummary().SlowCount);
    }

    [Fact]
    public void FullHistoryExecution_UtcFailurePreservesDurationSlowAndRetentionTimestamp()
    {
        var clock = new SwitchableFailureTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 45, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                retention: TimeSpan.FromSeconds(10),
                slowThreshold: TimeSpan.FromSeconds(4)),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsType<
            QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                diagnostics.StartExecution(sql: null));
        DateTimeOffset startedAtUtc = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).StartedAtUtc;

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FailNextUtcNow();
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(30), recent.Duration);
        Assert.Equal(startedAtUtc, recent.CompletedAtUtc);
        Assert.Equal(1, diagnostics.GetSummary().SlowCount);
    }

    [Fact]
    public void FullHistoryExecution_TtfrFailurePreservesTerminalTimingSlowAndRetention()
    {
        var clock = new SwitchableFailureTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 50, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                retention: TimeSpan.FromSeconds(10),
                slowThreshold: TimeSpan.FromSeconds(4)),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsType<
            QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                diagnostics.StartExecution(sql: null));

        clock.Advance(TimeSpan.FromSeconds(5));
        observation.OnFirstRowProduced();
        clock.Advance(TimeSpan.FromSeconds(25));
        clock.FailElapsedCalculation(callNumber: 2);
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.FromSeconds(30), recent.Duration);
        Assert.Null(recent.TimeToFirstResult);
        Assert.Null(recent.ResultConsumptionDuration);
        Assert.Equal(clock.GetUtcNow(), recent.CompletedAtUtc);
        Assert.Equal(1, diagnostics.GetSummary().SlowCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void HistoryExecution_TerminalElapsedFailurePreservesUtcAndRetentionTimestamp(
        bool useLeanRuntime)
    {
        var clock = new SwitchableFailureTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 56, 55, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                retention: TimeSpan.FromSeconds(10),
                slowThreshold: TimeSpan.FromSeconds(4)),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(
                    sql: null,
                    allowLeanRuntime: useLeanRuntime));

        clock.Advance(TimeSpan.FromSeconds(30));
        clock.FailElapsedCalculation(callNumber: 1);
        observation.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Exhausted,
            RowsProduced: 1));

        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(TimeSpan.Zero, recent.Duration);
        Assert.Equal(clock.GetUtcNow(), recent.CompletedAtUtc);
        Assert.Equal(0, diagnostics.GetSummary().SlowCount);
    }

    [Fact]
    public async Task LeanSlots_ShareCapacityUseUniqueIdsAndIgnoreStaleCallbacksAfterReuse()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 57, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1, recentCapacity: 3),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation first = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId firstId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;

        QueryOperation rejected = Assert.IsType<QueryOperation>(
            diagnostics.Start(sql: null));
        _ = rejected.Observe(new QueryResult(rowsAffected: 2));
        Assert.Equal(1, diagnostics.GetActiveSnapshot(10).DroppedCount);

        QueryResult firstResult = first.Observe(
            QueryResult.FromSyncLookup([DbValue.FromInteger(1)], []));
        Assert.True(await firstResult.MoveNextAsync(Ct));
        await firstResult.DisposeAsync();

        IQueryExecutionObservation second = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId secondId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        Assert.NotEqual(firstId, secondId);

        first.OnFirstRowProduced();
        first.Fail(new InvalidOperationException("stale callback"));
        first.ExplicitPlanObserver?.OnPlanCacheLookup(hit: false);
        _ = second.Observe(new QueryResult(rowsAffected: 3));

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        RecentQuerySnapshot[] recent = diagnostics.GetRecentSnapshot(10).Records.ToArray();
        Assert.Equal(3, recent.Length);
        Assert.Equal(secondId, recent[0].OperationId);
        Assert.Equal(firstId, recent[1].OperationId);
        Assert.Equal(3, recent[0].RowsAffected);
        Assert.Equal(1, recent[1].RowsProduced);
        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(3, summary.SucceededCount);
        Assert.Equal(1, summary.RowsProduced);
        Assert.Equal(5, summary.RowsAffected);
        Assert.Equal(1, diagnostics.GetActiveSnapshot(10).DroppedCount);
    }

    [Fact]
    public async Task LeanDirectLifecycle_PostTerminalDisposeCannotMutateReusedSlot()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 57, 30, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1, recentCapacity: 3),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation first = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        object firstSlot = GetLeanSlot(first);
        var firstOperator = new CountingDisposeSingleRowOperator();
        QueryResult firstResult = first.Observe(new QueryResult(firstOperator));
        Assert.True(await firstResult.MoveNextAsync(Ct));
        Assert.False(await firstResult.MoveNextAsync(Ct));
        OpaqueDiagnosticsId firstId = Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records).OperationId;

        IQueryExecutionObservation second = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        Assert.Same(firstSlot, GetLeanSlot(second));
        ActiveQuerySnapshot beforeDelayedDispose = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records);
        OpaqueDiagnosticsId secondId = beforeDelayedDispose.OperationId;
        Assert.NotEqual(firstId, secondId);
        Assert.Equal(QueryExecutionPhase.Planning, beforeDelayedDispose.Phase);
        second.ExplicitPlanObserver!.OnPlanCacheLookup(hit: true);

        await firstResult.DisposeAsync();
        Assert.Equal(1, firstOperator.DisposeCount);
        first.Fail(new InvalidOperationException("stale failure"));
        first.OnCompleted(new QueryResultCompletion(
            QueryResultCompletionReason.Failed,
            RowsProduced: 99,
            new InvalidOperationException("stale completion")));
        first.ExplicitPlanObserver!.OnPlanChanged(
            QueryPlanChangeKind.AdaptiveReoptimized);

        ActiveQuerySnapshot afterDelayedDispose = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records);
        Assert.Equal(secondId, afterDelayedDispose.OperationId);
        Assert.Equal(QueryExecutionPhase.Planning, afterDelayedDispose.Phase);
        QueryPlanDiagnosticsSnapshot activePlan = Assert.IsType<
            QueryPlanDiagnosticsSnapshot>(diagnostics.GetPlanSnapshot(secondId));
        Assert.True(activePlan.PlanCacheHit);
        Assert.False(activePlan.Reoptimized);

        _ = second.Observe(new QueryResult(rowsAffected: 2));
        RecentQuerySnapshot[] recent = diagnostics.GetRecentSnapshot(10).Records.ToArray();
        Assert.Equal(new[] { secondId, firstId },
            recent.Select(static item => item.OperationId));
        Assert.Equal(2, recent[0].RowsAffected);
        Assert.Equal(1, recent[1].RowsProduced);
        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(2, summary.RequestCount);
        Assert.Equal(2, summary.SucceededCount);
        Assert.Equal(1, summary.RowsProduced);
        Assert.Equal(2, summary.RowsAffected);
    }

    [Fact]
    public void LeanAndFullHistory_ShareRecentCapacityAndNewestFirstOrdering()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 58, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 2, recentCapacity: 2),
            clock,
            startLongRunningSweepTimer: false);

        IQueryExecutionObservation lean = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId leanId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        _ = lean.Observe(new QueryResult(rowsAffected: 1));

        QueryOperation full = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        OpaqueDiagnosticsId fullId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        _ = full.Observe(new QueryResult(rowsAffected: 2));

        RecentQuerySnapshot[] mixed = diagnostics.GetRecentSnapshot(10).Records.ToArray();
        Assert.Equal(new[] { fullId, leanId }, mixed.Select(static item => item.OperationId));

        IQueryExecutionObservation newest = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId newestId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        _ = newest.Observe(new QueryResult(rowsAffected: 3));

        RecentQuerySnapshot[] bounded = diagnostics.GetRecentSnapshot(10).Records.ToArray();
        Assert.Equal(new[] { newestId, fullId }, bounded.Select(static item => item.OperationId));
        Assert.Equal(new long[] { 3, 2 }, bounded.Select(static item => item.RowsAffected));
        Assert.Equal(1, diagnostics.GetRecentSnapshot(10).DroppedCount);
    }

    [Fact]
    public async Task LeanPromotionFailure_AbandonsActivationAndNeverChangesQueryResult()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 59, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 2, recentCapacity: 2),
            clock);
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        IQueryExecutionObservation lean = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        ActiveQuerySnapshot leanActive = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records);

        CSharpDbOperationContext duplicateContext =
            CSharpDbOperationContext.CreateCapturedRoot(
                leanActive.OperationId,
                leanActive.Transport,
                "query-runtime-tests",
                leanActive.Fingerprint,
                clock,
                leanActive.StartedAtUtc,
                startingTimestamp: clock.GetTimestamp());
        QueryRuntimeDiagnostics.QueryRuntimeOperation duplicate = Assert.IsType<
            QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(duplicateContext));

        using IDisposable scope = lean.EnterScope();
        lean.MarkExecuting();
        QueryResult result = QueryResult.FromSyncLookup(
            [DbValue.FromInteger(9)],
            []);
        Assert.Same(result, lean.Observe(result));
        Assert.True(await result.MoveNextAsync(Ct));
        await result.DisposeAsync();
        lean.Fail(new InvalidOperationException("must remain inert"));

        ActiveQuerySnapshot remaining = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records);
        Assert.Equal(leanActive.OperationId, remaining.OperationId);
        duplicate.Abandon();
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(0, diagnostics.GetSummary().RequestCount);
    }

    [Fact]
    public void DisposingRuntime_AbandonsLeanActiveSlotsAndMakesCallbacksInert()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 59, 30, TimeSpan.Zero));
        var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 1),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation lean = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        Assert.Single(diagnostics.GetActiveSnapshot(10).Records);

        diagnostics.Dispose();
        using IDisposable scope = lean.EnterScope();
        lean.MarkExecuting();
        QueryResult result = QueryResult.FromSyncLookup(
            [DbValue.FromInteger(1)],
            []);
        Assert.Same(result, lean.Observe(result));
        lean.Fail(new InvalidOperationException("disposed diagnostics"));

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Empty(diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(0, diagnostics.GetSummary().ActiveCount);
    }

    [Fact]
    public async Task LeanPlanWriter_WaitsForConcurrentWriterInsteadOfDroppingMutation()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 59, 40, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId operationId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        object slot = GetLeanSlot(observation);
        FieldInfo mutationState = GetInstanceField(slot, "MutationState");
        var writerStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var writerReturned = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        mutationState.SetValue(slot, 1); // LeanMutationHeld
        Task writer = Task.Run(() =>
        {
            writerStarted.SetResult();
            observation.ExplicitPlanObserver!.OnPlanChanged(
                QueryPlanChangeKind.CachedPlanReclassified);
            writerReturned.SetResult();
        }, Ct);
        try
        {
            await writerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            _ = await Assert.ThrowsAsync<TimeoutException>(
                () => writerReturned.Task.WaitAsync(
                    TimeSpan.FromMilliseconds(100),
                    Ct));
        }
        finally
        {
            mutationState.SetValue(slot, 0); // LeanMutationAvailable
        }

        await writer.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        _ = observation.Observe(new QueryResult(rowsAffected: 1));
        QueryPlanDiagnosticsSnapshot plan = Assert.IsType<QueryPlanDiagnosticsSnapshot>(
            diagnostics.GetPlanSnapshot(operationId));
        Assert.True(plan.CachedPlanReclassified);
    }

    [Fact]
    public async Task LeanActiveSnapshot_WaitsForPlanMutationAndCapturesCoherentState()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 8, 59, 50, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(),
            clock,
            startLongRunningSweepTimer: false);
        IQueryExecutionObservation observation = Assert.IsAssignableFrom<
            IQueryExecutionObservation>(
                diagnostics.StartExecution(sql: null, allowLeanRuntime: true));
        OpaqueDiagnosticsId operationId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;
        object slot = GetLeanSlot(observation);
        FieldInfo mutationState = GetInstanceField(slot, "MutationState");
        FieldInfo phase = GetInstanceField(slot, "Phase");
        FieldInfo planState = GetInstanceField(slot, "Plan");
        var snapshotStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        mutationState.SetValue(slot, 1); // LeanMutationHeld
        Task<QueryPlanDiagnosticsSnapshot?> snapshot = Task.Run(() =>
        {
            snapshotStarted.SetResult();
            return diagnostics.GetPlanSnapshot(operationId);
        }, Ct);
        try
        {
            await snapshotStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            _ = await Assert.ThrowsAsync<TimeoutException>(
                () => snapshot.WaitAsync(
                    TimeSpan.FromMilliseconds(100),
                    Ct));

            var stagedPlan = new QueryRuntimeDiagnostics.QueryPlanState();
            stagedPlan.RecordPlanCacheLookup(hit: true);
            stagedPlan.RecordAccessPath(
                QueryPlanAccessPathCategory.PrimaryKeyLookup,
                estimatedRows: 1);
            planState.SetValue(slot, stagedPlan);
            phase.SetValue(slot, QueryExecutionPhase.Streaming);
        }
        finally
        {
            mutationState.SetValue(slot, 0); // LeanMutationAvailable
        }

        QueryPlanDiagnosticsSnapshot captured = Assert.IsType<
            QueryPlanDiagnosticsSnapshot>(await snapshot);
        Assert.True(captured.PlanCacheHit);
        Assert.Equal(QueryAccessPathCategory.PrimaryKeyLookup, captured.AccessPath);
        Assert.Equal(1, captured.EstimatedRows);
        Assert.Equal(
            QueryExecutionPhase.Streaming,
            Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);

        _ = observation.Observe(new QueryResult(rowsAffected: 1));
    }

    [Fact]
    public void CapacityAndRetention_ReportIndependentDropsAndKeepNewestHistory()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 9, 0, 0, TimeSpan.Zero));
        var diagnostics = new QueryObservability(
            CreateOptions(activeCapacity: 2, recentCapacity: 2, retention: TimeSpan.FromMinutes(1)),
            clock);
        QueryOperation[] operations = Enumerable.Range(0, 3)
            .Select(_ => Assert.IsType<QueryOperation>(diagnostics.Start(sql: null)))
            .ToArray();

        BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> active =
            diagnostics.GetActiveSnapshot(maximumRecords: 10);
        Assert.Equal(2, active.Records.Count);
        Assert.Equal(1, active.DroppedCount);
        Assert.True(active.IsTruncated);

        foreach (QueryOperation operation in operations)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            operation.MarkExecuting();
            operation.Observe(new QueryResult(rowsAffected: 1));
        }

        BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
            diagnostics.GetRecentSnapshot(maximumRecords: 10);
        Assert.Equal(2, recent.Records.Count);
        Assert.Equal(1, recent.DroppedCount);
        Assert.True(recent.IsTruncated);
        Assert.Equal(
            new[] { TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(2) },
            recent.Records.Select(static item => item.Duration));
        Assert.Equal(3, diagnostics.GetSummary().SucceededCount);
        Assert.Equal(3, diagnostics.GetSummary().RowsAffected);

        BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> completedActive =
            diagnostics.GetActiveSnapshot(maximumRecords: 10);
        Assert.Empty(completedActive.Records);
        Assert.Equal(1, completedActive.DroppedCount);

        clock.Advance(TimeSpan.FromMinutes(1) + TimeSpan.FromTicks(1));
        BoundedDiagnosticsSnapshot<RecentQuerySnapshot> expired =
            diagnostics.GetRecentSnapshot(maximumRecords: 10);
        Assert.Empty(expired.Records);
        Assert.Equal(3, expired.DroppedCount);
        Assert.True(expired.IsTruncated);
    }

    [Fact]
    public async Task StreamingPhaseCannotRegressAndDisposalIsVisibleBeforeTerminalTransfer()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 10, 0, 0, TimeSpan.Zero));
        var diagnostics = new QueryObservability(CreateOptions(), clock);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        operation.MarkExecuting();
        var blockingOperator = new BlockingDisposeOperator();
        QueryResult result = operation.Observe(new QueryResult(blockingOperator));

        Assert.Equal(
            QueryExecutionPhase.Streaming,
            Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);
        operation.MarkExecuting();
        Assert.Equal(
            QueryExecutionPhase.Streaming,
            Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);

        Task disposal = result.DisposeAsync().AsTask();
        try
        {
            await blockingOperator.DisposeStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Equal(
                QueryExecutionPhase.Disposing,
                Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);
        }
        finally
        {
            blockingOperator.ReleaseDispose();
            await disposal;
        }

        operation.MarkExecuting();
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Single(diagnostics.GetRecentSnapshot(10).Records);
    }

    [Fact]
    public void DuplicateOperationId_HasOnlyOneTerminalOwner()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 11, 0, 0, TimeSpan.Zero));
        var diagnostics = new QueryObservability(CreateOptions(), clock);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Embedded,
            "query-runtime-tests",
            timeProvider: clock);

        using IDisposable scope = CSharpDbOperationScope.Enter(context);
        QueryOperation first = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        Assert.Null(diagnostics.Start(sql: null));
        Assert.Single(diagnostics.GetActiveSnapshot(10).Records);

        first.Observe(new QueryResult(rowsAffected: 1));
        Assert.Null(diagnostics.Start(sql: null));

        RecentQuerySnapshot recent = Assert.Single(diagnostics.GetRecentSnapshot(10).Records);
        Assert.Equal(context.OperationId, recent.OperationId);
        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(1, summary.RequestCount);
        Assert.Equal(1, summary.StatementExecutionCount);
        Assert.Equal(1, summary.SucceededCount);
        Assert.Equal(0, summary.FailedCount);
    }

    [Fact]
    public async Task CompletionAndSnapshotRaces_DoNotLeakOrExposeCompletedActiveRecords()
    {
        const int operationCount = 500;
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        var diagnostics = new QueryObservability(
            CreateOptions(
                activeCapacity: operationCount,
                recentCapacity: operationCount,
                retention: TimeSpan.FromMinutes(5)),
            clock);
        QueryOperation[] operations = Enumerable.Range(0, operationCount)
            .Select(_ => Assert.IsType<QueryOperation>(diagnostics.Start(sql: null)))
            .ToArray();
        int stopSnapshots = 0;

        Task snapshotter = Task.Run(() =>
        {
            while (Volatile.Read(ref stopSnapshots) == 0)
            {
                BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> snapshot =
                    diagnostics.GetActiveSnapshot(operationCount);
                Assert.DoesNotContain(
                    snapshot.Records,
                    static record => record.Phase == QueryExecutionPhase.Completed);
            }
        }, Ct);

        try
        {
            await Task.Run(() => Parallel.For(0, operationCount, index =>
            {
                QueryOperation operation = operations[index];
                operation.MarkExecuting();
                operation.OnFirstRowProduced();
                if ((index & 1) == 0)
                    operation.Observe(new QueryResult(rowsAffected: 1));
                else
                    operation.Fail(new OperationCanceledException());

                operation.Fail(new InvalidOperationException("second-terminal"));
                operation.MarkExecuting();
            }), Ct);
        }
        finally
        {
            Volatile.Write(ref stopSnapshots, 1);
            await snapshotter;
        }

        BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> active =
            diagnostics.GetActiveSnapshot(operationCount);
        Assert.Empty(active.Records);
        Assert.Equal(0, active.DroppedCount);

        BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
            diagnostics.GetRecentSnapshot(operationCount);
        Assert.Equal(operationCount, recent.Records.Count);
        Assert.Equal(operationCount, recent.Records.Select(static item => item.OperationId).Distinct().Count());
        Assert.Equal(0, recent.DroppedCount);
        Assert.DoesNotContain(
            recent.Records,
            static record => record.Outcome == CSharpDbOperationOutcome.Unknown);

        QueryDiagnosticsSummary summary = diagnostics.GetSummary();
        Assert.Equal(operationCount, summary.RequestCount);
        Assert.Equal(operationCount, summary.StatementExecutionCount);
        Assert.Equal(operationCount / 2, summary.SucceededCount);
        Assert.Equal(operationCount / 2, summary.CanceledCount);
        Assert.Equal(0, summary.FailedCount);
        Assert.Equal(operationCount / 2, summary.RowsAffected);
        Assert.Equal(0, summary.ActiveCount);
    }

    [Fact]
    public void LongRunningSweep_PublishesOnceAtThresholdAndRetainsFinalSlowEvent()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(2);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name =>
                name == CSharpDbLogEvents.LongRunningQuery.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name);
        using var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                slowThreshold: threshold,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        operation.MarkExecuting();

        clock.Advance(threshold - TimeSpan.FromTicks(1));
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());
        Assert.Empty(received);

        clock.Advance(TimeSpan.FromTicks(1));
        Assert.Equal(1, diagnostics.SweepLongRunningQueries());
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());

        CSharpDbLongRunningQueryEvent longRunning = Assert.Single(
            received.OfType<CSharpDbLongRunningQueryEvent>());
        Assert.Equal(threshold, longRunning.Elapsed);
        Assert.Equal(threshold, longRunning.LongRunningQueryThreshold);
        Assert.Equal(QueryExecutionPhase.Executing, longRunning.Phase);
        Assert.Equal(clock.GetUtcNow(), longRunning.ObservedAtUtc);

        operation.Observe(new QueryResult(rowsAffected: 1));

        CSharpDbSlowQueryEvent slow = Assert.Single(received.OfType<CSharpDbSlowQueryEvent>());
        Assert.Equal(longRunning.Context.OperationId, slow.Context.OperationId);
        Assert.Equal(threshold, slow.TotalDuration);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, slow.Outcome);
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Single(diagnostics.GetRecentSnapshot(10).Records);
    }

    [Fact]
    public void LongRunningListenerAddedAfterStart_BeginsWithNextOperation()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 30, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        QueryOperation beforeSubscription =
            Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        OpaqueDiagnosticsId beforeSubscriptionId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records).OperationId;

        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        clock.Advance(threshold);
        Assert.Equal(1, diagnostics.SweepLongRunningQueries());
        Assert.Empty(received);

        QueryOperation afterSubscription =
            Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        OpaqueDiagnosticsId afterSubscriptionId = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records,
            record => record.OperationId != beforeSubscriptionId).OperationId;
        clock.Advance(threshold);
        Assert.Equal(1, diagnostics.SweepLongRunningQueries());

        CSharpDbLongRunningQueryEvent published = Assert.Single(
            received.OfType<CSharpDbLongRunningQueryEvent>());
        Assert.NotEqual(beforeSubscriptionId, published.Context.OperationId);
        Assert.Equal(afterSubscriptionId, published.Context.OperationId);

        beforeSubscription.Observe(new QueryResult(rowsAffected: 1));
        afterSubscription.Observe(new QueryResult(rowsAffected: 1));
    }

    [Fact]
    public void LongRunningListenerRemovedAfterStart_StopsDeliveryPermanently()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 45, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        subscription.Dispose();

        clock.Advance(threshold);
        Assert.Equal(1, diagnostics.SweepLongRunningQueries());
        Assert.Empty(received);

        using IDisposable lateSubscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());
        Assert.Empty(received);

        operation.Observe(new QueryResult(rowsAffected: 1));
    }

    [Fact]
    public void HistoryAndNoListenerStarts_HaveNoLongRunningPublicationClaim()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 50, 0, TimeSpan.Zero));
        using var historyState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(loggingEnabled: false, slowQueries: false),
            clock);
        QueryRuntimeDiagnostics historyRegistry = QueryRuntimeDiagnostics.GetOrCreate(
            historyState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation historyOperation =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                historyRegistry.TryStart(
                    CreateContext(clock),
                    QueryExecutionPhase.Planning));

        Assert.Null(historyOperation.PublicationClaim);
        historyOperation.Abandon();

        Assert.False(CSharpDbDiagnostics.EventPublisher.IsEnabled(
            CSharpDbLogEvents.LongRunningQuery));
        using var loggingState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(loggingEnabled: true, slowQueries: true),
            clock);
        QueryRuntimeDiagnostics loggingRegistry = QueryRuntimeDiagnostics.GetOrCreate(
            loggingState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation noListenerOperation =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                loggingRegistry.TryStart(
                    CreateContext(clock),
                    QueryExecutionPhase.Planning));

        Assert.Null(noListenerOperation.PublicationClaim);
        noListenerOperation.Abandon();
    }

    [Fact]
    public async Task CompletionAndSweepRace_NeverDuplicatesLongRunningOrTerminalEvents()
    {
        const int operationCount = 128;
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 14, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name =>
                name == CSharpDbLogEvents.LongRunningQuery.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name);
        using var diagnostics = new QueryObservability(
            CreateOptions(
                activeCapacity: operationCount,
                recentCapacity: operationCount,
                loggingEnabled: true,
                slowQueries: true,
                slowThreshold: threshold,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        QueryOperation[] operations = Enumerable.Range(0, operationCount)
            .Select(_ => Assert.IsType<QueryOperation>(diagnostics.Start(sql: null)))
            .ToArray();
        foreach (QueryOperation operation in operations)
            operation.MarkExecuting();
        clock.Advance(threshold);

        Task sweep = Task.Run(() => diagnostics.SweepLongRunningQueries(), Ct);
        Task completion = Task.Run(() => Parallel.ForEach(
            operations,
            static operation => operation.Observe(new QueryResult(rowsAffected: 1))), Ct);
        await Task.WhenAll(sweep, completion);
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());

        CSharpDbLongRunningQueryEvent[] longRunning = received
            .OfType<CSharpDbLongRunningQueryEvent>()
            .ToArray();
        CSharpDbSlowQueryEvent[] slow = received
            .OfType<CSharpDbSlowQueryEvent>()
            .ToArray();
        Assert.InRange(longRunning.Length, 0, operationCount);
        Assert.Equal(
            longRunning.Length,
            longRunning.Select(static item => item.Context.OperationId).Distinct().Count());
        Assert.Equal(operationCount, slow.Length);
        Assert.Equal(
            operationCount,
            slow.Select(static item => item.Context.OperationId).Distinct().Count());
        Assert.All(
            longRunning,
            item => Assert.Contains(
                slow,
                terminal => terminal.Context.OperationId == item.Context.OperationId));
        Assert.Empty(diagnostics.GetActiveSnapshot(operationCount).Records);
        Assert.Equal(operationCount, diagnostics.GetRecentSnapshot(operationCount).Records.Count);
    }

    [Fact]
    public void LongRunningListener_CanCompleteAndThrowWithoutEscapingOrDeadlockingSweep()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 15, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        QueryOperation? operation = null;
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new ReentrantThrowingObserver(
                received,
                () => operation!.Observe(new QueryResult(rowsAffected: 1))),
            static name =>
                name == CSharpDbLogEvents.LongRunningQuery.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name);
        using var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                slowThreshold: threshold,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        operation.MarkExecuting();
        clock.Advance(threshold);

        Exception? exception = Record.Exception(() =>
            Assert.Equal(1, diagnostics.SweepLongRunningQueries()));

        Assert.Null(exception);
        Assert.Single(received.OfType<CSharpDbLongRunningQueryEvent>());
        Assert.Single(received.OfType<CSharpDbSlowQueryEvent>());
        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Single(diagnostics.GetRecentSnapshot(10).Records);
    }

    [Fact]
    public async Task SweepIsNonOverlappingWhenClockCallbackBlocks()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new BlockingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 16, 0, 0, TimeSpan.Zero));
        using var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock,
            startLongRunningSweepTimer: false);
        _ = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        clock.Advance(threshold);
        clock.BlockElapsedCalculation();

        Task<int> firstSweep = Task.Run(diagnostics.SweepLongRunningQueries, Ct);
        await clock.ElapsedCalculationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());
        clock.ReleaseElapsedCalculation();

        Assert.Equal(1, await firstSweep);
        Assert.Equal(0, diagnostics.SweepLongRunningQueries());
    }

    [Fact]
    public void TimerCreationSuppressesAmbientBoundaryAndDisposalStopsCallbacks()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new CapturingTimerTimeProvider(
            new DateTimeOffset(2026, 8, 10, 17, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        QueryObservability diagnostics;
        QueryOperation operation;

        using (CSharpDbOperationScope.EnterBoundary(CSharpDbTransport.Direct))
        {
            diagnostics = new QueryObservability(
                CreateOptions(
                    loggingEnabled: true,
                    slowQueries: true,
                    longRunningThreshold: threshold),
                clock);
            operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
            operation.MarkExecuting();
            QueryOperation second = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
            second.Observe(new QueryResult(rowsAffected: 1));
            Assert.Equal(1, clock.CreateTimerCount);
            Assert.Null(clock.Timer.CapturedExecutionContext);
        }

        clock.Advance(threshold);
        clock.Timer.Fire();
        Assert.Single(received.OfType<CSharpDbLongRunningQueryEvent>());

        diagnostics.Dispose();
        diagnostics.Dispose();
        Assert.True(clock.Timer.IsDisposed);
        clock.Timer.Fire();
        Assert.Single(received.OfType<CSharpDbLongRunningQueryEvent>());
        operation.Observe(new QueryResult(rowsAffected: 1));
    }

    [Fact]
    public async Task DisposalRacingInFlightTimerCallbackSuppressesPublicationAndDisposesTimer()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new CapturingTimerTimeProvider(
            new DateTimeOffset(2026, 8, 10, 18, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        var diagnostics = new QueryObservability(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
        operation.MarkExecuting();
        clock.Advance(threshold);
        clock.BlockElapsedCalculation();

        Task callback = Task.Run(clock.Timer.Fire, Ct);
        await clock.ElapsedCalculationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        diagnostics.Dispose();
        clock.ReleaseElapsedCalculation();
        await callback;

        Assert.True(clock.Timer.IsDisposed);
        Assert.Empty(received);
        operation.Observe(new QueryResult(rowsAffected: 1));
    }

    [Fact]
    public void ExactRuntimeState_CachesOneRegistryAndTimerUntilStateDisposal()
    {
        var clock = new CapturingTimerTimeProvider(
            new DateTimeOffset(2026, 8, 10, 19, 0, 0, TimeSpan.Zero));
        var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: TimeSpan.FromSeconds(1)),
            clock);
        var wrappers = new QueryObservability[16];

        Parallel.For(
            0,
            wrappers.Length,
            index => wrappers[index] = new QueryObservability(state));

        Assert.Equal(1, clock.CreateTimerCount);
        QueryOperation operation = Assert.IsType<QueryOperation>(wrappers[0].Start(sql: null));
        ActiveQuerySnapshot visibleThroughSecondWrapper = Assert.Single(
            wrappers[1].GetActiveSnapshot(10).Records);
        Assert.Equal(
            Assert.Single(wrappers[0].GetActiveSnapshot(10).Records).OperationId,
            visibleThroughSecondWrapper.OperationId);

        foreach (QueryObservability wrapper in wrappers)
            wrapper.Dispose();
        Assert.False(clock.Timer.IsDisposed);

        operation.Observe(new QueryResult(rowsAffected: 1));
        Assert.Single(wrappers[2].GetRecentSnapshot(10).Records);
        state.Dispose();
        state.Dispose();
        Assert.True(clock.Timer.IsDisposed);
    }

    [Fact]
    public void AmbientQueuedLease_IsAdoptedOnceAndCannotCreateListenerOnlyDuplicate()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 20, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name =>
                name == CSharpDbLogEvents.QueryCompleted.Name ||
                name == CSharpDbLogEvents.SlowQuery.Name);
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                queries: true,
                slowQueries: true,
                slowThreshold: threshold,
                longRunningThreshold: threshold),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        QueryRuntimeDiagnostics.QueryRuntimeOperation queued =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(context, QueryExecutionPhase.Queued));

        Assert.Equal(
            QueryExecutionPhase.Queued,
            Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);
        using (CSharpDbOperationScope.Enter(context, queued))
        {
            QueryOperation adopted = Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
            Assert.Equal(
                QueryExecutionPhase.Planning,
                Assert.Single(diagnostics.GetActiveSnapshot(10).Records).Phase);

            // Adoption is a one-shot handoff. Even with query and slow-event
            // listeners enabled, the repeated start must not become a
            // listener-only operation with a second terminal event.
            Assert.Null(diagnostics.Start(sql: null));
            clock.Advance(threshold);
            adopted.Observe(new QueryResult(rowsAffected: 1));
        }

        Assert.Empty(diagnostics.GetActiveSnapshot(10).Records);
        Assert.Single(diagnostics.GetRecentSnapshot(10).Records);
        Assert.Single(received.OfType<CSharpDbQueryCompletedEvent>());
        Assert.Single(received.OfType<CSharpDbSlowQueryEvent>());
    }

    [Fact]
    public void AmbientLeaseAdoption_RequiresExactOwnerContextReferenceAndOperationId()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 21, 0, 0, TimeSpan.Zero));
        using var firstState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics second = QueryRuntimeDiagnostics.GetOrCreate(
            secondState,
            startSweepTimer: false);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        CSharpDbOperationContext copiedContext = context with { };
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                first.TryStart(context, QueryExecutionPhase.Queued));

        Assert.NotSame(context, copiedContext);
        Assert.Equal(context.OperationId, copiedContext.OperationId);
        Assert.False(lease.TryAdopt(second, context));
        Assert.False(lease.TryAdopt(first, copiedContext));
        Assert.True(lease.TryAdopt(first, context));
        Assert.False(lease.TryAdopt(first, context));
    }

    [Fact]
    public void PreClaimContextCopy_CannotStartDuplicateRegistryHistory()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 21, 30, 0, TimeSpan.Zero));
        using var firstState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics second = QueryRuntimeDiagnostics.GetOrCreate(
            secondState,
            startSweepTimer: false);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        CSharpDbOperationContext copiedBeforeClaim = context with { };

        QueryRuntimeDiagnostics.QueryRuntimeOperation firstLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                first.TryStart(context, QueryExecutionPhase.Queued));
        Assert.Null(second.TryStart(
            copiedBeforeClaim,
            QueryExecutionPhase.Queued,
            out bool operationAlreadyClaimed));

        Assert.True(operationAlreadyClaimed);
        Assert.Single(first.GetActiveSnapshot(10).Records);
        Assert.Empty(second.GetActiveSnapshot(10).Records);

        firstLease.Abandon();
        Assert.Empty(first.GetActiveSnapshot(10).Records);
    }

    [Fact]
    public void WrongStateAmbientLease_IsAuthoritativeAndCannotCreateSecondHistory()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 22, 0, 0, TimeSpan.Zero));
        using var firstState = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        var second = new QueryObservability(
            secondState,
            startLongRunningSweepTimer: false);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        QueryRuntimeDiagnostics.QueryRuntimeOperation firstLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                first.TryStart(context, QueryExecutionPhase.Queued));

        using (CSharpDbOperationScope.Enter(context, firstLease))
            Assert.Null(second.Start(sql: null));

        Assert.Single(first.GetActiveSnapshot(10).Records);
        Assert.Empty(second.GetActiveSnapshot(10).Records);
        Assert.Empty(second.GetRecentSnapshot(10).Records);
    }

    [Fact]
    public void ChildWithoutAmbientLease_StartsDistinctRegistryEntry()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 23, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);
        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        QueryRuntimeDiagnostics.QueryRuntimeOperation parentLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(parent, QueryExecutionPhase.Queued));
        CSharpDbOperationContext child = CSharpDbOperationContext.CreateStatement(parent);

        using (CSharpDbOperationScope.Enter(parent, parentLease))
        using (CSharpDbOperationScope.Enter(child))
        {
            QueryOperation childOperation =
                Assert.IsType<QueryOperation>(diagnostics.Start(sql: null));
            Assert.Equal(
                new[] { child.OperationId, parent.OperationId }
                    .OrderBy(static id => id.Value),
                diagnostics.GetActiveSnapshot(10).Records
                    .Select(static record => record.OperationId)
                    .OrderBy(static id => id.Value));

            childOperation.Observe(new QueryResult(rowsAffected: 1));
        }

        ActiveQuerySnapshot remaining = Assert.Single(
            diagnostics.GetActiveSnapshot(10).Records);
        Assert.Equal(parent.OperationId, remaining.OperationId);
        Assert.Equal(child.OperationId, Assert.Single(
            diagnostics.GetRecentSnapshot(10).Records).OperationId);
    }

    [Fact]
    public void Abandon_IsIdempotentAndProducesNoHistoryCountersOrDrops()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 0, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(context, QueryExecutionPhase.Queued));

        lease.Abandon();
        lease.Abandon();
        lease.SetPhase(QueryExecutionPhase.Executing);
        CompleteLease(lease, clock);
        clock.Advance(TimeSpan.FromMinutes(1));

        Assert.Equal(0, registry.SweepLongRunningQueries());
        BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> active =
            registry.GetActiveSnapshot(10);
        BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
            registry.GetRecentSnapshot(10);
        QueryDiagnosticsSummary summary = registry.GetSummary();
        Assert.Empty(active.Records);
        Assert.Equal(0, active.DroppedCount);
        Assert.Empty(recent.Records);
        Assert.Equal(0, recent.DroppedCount);
        Assert.Equal(0, summary.RequestCount);
        Assert.Equal(0, summary.StatementExecutionCount);
        Assert.Equal(0, summary.SucceededCount);
        Assert.Equal(0, summary.ActiveCount);
        Assert.False(lease.TryAdopt(registry, context));
    }

    [Fact]
    public void CapacityRejectedLease_CanBeAbandonedWithoutChangingDropAccounting()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 1, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 1),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation registered =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));
        QueryRuntimeDiagnostics.QueryRuntimeOperation rejected =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));

        rejected.Abandon();
        rejected.Abandon();
        CompleteLease(rejected, clock);

        BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> active =
            registry.GetActiveSnapshot(10);
        Assert.Single(active.Records);
        Assert.Equal(1, active.DroppedCount);
        Assert.Empty(registry.GetRecentSnapshot(10).Records);
        QueryDiagnosticsSummary summary = registry.GetSummary();
        Assert.Equal(1, summary.ActiveCount);
        Assert.Equal(0, summary.RequestCount);
        Assert.Equal(0, summary.SucceededCount);

        registered.Abandon();
        Assert.Empty(registry.GetActiveSnapshot(10).Records);
        Assert.Equal(1, registry.GetActiveSnapshot(10).DroppedCount);
    }

    [Fact]
    public async Task AbandonDuringInFlightSweep_PreventsMarkingAndPublication()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new BlockingTimeProvider(
            new DateTimeOffset(2026, 8, 11, 2, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));
        clock.Advance(threshold);
        clock.BlockElapsedCalculation();

        Task<int> sweep = Task.Run(registry.SweepLongRunningQueries, Ct);
        await clock.ElapsedCalculationStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        lease.Abandon();
        clock.ReleaseElapsedCalculation();

        Assert.Equal(0, await sweep);
        Assert.Empty(registry.GetActiveSnapshot(10).Records);
        Assert.Empty(registry.GetRecentSnapshot(10).Records);
        Assert.Equal(0, registry.GetSummary().RequestCount);
    }

    [Fact]
    public void AbandonAndCompletionRace_HasOneCoherentTerminalDispositionPerLease()
    {
        const int operationCount = 128;
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 3, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                activeCapacity: operationCount,
                recentCapacity: operationCount),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation[] leases =
            Enumerable.Range(0, operationCount)
                .Select(_ => Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                    registry.TryStart(CreateContext(clock), QueryExecutionPhase.Queued)))
                .ToArray();

        Parallel.ForEach(
            leases,
            lease => Parallel.Invoke(
                lease.Abandon,
                () => CompleteLease(lease, clock)));

        BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
            registry.GetRecentSnapshot(operationCount);
        QueryDiagnosticsSummary summary = registry.GetSummary();
        Assert.Empty(registry.GetActiveSnapshot(operationCount).Records);
        Assert.Equal(0, registry.GetActiveSnapshot(operationCount).DroppedCount);
        Assert.Equal(0, recent.DroppedCount);
        Assert.Equal(
            recent.Records.Count,
            recent.Records.Select(static record => record.OperationId).Distinct().Count());
        Assert.Equal(recent.Records.Count, summary.RequestCount);
        Assert.Equal(recent.Records.Count, summary.StatementExecutionCount);
        Assert.Equal(recent.Records.Count, summary.SucceededCount);
        Assert.Equal(0, summary.ActiveCount);
    }

    [Fact]
    public void PublicCollections_ShareOneCaptureMetadataAndSupportEmptyResults()
    {
        var clock = new CountingTimeProvider(
            new DateTimeOffset(2026, 8, 11, 4, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);

        clock.ResetCallCounts();
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> empty =
            registry.GetActiveCollectionSnapshot(10);
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<ActiveQuerySnapshot>>(
            empty.Records));
        Assert.Equal(16, empty.Capacity);
        Assert.Null(empty.Retention);
        Assert.Equal(0, empty.DroppedCount);
        Assert.False(empty.IsTruncated);
        Assert.Equal(1, clock.TimestampCallCount);
        Assert.Equal(1, clock.UtcNowCallCount);

        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Planning));
        clock.Advance(TimeSpan.FromSeconds(2));
        clock.ResetCallCounts();

        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> active =
            registry.GetActiveCollectionSnapshot(10);
        ActiveQuerySnapshot record = Assert.Single(active.Records!);
        Assert.Equal(active.Metadata, record.Metadata);
        Assert.Equal(TimeSpan.FromSeconds(2), record.Elapsed);
        Assert.Equal(1, clock.TimestampCallCount);
        Assert.Equal(1, clock.UtcNowCallCount);
        lease.Abandon();
    }

    [Fact]
    public void ActiveElapsed_RemainsMonotonicWhenUtcClockRegresses()
    {
        var clock = new RegressingUtcTimeProvider(
            new DateTimeOffset(2026, 8, 11, 4, 30, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Executing));

        clock.AdvanceTimestamp(TimeSpan.FromSeconds(7));
        clock.MoveUtc(TimeSpan.FromHours(-1));

        ActiveQuerySnapshot record = Assert.Single(
            registry.GetActiveCollectionSnapshot(10).Records!);
        Assert.Equal(TimeSpan.FromSeconds(7), record.Elapsed);
        Assert.True(record.Metadata.CapturedAtUtc < record.StartedAtUtc);
        lease.Abandon();
    }

    [Fact]
    public void CumulativeCountersAndDropAccounting_SaturateAtInt64Maximum()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 5, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 1, recentCapacity: 1),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        registry.SetCumulativeCountersForTesting(
            long.MaxValue - 1,
            long.MaxValue - 1,
            long.MaxValue - 1,
            long.MaxValue,
            long.MaxValue,
            long.MaxValue - 1,
            long.MaxValue - 1,
            long.MaxValue - 1,
            long.MaxValue - 1,
            long.MaxValue - 1);

        QueryRuntimeDiagnostics.QueryRuntimeOperation first =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Executing));
        QueryRuntimeDiagnostics.QueryRuntimeOperation rejected =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(CreateContext(clock), QueryExecutionPhase.Executing));
        _ = registry.TryStart(CreateContext(clock), QueryExecutionPhase.Executing);
        first.Complete(
            CSharpDbOperationOutcome.Succeeded,
            clock.GetUtcNow(),
            TimeSpan.Zero,
            timeToFirstResult: null,
            rowsProduced: 10,
            rowsAffected: 10,
            error: null,
            isSlow: true);
        rejected.Complete(
            CSharpDbOperationOutcome.Succeeded,
            clock.GetUtcNow(),
            TimeSpan.Zero,
            timeToFirstResult: null,
            rowsProduced: 10,
            rowsAffected: 10,
            error: null,
            isSlow: true);

        QueryDiagnosticsSummary summary = registry.GetSummary();
        Assert.Equal(long.MaxValue, summary.RequestCount);
        Assert.Equal(long.MaxValue, summary.StatementExecutionCount);
        Assert.Equal(long.MaxValue, summary.SucceededCount);
        Assert.Equal(long.MaxValue, summary.FailedCount);
        Assert.Equal(long.MaxValue, summary.CanceledCount);
        Assert.Equal(long.MaxValue, summary.SlowCount);
        Assert.Equal(long.MaxValue, summary.RowsProduced);
        Assert.Equal(long.MaxValue, summary.RowsAffected);
        Assert.Equal(long.MaxValue, registry.GetActiveSnapshot(10).DroppedCount);
        Assert.Equal(long.MaxValue, registry.GetRecentSnapshot(10).DroppedCount);
    }

    [Fact]
    public void WaitingLease_RestoresOnlyItsExactPriorGeneration()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 6, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(CreateOptions(), clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        CSharpDbOperationContext context = CreateContext(clock);
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(context, QueryExecutionPhase.Executing));

        using (CSharpDbOperationScope.Enter(context, lease))
        {
            IDisposable waiting = Assert.IsAssignableFrom<IDisposable>(
                registry.EnterCurrentWaiting());
            Assert.Equal(
                QueryExecutionPhase.Waiting,
                Assert.Single(registry.GetActiveSnapshot(10).Records).Phase);
            waiting.Dispose();
            Assert.Equal(
                QueryExecutionPhase.Executing,
                Assert.Single(registry.GetActiveSnapshot(10).Records).Phase);

            IDisposable stale = Assert.IsAssignableFrom<IDisposable>(
                registry.EnterCurrentWaiting());
            lease.SetPhase(QueryExecutionPhase.Disposing);
            stale.Dispose();
            Assert.Equal(
                QueryExecutionPhase.Disposing,
                Assert.Single(registry.GetActiveSnapshot(10).Records).Phase);
        }

        lease.Abandon();
    }

    [Fact]
    public void Rebind_AfterSourcePublication_DoesNotPublishLogicalOperationTwice()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 7, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        using var firstState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics second = QueryRuntimeDiagnostics.GetOrCreate(
            secondState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation source =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                first.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));
        clock.Advance(threshold);

        Assert.Equal(1, first.SweepLongRunningQueries());
        QueryRuntimeDiagnostics.QueryRuntimeOperation rebound =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                source.RebindTo(second, QueryExecutionPhase.Planning));
        Assert.Equal(1, second.SweepLongRunningQueries());

        Assert.Single(received.OfType<CSharpDbLongRunningQueryEvent>());
        CompleteLease(rebound, clock);
        Assert.Empty(first.GetRecentSnapshot(10).Records);
        Assert.Single(second.GetRecentSnapshot(10).Records);
    }

    [Fact]
    public void Rebind_AfterSourceMarkAndDispose_AllowsSuccessorPublication()
    {
        TimeSpan threshold = TimeSpan.FromSeconds(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 8, 0, 0, TimeSpan.Zero));
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.LongRunningQuery.Name);
        var firstState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                loggingEnabled: true,
                slowQueries: true,
                longRunningThreshold: threshold),
            clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics second = QueryRuntimeDiagnostics.GetOrCreate(
            secondState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics.QueryRuntimeOperation source =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                first.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));
        QueryRuntimeDiagnostics.QueryRuntimeOperation? rebound = null;
        clock.Advance(threshold);

        Assert.Equal(1, first.SweepLongRunningQueries(() =>
        {
            rebound = source.RebindTo(second, QueryExecutionPhase.Planning);
            firstState.Dispose();
        }));
        Assert.Empty(received);
        Assert.NotNull(rebound);
        Assert.Equal(1, second.SweepLongRunningQueries());
        Assert.Single(received.OfType<CSharpDbLongRunningQueryEvent>());
        CompleteLease(rebound!, clock);
    }

    [Fact]
    public void ConcurrentRebindAndComplete_LeavesNoActiveAndExactlyOneRecent()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 9, 0, 0, TimeSpan.Zero));
        using var firstState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        using var secondState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(activeCapacity: 256, recentCapacity: 256),
            clock);
        QueryRuntimeDiagnostics first = QueryRuntimeDiagnostics.GetOrCreate(
            firstState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics second = QueryRuntimeDiagnostics.GetOrCreate(
            secondState,
            startSweepTimer: false);

        for (int index = 0; index < 128; index++)
        {
            QueryRuntimeDiagnostics.QueryRuntimeOperation source =
                Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                    first.TryStart(CreateContext(clock), QueryExecutionPhase.Queued));
            QueryRuntimeDiagnostics.QueryRuntimeOperation? rebound = null;
            Parallel.Invoke(
                () => rebound = source.RebindTo(second, QueryExecutionPhase.Planning),
                () => CompleteLease(source, clock));
            if (rebound is not null)
                CompleteLease(rebound, clock);
        }

        Assert.Empty(first.GetActiveSnapshot(256).Records);
        Assert.Empty(second.GetActiveSnapshot(256).Records);
        int recentCount = first.GetRecentSnapshot(256).Records.Count +
                          second.GetRecentSnapshot(256).Records.Count;
        Assert.Equal(128, recentCount);
    }

    private static CSharpDbOperationContext CreateContext(TimeProvider clock)
        => CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Direct,
            "query-runtime-tests",
            timeProvider: clock);

    private static void CompleteLease(
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease,
        TimeProvider clock)
        => lease.Complete(
            CSharpDbOperationOutcome.Succeeded,
            clock.GetUtcNow(),
            TimeSpan.Zero,
            timeToFirstResult: null,
            rowsProduced: 0,
            rowsAffected: 0,
            error: null,
            isSlow: false);

    private static object GetLeanSlot(IQueryExecutionObservation observation)
        => GetInstanceField(observation, "_slot").GetValue(observation) ??
           throw new InvalidOperationException("The lean observation slot is unavailable.");

    private static object? GetExecutionFeatures(QueryResult result)
        => typeof(QueryResult).GetField(
               "_executionFeatures",
               BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(result);

    private static FieldInfo GetInstanceField(object target, string name)
        => target.GetType().GetField(
               name,
               BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) ??
           throw new InvalidOperationException(
               $"Field '{name}' was not found on '{target.GetType().FullName}'.");

    private static async Task WaitForInt64FlagAsync(
        object target,
        string fieldName,
        long flag)
    {
        FieldInfo field = GetInstanceField(target, fieldName);
        for (int attempt = 0; attempt < 5_000; attempt++)
        {
            if (((long)(field.GetValue(target) ?? 0L) & flag) != 0)
                return;

            await Task.Delay(1, Ct);
        }

        throw new TimeoutException(
            $"Field '{fieldName}' did not publish flag 0x{flag:x}.");
    }

    private static CSharpDbObservabilityOptions CreateOptions(
        int activeCapacity = 16,
        int recentCapacity = 16,
        TimeSpan? retention = null,
        bool loggingEnabled = false,
        bool queries = false,
        bool slowQueries = false,
        TimeSpan? slowThreshold = null,
        TimeSpan? longRunningThreshold = null)
        => new()
        {
            Enabled = true,
            DatabaseAlias = "query-runtime-tests",
            LongRunningQueryThreshold = longRunningThreshold ?? TimeSpan.FromSeconds(5),
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = loggingEnabled,
                Queries = queries,
                SlowQueries = slowQueries,
                SlowQueryThreshold = slowThreshold ?? TimeSpan.FromSeconds(4),
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = activeCapacity,
                RecentQueryCapacity = recentCapacity,
                RecentOperationCapacity = 8,
                Retention = retention ?? TimeSpan.FromMinutes(1),
            },
        };

    private sealed class BlockingDisposeOperator : IOperator
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource DisposeStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public ColumnDefinition[] OutputSchema { get; } = [];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => throw new InvalidOperationException("No current row.");
        public ValueTask OpenAsync(CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default) => ValueTask.FromResult(false);

        public async ValueTask DisposeAsync()
        {
            DisposeStarted.TrySetResult();
            await _release.Task;
        }

        internal void ReleaseDispose() => _release.TrySetResult();
    }

    private sealed class BlockingOpenOperator : IOperator
    {
        private readonly TaskCompletionSource _releaseOpen = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource OpenStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public ColumnDefinition[] OutputSchema { get; } = [];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => throw new InvalidOperationException("No current row.");

        public async ValueTask OpenAsync(CancellationToken ct = default)
        {
            OpenStarted.TrySetResult();
            await _releaseOpen.Task.WaitAsync(ct);
        }

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
            => ValueTask.FromResult(false);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        internal void ReleaseOpen() => _releaseOpen.TrySetResult();
    }

    private sealed class CountingDisposeSingleRowOperator : IOperator
    {
        private static readonly DbValue[] Row = [DbValue.FromInteger(1)];
        private int _moved;
        private int _disposeCount;

        internal int DisposeCount => Volatile.Read(ref _disposeCount);

        public ColumnDefinition[] OutputSchema { get; } = [];
        public bool ReusesCurrentRowBuffer => false;
        public DbValue[] Current => Row;
        public ValueTask OpenAsync(CancellationToken ct = default)
            => ValueTask.CompletedTask;

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
            => ValueTask.FromResult(Interlocked.Exchange(ref _moved, 1) == 0);

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CountingScope : IDisposable
    {
        internal CountingScope(Action onEnter) => onEnter();

        public void Dispose()
        {
        }
    }

    private sealed class CountingResultObserver : IQueryResultObserver
    {
        private int _firstRows;
        private int _terminals;

        internal int FirstRows => Volatile.Read(ref _firstRows);
        internal int Terminals => Volatile.Read(ref _terminals);

        public void OnFirstRowProduced()
            => Interlocked.Increment(ref _firstRows);

        public void OnCompleted(QueryResultCompletion completion)
            => Interlocked.Increment(ref _terminals);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(_baseUtcTicks + Interlocked.Read(ref _timestamp), TimeSpan.Zero);

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);
    }

    private sealed class SwitchableFailureTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private long _timestamp;
        private int _failNextUtcNow;
        private int _elapsedCalculationCalls;
        private int _failElapsedCalculationCall;

        public override long TimestampFrequency
        {
            get
            {
                int call = Interlocked.Increment(ref _elapsedCalculationCalls);
                if (call == Volatile.Read(ref _failElapsedCalculationCall))
                    throw new InvalidOperationException("Injected elapsed-time failure.");

                return TimeSpan.TicksPerSecond;
            }
        }

        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Exchange(ref _failNextUtcNow, 0) != 0)
                throw new InvalidOperationException("Injected UTC clock failure.");

            return new DateTimeOffset(
                _baseUtcTicks + Interlocked.Read(ref _timestamp),
                TimeSpan.Zero);
        }

        public override long GetTimestamp()
            => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);

        internal void FailNextUtcNow()
            => Volatile.Write(ref _failNextUtcNow, 1);

        internal void FailElapsedCalculation(int callNumber)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(callNumber);
            Volatile.Write(ref _elapsedCalculationCalls, 0);
            Volatile.Write(ref _failElapsedCalculationCall, callNumber);
        }
    }

    private sealed class MinimumTimestampTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private long _elapsedTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(
                _baseUtcTicks + Interlocked.Read(ref _elapsedTicks),
                TimeSpan.Zero);

        public override long GetTimestamp()
            => unchecked(long.MinValue + Interlocked.Read(ref _elapsedTicks));

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _elapsedTicks, duration.Ticks);
    }

    private sealed class ReentrantFirstRowTimeProvider(DateTimeOffset utcNow)
        : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private int _timestampCallCount;
        private long _timestamp;

        internal Action? OnFirstRowTimestamp { get; set; }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(
                _baseUtcTicks + Interlocked.Read(ref _timestamp),
                TimeSpan.Zero);

        public override long GetTimestamp()
        {
            int call = Interlocked.Increment(ref _timestampCallCount);
            long timestamp = call switch
            {
                1 => 0,
                2 => TimeSpan.FromSeconds(5).Ticks,
                _ => TimeSpan.FromSeconds(10).Ticks,
            };
            if (call == 2)
                OnFirstRowTimestamp?.Invoke();

            Interlocked.Exchange(ref _timestamp, timestamp);
            return timestamp;
        }
    }

    private sealed class ThrowingFirstRowTimestampTimeProvider(DateTimeOffset utcNow)
        : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private int _timestampCallCount;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(
                _baseUtcTicks + Interlocked.Read(ref _timestamp),
                TimeSpan.Zero);

        public override long GetTimestamp()
        {
            int call = Interlocked.Increment(ref _timestampCallCount);
            if (call == 2)
                throw new InvalidOperationException("Injected first-row clock failure.");

            long timestamp = call == 1
                ? 0
                : TimeSpan.FromSeconds(10).Ticks;
            Interlocked.Exchange(ref _timestamp, timestamp);
            return timestamp;
        }
    }

    private sealed class BlockingFirstRowTimestampTimeProvider(DateTimeOffset utcNow)
        : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private readonly TaskCompletionSource _releaseFirstRowTimestamp = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private int _timestampCallCount;
        private long _lastTimestamp;

        internal TaskCompletionSource FirstRowTimestampStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(
                _baseUtcTicks + Interlocked.Read(ref _lastTimestamp),
                TimeSpan.Zero);

        public override long GetTimestamp()
        {
            int call = Interlocked.Increment(ref _timestampCallCount);
            long timestamp;
            if (call == 1)
            {
                timestamp = 0;
            }
            else if (call == 2)
            {
                FirstRowTimestampStarted.TrySetResult();
                _releaseFirstRowTimestamp.Task.GetAwaiter().GetResult();
                timestamp = TimeSpan.FromSeconds(5).Ticks;
            }
            else
            {
                timestamp = TimeSpan.FromSeconds(10).Ticks;
            }

            Interlocked.Exchange(ref _lastTimestamp, timestamp);
            return timestamp;
        }

        internal void ReleaseFirstRowTimestamp()
            => _releaseFirstRowTimestamp.TrySetResult();
    }

    private sealed class CountingTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private long _timestamp;
        private int _timestampCallCount;
        private int _utcNowCallCount;

        internal int TimestampCallCount => Volatile.Read(ref _timestampCallCount);
        internal int UtcNowCallCount => Volatile.Read(ref _utcNowCallCount);
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
        {
            Interlocked.Increment(ref _utcNowCallCount);
            return new DateTimeOffset(
                _baseUtcTicks + Interlocked.Read(ref _timestamp),
                TimeSpan.Zero);
        }

        public override long GetTimestamp()
        {
            Interlocked.Increment(ref _timestampCallCount);
            return Interlocked.Read(ref _timestamp);
        }

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);

        internal void ResetCallCounts()
        {
            Volatile.Write(ref _timestampCallCount, 0);
            Volatile.Write(ref _utcNowCallCount, 0);
        }
    }

    private sealed class RegressingUtcTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _utcTicks = utcNow.UtcTicks;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void AdvanceTimestamp(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);

        internal void MoveUtc(TimeSpan duration)
            => Interlocked.Add(ref _utcTicks, duration.Ticks);
    }

    private sealed class BlockingTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private long _timestamp;
        private int _blockElapsed;

        internal TaskCompletionSource ElapsedCalculationStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public override long TimestampFrequency
        {
            get
            {
                if (Volatile.Read(ref _blockElapsed) != 0)
                {
                    ElapsedCalculationStarted.TrySetResult();
                    _release.Task.GetAwaiter().GetResult();
                }

                return TimeSpan.TicksPerSecond;
            }
        }

        public override DateTimeOffset GetUtcNow()
            => new(_baseUtcTicks + Interlocked.Read(ref _timestamp), TimeSpan.Zero);

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);

        internal void BlockElapsedCalculation()
            => Volatile.Write(ref _blockElapsed, 1);

        internal void ReleaseElapsedCalculation()
        {
            Volatile.Write(ref _blockElapsed, 0);
            _release.TrySetResult();
        }
    }

    private sealed class CapturingTimerTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        private long _timestamp;
        private int _blockElapsed;
        private int _createTimerCount;

        internal ManualTimer Timer { get; private set; } = null!;
        internal int CreateTimerCount => Volatile.Read(ref _createTimerCount);
        internal TaskCompletionSource ElapsedCalculationStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public override long TimestampFrequency
        {
            get
            {
                if (Volatile.Read(ref _blockElapsed) != 0)
                {
                    ElapsedCalculationStarted.TrySetResult();
                    _release.Task.GetAwaiter().GetResult();
                }

                return TimeSpan.TicksPerSecond;
            }
        }
        public override DateTimeOffset GetUtcNow()
            => new(_baseUtcTicks + Interlocked.Read(ref _timestamp), TimeSpan.Zero);
        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            Interlocked.Increment(ref _createTimerCount);
            Timer = new ManualTimer(
                callback,
                state,
                ExecutionContext.Capture());
            return Timer;
        }

        internal void Advance(TimeSpan duration)
            => Interlocked.Add(ref _timestamp, duration.Ticks);

        internal void BlockElapsedCalculation()
            => Volatile.Write(ref _blockElapsed, 1);

        internal void ReleaseElapsedCalculation()
        {
            Volatile.Write(ref _blockElapsed, 0);
            _release.TrySetResult();
        }
    }

    private sealed class ManualTimer(
        TimerCallback callback,
        object? state,
        ExecutionContext? capturedExecutionContext) : ITimer
    {
        private int _disposed;

        internal ExecutionContext? CapturedExecutionContext { get; } = capturedExecutionContext;
        internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        public bool Change(TimeSpan dueTime, TimeSpan period)
            => !IsDisposed;

        public void Dispose()
            => Interlocked.Exchange(ref _disposed, 1);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        internal void Fire()
        {
            if (IsDisposed)
                return;

            if (CapturedExecutionContext is null)
            {
                callback(state);
                return;
            }

            ExecutionContext.Run(
                CapturedExecutionContext.CreateCopy(),
                static callbackState =>
                {
                    var invocation = ((TimerCallback Callback, object? State))callbackState!;
                    invocation.Callback(invocation.State);
                },
                (callback, state));
        }
    }

    private sealed class EventObserver(ConcurrentQueue<object> received)
        : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not null)
                received.Enqueue(value.Value);
        }
    }

    private sealed class ReentrantThrowingObserver(
        ConcurrentQueue<object> received,
        Action completeOperation) : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not null)
                received.Enqueue(value.Value);
            if (value.Value is not CSharpDbLongRunningQueryEvent)
                return;

            completeOperation();
            throw new InvalidOperationException("listener failures must stay diagnostic-only");
        }
    }
}

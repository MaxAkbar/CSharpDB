using System.Reflection;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class ClientRuntimeDiagnosticsCapabilityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DisabledDirectCapability_IsStableExplicitAndDoesNotOpenDatabase()
    {
        int openCount = 0;
        await using var client = new EngineTransportClient(
            ":memory:disabled-capability",
            (_, _, _) =>
            {
                Interlocked.Increment(ref openCount);
                return Task.FromException<Database>(
                    new InvalidOperationException("diagnostics must not open the database"));
            },
            new DatabaseOptions());
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> first =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> second =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
            await diagnostics.GetActiveQueriesAsync(10, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await diagnostics.GetRecentQueriesAsync(10, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> sessions =
            await diagnostics.GetSessionsAsync(10, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> plan =
            await diagnostics.GetQueryPlanDiagnosticsAsync(OpaqueDiagnosticsId.Create(), Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await diagnostics.GetQueryDetailAsync(OpaqueDiagnosticsId.Create(), Ct);

        Assert.Equal(0, Volatile.Read(ref openCount));
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Metadata.Availability);
        Assert.Equal(first.Metadata.ServerInstanceId, second.Metadata.ServerInstanceId);
        Assert.Equal(first.Metadata.CounterEpoch, second.Metadata.CounterEpoch);
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Aggregate.Queries.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Aggregate.Connections.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, active.Metadata.Availability);
        Assert.Null(active.Aggregate.Records);
        Assert.Null(active.Aggregate.Capacity);
        Assert.Equal(DiagnosticsAvailability.Disabled, recent.Metadata.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, sessions.Metadata.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, plan.Metadata.Availability);
        Assert.Null(plan.Aggregate.Value);
        Assert.Equal(DiagnosticsAvailability.Disabled, detail.Metadata.Availability);
        Assert.Null(detail.Aggregate.Value);
        Assert.All(
            new[]
            {
                active.Metadata.ServerInstanceId,
                recent.Metadata.ServerInstanceId,
                sessions.Metadata.ServerInstanceId,
                plan.Metadata.ServerInstanceId,
                detail.Metadata.ServerInstanceId,
            },
            id => Assert.Equal(first.Metadata.ServerInstanceId, id));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => diagnostics.GetActiveQueriesAsync(0, Ct));
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(canceled.Token));
    }

    [Fact]
    public async Task EnabledDirectCapability_ProjectsSummaryHistoryPlanSessionsAndNoQueryDetail()
    {
        await using var client = CreateClient(CreateOptions("direct-capability"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        SqlExecutionResult result = await client.ExecuteSqlAsync("SELECT 1", Ct);
        Assert.Null(result.Error);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await diagnostics.GetRecentQueriesAsync(10, Ct);
        RecentQuerySnapshot query = Assert.Single(recent.Aggregate.Records!);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> plan =
            await diagnostics.GetQueryPlanDiagnosticsAsync(query.OperationId, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await diagnostics.GetQueryDetailAsync(query.OperationId, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> sessions =
            await diagnostics.GetSessionsAsync(10, Ct);

        Assert.Equal(DiagnosticsScope.Instance, runtime.Metadata.Scope);
        Assert.Equal(DiagnosticsAvailability.Available, runtime.Metadata.Availability);
        QueryDiagnosticsSummary summary = Assert.IsType<QueryDiagnosticsSummary>(
            runtime.Aggregate.Queries.Value);
        Assert.True(summary.RequestCount >= 1);
        Assert.Equal(runtime.Metadata, summary.Metadata);
        Assert.Equal(runtime.Metadata, runtime.Aggregate.Connections.Value!.Metadata);
        Assert.Equal(DiagnosticsScope.Instance, recent.Metadata.Scope);
        Assert.Equal(recent.Metadata, query.Metadata);
        Assert.Equal(CSharpDB.Observability.CSharpDbTransport.Direct, query.Transport);
        Assert.NotNull(query.SessionId);
        Assert.Equal(DiagnosticsAvailability.Available, plan.Metadata.Availability);
        Assert.Equal(query.OperationId, plan.Aggregate.Value!.OperationId);
        Assert.Equal(plan.Metadata, plan.Aggregate.Value.Metadata);
        Assert.Equal(DiagnosticsAvailability.Unavailable, detail.Metadata.Availability);
        Assert.Null(detail.Aggregate.Value);
        SessionDiagnosticsSnapshot directSession = Assert.Single(sessions.Aggregate.Records!);
        Assert.Equal(query.SessionId, directSession.SessionId);
        Assert.False(directSession.HasActiveTransaction);
        Assert.Equal(DiagnosticsSessionState.Idle, directSession.State);

        var typeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(recent.GetType())!;
        string json = JsonSerializer.Serialize(recent, typeInfo);
        Assert.DoesNotContain("SELECT 1", json, StringComparison.Ordinal);
        Assert.DoesNotContain("capturedSqlText", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ExplicitRawCapture_IsAvailableOnlyThroughTheSeparateDetailCapability()
    {
        const string sql = "SELECT 'separately-authorized-client-detail'";
        DatabaseOptions options = CreateOptions("direct-detail-capability");
        options.ObservabilityOptions!.Logging.SqlText = SqlTextCaptureMode.Raw;
        await using var client = CreateClient(options);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        Assert.Null((await client.ExecuteSqlAsync(sql, Ct)).Error);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await diagnostics.GetRecentQueriesAsync(10, Ct);
        RecentQuerySnapshot query = Assert.Single(
            recent.Aggregate.Records!,
            record => record.Fingerprint ==
                      SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct));
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await diagnostics.GetQueryDetailAsync(query.OperationId, Ct);

        Assert.Equal(DiagnosticsScope.Instance, detail.Metadata.Scope);
        Assert.Equal(DiagnosticsAvailability.Available, detail.Metadata.Availability);
        QueryDetailSnapshot value = Assert.IsType<QueryDetailSnapshot>(
            detail.Aggregate.Value);
        Assert.Equal(detail.Metadata, value.Metadata);
        Assert.Equal(query.OperationId, value.OperationId);
        Assert.Equal(query.Fingerprint, value.Fingerprint);
        Assert.Equal(SqlTextCaptureMode.Raw, value.CaptureMode);
        Assert.Equal(sql, value.CapturedSqlText);
        var detailTypeInfo = CSharpDbObservabilityJsonContext.Default.GetTypeInfo(
            detail.GetType())!;
        string detailJson = JsonSerializer.Serialize(detail, detailTypeInfo);
        var detailRoundTrip = Assert.IsType<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(
            JsonSerializer.Deserialize(detailJson, detailTypeInfo));
        Assert.Equal(
            sql,
            detailRoundTrip.Aggregate.Value!.CapturedSqlText);
        Assert.DoesNotContain(
            sql,
            JsonSerializer.Serialize(
                recent,
                CSharpDbObservabilityJsonContext.Default.GetTypeInfo(recent.GetType())!),
            StringComparison.Ordinal);

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> missing =
            await diagnostics.GetQueryDetailAsync(OpaqueDiagnosticsId.Create(), Ct);
        Assert.Equal(DiagnosticsAvailability.Unavailable, missing.Metadata.Availability);
        Assert.Null(missing.Aggregate.Value);
    }

    [Fact]
    public async Task RetainedTransactionFamily_RemainsVisibleAlongsideReplacementFamily()
    {
        const string retainedDetailSql =
            "SELECT 1 AS retained_family_detail, 2 AS retained_family_second_value";
        DatabaseOptions options = CreateOptions("family-overlap");
        options.ObservabilityOptions!.Logging.SqlText = SqlTextCaptureMode.Raw;
        var client = CreateClient(options);
        string? transactionId = null;
        ForwardOnlyQueryCursor? cursor = null;
        try
        {
            var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
            transactionId = transaction.TransactionId;
            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                retainedDetailSql,
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 10", Ct)).Error);

            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);
            cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT 3",
                    Ct));

            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
                await diagnostics.GetActiveQueriesAsync(20, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
                await diagnostics.GetRecentQueriesAsync(20, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> sessions =
                await diagnostics.GetSessionsAsync(20, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
                cappedSessions = await diagnostics.GetSessionsAsync(1, Ct);
            RecentQuerySnapshot retainedFamilyQuery = Assert.Single(
                recent.Aggregate.Records!,
                record => record.Fingerprint ==
                          SqlQueryFingerprintProvider.Instance.CreateFingerprint(retainedDetailSql, Ct));
            DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
                retainedDetail = await diagnostics.GetQueryDetailAsync(
                    retainedFamilyQuery.OperationId,
                    Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, active.Metadata.Scope);
            Assert.Equal(DiagnosticsScope.Aggregate, recent.Metadata.Scope);
            Assert.Equal(DiagnosticsScope.Aggregate, retainedDetail.Metadata.Scope);
            Assert.Equal(DiagnosticsAvailability.Available, retainedDetail.Metadata.Availability);
            Assert.Equal(retainedDetailSql, retainedDetail.Aggregate.Value!.CapturedSqlText);
            Assert.Equal(2, retainedDetail.RuntimeFamilies!.Count);
            Assert.Single(
                retainedDetail.RuntimeFamilies,
                static family =>
                    family.Value.Metadata.Availability == DiagnosticsAvailability.Available &&
                    family.Value.Value is not null);
            Assert.Single(
                retainedDetail.RuntimeFamilies,
                static family =>
                    family.Value.Metadata.Availability == DiagnosticsAvailability.Unavailable &&
                    family.Value.Value is null);
            Assert.Equal(2, active.RuntimeFamilies!.Count);
            Assert.Equal(2, recent.RuntimeFamilies!.Count);
            Assert.All(active.Aggregate.Records!, record =>
                Assert.Equal(active.Metadata, record.Metadata));
            Assert.All(recent.Aggregate.Records!, record =>
                Assert.Equal(recent.Metadata, record.Metadata));
            Assert.Equal(
                [0L, 1L],
                active.RuntimeFamilies
                    .Select(static family => family.Value.Metadata.CounterEpoch)
                    .Order()
                    .ToArray());
            Assert.All(active.RuntimeFamilies, static family =>
                Assert.All(family.Value.Records!, record =>
                    Assert.Equal(family.Value.Metadata, record.Metadata)));
            ActiveQuerySnapshot activeTransaction = Assert.Single(active.Aggregate.Records!);
            SessionDiagnosticsSnapshot transactionSession = Assert.Single(
                sessions.Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Equal(activeTransaction.OperationId, transactionSession.CurrentOperationId);
            Assert.Equal(activeTransaction.SessionId, transactionSession.SessionId);
            Assert.True(transactionSession.HasActiveReader);
            Assert.Equal(DiagnosticsSessionState.SnapshotReader, transactionSession.State);
            SessionDiagnosticsSnapshot cappedTransaction = Assert.Single(
                cappedSessions.Aggregate.Records!);
            Assert.True(cappedTransaction.HasActiveTransaction);
            Assert.True(cappedTransaction.HasActiveReader);
            Assert.Equal(transactionSession.SessionId, cappedTransaction.SessionId);
            Assert.True(cappedSessions.Aggregate.IsTruncated);
            Assert.DoesNotContain(
                transaction.TransactionId,
                JsonSerializer.Serialize(
                    sessions,
                    CSharpDbObservabilityJsonContext.Default.GetTypeInfo(sessions.GetType())!),
                StringComparison.Ordinal);

            await cursor.DisposeAsync();
            cursor = null;
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
            transactionId = null;
        }
        finally
        {
            if (cursor is not null)
                await cursor.DisposeAsync();
            if (transactionId is not null)
            {
                try
                {
                    await client.RollbackTransactionAsync(transactionId, CancellationToken.None);
                }
                catch
                {
                }
            }

            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task TransactionReaders_AreOwnedByTheirSessionAndNotDoubleCounted()
    {
        await using var client = CreateClient(CreateOptions("reader-ownership"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo first = await client.BeginTransactionAsync(Ct);
        TransactionSessionInfo second = await client.BeginTransactionAsync(Ct);
        ForwardOnlyQueryCursor? cursor = null;
        try
        {
            cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync(
                    first.TransactionId,
                    "SELECT 1",
                    Ct));

            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
                sessions = await diagnostics.GetSessionsAsync(10, Ct);
            SessionDiagnosticsSnapshot[] transactions = sessions.Aggregate.Records!
                .Where(static record => record.HasActiveTransaction)
                .ToArray();
            Assert.Equal(2, transactions.Length);
            SessionDiagnosticsSnapshot reader = Assert.Single(
                transactions,
                static record => record.HasActiveReader);
            SessionDiagnosticsSnapshot nonReader = Assert.Single(
                transactions,
                static record => !record.HasActiveReader);
            Assert.Equal(DiagnosticsSessionState.SnapshotReader, reader.State);
            Assert.NotNull(reader.CurrentOperationId);
            Assert.Equal(DiagnosticsSessionState.Transaction, nonReader.State);
            Assert.Null(nonReader.CurrentOperationId);

            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
                capped = await diagnostics.GetSessionsAsync(1, Ct);
            SessionDiagnosticsSnapshot selected = Assert.Single(capped.Aggregate.Records!);
            Assert.Equal(reader.SessionId, selected.SessionId);
            Assert.True(capped.Aggregate.IsTruncated);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(1, runtime.Aggregate.Connections.Value!.ActiveReaders);
            Assert.Equal(2, runtime.Aggregate.Connections.Value.ActiveTransactions);
        }
        finally
        {
            if (cursor is not null)
                await cursor.DisposeAsync();
            await client.RollbackTransactionAsync(first.TransactionId, Ct);
            await client.RollbackTransactionAsync(second.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task AggregateCollections_PreserveFamilyDropsAndCallerTruncation()
    {
        DatabaseOptions options = CreateOptions("aggregate-drop-accounting");
        options.ObservabilityOptions!.History.RecentQueryCapacity = 4;
        var openedStates = new List<CSharpDbRuntimeDiagnosticsState>();
        await using var client = new EngineTransportClient(
            $":memory:aggregate-drop-{Guid.NewGuid():N}",
            async (_, runtimeOptions, ct) =>
            {
                lock (openedStates)
                    openedStates.Add(runtimeOptions.RuntimeDiagnosticsState!);
                return await Database.OpenInMemoryAsync(runtimeOptions, ct);
            },
            options);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        ForwardOnlyQueryCursor? transactionCursor = null;
        ForwardOnlyQueryCursor? directCursor = null;
        try
        {
            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 1",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);
            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 3", Ct)).Error);

            CSharpDbRuntimeDiagnosticsState[] states;
            lock (openedStates)
            {
                states = openedStates
                    .Distinct()
                    .ToArray();
            }

            Assert.Equal(2, states.Length);
            QueryRuntimeDiagnostics.GetOrCreate(states[0]).SetCumulativeCountersForTesting(
                requestCount: 1,
                statementExecutionCount: 1,
                succeededCount: 1,
                failedCount: 0,
                canceledCount: 0,
                slowCount: 0,
                rowsProduced: 1,
                rowsAffected: 0,
                activeRejectedCount: long.MaxValue,
                recentDroppedCount: 2);
            QueryRuntimeDiagnostics.GetOrCreate(states[1]).SetCumulativeCountersForTesting(
                requestCount: 1,
                statementExecutionCount: 1,
                succeededCount: 1,
                failedCount: 0,
                canceledCount: 0,
                slowCount: 0,
                rowsProduced: 1,
                rowsAffected: 0,
                activeRejectedCount: 1,
                recentDroppedCount: 3);

            transactionCursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT 4",
                    Ct));
            directCursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync("SELECT 5", Ct));

            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
                await diagnostics.GetActiveQueriesAsync(1, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
                await diagnostics.GetRecentQueriesAsync(1, Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, active.Metadata.Scope);
            Assert.Equal(2, active.RuntimeFamilies!.Count);
            Assert.Single(active.Aggregate.Records!);
            Assert.Equal(long.MaxValue, active.Aggregate.DroppedCount);
            Assert.True(active.Aggregate.IsTruncated);
            Assert.True(active.Metadata.RecordsTruncated);
            Assert.Equal(
                [1L, long.MaxValue],
                active.RuntimeFamilies
                    .Select(static family => family.Value.DroppedCount!.Value)
                    .Order()
                    .ToArray());

            Assert.Equal(DiagnosticsScope.Aggregate, recent.Metadata.Scope);
            Assert.Equal(2, recent.RuntimeFamilies!.Count);
            Assert.Single(recent.Aggregate.Records!);
            Assert.Equal(5, recent.Aggregate.DroppedCount);
            Assert.True(recent.Aggregate.IsTruncated);
            Assert.True(recent.Metadata.RecordsTruncated);
            Assert.Equal(
                recent.RuntimeFamilies.Sum(
                    static family => family.Value.DroppedCount!.Value),
                recent.Aggregate.DroppedCount);
        }
        finally
        {
            if (directCursor is not null)
                await directCursor.DisposeAsync();
            if (transactionCursor is not null)
                await transactionCursor.DisposeAsync();
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task MixedFamilyEnablement_PreservesExactEnabledDataWithoutStartingDisabledRegistry()
    {
        await using var client = CreateClient(CreateOptions("mixed-enablement"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        CSharpDbObservabilityOptions mutableOptions =
            client.DirectDatabaseOptions.ObservabilityOptions!;
        string? transactionId = null;
        ForwardOnlyQueryCursor? cursor = null;
        try
        {
            TransactionSessionInfo enabledTransaction = await client.BeginTransactionAsync(Ct);
            transactionId = enabledTransaction.TransactionId;
            Assert.Null((await client.ExecuteInTransactionAsync(
                transactionId,
                "SELECT 1",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);

            mutableOptions.Enabled = false;
            await client.ReleaseCachedDatabaseAsync(Ct);
            CSharpDbRuntimeDiagnosticsState disabledState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.False(disabledState.IsEnabled);
            Assert.Null(GetRuntimeComponents(disabledState));

            cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync(
                    transactionId,
                    "SELECT 4",
                    Ct));
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
                pendingReplacementActive = await diagnostics.GetActiveQueriesAsync(10, Ct);
            Assert.Equal(DiagnosticsScope.Aggregate, pendingReplacementActive.Metadata.Scope);
            RuntimeDiagnosticsFamilySection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
                establishedFamily = Assert.Single(pendingReplacementActive.RuntimeFamilies!);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                establishedFamily.Value.Metadata.Availability);
            Assert.Single(establishedFamily.Value.Records!);
            Assert.Null(GetRuntimeComponents(disabledState));

            Assert.Null((await client.ExecuteSqlAsync("SELECT 3", Ct)).Error);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> disabledCurrentRuntime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
                disabledCurrentActive = await diagnostics.GetActiveQueriesAsync(10, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
                disabledCurrentRecent = await diagnostics.GetRecentQueriesAsync(10, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
                disabledCurrentSessions = await diagnostics.GetSessionsAsync(10, Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, disabledCurrentRuntime.Metadata.Scope);
            Assert.Equal(DiagnosticsAvailability.Available, disabledCurrentRuntime.Metadata.Availability);
            Assert.Equal(
                [DiagnosticsAvailability.Available, DiagnosticsAvailability.Disabled],
                disabledCurrentRuntime.RuntimeFamilies!
                    .Select(static family => family.Value.Metadata.Availability)
                    .Order()
                    .ToArray());
            Assert.Equal(DiagnosticsScope.Aggregate, disabledCurrentActive.Metadata.Scope);
            Assert.Single(disabledCurrentActive.Aggregate.Records!);
            Assert.Contains(
                disabledCurrentActive.RuntimeFamilies!,
                static family =>
                    family.Value.Metadata.Availability == DiagnosticsAvailability.Disabled &&
                    family.Value.Records is null);
            Assert.NotEmpty(disabledCurrentRecent.Aggregate.Records!);
            Assert.Equal(DiagnosticsScope.Instance, disabledCurrentSessions.Metadata.Scope);
            Assert.Equal(
                DiagnosticsAvailability.Disabled,
                disabledCurrentSessions.Metadata.Availability);
            Assert.Null(disabledCurrentSessions.Aggregate.Records);
            Assert.Null(GetRuntimeComponents(disabledState));

            await cursor.DisposeAsync();
            cursor = null;
            await client.RollbackTransactionAsync(transactionId, Ct);
            transactionId = null;

            TransactionSessionInfo disabledTransaction = await client.BeginTransactionAsync(Ct);
            transactionId = disabledTransaction.TransactionId;
            Assert.Same(
                disabledState,
                client.CurrentRuntimeDiagnosticsState);
            Assert.Null((await client.ExecuteInTransactionAsync(
                transactionId,
                "SELECT 5",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 6", Ct)).Error);

            mutableOptions.Enabled = true;
            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 7", Ct)).Error);
            CSharpDbRuntimeDiagnosticsState reenabledState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.True(reenabledState.IsEnabled);
            Assert.NotSame(disabledState, reenabledState);

            cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync("SELECT 8", Ct));
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> enabledCurrentRuntime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
                enabledCurrentActive = await diagnostics.GetActiveQueriesAsync(10, Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
                enabledCurrentSessions = await diagnostics.GetSessionsAsync(10, Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, enabledCurrentRuntime.Metadata.Scope);
            Assert.Equal(
                [DiagnosticsAvailability.Available, DiagnosticsAvailability.Disabled],
                enabledCurrentRuntime.RuntimeFamilies!
                    .Select(static family => family.Value.Metadata.Availability)
                    .Order()
                    .ToArray());
            Assert.Single(enabledCurrentActive.Aggregate.Records!);
            Assert.Contains(
                enabledCurrentActive.RuntimeFamilies!,
                static family =>
                    family.Value.Metadata.Availability == DiagnosticsAvailability.Disabled &&
                    family.Value.Records is null);
            Assert.Equal(
                DiagnosticsAvailability.Available,
                enabledCurrentSessions.Metadata.Availability);
            Assert.Contains(
                enabledCurrentSessions.Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Null(GetRuntimeComponents(disabledState));
        }
        finally
        {
            if (cursor is not null)
                await cursor.DisposeAsync();
            if (transactionId is not null)
                await client.RollbackTransactionAsync(transactionId, Ct);
        }
    }

    [Fact]
    public async Task SuppliedDisabledRuntimeState_RetainsIdentityWithoutCreatingComponents()
    {
        var observability = new CSharpDbObservabilityOptions
        {
            Enabled = false,
            DatabaseAlias = "supplied-disabled-state",
        };
        using var state = new CSharpDbRuntimeDiagnosticsState(observability);
        await using Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = state,
            },
            Ct);

        Assert.Same(state, database.RuntimeDiagnosticsState);
        Assert.False(database.IsObservabilityEnabled);
        Assert.Null(database.GetQueryDiagnosticsSummary());
        Assert.Null(GetRuntimeComponents(state));

        await using CSharpDB.Execution.QueryResult result =
            await database.ExecuteAsync("SELECT 1", Ct);
        Assert.Single(await result.ToListAsync(Ct));

        Assert.Null(database.GetQueryDiagnosticsSummary());
        Assert.Null(GetRuntimeComponents(state));
    }

    [Fact]
    public async Task ConnectionSummary_RemainsInternallyCoherentDuringTransactionChurn()
    {
        await using var client = CreateClient(CreateOptions("connection-coherence"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        Task churn = Task.Run(
            async () =>
            {
                for (int index = 0; index < 40; index++)
                {
                    TransactionSessionInfo transaction =
                        await client.BeginTransactionAsync(Ct);
                    await Task.Yield();
                    await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
                }
            },
            Ct);

        do
        {
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            AssertConnectionSummaryCoherent(runtime.Aggregate.Connections.Value!);
            await Task.Yield();
        }
        while (!churn.IsCompleted);

        await churn;
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> final =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        AssertConnectionSummaryCoherent(final.Aggregate.Connections.Value!);
    }

    [Fact]
    public async Task TransactionSession_BecomesAbandonedOnlyAfterConfiguredIdleThreshold()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions("abandoned-session");
        options.ObservabilityOptions!.SessionAbandonmentThreshold = TimeSpan.FromMinutes(5);
        await using var client = CreateClient(options, clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        try
        {
            SessionDiagnosticsSnapshot initial = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, initial.State);

            clock.Advance(TimeSpan.FromMinutes(5));
            SessionDiagnosticsSnapshot abandoned = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Equal(initial.SessionId, abandoned.SessionId);
            Assert.Equal(initial.CreatedAtUtc, abandoned.CreatedAtUtc);
            Assert.Equal(DiagnosticsSessionState.Abandoned, abandoned.State);

            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 1",
                Ct)).Error);
            SessionDiagnosticsSnapshot activeAgain = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, activeAgain.State);
            Assert.Equal(clock.GetUtcNow(), activeAgain.LastActiveAtUtc);
        }
        finally
        {
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task TransactionSession_ActiveNonReaderGateCannotBeAbandoned()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions("active-non-reader-session");
        options.ObservabilityOptions!.SessionAbandonmentThreshold = TimeSpan.FromMinutes(5);
        await using var client = CreateClient(options, clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        object session = GetTransactionSession(client, transaction.TransactionId);
        bool gateEntered = false;
        Task<TransactionTableSnapshot?>? queuedRead = null;
        try
        {
            await EnterTransactionOperationAsync(session);
            gateEntered = true;
            queuedRead = client.ReadTableSnapshotAsync(
                transaction.TransactionId,
                "missing_table",
                Ct).AsTask();
            Assert.False(queuedRead.IsCompleted);

            clock.Advance(TimeSpan.FromMinutes(5));
            SessionDiagnosticsSnapshot active = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, active.State);
            Assert.Null(active.CurrentOperationId);
            Assert.False(active.HasActiveReader);

            ExitTransactionOperation(session);
            gateEntered = false;
            Assert.Null(await queuedRead);
            queuedRead = null;

            SessionDiagnosticsSnapshot completed = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, completed.State);
            Assert.Equal(clock.GetUtcNow(), completed.LastActiveAtUtc);

            clock.Advance(TimeSpan.FromMinutes(5));
            SessionDiagnosticsSnapshot abandoned = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Abandoned, abandoned.State);
        }
        finally
        {
            if (gateEntered)
                ExitTransactionOperation(session);
            if (queuedRead is not null)
                await queuedRead;
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task TransactionSession_ClearRefreshesActivityBeforePublishingIdleOperation()
    {
        var clock = new BlockingManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions("operation-completion-ordering");
        options.ObservabilityOptions!.SessionAbandonmentThreshold = TimeSpan.FromMinutes(5);
        await using var client = CreateClient(options, clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        object session = GetTransactionSession(client, transaction.TransactionId);
        OpaqueDiagnosticsId operationId = OpaqueDiagnosticsId.Create();
        bool gateEntered = false;
        Task? clearOperation = null;
        try
        {
            await EnterTransactionOperationAsync(session);
            gateEntered = true;
            InvokeTransactionSessionMethod(
                session,
                "SetCurrentDiagnosticsOperation",
                operationId);
            clock.Advance(TimeSpan.FromMinutes(5));
            clock.BlockNextUtcNow();
            clearOperation = Task.Run(
                () => InvokeTransactionSessionMethod(
                    session,
                    "ClearCurrentDiagnosticsOperation",
                    operationId),
                Ct);

            await clock.WaitUntilUtcNowBlockedAsync().WaitAsync(Ct);
            try
            {
                SessionDiagnosticsSnapshot clearing = Assert.Single(
                    (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                    static record => record.HasActiveTransaction);
                Assert.Equal(operationId, clearing.CurrentOperationId);
                Assert.Equal(DiagnosticsSessionState.Transaction, clearing.State);
            }
            finally
            {
                clock.ReleaseBlockedUtcNow();
            }

            await clearOperation;
            clearOperation = null;
            SessionDiagnosticsSnapshot cleared = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Null(cleared.CurrentOperationId);
            Assert.Equal(DiagnosticsSessionState.Transaction, cleared.State);
            Assert.Equal(clock.GetUtcNow(), cleared.LastActiveAtUtc);

            ExitTransactionOperation(session);
            gateEntered = false;
            SessionDiagnosticsSnapshot completed = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, completed.State);
            Assert.Equal(clock.GetUtcNow(), completed.LastActiveAtUtc);
        }
        finally
        {
            clock.ReleaseBlockedUtcNow();
            if (clearOperation is not null)
                await clearOperation;
            if (gateEntered)
                ExitTransactionOperation(session);
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task TransactionSession_FinalizingSnapshotIsNeverClassifiedAbandoned()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions("finalizing-session");
        options.ObservabilityOptions!.SessionAbandonmentThreshold = TimeSpan.FromMinutes(5);
        await using var client = CreateClient(options, clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        object session = GetTransactionSession(client, transaction.TransactionId);
        bool finalizationClaimed = false;
        try
        {
            clock.Advance(TimeSpan.FromMinutes(5));
            finalizationClaimed = Assert.IsType<bool>(
                InvokeTransactionSessionMethod(session, "TryClaimFinalization"));
            Assert.True(finalizationClaimed);

            SessionDiagnosticsSnapshot finalizing = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            Assert.Equal(DiagnosticsSessionState.Transaction, finalizing.State);
        }
        finally
        {
            if (finalizationClaimed)
                InvokeTransactionSessionMethod(session, "CancelFinalizationClaim");
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task TransactionDurations_UseMonotonicClockAcrossWallClockCorrections()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions("monotonic-session-duration");
        options.ObservabilityOptions!.SessionAbandonmentThreshold = TimeSpan.FromMinutes(5);
        await using var client = CreateClient(options, clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        try
        {
            clock.AdvanceUtc(TimeSpan.FromHours(1));
            clock.AdvanceTimestamp(TimeSpan.FromMinutes(1));
            SessionDiagnosticsSnapshot forwardJump = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> forwardRuntime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(DiagnosticsSessionState.Transaction, forwardJump.State);
            Assert.Equal(
                TimeSpan.FromMinutes(1),
                forwardRuntime.Aggregate.Connections.Value!.OldestTransactionAge);

            clock.AdvanceUtc(TimeSpan.FromHours(-2));
            clock.AdvanceTimestamp(TimeSpan.FromMinutes(4));
            SessionDiagnosticsSnapshot backwardJump = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static record => record.HasActiveTransaction);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> backwardRuntime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(DiagnosticsSessionState.Abandoned, backwardJump.State);
            Assert.Equal(
                TimeSpan.FromMinutes(5),
                backwardRuntime.Aggregate.Connections.Value!.OldestTransactionAge);
            Assert.True(backwardJump.LastActiveAtUtc > backwardJump.Metadata.CapturedAtUtc);
        }
        finally
        {
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    [Fact]
    public async Task ThrowingDiagnosticsClock_CannotBreakNormalClientWork()
    {
        DatabaseOptions options = CreateOptions("throwing-clock");
        await using var client = CreateClient(options, new ThrowingUtcTimeProvider());

        SqlExecutionResult result = await client.ExecuteSqlAsync("SELECT 1", Ct);
        Assert.Null(result.Error);

        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(DiagnosticsAvailability.Available, runtime.Metadata.Availability);
        Assert.Equal(TimeSpan.Zero, runtime.Metadata.CapturedAtUtc.Offset);
    }

    [Fact]
    public async Task RegressingDiagnosticsClock_CannotInvertOrRegressSessionTimestamps()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        await using var client = CreateClient(CreateOptions("regressing-clock"), clock);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
        clock.SetUtcNow(new DateTimeOffset(2026, 8, 10, 12, 10, 0, TimeSpan.Zero));
        Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);
        SessionDiagnosticsSnapshot directBeforeRegression = Assert.Single(
            (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
            static session => !session.HasActiveTransaction);

        clock.SetUtcNow(new DateTimeOffset(2026, 8, 10, 11, 0, 0, TimeSpan.Zero));
        Assert.Null((await client.ExecuteSqlAsync("SELECT 3", Ct)).Error);
        SessionDiagnosticsSnapshot directAfterRegression = Assert.Single(
            (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
            static session => !session.HasActiveTransaction);
        Assert.Equal(
            directBeforeRegression.LastActiveAtUtc,
            directAfterRegression.LastActiveAtUtc);
        Assert.True(
            directAfterRegression.LastActiveAtUtc >= directAfterRegression.CreatedAtUtc);

        clock.SetUtcNow(new DateTimeOffset(2026, 8, 10, 13, 0, 0, TimeSpan.Zero));
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        try
        {
            clock.SetUtcNow(new DateTimeOffset(2026, 8, 10, 13, 10, 0, TimeSpan.Zero));
            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 4",
                Ct)).Error);
            SessionDiagnosticsSnapshot transactionBeforeRegression = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static session => session.HasActiveTransaction);

            clock.SetUtcNow(new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 5",
                Ct)).Error);
            SessionDiagnosticsSnapshot transactionAfterRegression = Assert.Single(
                (await diagnostics.GetSessionsAsync(10, Ct)).Aggregate.Records!,
                static session => session.HasActiveTransaction);
            Assert.Equal(
                transactionBeforeRegression.LastActiveAtUtc,
                transactionAfterRegression.LastActiveAtUtc);
            Assert.True(
                transactionAfterRegression.LastActiveAtUtc >=
                transactionAfterRegression.CreatedAtUtc);
        }
        finally
        {
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
    }

    private static EngineTransportClient CreateClient(
        DatabaseOptions options,
        TimeProvider? timeProvider = null)
        => new(
            $":memory:capability-{Guid.NewGuid():N}",
            static async (_, runtimeOptions, ct) =>
                await Database.OpenInMemoryAsync(runtimeOptions, ct),
            options,
            observabilityTimeProvider: timeProvider);

    private static object? GetRuntimeComponents(CSharpDbRuntimeDiagnosticsState state)
        => typeof(CSharpDbRuntimeDiagnosticsState)
            .GetField("_components", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(state);

    private static object GetTransactionSession(
        EngineTransportClient client,
        string transactionId)
    {
        object transactions = typeof(EngineTransportClient)
            .GetField("_transactions", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(client)!;
        return transactions.GetType()
            .GetProperty("Item", BindingFlags.Instance | BindingFlags.Public)!
            .GetValue(transactions, [transactionId])!;
    }

    private static async Task EnterTransactionOperationAsync(object session)
    {
        object pending = InvokeTransactionSessionMethod(
            session,
            "TryEnterOperationAsync",
            Ct)!;
        var entered = Assert.IsType<Task<bool>>(
            pending.GetType()
                .GetMethod("AsTask", BindingFlags.Instance | BindingFlags.Public)!
                .Invoke(pending, null));
        Assert.True(await entered);
    }

    private static void ExitTransactionOperation(object session)
        => InvokeTransactionSessionMethod(session, "ExitOperation");

    private static object? InvokeTransactionSessionMethod(
        object session,
        string methodName,
        params object?[] arguments)
        => session.GetType()
            .GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public)!
            .Invoke(session, arguments);

    private static void AssertConnectionSummaryCoherent(
        ConnectionDiagnosticsSnapshot connections)
    {
        int transactions = Assert.IsType<int>(connections.ActiveTransactions);
        Assert.Equal(transactions + 1, connections.ActiveLogicalSessions);
        Assert.Equal(transactions == 1, connections.TransactionOwnerSessionId is not null);
        Assert.Equal(transactions == 0, connections.OldestTransactionAge is null);
    }

    private static DatabaseOptions CreateOptions(string alias)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = alias,
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = false,
                    Queries = false,
                    SlowQueries = false,
                },
                History = new CSharpDbHistoryOptions
                {
                    ActiveQueryCapacity = 32,
                    RecentQueryCapacity = 32,
                    RecentOperationCapacity = 16,
                    Retention = TimeSpan.FromMinutes(10),
                },
            },
        };

    private class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override DateTimeOffset GetUtcNow() => _utcNow;
        public override long GetTimestamp() => _timestamp;

        internal void Advance(TimeSpan elapsed)
        {
            AdvanceUtc(elapsed);
            AdvanceTimestamp(elapsed);
        }

        internal void AdvanceUtc(TimeSpan elapsed) => _utcNow += elapsed;

        internal void AdvanceTimestamp(TimeSpan elapsed) => _timestamp += elapsed.Ticks;

        internal void SetUtcNow(DateTimeOffset utcNow)
            => _utcNow = utcNow;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
            => new InertTimer();
    }

    private sealed class BlockingManualTimeProvider(DateTimeOffset utcNow)
        : ManualTimeProvider(utcNow)
    {
        private readonly ManualResetEventSlim _utcNowRelease = new(initialState: true);
        private TaskCompletionSource? _utcNowBlocked;
        private int _blockNextUtcNow;

        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Exchange(ref _blockNextUtcNow, 0) != 0)
            {
                Volatile.Read(ref _utcNowBlocked)?.TrySetResult();
                _utcNowRelease.Wait();
            }

            return base.GetUtcNow();
        }

        internal void BlockNextUtcNow()
        {
            _utcNowRelease.Reset();
            Volatile.Write(
                ref _utcNowBlocked,
                new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            Volatile.Write(ref _blockNextUtcNow, 1);
        }

        internal Task WaitUntilUtcNowBlockedAsync()
            => Volatile.Read(ref _utcNowBlocked)?.Task ??
               throw new InvalidOperationException("No UTC clock read is scheduled to block.");

        internal void ReleaseBlockedUtcNow() => _utcNowRelease.Set();
    }

    private sealed class ThrowingUtcTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow()
            => throw new InvalidOperationException("diagnostics clock failure");
    }

    private sealed class InertTimer : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;
        public void Dispose()
        {
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

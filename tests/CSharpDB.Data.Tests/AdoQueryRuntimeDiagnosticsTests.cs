using System.Data.Common;
using System.Diagnostics;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class AdoQueryRuntimeDiagnosticsTests : IAsyncLifetime
{
    private readonly List<string> _paths = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = null;
        AdoCommandObservation.QueueWaitStartingForTest = null;
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        AdoCommandObservation.QueueWaitStartingForTest = null;
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = null;
        await CSharpDbConnection.ClearAllPoolsAsync();
        foreach (string path in _paths)
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task DirectStreaming_UsesOneOperationForLedgerAndSessionUntilExhaustionOrDisposal()
    {
        DatabaseOptions options = CreateOptions("ado_direct_streaming");
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            connection,
            "CREATE TABLE runtime_rows (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            connection,
            "INSERT INTO runtime_rows VALUES (1), (2)");

        DirectDatabaseSession session = Assert.IsType<DirectDatabaseSession>(
            connection.GetSession());
        object state = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(session));
        QueryFingerprint fingerprint =
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(
                "SELECT id FROM runtime_rows ORDER BY id",
                Ct);

        OpaqueDiagnosticsId earlyDisposedId;
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = "SELECT id FROM runtime_rows ORDER BY id";
            DbDataReader reader = await command.ExecuteReaderAsync(Ct);
            ActiveQuerySnapshot active = Assert.Single(
                ActiveRecords(state),
                record => record.Fingerprint == fingerprint);
            earlyDisposedId = active.OperationId;
            Assert.Equal(QueryExecutionPhase.Streaming, active.Phase);

            DataConnectionDiagnosticsRawSnapshot raw = Assert.IsType<
                DataConnectionDiagnosticsRawSnapshot>(
                await session.CaptureRuntimeDiagnosticsAsync(4, Ct));
            DataSessionDiagnosticsRawSnapshot tracked = Assert.Single(raw.Sessions);
            Assert.Equal(active.OperationId, tracked.CurrentOperationId);
            Assert.Equal(active.SessionId, tracked.SessionId);

            Assert.True(await reader.ReadAsync(Ct));
            await reader.DisposeAsync();
            await reader.DisposeAsync();
        }

        RecentQuerySnapshot earlyDisposed = Assert.Single(
            RecentRecords(state),
            record => record.OperationId == earlyDisposedId);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, earlyDisposed.Outcome);
        Assert.Equal(1, earlyDisposed.RowsProduced);
        Assert.DoesNotContain(
            ActiveRecords(state),
            record => record.OperationId == earlyDisposedId);

        OpaqueDiagnosticsId exhaustedId;
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = "SELECT id FROM runtime_rows ORDER BY id";
            await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
            exhaustedId = Assert.Single(
                ActiveRecords(state),
                record => record.Fingerprint == fingerprint).OperationId;

            int rows = 0;
            while (await reader.ReadAsync(Ct))
                rows++;
            Assert.Equal(2, rows);
        }

        RecentQuerySnapshot exhausted = Assert.Single(
            RecentRecords(state),
            record => record.OperationId == exhaustedId);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, exhausted.Outcome);
        Assert.Equal(2, exhausted.RowsProduced);
        Assert.Empty(ActiveRecords(state));
    }

    [Fact]
    public async Task ListenerFreeBindAndClassificationFailures_CompleteOneRecentRecord()
    {
        DatabaseOptions directOptions = CreateOptions("ado_bind_failure");
        await using (var direct = new CSharpDbConnection(
                         "Data Source=:memory:;Pooling=false",
                         directOptions))
        {
            await direct.OpenAsync(Ct);
            object state = Assert.IsAssignableFrom<object>(
                AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                    direct.GetSession()));
            const string sql = "SELECT @missing AS value";
            QueryFingerprint fingerprint =
                SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct);

            await using CSharpDbCommand command =
                (CSharpDbCommand)direct.CreateCommand();
            command.CommandText = sql;
            command.Prepare();
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => command.ExecuteScalarAsync(Ct));

            RecentQuerySnapshot failed = Assert.Single(
                RecentRecords(state),
                record => record.Fingerprint == fingerprint);
            Assert.Equal(CSharpDbOperationOutcome.Failed, failed.Outcome);
            Assert.NotNull(failed.Error);
            Assert.DoesNotContain(
                ActiveRecords(state),
                record => record.OperationId == failed.OperationId);
        }

        string path = CreatePath();
        DatabaseOptions pooledOptions = CreateOptions("ado_classification_failure");
        await using var pooled = new CSharpDbConnection(
            $"Data Source={path};Pooling=true;Max Pool Size=1",
            pooledOptions);
        await pooled.OpenAsync(Ct);
        object pooledState = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                pooled.GetSession()));
        const string invalidSql = "SELEC classification_secret_27";
        QueryFingerprint invalidFingerprint =
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(invalidSql, Ct);

        await using CSharpDbCommand invalid =
            (CSharpDbCommand)pooled.CreateCommand();
        invalid.CommandText = invalidSql;
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => invalid.ExecuteScalarAsync(Ct));

        RecentQuerySnapshot classified = Assert.Single(
            RecentRecords(pooledState),
            record => record.Fingerprint == invalidFingerprint);
        Assert.Equal(CSharpDbOperationOutcome.Failed, classified.Outcome);
        Assert.DoesNotContain(
            ActiveRecords(pooledState),
            record => record.OperationId == classified.OperationId);
    }

    [Fact]
    public async Task PooledGateWait_IsQueuedAndCancellationCompletesTheSameOperationOnce()
    {
        using var blockerEntered = new ManualResetEventSlim();
        using var releaseBlocker = new ManualResetEventSlim();
        using var queueWaitStarted = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions("ado_pool_queue")
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoRuntimeGate",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    blockerEntered.Set();
                    releaseBlocker.Wait();
                    return DbValue.FromInteger(1);
                }));
        string path = CreatePath();
        string connectionString =
            $"Data Source={path};Pooling=true;Max Pool Size=2";
        await using var blocker = new CSharpDbConnection(connectionString, options);
        await using var queued = new CSharpDbConnection(connectionString, options);
        await blocker.OpenAsync(Ct);
        await queued.OpenAsync(Ct);
        Assert.Same(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                blocker.GetSession()),
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                queued.GetSession()));
        await ExecuteNonQueryAsync(
            blocker,
            "CREATE TABLE queued_rows (id INTEGER PRIMARY KEY)");

        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blocker.CreateCommand();
        blockingCommand.CommandText =
            "INSERT INTO queued_rows VALUES (HoldAdoRuntimeGate())";
        Task<int> blockingTask = Task.Run(
            () => blockingCommand.ExecuteNonQueryAsync(Ct),
            Ct);

        try
        {
            Assert.True(await Task.Run(
                () => blockerEntered.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            AdoCommandObservation.QueueWaitStartingForTest = queueWaitStarted.Set;

            const string queuedSql = "SELECT COUNT(*) FROM queued_rows";
            QueryFingerprint queuedFingerprint =
                SqlQueryFingerprintProvider.Instance.CreateFingerprint(queuedSql, Ct);
            object state = Assert.IsAssignableFrom<object>(
                AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                    queued.GetSession()));
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queued.CreateCommand();
            queuedCommand.CommandText = queuedSql;
            Task<object?> queuedTask = queuedCommand.ExecuteScalarAsync(
                cancellation.Token);

            Assert.True(await Task.Run(
                () => queueWaitStarted.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            ActiveQuerySnapshot queuedActive = Assert.Single(
                ActiveRecords(state),
                record => record.Fingerprint == queuedFingerprint);
            Assert.Equal(QueryExecutionPhase.Queued, queuedActive.Phase);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queuedTask);
            RecentQuerySnapshot canceled = Assert.Single(
                RecentRecords(state),
                record => record.OperationId == queuedActive.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Canceled, canceled.Outcome);
            Assert.DoesNotContain(
                ActiveRecords(state),
                record => record.OperationId == queuedActive.OperationId);
        }
        finally
        {
            AdoCommandObservation.QueueWaitStartingForTest = null;
            releaseBlocker.Set();
            await blockingTask;
        }
    }

    [Fact]
    public void AdoStart_SnapshotsEventInterestAndUsesExactClockWithoutListeners()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 18, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions options = CreateObservability(
            "ado_interest_snapshot",
            loggingEnabled: true);
        options.Logging.SlowQueries = true;
        options.Logging.SlowQueryThreshold = TimeSpan.FromSeconds(1);
        using IDisposable state =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(
                options,
                clock);
        AdoCommandObservation observation = Assert.IsType<AdoCommandObservation>(
            AdoCommandObservation.TryStartForTest(
                options,
                state,
                "SELECT 7",
                OpaqueDiagnosticsId.Create()));
        OpaqueDiagnosticsId operationId = Assert.Single(ActiveRecords(state)).OperationId;

        clock.Advance(TimeSpan.FromSeconds(3));
        using var lateEvents = new QueryEventRecorder();
        observation.FailBeforeDispatch(new OperationCanceledException());
        observation.FailBeforeDispatch(new InvalidOperationException());
        observation.Dispose();

        RecentQuerySnapshot recent = Assert.Single(
            RecentRecords(state),
            record => record.OperationId == operationId);
        Assert.Equal(CSharpDbOperationOutcome.Canceled, recent.Outcome);
        Assert.Equal(TimeSpan.FromSeconds(3), recent.Duration);
        Assert.Equal(
            new DateTimeOffset(2026, 8, 10, 18, 0, 0, TimeSpan.Zero),
            recent.StartedAtUtc);
        Assert.Equal(
            new DateTimeOffset(2026, 8, 10, 18, 0, 3, TimeSpan.Zero),
            recent.CompletedAtUtc);
        Assert.Empty(lateEvents.Events);
    }

    [Fact]
    public async Task QueuedAdoHandoff_LateSubscriberBeginsWithNextOperation()
    {
        using var blockerEntered = new ManualResetEventSlim();
        using var releaseBlocker = new ManualResetEventSlim();
        using var queueWaitStarted = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions("ado_late_handoff")
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoLateHandoff",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    blockerEntered.Set();
                    releaseBlocker.Wait();
                    return DbValue.FromInteger(1);
                }));
        options.ObservabilityOptions!.Logging.Enabled = true;
        string connectionString =
            $"Data Source={CreatePath()};Pooling=true;Max Pool Size=2";
        await using var blocker = new CSharpDbConnection(connectionString, options);
        await using var queued = new CSharpDbConnection(connectionString, options);
        await blocker.OpenAsync(Ct);
        await queued.OpenAsync(Ct);

        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blocker.CreateCommand();
        blockingCommand.CommandText = "SELECT HoldAdoLateHandoff()";
        Task<object?> blockingTask = Task.Run(
            () => blockingCommand.ExecuteScalarAsync(Ct),
            Ct);

        try
        {
            Assert.True(await Task.Run(
                () => blockerEntered.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            AdoCommandObservation.QueueWaitStartingForTest = queueWaitStarted.Set;
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queued.CreateCommand();
            queuedCommand.CommandText = "SELECT 27";
            Task<object?> queuedTask = queuedCommand.ExecuteScalarAsync(Ct);
            Assert.True(await Task.Run(
                () => queueWaitStarted.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            AdoCommandObservation.QueueWaitStartingForTest = null;

            using var lateSubscriber = new QueryEventRecorder();
            releaseBlocker.Set();
            Assert.Equal(1L, Convert.ToInt64(await blockingTask));
            Assert.Equal(27L, Convert.ToInt64(await queuedTask));

            Assert.Empty(lateSubscriber.Events);
        }
        finally
        {
            AdoCommandObservation.QueueWaitStartingForTest = null;
            releaseBlocker.Set();
            await blockingTask;
        }
    }

    [Fact]
    public async Task QueuedAdoHandoff_StartTimeInterestSurvivesFilterUnsubscribe()
    {
        using var blockerEntered = new ManualResetEventSlim();
        using var releaseBlocker = new ManualResetEventSlim();
        using var queueWaitStarted = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions("ado_unsubscribe_handoff")
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoUnsubscribeHandoff",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    blockerEntered.Set();
                    releaseBlocker.Wait();
                    return DbValue.FromInteger(1);
                }));
        options.ObservabilityOptions!.Logging.Enabled = true;
        string connectionString =
            $"Data Source={CreatePath()};Pooling=true;Max Pool Size=2";
        await using var blocker = new CSharpDbConnection(connectionString, options);
        await using var queued = new CSharpDbConnection(connectionString, options);
        await blocker.OpenAsync(Ct);
        await queued.OpenAsync(Ct);

        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blocker.CreateCommand();
        blockingCommand.CommandText = "SELECT HoldAdoUnsubscribeHandoff()";
        Task<object?> blockingTask = Task.Run(
            () => blockingCommand.ExecuteScalarAsync(Ct),
            Ct);

        try
        {
            Assert.True(await Task.Run(
                () => blockerEntered.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            using var startTimeSubscriber = new SwitchableQueryEventRecorder();
            AdoCommandObservation.QueueWaitStartingForTest = queueWaitStarted.Set;
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queued.CreateCommand();
            queuedCommand.CommandText = "SELECT 41";
            Task<object?> queuedTask = queuedCommand.ExecuteScalarAsync(Ct);
            Assert.True(await Task.Run(
                () => queueWaitStarted.Wait(TimeSpan.FromSeconds(5)),
                Ct));
            AdoCommandObservation.QueueWaitStartingForTest = null;

            // Keep the observer registered so delivery can be asserted, but
            // make its DiagnosticListener interest predicate return false.
            // The operation must retain the decision made before admission.
            startTimeSubscriber.DisableInterest();
            releaseBlocker.Set();
            Assert.Equal(1L, Convert.ToInt64(await blockingTask));
            Assert.Equal(41L, Convert.ToInt64(await queuedTask));

            CSharpDbQueryCompletedEvent completed = Assert.Single(
                startTimeSubscriber.CompletedEvents);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, completed.Outcome);
        }
        finally
        {
            AdoCommandObservation.QueueWaitStartingForTest = null;
            releaseBlocker.Set();
            await blockingTask;
        }
    }

    [Fact]
    public async Task DirectStreamingHandoff_RetainsStartTimeInterestUntilReaderTerminal()
    {
        DatabaseOptions options = CreateOptions("ado_stream_interest");
        options.ObservabilityOptions!.Logging.Enabled = true;
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            connection,
            "CREATE TABLE interest_rows (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            connection,
            "INSERT INTO interest_rows VALUES (1), (2)");

        using var startTimeSubscriber = new SwitchableQueryEventRecorder();
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = "SELECT id FROM interest_rows ORDER BY id";
            await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
            startTimeSubscriber.DisableInterest();
            while (await reader.ReadAsync(Ct))
            {
            }
        }

        Assert.Single(startTimeSubscriber.CompletedEvents);

        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = "SELECT id FROM interest_rows ORDER BY id";
            await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
            using var lateSubscriber = new QueryEventRecorder();
            while (await reader.ReadAsync(Ct))
            {
            }

            Assert.Empty(lateSubscriber.Events);
        }
        Assert.Single(startTimeSubscriber.CompletedEvents);

        using var earlyDisposeSubscriber = new SwitchableQueryEventRecorder();
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = "SELECT id FROM interest_rows WHERE id = 1";
            await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);
            Assert.True(await reader.ReadAsync(Ct));
            earlyDisposeSubscriber.DisableInterest();
        }

        CSharpDbQueryCompletedEvent earlyDisposed = Assert.Single(
            earlyDisposeSubscriber.CompletedEvents);
        Assert.Equal(1, earlyDisposed.RowsProduced);
    }

    [Fact]
    public async Task ResolverOwnedState_FreezesMutableOptionsAndReResolvesAfterDirectClose()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 19, 0, 0, TimeSpan.Zero));
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = clock;
        DatabaseOptions options = CreateOptions("first_runtime_family");
        options.ObservabilityOptions!.Logging.Enabled = true;
        options.ObservabilityOptions.Logging.SlowQueries = true;
        options.ObservabilityOptions!.History.ActiveQueryCapacity = 4;
        var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);

        await connection.OpenAsync(Ct);
        object first = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                connection.GetSession()));
        AdoRuntimeDiagnosticsStateInfoForTest firstInfo =
            AdoCommandObservation.GetRuntimeDiagnosticsStateInfoForTest(first);
        Assert.Equal("first_runtime_family", firstInfo.DatabaseAlias);
        Assert.Equal(4, firstInfo.ActiveQueryCapacity);
        Assert.Equal(
            firstInfo.ServerInstanceId,
            AdoCommandObservation.GetRuntimeDiagnosticsStateInfoForTest(first)
                .ServerInstanceId);
        Assert.Single(clock.Timers);

        options.ObservabilityOptions.DatabaseAlias = "second_runtime_family";
        options.ObservabilityOptions.History.ActiveQueryCapacity = 9;
        Assert.Equal(
            "first_runtime_family",
            AdoCommandObservation.GetRuntimeDiagnosticsStateInfoForTest(first)
                .DatabaseAlias);
        Assert.Equal(
            4,
            AdoCommandObservation.GetRuntimeDiagnosticsStateInfoForTest(first)
                .ActiveQueryCapacity);

        await connection.CloseAsync();
        Assert.True(clock.Timers[0].IsDisposed);
        Assert.Throws<ObjectDisposedException>(
            () => AdoCommandObservation.CaptureActiveQueriesForTest(first));

        await connection.OpenAsync(Ct);
        object second = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                connection.GetSession()));
        AdoRuntimeDiagnosticsStateInfoForTest secondInfo =
            AdoCommandObservation.GetRuntimeDiagnosticsStateInfoForTest(second);
        // Direct close destroys the physical Database family, so reopening the
        // same ADO connection truthfully creates a new server identity. Pools
        // and named hosts instead retain their identity across logical closes.
        Assert.NotSame(first, second);
        Assert.NotEqual(firstInfo.ServerInstanceId, secondInfo.ServerInstanceId);
        Assert.Equal("second_runtime_family", secondInfo.DatabaseAlias);
        Assert.Equal(9, secondInfo.ActiveQueryCapacity);
        Assert.Equal(2, clock.Timers.Count);
        Assert.False(clock.Timers[1].IsDisposed);

        await connection.DisposeAsync();
        Assert.True(clock.Timers[1].IsDisposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SharedPhysicalFamily_OwnsOneStateUntilPoolOrHostRetirement(
        bool namedSharedMemory)
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 20, 0, 0, TimeSpan.Zero));
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = clock;
        string connectionString = namedSharedMemory
            ? $"Data Source=:memory:{Guid.NewGuid():N}"
            : $"Data Source={CreatePath()};Pooling=true;Max Pool Size=2";
        DatabaseOptions options = CreateOptions(
            namedSharedMemory ? "ado_shared_owner" : "ado_pool_owner");
        options.ObservabilityOptions!.Logging.Enabled = true;
        options.ObservabilityOptions.Logging.SlowQueries = true;
        var first = new CSharpDbConnection(connectionString, options);
        var second = new CSharpDbConnection(connectionString, options);

        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);
        object state = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                first.GetSession()));
        Assert.Same(
            state,
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                second.GetSession()));
        Assert.Single(clock.Timers);

        await first.DisposeAsync();
        Assert.False(clock.Timers[0].IsDisposed);
        await second.DisposeAsync();
        Assert.False(clock.Timers[0].IsDisposed);

        await CSharpDbConnection.ClearPoolAsync(connectionString);
        Assert.True(clock.Timers[0].IsDisposed);
    }

    [Fact]
    public async Task ExistingPool_RejectsAndDisposesUnadoptedResolverState()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 20, 30, 0, TimeSpan.Zero));
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = clock;
        DatabaseOptions options = CreateOptions("ado_unadopted_pool_state");
        options.ObservabilityOptions!.Logging.Enabled = true;
        options.ObservabilityOptions.Logging.SlowQueries = true;
        string dataSource = CreatePath();
        var builder = new CSharpDbConnectionStringBuilder(
            $"Data Source={dataSource};Pooling=true;Max Pool Size=2");
        PoolKey key = new(
            dataSource,
            2,
            CSharpDbEmbeddedOpenMode.Direct,
            EffectiveStoragePreset: null,
            EffectiveAdaptiveQueryReoptimization: false,
            ExplicitDirectDatabaseOptions: options,
            ExplicitHybridDatabaseOptions: null);

        ResolvedEmbeddedConfiguration adoptedConfiguration =
            CSharpDbEmbeddedConfigurationResolver.Resolve(
                builder,
                options,
                hybridDatabaseOptions: null);
        PooledDatabaseSession first =
            await CSharpDbConnectionPoolRegistry.OpenPooledSessionAsync(
                key,
                ct => Database.OpenInMemoryAsync(
                    adoptedConfiguration.RuntimeDirectDatabaseOptions,
                    ct),
                adoptedConfiguration.RuntimeDirectDatabaseOptions
                    .ObservabilityOptions,
                Ct,
                clock,
                adoptedConfiguration.RuntimeDiagnosticsStateOwner);
        Assert.Single(clock.Timers);
        Assert.False(clock.Timers[0].IsDisposed);

        ResolvedEmbeddedConfiguration rejectedConfiguration =
            CSharpDbEmbeddedConfigurationResolver.Resolve(
                builder,
                options,
                hybridDatabaseOptions: null);
        object rejectedState = Assert.IsAssignableFrom<object>(
            rejectedConfiguration.RuntimeDiagnosticsStateForTest);
        _ = ActiveRecords(rejectedState);
        Assert.Equal(2, clock.Timers.Count);
        Assert.False(clock.Timers[1].IsDisposed);
        int rejectedOpenCalls = 0;

        PooledDatabaseSession second =
            await CSharpDbConnectionPoolRegistry.OpenPooledSessionAsync(
                key,
                _ =>
                {
                    Interlocked.Increment(ref rejectedOpenCalls);
                    throw new InvalidOperationException(
                        "The existing pool must own physical open.");
                },
                rejectedConfiguration.RuntimeDirectDatabaseOptions
                    .ObservabilityOptions,
                Ct,
                clock,
                rejectedConfiguration.RuntimeDiagnosticsStateOwner);

        Assert.Equal(0, Volatile.Read(ref rejectedOpenCalls));
        Assert.True(clock.Timers[1].IsDisposed);
        Assert.Same(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(first),
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(second));
        Assert.False(clock.Timers[0].IsDisposed);

        await first.DisposeAsync();
        await second.DisposeAsync();
        await CSharpDbConnectionPoolRegistry.ClearPoolAsync(key);
        Assert.True(clock.Timers[0].IsDisposed);
    }

    [Fact]
    public async Task FailedDirectPhysicalOpen_DisposesUnadoptedResolverState()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 20, 40, 0, TimeSpan.Zero));
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = clock;
        DatabaseOptions options = CreateOptions("ado_failed_open_state");
        options.ObservabilityOptions!.Logging.Enabled = true;
        options.ObservabilityOptions.Logging.SlowQueries = true;
        string dataSource = CreatePath();
        var builder = new CSharpDbConnectionStringBuilder(
            $"Data Source={dataSource};Pooling=false");
        ResolvedEmbeddedConfiguration configuration =
            CSharpDbEmbeddedConfigurationResolver.Resolve(
                builder,
                options,
                hybridDatabaseOptions: null);
        object rejectedState = Assert.IsAssignableFrom<object>(
            configuration.RuntimeDiagnosticsStateForTest);
        _ = ActiveRecords(rejectedState);
        Assert.Single(clock.Timers);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await CSharpDbConnectionPoolRegistry.OpenDirectSessionAsync(
                dataSource,
                _ => throw new InvalidOperationException("physical-open-failed"),
                configuration.RuntimeDirectDatabaseOptions.ObservabilityOptions,
                Ct,
                clock,
                configuration.RuntimeDiagnosticsStateOwner));

        Assert.True(clock.Timers[0].IsDisposed);
        Assert.Throws<ObjectDisposedException>(
            () => AdoCommandObservation.CaptureActiveQueriesForTest(rejectedState));
    }

    [Fact]
    public async Task CallerSuppliedRuntimeState_IsNeverDisposedByDirectSession()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 20, 45, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability = CreateObservability(
            "ado_caller_owned_state",
            loggingEnabled: true);
        observability.Logging.SlowQueries = true;
        IDisposable callerState =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(
                observability,
                clock);
        try
        {
            DatabaseOptions options =
                DataObservabilityOptionsSnapshot.WithRuntimeDiagnosticsStateForTest(
                    new DatabaseOptions
                    {
                        ObservabilityOptions = observability,
                    },
                    callerState);
            var connection = new CSharpDbConnection(
                "Data Source=:memory:;Pooling=false",
                options);
            await connection.OpenAsync(Ct);
            Assert.Same(
                callerState,
                AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                    connection.GetSession()));
            Assert.Single(clock.Timers);

            await connection.DisposeAsync();
            Assert.False(clock.Timers[0].IsDisposed);
        }
        finally
        {
            callerState.Dispose();
        }

        Assert.True(clock.Timers[0].IsDisposed);
    }

    [Fact]
    public async Task DisabledResolverAndDirectSession_CreateNoRuntimeStateOrDiagnosticsSidecar()
    {
        var clock = new TrackingTimeProvider(
            new DateTimeOffset(2026, 8, 10, 21, 0, 0, TimeSpan.Zero));
        CSharpDbEmbeddedConfigurationResolver.RuntimeDiagnosticsTimeProviderForTest = clock;
        DatabaseOptions options = new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = false,
            },
        };
        var builder = new CSharpDbConnectionStringBuilder(
            "Data Source=:memory:;Pooling=false");
        ResolvedEmbeddedConfiguration resolved =
            CSharpDbEmbeddedConfigurationResolver.Resolve(
                builder,
                options,
                hybridDatabaseOptions: null);
        Assert.False(resolved.HasRuntimeDiagnosticsStateForTest);
        Assert.Null(resolved.RuntimeDiagnosticsStateOwner);
        Assert.Empty(clock.Timers);

        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);
        DirectDatabaseSession session = Assert.IsType<DirectDatabaseSession>(
            connection.GetSession());
        Assert.False(session.HasDiagnosticsSidecarForTest);
        Assert.Null(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(session));
        Assert.Empty(clock.Timers);

        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT 1";
        Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        Assert.False(session.HasDiagnosticsSidecarForTest);
        Assert.Empty(clock.Timers);
    }

    private string CreatePath()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_ado_runtime_{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static DatabaseOptions CreateOptions(string alias)
        => new()
        {
            ObservabilityOptions = CreateObservability(alias),
        };

    private static CSharpDbObservabilityOptions CreateObservability(
        string alias,
        bool loggingEnabled = false)
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = loggingEnabled,
                Queries = true,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 32,
                RecentQueryCapacity = 32,
                RecentOperationCapacity = 16,
                Retention = TimeSpan.FromMinutes(5),
            },
        };

    private static IReadOnlyList<ActiveQuerySnapshot> ActiveRecords(
        object state)
        => Assert.IsAssignableFrom<IReadOnlyList<ActiveQuerySnapshot>>(
            AdoCommandObservation.CaptureActiveQueriesForTest(state).Records);

    private static IReadOnlyList<RecentQuerySnapshot> RecentRecords(
        object state)
        => Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
            AdoCommandObservation.CaptureRecentQueriesForTest(state).Records);

    private static async Task<int> ExecuteNonQueryAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteNonQueryAsync(Ct);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class QueryEventRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private readonly List<object> _events = [];
        private readonly IDisposable _subscription;

        internal QueryEventRecorder()
        {
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name => name.StartsWith(
                    "CSharpDB.Query.",
                    StringComparison.Ordinal));
        }

        internal IReadOnlyList<object> Events => _events;

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not null)
                _events.Add(value.Value);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }

    private sealed class SwitchableQueryEventRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private readonly object _gate = new();
        private readonly List<CSharpDbQueryCompletedEvent> _completed = [];
        private readonly IDisposable _subscription;
        private int _interested = 1;

        internal SwitchableQueryEventRecorder()
        {
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                name => Volatile.Read(ref _interested) != 0 &&
                        name == CSharpDbLogEvents.QueryCompleted.Name);
        }

        internal IReadOnlyList<CSharpDbQueryCompletedEvent> CompletedEvents
        {
            get
            {
                lock (_gate)
                    return _completed.ToArray();
            }
        }

        internal void DisableInterest()
            => Volatile.Write(ref _interested, 0);

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbQueryCompletedEvent completed)
                return;

            lock (_gate)
                _completed.Add(completed);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }

    private sealed class TrackingTimeProvider(DateTimeOffset initialUtcNow) : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<TrackingTimer> _timers = [];
        private long _utcTicks = initialUtcNow.UtcTicks;
        private long _timestamp;

        internal IReadOnlyList<TrackingTimer> Timers
        {
            get
            {
                lock (_gate)
                    return _timers.ToArray();
            }
        }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public override long GetTimestamp()
            => Volatile.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
        {
            Interlocked.Add(ref _utcTicks, elapsed.Ticks);
            Interlocked.Add(ref _timestamp, elapsed.Ticks);
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            var timer = new TrackingTimer();
            lock (_gate)
                _timers.Add(timer);
            return timer;
        }
    }

    internal sealed class TrackingTimer : ITimer
    {
        private int _disposed;

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
    }
}

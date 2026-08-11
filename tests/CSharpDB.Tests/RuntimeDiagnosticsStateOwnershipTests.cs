using System.Collections.Concurrent;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class RuntimeDiagnosticsStateOwnershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CachedDatabaseReopen_RetainsIdentityAndAdvancesEpochAfterSuccess()
    {
        string databasePath = NewDatabasePath();
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = CreateObservability("primary") });

        try
        {
            Database firstDatabase = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary first = Assert.IsType<QueryDiagnosticsSummary>(
                firstDatabase.GetQueryDiagnosticsSummary());

            Assert.True(client.UsesCurrentRuntimeDiagnosticsState(firstDatabase));
            Assert.Equal(0, first.Metadata.CounterEpoch);
            Assert.Equal(0, client.RuntimeDiagnosticsCounterEpoch);

            await client.ReleaseCachedDatabaseAsync(Ct);

            // Disposing the old counter family only makes an epoch rollover
            // pending. No new family exists until a replacement opens.
            Assert.Equal(0, client.RuntimeDiagnosticsCounterEpoch);

            Database reopenedDatabase = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary reopened = Assert.IsType<QueryDiagnosticsSummary>(
                reopenedDatabase.GetQueryDiagnosticsSummary());

            Assert.True(client.UsesCurrentRuntimeDiagnosticsState(reopenedDatabase));
            Assert.Equal(first.Metadata.ServerInstanceId, reopened.Metadata.ServerInstanceId);
            Assert.Equal("primary", reopened.Metadata.DatabaseAlias);
            Assert.Equal(1, reopened.Metadata.CounterEpoch);
            Assert.Equal(1, client.RuntimeDiagnosticsCounterEpoch);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task FailedReopen_PreservesPendingEpochUntilNextSuccessfulReplacement()
    {
        string databasePath = NewDatabasePath();
        var factory = new FailNextOpenStorageEngineFactory();
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("retry"),
                StorageEngineFactory = factory,
            });

        try
        {
            Database firstDatabase = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary first = Assert.IsType<QueryDiagnosticsSummary>(
                firstDatabase.GetQueryDiagnosticsSummary());
            await client.ReleaseCachedDatabaseAsync(Ct);

            factory.FailNextOpen();
            await Assert.ThrowsAsync<InjectedOpenException>(
                () => client.TryGetDatabaseAsync(Ct).AsTask());

            Assert.Equal(0, client.RuntimeDiagnosticsCounterEpoch);

            Database recoveredDatabase = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary recovered = Assert.IsType<QueryDiagnosticsSummary>(
                recoveredDatabase.GetQueryDiagnosticsSummary());

            Assert.Equal(first.Metadata.ServerInstanceId, recovered.Metadata.ServerInstanceId);
            Assert.Equal(1, recovered.Metadata.CounterEpoch);
            Assert.Equal(1, client.RuntimeDiagnosticsCounterEpoch);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ClientHostReplacement_CanChangeAliasWithoutChangingInstanceIdentity()
    {
        string databasePath = NewDatabasePath();
        DatabaseOptions replacementOptions;
        QueryDiagnosticsSummary primary;

        await using (var primaryClient = new EngineTransportClient(
                         databasePath,
                         new DatabaseOptions
                         {
                             ObservabilityOptions = CreateObservability("primary"),
                         }))
        {
            Database primaryDatabase = Assert.IsType<Database>(
                await primaryClient.TryGetDatabaseAsync(Ct));
            primary = Assert.IsType<QueryDiagnosticsSummary>(
                primaryDatabase.GetQueryDiagnosticsSummary());

            replacementOptions = primaryClient.DirectDatabaseOptions.CreateRuntimeReplacement(
                CreateObservability("replacement", recentCapacity: 7));
        }

        var replacementConfiguration = Assert.IsType<CSharpDbObservabilityOptions>(
            replacementOptions.ObservabilityOptions);
        var replacementClient = new EngineTransportClient(
            databasePath,
            replacementOptions);
        replacementConfiguration.DatabaseAlias = "mutated-after-client-construction";
        replacementConfiguration.History.RecentQueryCapacity = 99;

        try
        {
            Assert.Equal(0, replacementClient.RuntimeDiagnosticsCounterEpoch);

            Database replacementDatabase = Assert.IsType<Database>(
                await replacementClient.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary replacement = Assert.IsType<QueryDiagnosticsSummary>(
                replacementDatabase.GetQueryDiagnosticsSummary());
            for (int index = 0; index < 8; index++)
            {
                await using var result = await replacementDatabase.ExecuteAsync("SELECT 1", Ct);
                _ = await result.ToListAsync(Ct);
            }

            BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
                Assert.IsType<BoundedDiagnosticsSnapshot<RecentQuerySnapshot>>(
                    replacementDatabase.GetRecentQueryDiagnosticsSnapshot(maximumRecords: 20));

            Assert.Equal(primary.Metadata.ServerInstanceId, replacement.Metadata.ServerInstanceId);
            Assert.Equal(primary.Metadata.CounterEpoch + 1, replacement.Metadata.CounterEpoch);
            Assert.Equal("replacement", replacement.Metadata.DatabaseAlias);
            Assert.Equal(7, recent.Records.Count);
            Assert.Equal(1, recent.DroppedCount);
            Assert.Equal(1, replacementClient.RuntimeDiagnosticsCounterEpoch);
        }
        finally
        {
            await replacementClient.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ClientHostReplacementBeforeAnyOpen_KeepsFirstSuccessfulEpochAtZero()
    {
        string databasePath = NewDatabasePath();
        DatabaseOptions replacementOptions;

        await using (var unopenedClient = new EngineTransportClient(
                         databasePath,
                         new DatabaseOptions
                         {
                             ObservabilityOptions = CreateObservability("unopened"),
                         }))
        {
            replacementOptions = unopenedClient.DirectDatabaseOptions.CreateRuntimeReplacement(
                CreateObservability("first-real-open"));
        }

        var replacementClient = new EngineTransportClient(databasePath, replacementOptions);
        try
        {
            Database database = Assert.IsType<Database>(
                await replacementClient.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary summary = Assert.IsType<QueryDiagnosticsSummary>(
                database.GetQueryDiagnosticsSummary());

            Assert.Equal(0, summary.Metadata.CounterEpoch);
            Assert.Equal(0, replacementClient.RuntimeDiagnosticsCounterEpoch);
            Assert.Equal("first-real-open", summary.Metadata.DatabaseAlias);
        }
        finally
        {
            await replacementClient.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ReleaseAndOpenRace_AdvancesOneEpochAfterOldHandleIsDisposed()
    {
        string databasePath = NewDatabasePath();
        var firstOpenEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        EngineTransportClient? client = null;
        int openCount = 0;

        try
        {
            client = new EngineTransportClient(
                databasePath,
                async (path, options, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(
                        path,
                        options,
                        ct);
                },
                new DatabaseOptions
                {
                    ObservabilityOptions = CreateObservability("release-race"),
                });
            string instanceId = Assert.IsType<string>(
                client.RuntimeDiagnosticsServerInstanceId);

            Task<Database?> firstGet =
                client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(Ct);

            Task release = client.ReleaseCachedDatabaseAsync(Ct).AsTask();
            Task<Database?> replacement = client.TryGetDatabaseAsync(Ct).AsTask();
            Assert.False(replacement.IsCompleted);

            allowFirstOpen.TrySetResult();
            await release;
            Database reopenedDatabase = Assert.IsType<Database>(await replacement);
            Assert.Same(reopenedDatabase, await firstGet);
            QueryDiagnosticsSummary reopened = Assert.IsType<QueryDiagnosticsSummary>(
                reopenedDatabase.GetQueryDiagnosticsSummary());

            Assert.Equal(2, Volatile.Read(ref openCount));
            Assert.Equal(instanceId, reopened.Metadata.ServerInstanceId);
            Assert.Equal(1, reopened.Metadata.CounterEpoch);
            Assert.Equal(1, client.RuntimeDiagnosticsCounterEpoch);
        }
        finally
        {
            allowFirstOpen.TrySetResult();
            if (client is not null)
                await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task InternalOptionCopies_RetainTheClientRuntimeState()
    {
        string databasePath = NewDatabasePath();
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = CreateObservability("retained") });

        try
        {
            Database clientDatabase = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary baseline = Assert.IsType<QueryDiagnosticsSummary>(
                clientDatabase.GetQueryDiagnosticsSummary());
            DatabaseOptions source = client.DirectDatabaseOptions;
            DatabaseOptions[] copies =
            [
                source.ConfigureStorageEngine(_ => { }),
                source.ConfigureFunctions(_ => { }),
                source.EnableAdaptiveQueryReoptimization(),
                RetainedDatabaseSnapshot.CreateBoundedDatabaseOptions(
                    source,
                    new RetainedDatabaseSnapshotOptions()),
            ];

            Assert.All(copies, static copy => Assert.True(copy.HasRuntimeDiagnosticsState));
            foreach (DatabaseOptions copy in copies)
            {
                await using Database copiedDatabase =
                    await Database.OpenInMemoryAsync(copy, Ct);
                QueryDiagnosticsSummary copied = Assert.IsType<QueryDiagnosticsSummary>(
                    copiedDatabase.GetQueryDiagnosticsSummary());

                Assert.True(client.UsesCurrentRuntimeDiagnosticsState(copiedDatabase));
                Assert.Equal(baseline.Metadata.ServerInstanceId, copied.Metadata.ServerInstanceId);
                Assert.Equal(baseline.Metadata.CounterEpoch, copied.Metadata.CounterEpoch);
                Assert.Equal("retained", copied.Metadata.DatabaseAlias);
            }
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task NullAndDisabledOptions_DoNotCreateRuntimeState()
    {
        string defaultPath = NewDatabasePath();
        string disabledPath = NewDatabasePath();
        var defaultClient = new EngineTransportClient(defaultPath);
        var disabledClient = new EngineTransportClient(
            disabledPath,
            new DatabaseOptions
            {
                ObservabilityOptions = new CSharpDbObservabilityOptions
                {
                    Enabled = false,
                    DatabaseAlias = "disabled",
                },
            });

        try
        {
            Assert.False(defaultClient.DirectDatabaseOptions.HasRuntimeDiagnosticsState);
            Assert.False(disabledClient.DirectDatabaseOptions.HasRuntimeDiagnosticsState);
            Assert.Null(defaultClient.RuntimeDiagnosticsServerInstanceId);
            Assert.Null(disabledClient.RuntimeDiagnosticsServerInstanceId);

            Database defaultDatabase = Assert.IsType<Database>(
                await defaultClient.TryGetDatabaseAsync(Ct));
            Database disabledDatabase = Assert.IsType<Database>(
                await disabledClient.TryGetDatabaseAsync(Ct));

            Assert.Null(defaultDatabase.RuntimeDiagnosticsState);
            Assert.Null(disabledDatabase.RuntimeDiagnosticsState);
            Assert.Null(defaultDatabase.GetQueryDiagnosticsSummary());
            Assert.Null(disabledDatabase.GetQueryDiagnosticsSummary());
        }
        finally
        {
            await disabledClient.DisposeAsync();
            await defaultClient.DisposeAsync();
            DeleteDatabaseFiles(defaultPath);
            DeleteDatabaseFiles(disabledPath);
        }
    }

    [Fact]
    public async Task IndependentClients_GetIndependentRuntimeIdentities()
    {
        string firstPath = NewDatabasePath();
        string secondPath = NewDatabasePath();
        var configured = new DatabaseOptions
        {
            ObservabilityOptions = CreateObservability("shared-configuration"),
        };
        var firstClient = new EngineTransportClient(firstPath, configured);
        var secondClient = new EngineTransportClient(secondPath, configured);

        try
        {
            Database firstDatabase = Assert.IsType<Database>(
                await firstClient.TryGetDatabaseAsync(Ct));
            Database secondDatabase = Assert.IsType<Database>(
                await secondClient.TryGetDatabaseAsync(Ct));
            QueryDiagnosticsSummary first = Assert.IsType<QueryDiagnosticsSummary>(
                firstDatabase.GetQueryDiagnosticsSummary());
            QueryDiagnosticsSummary second = Assert.IsType<QueryDiagnosticsSummary>(
                secondDatabase.GetQueryDiagnosticsSummary());

            Assert.NotEqual(first.Metadata.ServerInstanceId, second.Metadata.ServerInstanceId);
            Assert.Equal(0, first.Metadata.CounterEpoch);
            Assert.Equal(0, second.Metadata.CounterEpoch);
        }
        finally
        {
            await secondClient.DisposeAsync();
            await firstClient.DisposeAsync();
            DeleteDatabaseFiles(firstPath);
            DeleteDatabaseFiles(secondPath);
        }
    }

    [Fact]
    public async Task ResettableCounterFamilies_AdvanceTheSharedEpoch()
    {
        string databasePath = NewDatabasePath();
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = CreateObservability("resets") });

        try
        {
            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            Assert.Equal(0, client.RuntimeDiagnosticsCounterEpoch);

            database.ResetWalFlushDiagnostics();
            database.ResetCommitPathDiagnostics();
            database.ResetRowIdReservationDiagnostics();
            database.ResetAdaptiveQueryReoptimizationDiagnostics();
            database.ResetMutationTargetCollectionDiagnostics();

            QueryDiagnosticsSummary summary = Assert.IsType<QueryDiagnosticsSummary>(
                database.GetQueryDiagnosticsSummary());
            Assert.Equal(5, client.RuntimeDiagnosticsCounterEpoch);
            Assert.Equal(5, summary.Metadata.CounterEpoch);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task CompositeLedger_RecordsScriptParentAndStatementChildrenWithoutLogging()
    {
        var client = new EngineTransportClient(
            ":memory:script-runtime-hierarchy",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("script-runtime-hierarchy"),
            });

        try
        {
            CSharpDbRuntimeDiagnosticsState runtimeState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState,
                startSweepTimer: false);
            QueryDiagnosticsSummary before = registry.GetSummary();

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                "SELECT 1; SELECT 2;",
                Ct);

            Assert.Null(result.Error);
            BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
                registry.GetRecentSnapshot(10);
            RecentQuerySnapshot parent = Assert.Single(
                recent.Records,
                static record =>
                    record.OperationClass == CSharpDbOperationClass.Script &&
                    record.Role == CSharpDbOperationRole.Request);
            RecentQuerySnapshot[] statements = recent.Records
                .Where(static record => record.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(2, statements.Length);
            Assert.All(statements, statement =>
            {
                Assert.Equal(CSharpDbOperationClass.Query, statement.OperationClass);
                Assert.Equal(parent.OperationId, statement.ParentOperationId);
            });

            QueryDiagnosticsSummary after = registry.GetSummary();
            Assert.Equal(1, after.RequestCount - before.RequestCount);
            Assert.Equal(2, after.StatementExecutionCount - before.StatementExecutionCount);
            Assert.Equal(3, after.SucceededCount - before.SucceededCount);
        }
        finally
        {
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompositeLedger_RecordsProcedureParentAndStatementChildrenWithoutLogging()
    {
        var client = new EngineTransportClient(
            ":memory:procedure-runtime-hierarchy",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("procedure-runtime-hierarchy"),
            });

        try
        {
            await client.CreateProcedureAsync(
                new ProcedureDefinition
                {
                    Name = "RuntimeHierarchyProcedure",
                    BodySql = "SELECT 1; SELECT 2;",
                    Parameters = [],
                    IsEnabled = true,
                    CreatedUtc = DateTime.UtcNow,
                    UpdatedUtc = DateTime.UtcNow,
                },
                Ct);
            CSharpDbRuntimeDiagnosticsState runtimeState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState,
                startSweepTimer: false);
            QueryDiagnosticsSummary before = registry.GetSummary();
            HashSet<OpaqueDiagnosticsId> existingIds = registry
                .GetRecentSnapshot(100)
                .Records
                .Select(static record => record.OperationId)
                .ToHashSet();

            ProcedureExecutionResult result = await client.ExecuteProcedureAsync(
                "RuntimeHierarchyProcedure",
                new Dictionary<string, object?>(),
                Ct);

            Assert.True(result.Succeeded);
            RecentQuerySnapshot[] added = registry
                .GetRecentSnapshot(100)
                .Records
                .Where(record => !existingIds.Contains(record.OperationId))
                .ToArray();
            RecentQuerySnapshot parent = Assert.Single(
                added,
                static record =>
                    record.OperationClass == CSharpDbOperationClass.Procedure &&
                    record.Role == CSharpDbOperationRole.Request);
            RecentQuerySnapshot[] statements = added
                .Where(static record => record.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(2, statements.Length);
            Assert.All(statements, statement =>
            {
                Assert.Equal(CSharpDbOperationClass.Query, statement.OperationClass);
                Assert.Equal(parent.OperationId, statement.ParentOperationId);
            });

            QueryDiagnosticsSummary after = registry.GetSummary();
            Assert.Equal(1, after.RequestCount - before.RequestCount);
            Assert.Equal(2, after.StatementExecutionCount - before.StatementExecutionCount);
            Assert.Equal(3, after.SucceededCount - before.SucceededCount);
        }
        finally
        {
            await client.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NestedCompositeLedger_UsesExactRuntimeClock(
        bool executeProcedure,
        bool loggingEnabled)
    {
        DateTimeOffset runtimeStartedAt =
            new(2026, 8, 10, 13, 0, 0, TimeSpan.Zero);
        var runtimeClock = new ManualRuntimeTimeProvider(runtimeStartedAt);
        var foreignClock = new ManualRuntimeTimeProvider(
            new DateTimeOffset(2036, 1, 1, 0, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability =
            CreateObservability("nested-composite-clock");
        observability.Logging.Enabled = loggingEnabled;
        observability.Logging.Queries = loggingEnabled;
        using var runtimeState = new CSharpDbRuntimeDiagnosticsState(
            observability,
            runtimeClock);
        var client = new EngineTransportClient(
            ":memory:nested-composite-clock",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = runtimeState,
            },
            observabilityTimeProvider: TimeProvider.System);

        try
        {
            const string procedureName = "NestedClockProcedure";
            if (executeProcedure)
            {
                await client.CreateProcedureAsync(
                    new ProcedureDefinition
                    {
                        Name = procedureName,
                        BodySql = "SELECT 1; SELECT 2;",
                        Parameters = [],
                        IsEnabled = true,
                        CreatedUtc = DateTime.UtcNow,
                        UpdatedUtc = DateTime.UtcNow,
                    },
                    Ct);
            }

            CSharpDbRuntimeDiagnosticsState currentState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.Same(runtimeClock, currentState.TimeProvider);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                currentState,
                startSweepTimer: false);
            HashSet<OpaqueDiagnosticsId> existingIds = registry
                .GetRecentSnapshot(100)
                .Records
                .Select(static record => record.OperationId)
                .ToHashSet();
            CSharpDbOperationContext foreignParent =
                CSharpDbOperationContext.CreateRoot(
                    CSharpDbOperationClass.Maintenance,
                    CSharpDB.Observability.CSharpDbTransport.Direct,
                    "foreign-parent",
                    timeProvider: foreignClock);
            var observer = new RecordingDiagnosticObserver();
            using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                observer,
                static name => name == CSharpDbLogEvents.QueryCompleted.Name);

            using (CSharpDbOperationScope.Enter(foreignParent))
            {
                if (executeProcedure)
                {
                    ProcedureExecutionResult result = await client.ExecuteProcedureAsync(
                        procedureName,
                        new Dictionary<string, object?>(),
                        Ct);
                    Assert.True(result.Succeeded);
                }
                else
                {
                    SqlExecutionResult result = await client.ExecuteSqlAsync(
                        "SELECT 1; SELECT 2;",
                        Ct);
                    Assert.Null(result.Error);
                }
            }
            Assert.Same(currentState, client.CurrentRuntimeDiagnosticsState);

            CSharpDbOperationClass expectedClass = executeProcedure
                ? CSharpDbOperationClass.Procedure
                : CSharpDbOperationClass.Script;
            RecentQuerySnapshot parent = Assert.Single(
                registry.GetRecentSnapshot(100).Records,
                record =>
                    !existingIds.Contains(record.OperationId) &&
                    record.OperationClass == expectedClass &&
                    record.Role == CSharpDbOperationRole.Request);
            Assert.Equal(foreignParent.OperationId, parent.ParentOperationId);
            Assert.Equal(runtimeStartedAt, parent.StartedAtUtc);

            CSharpDbQueryCompletedEvent[] parentEvents = observer.Events
                .Select(static item => item.Value)
                .OfType<CSharpDbQueryCompletedEvent>()
                .Where(item => item.Context.OperationId == parent.OperationId)
                .ToArray();
            if (loggingEnabled)
            {
                CSharpDbQueryCompletedEvent parentEvent = Assert.Single(parentEvents);
                Assert.Equal(runtimeStartedAt, parentEvent.Context.StartedAtUtc);
            }
            else
            {
                Assert.Empty(parentEvents);
            }
        }
        finally
        {
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompositeLateSubscriber_DoesNotReceiveTerminalOrSlowEvent()
    {
        DateTimeOffset startedAt =
            new(2026, 8, 10, 14, 0, 0, TimeSpan.Zero);
        var clock = new ManualRuntimeTimeProvider(startedAt);
        var openEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        CSharpDbObservabilityOptions observability =
            CreateObservability("late-composite-subscriber");
        observability.Logging.Enabled = true;
        observability.Logging.Queries = true;
        observability.Logging.SlowQueries = true;
        observability.Logging.SlowQueryThreshold = TimeSpan.FromMilliseconds(10);
        var client = new EngineTransportClient(
            ":memory:late-composite-subscriber",
            async (_, options, ct) =>
            {
                openEntered.TrySetResult();
                await allowOpen.Task;
                return await Database.OpenInMemoryAsync(options, ct);
            },
            new DatabaseOptions { ObservabilityOptions = observability },
            observabilityTimeProvider: clock);
        using var cancellation = new CancellationTokenSource();

        try
        {
            CSharpDbRuntimeDiagnosticsState runtimeState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState,
                startSweepTimer: false);
            Task<SqlExecutionResult> execution =
                client.ExecuteSqlAsync("SELECT 1", cancellation.Token);
            await openEntered.Task.WaitAsync(Ct);
            await WaitForActiveCountAsync(registry, expectedCount: 1);

            var observer = new RecordingDiagnosticObserver();
            using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                observer,
                static name =>
                    name == CSharpDbLogEvents.QueryCompleted.Name ||
                    name == CSharpDbLogEvents.QueryFailed.Name ||
                    name == CSharpDbLogEvents.QueryCanceled.Name ||
                    name == CSharpDbLogEvents.SlowQuery.Name);
            clock.Advance(TimeSpan.FromSeconds(1));
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await execution);
            Assert.Empty(observer.Events);
            RecentQuerySnapshot terminal = Assert.Single(
                registry.GetRecentSnapshot(10).Records);
            Assert.Equal(CSharpDbOperationOutcome.Canceled, terminal.Outcome);
            Assert.True(terminal.Duration >= TimeSpan.FromSeconds(1));
        }
        finally
        {
            cancellation.Cancel();
            allowOpen.TrySetResult();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompositeLedger_UsesRetainedStateClockAndCapturesQueuedWithoutLogging()
    {
        DateTimeOffset startedAt =
            new(2026, 8, 10, 12, 34, 56, TimeSpan.Zero);
        var clock = new ManualRuntimeTimeProvider(startedAt);
        CSharpDbObservabilityOptions observability = CreateObservability("queued-clock");
        using var retainedState = new CSharpDbRuntimeDiagnosticsState(
            observability,
            clock);
        var openEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new EngineTransportClient(
            ":memory:queued-clock",
            async (_, options, ct) =>
            {
                openEntered.TrySetResult();
                await allowOpen.Task.WaitAsync(ct);
                return await Database.OpenInMemoryAsync(options, ct);
            },
            new DatabaseOptions
            {
                ObservabilityOptions = observability,
                RuntimeDiagnosticsState = retainedState,
            },
            observabilityTimeProvider: TimeProvider.System);
        using var queuedCancellation = new CancellationTokenSource();

        try
        {
            CSharpDbRuntimeDiagnosticsState runtimeState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.Same(clock, runtimeState.TimeProvider);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState,
                startSweepTimer: false);

            Task<SqlExecutionResult> admitted =
                client.ExecuteSqlAsync("SELECT 1", Ct);
            await openEntered.Task.WaitAsync(Ct);
            Task<SqlExecutionResult> queued =
                client.ExecuteSqlAsync("SELECT 2", queuedCancellation.Token);
            await WaitForActiveCountAsync(registry, expectedCount: 2);

            BoundedDiagnosticsSnapshot<ActiveQuerySnapshot> active =
                registry.GetActiveSnapshot(10);
            Assert.Equal(
                [QueryExecutionPhase.Queued, QueryExecutionPhase.Planning],
                active.Records
                    .Select(static record => record.Phase)
                    .OrderBy(static phase => phase));
            Assert.All(active.Records, record =>
            {
                Assert.Equal(startedAt, record.StartedAtUtc);
                Assert.Equal("queued-clock", record.Metadata.DatabaseAlias);
            });

            queuedCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await queued);
            allowOpen.TrySetResult();
            SqlExecutionResult admittedResult = await admitted;
            Assert.Null(admittedResult.Error);

            BoundedDiagnosticsSnapshot<RecentQuerySnapshot> recent =
                registry.GetRecentSnapshot(10);
            Assert.Equal(2, recent.Records.Count);
            Assert.Single(
                recent.Records,
                static record => record.Outcome == CSharpDbOperationOutcome.Canceled);
            Assert.Single(
                recent.Records,
                static record => record.Outcome == CSharpDbOperationOutcome.Succeeded);
            Assert.Empty(registry.GetActiveSnapshot(10).Records);
        }
        finally
        {
            allowOpen.TrySetResult();
            queuedCancellation.Cancel();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task QueuedQuery_RebindsAcrossReleaseToOneCurrentFamilyTerminal()
    {
        var firstOpenEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;
        var client = new EngineTransportClient(
            ":memory:queued-release-success",
            async (_, options, ct) =>
            {
                if (Interlocked.Increment(ref openCount) == 1)
                {
                    firstOpenEntered.TrySetResult();
                    await allowFirstOpen.Task.WaitAsync(ct);
                }

                return await Database.OpenInMemoryAsync(options, ct);
            },
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("queued-release-success"),
            });

        try
        {
            Task<Database?> initialGet =
                client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(Ct);
            CSharpDbRuntimeDiagnosticsState oldState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics oldRegistry = QueryRuntimeDiagnostics.GetOrCreate(
                oldState,
                startSweepTimer: false);

            Task release = client.ReleaseCachedDatabaseAsync(Ct).AsTask();
            Task<SqlExecutionResult> queued = client.ExecuteSqlAsync("SELECT 42", Ct);
            await WaitForActiveCountAsync(oldRegistry, expectedCount: 1);
            Assert.Equal(
                QueryExecutionPhase.Queued,
                Assert.Single(oldRegistry.GetActiveSnapshot(10).Records).Phase);

            allowFirstOpen.TrySetResult();
            await release;
            SqlExecutionResult result = await queued;
            _ = await initialGet;
            Assert.Null(result.Error);

            CSharpDbRuntimeDiagnosticsState currentState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(oldState, currentState);
            QueryRuntimeDiagnostics currentRegistry = QueryRuntimeDiagnostics.GetOrCreate(
                currentState,
                startSweepTimer: false);
            Assert.Empty(oldRegistry.GetActiveSnapshot(10).Records);
            Assert.Empty(oldRegistry.GetRecentSnapshot(10).Records);
            Assert.Empty(currentRegistry.GetActiveSnapshot(10).Records);
            RecentQuerySnapshot terminal = Assert.Single(
                currentRegistry.GetRecentSnapshot(10).Records);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
        }
        finally
        {
            allowFirstOpen.TrySetResult();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task QueuedCancellation_AfterFamilySwapLandsOnceInCurrentFamily()
    {
        var firstOpenEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;
        var client = new EngineTransportClient(
            ":memory:queued-release-cancel",
            async (_, options, ct) =>
            {
                if (Interlocked.Increment(ref openCount) == 1)
                {
                    firstOpenEntered.TrySetResult();
                    await allowFirstOpen.Task.WaitAsync(ct);
                }

                return await Database.OpenInMemoryAsync(options, ct);
            },
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("queued-release-cancel"),
            });
        using var queuedCancellation = new CancellationTokenSource();
        var blockingDisposal = new BlockingDisposeComponent();

        try
        {
            Task<Database?> initialGet =
                client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(Ct);
            CSharpDbRuntimeDiagnosticsState oldState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.Same(
                blockingDisposal,
                oldState.GetOrCreateComponent(() => blockingDisposal));
            QueryRuntimeDiagnostics oldRegistry = QueryRuntimeDiagnostics.GetOrCreate(
                oldState,
                startSweepTimer: false);

            Task release = client.ReleaseCachedDatabaseAsync(Ct).AsTask();
            Task<SqlExecutionResult> queued =
                client.ExecuteSqlAsync("SELECT 7", queuedCancellation.Token);
            await WaitForActiveCountAsync(oldRegistry, expectedCount: 1);
            allowFirstOpen.TrySetResult();
            await blockingDisposal.DisposeStarted.Task.WaitAsync(Ct);

            CSharpDbRuntimeDiagnosticsState currentState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(oldState, currentState);
            QueryRuntimeDiagnostics currentRegistry = QueryRuntimeDiagnostics.GetOrCreate(
                currentState,
                startSweepTimer: false);

            queuedCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await queued);
            Assert.Empty(oldRegistry.GetActiveSnapshot(10).Records);
            Assert.Empty(oldRegistry.GetRecentSnapshot(10).Records);
            Assert.Empty(currentRegistry.GetActiveSnapshot(10).Records);
            RecentQuerySnapshot terminal = Assert.Single(
                currentRegistry.GetRecentSnapshot(10).Records);
            Assert.Equal(CSharpDbOperationOutcome.Canceled, terminal.Outcome);

            blockingDisposal.Release();
            await release;
            _ = await initialGet;
        }
        finally
        {
            allowFirstOpen.TrySetResult();
            queuedCancellation.Cancel();
            blockingDisposal.Release();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task ActiveTransaction_RetainsFrozenFamilyAcrossTransientCachedRelease()
    {
        int openCount = 0;
        var client = new EngineTransportClient(
            ":memory:transaction-family-overlap",
            async (_, options, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(options, ct);
            },
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("transaction-family-overlap"),
            });
        var disposal = new TrackingDisposeComponent();
        string? transactionId = null;

        try
        {
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
            transactionId = transaction.TransactionId;
            CSharpDbRuntimeDiagnosticsState transactionState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics transactionRegistry =
                QueryRuntimeDiagnostics.GetOrCreate(
                    transactionState,
                    startSweepTimer: false);
            Assert.Same(
                disposal,
                transactionState.GetOrCreateComponent(() => disposal));

            SqlExecutionResult inTransaction = await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 1",
                Ct);
            Assert.Null(inTransaction.Error);
            SqlExecutionResult competing = await client.ExecuteSqlAsync("SELECT 2", Ct);
            Assert.Null(competing.Error);
            Assert.Equal(2, transactionRegistry.GetRecentSnapshot(10).Records.Count);

            await client.ReleaseCachedDatabaseAsync(Ct);
            CSharpDbRuntimeDiagnosticsState replacementState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(transactionState, replacementState);
            Assert.Equal(0, transactionState.CounterEpoch);
            Assert.Equal(0, replacementState.CounterEpoch);
            Assert.Equal(0, disposal.DisposeCount);

            SqlExecutionResult replacement = await client.ExecuteSqlAsync("SELECT 3", Ct);
            Assert.Null(replacement.Error);
            Assert.Equal(0, transactionState.CounterEpoch);
            Assert.Equal(1, replacementState.CounterEpoch);
            QueryRuntimeDiagnostics replacementRegistry =
                QueryRuntimeDiagnostics.GetOrCreate(
                    replacementState,
                    startSweepTimer: false);
            Assert.Equal(
                2,
                transactionRegistry.GetRecentSnapshot(10).Records.Count);
            RecentQuerySnapshot replacementTerminal = Assert.Single(
                replacementRegistry.GetRecentSnapshot(10).Records);
            Assert.Equal(0, Assert.Single(
                transactionRegistry.GetRecentSnapshot(10).Records
                    .Select(static record => record.Metadata.CounterEpoch)
                    .Distinct()));
            Assert.Equal(1, replacementTerminal.Metadata.CounterEpoch);
            Assert.Equal(
                transactionState.ServerInstanceId,
                replacementState.ServerInstanceId);

            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
            transactionId = null;
            Assert.Equal(1, disposal.DisposeCount);
            Assert.Empty(transactionRegistry.GetActiveSnapshot(10).Records);
            Assert.Equal(3, Volatile.Read(ref openCount));
        }
        finally
        {
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
    public async Task TransactionQueuedSuccessAndCancellation_StayOnOwningFamilyAfterRelease()
    {
        var client = new EngineTransportClient(
            ":memory:transaction-queued-family",
            static async (_, options, ct) =>
                await Database.OpenInMemoryAsync(options, ct),
            new DatabaseOptions
            {
                ObservabilityOptions = CreateObservability("transaction-queued-family"),
            });
        using var queuedCancellation = new CancellationTokenSource();
        string? transactionId = null;

        try
        {
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
            transactionId = transaction.TransactionId;
            CSharpDbRuntimeDiagnosticsState transactionState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics transactionRegistry =
                QueryRuntimeDiagnostics.GetOrCreate(
                    transactionState,
                    startSweepTimer: false);

            Assert.Null((await client.ExecuteSqlAsync("SELECT 10", Ct)).Error);
            await client.ReleaseCachedDatabaseAsync(Ct);
            CSharpDbRuntimeDiagnosticsState pendingReplacement =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(transactionState, pendingReplacement);
            QueryRuntimeDiagnostics replacementRegistry =
                QueryRuntimeDiagnostics.GetOrCreate(
                    pendingReplacement,
                    startSweepTimer: false);

            ForwardOnlyQueryCursor firstCursor =
                Assert.IsType<ForwardOnlyQueryCursor>(
                    await client.TryOpenForwardOnlyQueryCursorAsync(
                        transaction.TransactionId,
                        "SELECT 1",
                        Ct));
            Task<SqlExecutionResult> queuedSuccess =
                client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT 2",
                    Ct);
            OpaqueDiagnosticsId succeededId = await WaitForQueuedOperationAsync(
                transactionRegistry);
            await firstCursor.DisposeAsync();
            Assert.Null((await queuedSuccess).Error);
            RecentQuerySnapshot succeeded = Assert.Single(
                transactionRegistry.GetRecentSnapshot(20).Records,
                record => record.OperationId == succeededId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, succeeded.Outcome);

            ForwardOnlyQueryCursor secondCursor =
                Assert.IsType<ForwardOnlyQueryCursor>(
                    await client.TryOpenForwardOnlyQueryCursorAsync(
                        transaction.TransactionId,
                        "SELECT 3",
                        Ct));
            try
            {
                Task<SqlExecutionResult> queuedCanceled =
                    client.ExecuteInTransactionAsync(
                        transaction.TransactionId,
                        "SELECT 4",
                        queuedCancellation.Token);
                OpaqueDiagnosticsId canceledId = await WaitForQueuedOperationAsync(
                    transactionRegistry);
                queuedCancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    async () => await queuedCanceled);
                RecentQuerySnapshot canceled = Assert.Single(
                    transactionRegistry.GetRecentSnapshot(20).Records,
                    record => record.OperationId == canceledId);
                Assert.Equal(CSharpDbOperationOutcome.Canceled, canceled.Outcome);
            }
            finally
            {
                await secondCursor.DisposeAsync();
            }

            Assert.Empty(replacementRegistry.GetActiveSnapshot(10).Records);
            Assert.Empty(replacementRegistry.GetRecentSnapshot(10).Records);
            Assert.All(
                transactionRegistry.GetRecentSnapshot(20).Records,
                record => Assert.Equal(0, record.Metadata.CounterEpoch));
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
            transactionId = null;
            Assert.Empty(transactionRegistry.GetActiveSnapshot(20).Records);
        }
        finally
        {
            queuedCancellation.Cancel();
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

    private static CSharpDbObservabilityOptions CreateObservability(
        string databaseAlias,
        int recentCapacity = 32)
        => new()
        {
            Enabled = true,
            DatabaseAlias = databaseAlias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                Queries = false,
                SlowQueries = false,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 32,
                RecentQueryCapacity = recentCapacity,
                RecentOperationCapacity = 16,
                Retention = TimeSpan.FromMinutes(5),
            },
        };

    private static async Task WaitForActiveCountAsync(
        QueryRuntimeDiagnostics registry,
        int expectedCount)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            timeout.Token,
            Ct);
        while (registry.GetActiveSnapshot(100).Records.Count != expectedCount)
            await Task.Delay(TimeSpan.FromMilliseconds(10), linked.Token);
    }

    private static async Task<OpaqueDiagnosticsId> WaitForQueuedOperationAsync(
        QueryRuntimeDiagnostics registry)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            timeout.Token,
            Ct);
        while (true)
        {
            ActiveQuerySnapshot? queued = registry
                .GetActiveSnapshot(100)
                .Records
                .SingleOrDefault(
                    static record => record.Phase == QueryExecutionPhase.Queued);
            if (queued is not null)
                return queued.OperationId;

            await Task.Delay(TimeSpan.FromMilliseconds(10), linked.Token);
        }
    }

    private static string NewDatabasePath()
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_runtime_state_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseFiles(string databasePath)
    {
        DeleteIfExists(databasePath);
        DeleteIfExists(databasePath + ".wal");
        DeleteIfExists(databasePath + "-wal");
        DeleteIfExists(databasePath + "-shm");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class InjectedOpenException : Exception
    {
    }

    private sealed class BlockingDisposeComponent : IDisposable
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource DisposeStarted { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public void Dispose()
        {
            DisposeStarted.TrySetResult();
            _release.Task.GetAwaiter().GetResult();
        }

        internal void Release() => _release.TrySetResult();
    }

    private sealed class TrackingDisposeComponent : IDisposable
    {
        private int _disposeCount;

        internal int DisposeCount => Volatile.Read(ref _disposeCount);

        public void Dispose() => Interlocked.Increment(ref _disposeCount);
    }

    private sealed class RecordingDiagnosticObserver :
        IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<KeyValuePair<string, object?>> _events = new();

        internal IReadOnlyCollection<KeyValuePair<string, object?>> Events =>
            _events.ToArray();

        public void OnNext(KeyValuePair<string, object?> value) => _events.Enqueue(value);
        public void OnError(Exception error)
        {
        }
        public void OnCompleted()
        {
        }
    }

    private sealed class ManualRuntimeTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly long _baseUtcTicks = utcNow.UtcTicks;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(
                checked(_baseUtcTicks + Volatile.Read(ref _timestamp)),
                TimeSpan.Zero);

        public override long GetTimestamp() => Volatile.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
        {
            if (elapsed < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(elapsed));

            Interlocked.Add(ref _timestamp, elapsed.Ticks);
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
            => new InertTimer();

        private sealed class InertTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose()
            {
            }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class FailNextOpenStorageEngineFactory : IStorageEngineFactory
    {
        private readonly DefaultStorageEngineFactory _inner = new();
        private int _failNextOpen;

        internal void FailNextOpen()
            => Interlocked.Exchange(ref _failNextOpen, 1);

        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            if (Interlocked.Exchange(ref _failNextOpen, 0) != 0)
            {
                return ValueTask.FromException<StorageEngineContext>(
                    new InjectedOpenException());
            }

            return _inner.OpenAsync(filePath, options, ct);
        }

        public ValueTask<StorageEngineContext> CreateNewAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
            => _inner.CreateNewAsync(filePath, options, ct);
    }
}

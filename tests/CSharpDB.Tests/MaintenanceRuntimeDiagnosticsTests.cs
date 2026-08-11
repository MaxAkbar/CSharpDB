using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class MaintenanceRuntimeDiagnosticsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Registry_BoundsActiveAndRecentWithoutPermanentActiveTruncation()
    {
        var clock = new ManualTimeProvider();
        using var registry = new MaintenanceRuntimeDiagnostics(
            capacity: 1,
            retention: TimeSpan.FromMinutes(1),
            clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation first =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Queued);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation hidden =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Validating);

        MaintenanceRuntimeDiagnosticsCapture saturated = registry.Capture();
        Assert.Single(saturated.Active);
        Assert.Equal(1, saturated.ActiveRejectedCount);
        Assert.True(saturated.ActiveRecordsTruncated);

        hidden.Succeed(completedUnits: 12, totalUnits: 10);
        MaintenanceRuntimeDiagnosticsCapture afterHiddenTerminal = registry.Capture();
        Assert.Equal(0, afterHiddenTerminal.ActiveRejectedCount);
        Assert.False(afterHiddenTerminal.ActiveRecordsTruncated);
        MaintenanceRuntimeRecord hiddenRecent = Assert.Single(
            afterHiddenTerminal.Recent);
        Assert.Equal(10, hiddenRecent.CompletedUnits);
        Assert.Equal(10, hiddenRecent.TotalUnits);

        first.Succeed();
        first.Succeed();
        MaintenanceRuntimeDiagnosticsCapture bounded = registry.Capture();
        Assert.Empty(bounded.Active);
        Assert.Single(bounded.Recent);
        Assert.Equal(first.Context.OperationId, bounded.Recent[0].Context.OperationId);
        Assert.Equal(1, bounded.RecentDroppedCount);

        clock.Advance(TimeSpan.FromMinutes(2));
        MaintenanceRuntimeDiagnosticsCapture expired = registry.Capture();
        Assert.Empty(expired.Recent);
        Assert.Equal(2, expired.RecentDroppedCount);
    }

    [Fact]
    public void Registry_OverflowDisposeAndLateTerminal_DoNotPinOrReleaseNewOwner()
    {
        var clock = new ManualTimeProvider();
        using var registry = new MaintenanceRuntimeDiagnostics(
            capacity: 1,
            retention: TimeSpan.FromMinutes(1),
            clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation registered =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Queued);
        CSharpDbOperationContext hiddenContext = CreateContext(clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation hidden =
            Start(registry, hiddenContext, MaintenanceOperationPhase.Validating);

        var replacementOwner = new object();
        Assert.True(hiddenContext.TryClaimRuntimeDiagnostics(replacementOwner));

        registry.Dispose();
        var registeredReplacementOwner = new object();
        Assert.True(registered.Context.TryClaimRuntimeDiagnostics(
            registeredReplacementOwner));

        hidden.Succeed();
        hidden.Succeed();
        Assert.Empty(registry.Capture().Active);
        Assert.Empty(registry.Capture().Recent);
        Assert.False(hiddenContext.TryClaimRuntimeDiagnostics(new object()));

        hiddenContext.ReleaseRuntimeDiagnostics(replacementOwner);
        var finalOwner = new object();
        Assert.True(hiddenContext.TryClaimRuntimeDiagnostics(finalOwner));
        hiddenContext.ReleaseRuntimeDiagnostics(finalOwner);
        registered.Context.ReleaseRuntimeDiagnostics(registeredReplacementOwner);
    }

    [Fact]
    public void Registry_UnknownTerminalTimestampDoesNotExpireOrBlockKnownRetention()
    {
        var clock = new ManualTimeProvider();
        using var registry = new MaintenanceRuntimeDiagnostics(
            capacity: 4,
            retention: TimeSpan.FromMinutes(1),
            clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation unknown =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Validating);
        clock.ThrowTimestamps = true;
        unknown.Fail(new IOException("private-path-canary"));

        clock.ThrowTimestamps = false;
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation known =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Validating);
        known.Succeed();
        clock.Advance(TimeSpan.FromMinutes(2));

        MaintenanceRuntimeDiagnosticsCapture capture = registry.Capture();
        MaintenanceRuntimeRecord retained = Assert.Single(capture.Recent);
        Assert.Equal(unknown.Context.OperationId, retained.Context.OperationId);
        Assert.Equal(CSharpDbOperationOutcome.Failed, retained.Outcome);
        Assert.NotNull(retained.Error);
        string safeProjection = string.Join(
            '|',
            retained.Error!.Code,
            retained.Error.ErrorType,
            retained.Error.PublicDetail);
        Assert.DoesNotContain(
            "private-path-canary",
            safeProjection,
            StringComparison.Ordinal);
        Assert.Equal(1, capture.RecentDroppedCount);
    }

    [Fact]
    public async Task Registry_CompleteCaptureDisposeRace_IsExactOnceAndReleasesClaim()
    {
        var clock = new BlockingTimeProvider();
        var registry = new MaintenanceRuntimeDiagnostics(
            capacity: 2,
            retention: TimeSpan.FromMinutes(1),
            clock);
        CSharpDbOperationContext context = CreateContext(clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation operation =
            Start(registry, context, MaintenanceOperationPhase.Copying);
        clock.BlockNextTimestamp();
        Task completion = Task.Run(() => operation.Succeed(), Ct);
        await clock.WaitUntilBlockedAsync().WaitAsync(Ct);

        try
        {
            Assert.Single(registry.Capture().Active);
            registry.Dispose();
        }
        finally
        {
            clock.Release();
        }

        await completion;
        operation.Succeed();
        MaintenanceRuntimeDiagnosticsCapture disposed = registry.Capture();
        Assert.Empty(disposed.Active);
        Assert.Empty(disposed.Recent);
        var replacementOwner = new object();
        Assert.True(context.TryClaimRuntimeDiagnostics(replacementOwner));
        context.ReleaseRuntimeDiagnostics(replacementOwner);
    }

    [Fact]
    public void Registry_ActiveProgressIsBoundedAndPreservedAtTerminal()
    {
        var clock = new ManualTimeProvider();
        using var registry = new MaintenanceRuntimeDiagnostics(
            capacity: 2,
            retention: TimeSpan.FromMinutes(1),
            clock);
        MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation operation =
            Start(registry, CreateContext(clock), MaintenanceOperationPhase.Copying);

        operation.SetProgress(completedUnits: 3, totalUnits: 5);
        MaintenanceRuntimeRecord active = Assert.Single(registry.Capture().Active);
        Assert.Equal(3, active.CompletedUnits);
        Assert.Equal(5, active.TotalUnits);

        operation.SetProgress(completedUnits: 9, totalUnits: 5);
        MaintenanceRuntimeRecord bounded = Assert.Single(registry.Capture().Active);
        Assert.Equal(5, bounded.CompletedUnits);
        Assert.Equal(5, bounded.TotalUnits);

        operation.Fail(new InvalidOperationException("progress failure"));
        MaintenanceRuntimeRecord terminal = Assert.Single(registry.Capture().Recent);
        Assert.Equal(CSharpDbOperationOutcome.Failed, terminal.Outcome);
        Assert.Equal(5, terminal.CompletedUnits);
        Assert.Equal(5, terminal.TotalUnits);
    }

    [Fact]
    public async Task ClientCheckpoint_SharesLifecycleStorageIdentityWithoutFallbackDuplicate()
    {
        string databasePath = CreateDatabasePath("maintenance-checkpoint-client");
        var phases = new List<MaintenanceOperationPhase>();
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-checkpoint-client"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE checkpoint_items (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO checkpoint_items VALUES (1)",
                Ct)).Error);
            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            await client.CheckpointAsync(Ct);

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(MaintenanceOperationKind.Checkpoint, terminal.Kind);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            Assert.Null(terminal.CompletedUnits);
            Assert.Null(terminal.TotalUnits);
            Assert.Null(MaintenanceRuntimeDiagnostics.TryGet(
                database.RuntimeDiagnosticsState));
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.CheckpointCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);
            StorageRuntimeDiagnostics.CheckpointOperation storageCheckpoint =
                Assert.IsType<StorageRuntimeDiagnostics.CheckpointOperation>(
                    GetStorageRuntimeRegistration(database).Checkpoint);
            Assert.Equal(
                terminal.Context.OperationId,
                storageCheckpoint.OperationId);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Checkpointing,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientCheckpoint_LoggingOnlyPreservesLifecycleWithoutRuntimeRegistry()
    {
        string databasePath = CreateDatabasePath(
            "maintenance-checkpoint-logging-only");
        using var events = new LifecycleRecorder();
        using var disabledRuntimeState = new CSharpDbRuntimeDiagnosticsState(
            new CSharpDbObservabilityOptions { Enabled = false });
        var databaseOptions = new DatabaseOptions
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "maintenance-checkpoint-logging-only",
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = false,
                    SlowQueries = false,
                },
            },
            RuntimeDiagnosticsState = disabledRuntimeState,
        };
        await using var client = new EngineTransportClient(
            databasePath,
            (path, _, ct) => Database.OpenAsync(
                path,
                databaseOptions,
                ct).AsTask(),
            new DatabaseOptions());

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE checkpoint_logging_items (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));

            await client.CheckpointAsync(Ct);

            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.CheckpointCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);
            Assert.Null(GetClientRegistryOrNull(client));
            Assert.Null(client.CurrentRuntimeDiagnosticsState);
            CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(database.RuntimeDiagnosticsState);
            Assert.Same(disabledRuntimeState, state);
            Assert.False(state.IsEnabled);
            Assert.Null(MaintenanceRuntimeDiagnostics.TryGet(state));
            Assert.Null(GetRuntimeComponents(state));
        }
        finally
        {
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task DirectCheckpoint_UsesStateFallbackAndSharesLifecycleStorageIdentity()
    {
        string databasePath = CreateDatabasePath("maintenance-checkpoint-direct");
        var phases = new List<MaintenanceOperationPhase>();
        Database? database = null;

        try
        {
            database = await Database.OpenAsync(
                databasePath,
                CreateOptions("maintenance-checkpoint-direct"),
                Ct);
            _ = await database.ExecuteAsync(
                "CREATE TABLE checkpoint_direct_items (id INTEGER PRIMARY KEY)",
                Ct);
            _ = await database.ExecuteAsync(
                "INSERT INTO checkpoint_direct_items VALUES (1)",
                Ct);
            using var events = new LifecycleRecorder();
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            await database.CheckpointAsync(Ct);

            CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(database.RuntimeDiagnosticsState);
            MaintenanceRuntimeDiagnostics registry = Assert.IsType<
                MaintenanceRuntimeDiagnostics>(
                MaintenanceRuntimeDiagnostics.TryGet(state));
            MaintenanceRuntimeRecord terminal = Assert.Single(
                registry.Capture().Recent);
            Assert.Equal(MaintenanceOperationKind.Checkpoint, terminal.Kind);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.CheckpointCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            StorageRuntimeDiagnostics.CheckpointOperation storageCheckpoint =
                Assert.IsType<StorageRuntimeDiagnostics.CheckpointOperation>(
                    GetStorageRuntimeRegistration(database).Checkpoint);
            Assert.Equal(
                terminal.Context.OperationId,
                storageCheckpoint.OperationId);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Checkpointing,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            if (database is not null)
                await database.DisposeAsync();
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientReindex_ProjectsProgressAndSurvivesOneFamilyReset()
    {
        string databasePath = CreateDatabasePath("maintenance-reindex");
        var phases = new List<MaintenanceOperationPhase>();
        var staging = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseStaging = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-reindex"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE reindex_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX idx_reindex_items_value ON reindex_items(value)",
                Ct)).Error);
            CSharpDbRuntimeDiagnosticsState stateBefore = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
                if (phase == MaintenanceOperationPhase.Staging &&
                    staging.TrySetResult())
                {
                    releaseStaging.Wait(Ct);
                }
            };

            Task<ReindexResult> reindex = Task.Run(
                () => client.ReindexAsync(
                    new ReindexRequest
                    {
                        Scope = ReindexScope.Index,
                        Name = "idx_reindex_items_value",
                    },
                    Ct),
                Ct);
            await staging.Task.WaitAsync(Ct);

            MaintenanceRuntimeRecord active = Assert.Single(
                GetClientRegistry(client).Capture().Active);
            Assert.Equal(MaintenanceOperationKind.Reindex, active.Kind);
            Assert.Equal(MaintenanceOperationPhase.Staging, active.Phase);
            Assert.Equal(1, active.CompletedUnits);
            Assert.Equal(1, active.TotalUnits);
            CSharpDbRuntimeDiagnosticsState replacementState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(stateBefore, replacementState);
            Assert.Null(MaintenanceRuntimeDiagnostics.TryGet(replacementState));

            releaseStaging.Set();
            ReindexResult result = await reindex;
            Assert.Equal(1, result.RebuiltIndexCount);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(active.Context.OperationId, terminal.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            Assert.Equal(1, terminal.CompletedUnits);
            Assert.Equal(1, terminal.TotalUnits);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.MaintenanceCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);

            long epochBeforeReopen = replacementState.CounterEpoch;
            Assert.Null((await client.ExecuteSqlAsync(
                "SELECT * FROM reindex_items",
                Ct)).Error);
            Assert.True(replacementState.CounterEpoch > epochBeforeReopen);
            Assert.Equal(
                terminal.Context.OperationId,
                Assert.Single(GetClientRegistry(client).Capture().Recent)
                    .Context.OperationId);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Copying,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseStaging.Set();
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ReindexAdmissionConflict_IsRejectedWhileStillQueued()
    {
        string databasePath = CreateDatabasePath("maintenance-reindex-rejected");
        var phases = new List<MaintenanceOperationPhase>();
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-reindex-rejected"));
        TransactionSessionInfo? transaction = null;

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE reindex_rejected_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX idx_reindex_rejected ON reindex_rejected_items(value)",
                Ct)).Error);
            transaction = await client.BeginTransactionAsync(Ct);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.ReindexAsync(
                    new ReindexRequest
                    {
                        Scope = ReindexScope.Index,
                        Name = "idx_reindex_rejected",
                    },
                    Ct));

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(MaintenanceOperationKind.Reindex, terminal.Kind);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, terminal.Outcome);
            Assert.Equal("csharpdb.busy", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.MaintenanceCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, lifecycle.Outcome);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            Assert.Contains(MaintenanceOperationPhase.Queued, recorded);
            Assert.DoesNotContain(
                MaintenanceOperationPhase.AcquiringAccess,
                recorded);
            Assert.Contains(MaintenanceOperationPhase.Completed, recorded);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            if (transaction is not null)
            {
                await client.RollbackTransactionAsync(
                    transaction.TransactionId,
                    Ct);
            }
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientDispose_WaitsForInFlightReindexAndPreventsResurrection()
    {
        string databasePath = CreateDatabasePath("maintenance-reindex-dispose");
        var staging = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseStaging = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-reindex-dispose"));
        Task<ReindexResult>? reindex = null;
        Task? disposal = null;

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE reindex_dispose_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX idx_reindex_dispose ON reindex_dispose_items(value)",
                Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase == MaintenanceOperationPhase.Staging &&
                    staging.TrySetResult())
                {
                    releaseStaging.Wait(Ct);
                }
            };

            reindex = Task.Run(
                () => client.ReindexAsync(
                    new ReindexRequest
                    {
                        Scope = ReindexScope.Index,
                        Name = "idx_reindex_dispose",
                    },
                    Ct),
                Ct);
            await staging.Task.WaitAsync(Ct);
            MaintenanceRuntimeRecord active = Assert.Single(
                GetClientRegistry(client).Capture().Active);

            disposal = client.DisposeAsync().AsTask();
            await Task.Yield();
            Assert.False(disposal.IsCompleted);
            releaseStaging.Set();

            ReindexResult result = await reindex;
            Assert.Equal(1, result.RebuiltIndexCount);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.MaintenanceCompleted.Name));
            Assert.Equal(active.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);
            await disposal.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => client.ReindexAsync(
                    new ReindexRequest
                    {
                        Scope = ReindexScope.Index,
                        Name = "idx_reindex_dispose",
                    },
                    Ct));
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseStaging.Set();
            if (reindex is not null)
            {
                try { _ = await reindex; } catch { }
            }
            if (disposal is not null)
            {
                try { await disposal; } catch { }
            }
            else
            {
                await client.DisposeAsync();
            }
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientForeignKeyMigration_ProjectsValidationAndApplyProgress()
    {
        string databasePath = CreateDatabasePath("maintenance-fk-migration");
        var phases = new List<MaintenanceOperationPhase>();
        var staging = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseStaging = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-fk-migration"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE migration_parents (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE migration_children (id INTEGER PRIMARY KEY, parent_id INTEGER)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO migration_parents VALUES (1)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO migration_children VALUES (10, 1)",
                Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
                if (phase == MaintenanceOperationPhase.Staging &&
                    staging.TrySetResult())
                {
                    releaseStaging.Wait(Ct);
                }
            };

            Task<ForeignKeyMigrationResult> migration = Task.Run(
                () => client.MigrateForeignKeysAsync(
                    new ForeignKeyMigrationRequest
                    {
                        Constraints =
                        [
                            new ForeignKeyMigrationConstraintSpec
                            {
                                TableName = "migration_children",
                                ColumnName = "parent_id",
                                ReferencedTableName = "migration_parents",
                                ReferencedColumnName = "id",
                            },
                        ],
                    },
                    Ct),
                Ct);
            await staging.Task.WaitAsync(Ct);

            MaintenanceRuntimeRecord active = Assert.Single(
                GetClientRegistry(client).Capture().Active);
            Assert.Equal(
                MaintenanceOperationKind.ForeignKeyMigration,
                active.Kind);
            Assert.Equal(MaintenanceOperationPhase.Staging, active.Phase);
            Assert.Equal(1, active.CompletedUnits);
            Assert.Equal(1, active.TotalUnits);

            releaseStaging.Set();
            ForeignKeyMigrationResult result = await migration;
            Assert.True(result.Succeeded);
            Assert.Equal(1, result.AffectedTables);
            Assert.Equal(1, result.AppliedForeignKeys);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(active.Context.OperationId, terminal.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            Assert.Equal(1, terminal.CompletedUnits);
            Assert.Equal(1, terminal.TotalUnits);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.MaintenanceCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Copying,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseStaging.Set();
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientVacuum_ProjectsTruthfulPhasesAndExactLifecycleIdentity()
    {
        string databasePath = CreateDatabasePath("maintenance-vacuum");
        var phases = new List<MaintenanceOperationPhase>();
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-vacuum"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE vacuum_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO vacuum_items VALUES (1, 'one')",
                Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            VacuumResult result = await client.VacuumAsync(Ct);

            Assert.True(result.DatabaseFileBytesAfter > 0);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(MaintenanceOperationKind.Vacuum, terminal.Kind);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            Assert.Null(terminal.CompletedUnits);
            Assert.Null(terminal.TotalUnits);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.MaintenanceCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Copying,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Replacing,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientBackup_ProjectsFullPhasesAndSharesExactLifecycleIdentity()
    {
        string databasePath = CreateDatabasePath("maintenance-client");
        string backupPath = CreateDatabasePath("maintenance-backup");
        string manifestPath = backupPath + ".manifest.json";
        var phases = new List<MaintenanceOperationPhase>();
        var copying = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseCopy = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-client"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE maintenance_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO maintenance_items VALUES (1, 'value')",
                Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
                if (phase == MaintenanceOperationPhase.Copying &&
                    copying.TrySetResult())
                {
                    releaseCopy.Wait(Ct);
                }
            };

            Task<BackupResult> backup = client.BackupAsync(
                new BackupRequest
                {
                    DestinationPath = backupPath,
                    WithManifest = true,
                },
                Ct);
            await copying.Task.WaitAsync(Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> active =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            MaintenanceOperationSnapshot operation = Assert.IsType<
                MaintenanceOperationSnapshot>(active.Aggregate.ActiveMaintenance.Value);
            Assert.Equal(MaintenanceOperationKind.Backup, operation.Kind);
            Assert.Equal(MaintenanceOperationPhase.Copying, operation.Phase);
            Assert.Equal(active.Metadata, operation.Metadata);
            Assert.Null(
                MaintenanceRuntimeDiagnostics.TryGet(
                    client.CurrentRuntimeDiagnosticsState));
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
                activeOperations = (await diagnostics
                    .GetActiveMaintenanceOperationsAsync(1, Ct)).Aggregate;
            MaintenanceOperationSnapshot dedicatedActive = Assert.Single(
                Assert.IsAssignableFrom<IReadOnlyList<MaintenanceOperationSnapshot>>(
                    activeOperations.Records));
            Assert.Equal(operation.OperationId, dedicatedActive.OperationId);
            Assert.Equal(activeOperations.Metadata, dedicatedActive.Metadata);
            Assert.Equal(8, activeOperations.Capacity);
            Assert.Null(activeOperations.Retention);

            releaseCopy.Set();
            BackupResult result = await backup;
            Assert.True(result.DatabaseFileBytes > 0);
            Assert.True(File.Exists(manifestPath));

            CSharpDbLifecycleCompletedEvent completed = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, completed.Outcome);
            Assert.Equal(operation.OperationId, completed.Context.OperationId);
            MaintenanceRuntimeDiagnosticsCapture history =
                GetClientRegistry(client).Capture();
            MaintenanceRuntimeRecord recent = Assert.Single(history.Recent);
            Assert.Equal(operation.OperationId, recent.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, recent.Outcome);
            Assert.Equal(result.DatabaseFileBytes, recent.CompletedUnits);
            Assert.Equal(result.DatabaseFileBytes, recent.TotalUnits);
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
                recentOperations = (await diagnostics
                    .GetRecentMaintenanceOperationsAsync(1, Ct)).Aggregate;
            MaintenanceOperationSnapshot dedicatedRecent = Assert.Single(
                Assert.IsAssignableFrom<IReadOnlyList<MaintenanceOperationSnapshot>>(
                    recentOperations.Records));
            Assert.Equal(operation.OperationId, dedicatedRecent.OperationId);
            Assert.Equal(recentOperations.Metadata, dedicatedRecent.Metadata);
            Assert.Equal(8, recentOperations.Capacity);
            Assert.Equal(TimeSpan.FromMinutes(10), recentOperations.Retention);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Checkpointing,
                MaintenanceOperationPhase.Copying,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Hashing,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseCopy.Set();
            DeleteDatabaseArtifacts(backupPath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task QueuedBackupCancellation_IsTerminalOnceAndMarksOldestSelectionTruncated()
    {
        string databasePath = CreateDatabasePath("maintenance-queue");
        string firstPath = CreateDatabasePath("maintenance-queue-first");
        string secondPath = CreateDatabasePath("maintenance-queue-second");
        var firstCopying = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondQueued = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseFirst = new ManualResetEventSlim(initialState: false);
        int queuedCount = 0;
        int copyClaim = 0;
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-queue"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE queue_items (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO queue_items VALUES (1)",
                Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase == MaintenanceOperationPhase.Queued &&
                    Interlocked.Increment(ref queuedCount) == 2)
                {
                    secondQueued.TrySetResult();
                }

                if (phase == MaintenanceOperationPhase.Copying &&
                    Interlocked.CompareExchange(ref copyClaim, 1, 0) == 0)
                {
                    firstCopying.TrySetResult();
                    releaseFirst.Wait(Ct);
                }
            };

            Task<BackupResult> first = client.BackupAsync(
                new BackupRequest { DestinationPath = firstPath },
                Ct);
            await firstCopying.Task.WaitAsync(Ct);

            using var cancellation =
                CancellationTokenSource.CreateLinkedTokenSource(Ct);
            Task<BackupResult> second = client.BackupAsync(
                new BackupRequest { DestinationPath = secondPath },
                cancellation.Token);
            await secondQueued.Task.WaitAsync(Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> active =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            MaintenanceOperationSnapshot selected = Assert.IsType<
                MaintenanceOperationSnapshot>(active.Aggregate.ActiveMaintenance.Value);
            Assert.Equal(MaintenanceOperationPhase.Copying, selected.Phase);
            Assert.True(active.Metadata.RecordsTruncated);

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second);
            MaintenanceRuntimeDiagnosticsCapture duringFirst =
                GetClientRegistry(client).Capture();
            Assert.Single(duringFirst.Active);
            MaintenanceRuntimeRecord canceled = Assert.Single(
                duringFirst.Recent,
                static record =>
                    record.Outcome == CSharpDbOperationOutcome.Canceled);
            Assert.Equal("operation_canceled", canceled.Error!.Code);

            releaseFirst.Set();
            await first;
            CSharpDbLifecycleCompletedEvent[] lifecycle =
                events.Events(CSharpDbLogEvents.BackupCompleted.Name);
            Assert.Equal(2, lifecycle.Length);
            Assert.Single(
                lifecycle,
                static item =>
                    item.Outcome == CSharpDbOperationOutcome.Canceled);
            Assert.Single(
                lifecycle,
                static item =>
                    item.Outcome == CSharpDbOperationOutcome.Succeeded);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseFirst.Set();
            DeleteDatabaseArtifacts(secondPath);
            DeleteDatabaseArtifacts(firstPath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task BackupAdmissionConflict_IsRejectedWithReviewedBusyProjection()
    {
        string databasePath = CreateDatabasePath("maintenance-rejected");
        string backupPath = CreateDatabasePath("maintenance-rejected-backup");
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-rejected"));
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);

        try
        {
            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.BackupAsync(
                    new BackupRequest { DestinationPath = backupPath },
                    Ct));

            MaintenanceRuntimeRecord recent = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, recent.Outcome);
            Assert.Equal("csharpdb.busy", recent.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Rejected, lifecycle.Outcome);
            Assert.Equal(recent.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal("csharpdb.busy", lifecycle.Error!.Code);
        }
        finally
        {
            await client.RollbackTransactionAsync(transaction.TransactionId, Ct);
            DeleteDatabaseArtifacts(backupPath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task DirectBackup_UsesStateFallbackWithoutCreatingClientDuplicate()
    {
        string databasePath = CreateDatabasePath("maintenance-direct");
        string backupPath = CreateDatabasePath("maintenance-direct-backup");
        var copying = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseCopy = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        await using var client = CreateMemoryClient(
            databasePath,
            CreateOptions("maintenance-direct"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        try
        {
            _ = await client.BeginTransactionAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE direct_backup_items (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase == MaintenanceOperationPhase.Copying &&
                    copying.TrySetResult())
                {
                    releaseCopy.Wait(Ct);
                }
            };

            Task<DatabaseBackupResult> backup = Task.Run(
                async () => await DatabaseBackupCoordinator.BackupAsync(
                    database,
                    databasePath,
                    backupPath,
                    withManifest: false,
                    Ct),
                Ct);
            await copying.Task.WaitAsync(Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> active =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            MaintenanceOperationSnapshot operation = Assert.IsType<
                MaintenanceOperationSnapshot>(active.Aggregate.ActiveMaintenance.Value);
            Assert.Equal(MaintenanceOperationPhase.Copying, operation.Phase);
            Assert.Equal(2, active.RuntimeFamilies!.Count);
            RuntimeDiagnosticsFamilySection<RuntimeDiagnosticsSnapshot>
                exactWithFallback = Assert.Single(
                    active.RuntimeFamilies,
                    static family =>
                        family.Value.ActiveMaintenance.Availability ==
                        DiagnosticsAvailability.Available);
            Assert.Equal(
                operation.OperationId,
                exactWithFallback.Value.ActiveMaintenance.Value!.OperationId);
            Assert.Single(
                active.RuntimeFamilies,
                static family =>
                    family.Value.ActiveMaintenance.Availability ==
                    DiagnosticsAvailability.NotApplicable);
            Assert.Null(GetClientRegistryOrNull(client));
            Assert.NotNull(
                MaintenanceRuntimeDiagnostics.TryGet(
                    client.CurrentRuntimeDiagnosticsState));

            releaseCopy.Set();
            await backup;
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(operation.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseCopy.Set();
            DeleteDatabaseArtifacts(backupPath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ValidateOnly_SurvivesFamilyResetAndClientDisposeWithoutExactDuplication()
    {
        string databasePath = CreateDatabasePath("maintenance-family");
        string sourcePath = await CreateValidSnapshotAsync();
        var validating = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseValidation = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        var client = CreateMemoryClient(
            databasePath,
            CreateOptions("maintenance-family"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        Task<RestoreResult>? validation = null;
        Task? disposal = null;

        try
        {
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 1", Ct)).Error);
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase == MaintenanceOperationPhase.Validating &&
                    validating.TrySetResult())
                {
                    releaseValidation.Wait(Ct);
                }
            };
            validation = Task.Run(
                () => client.RestoreAsync(
                    new RestoreRequest
                    {
                        SourcePath = sourcePath,
                        ValidateOnly = true,
                    },
                    Ct),
                Ct);
            await validating.Task.WaitAsync(Ct);

            await client.ReleaseCachedDatabaseAsync(Ct);
            Assert.Null((await client.ExecuteSqlAsync("SELECT 2", Ct)).Error);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            MaintenanceOperationSnapshot active = Assert.IsType<
                MaintenanceOperationSnapshot>(runtime.Aggregate.ActiveMaintenance.Value);
            Assert.Equal(MaintenanceOperationKind.RestoreValidation, active.Kind);
            Assert.Equal(MaintenanceOperationPhase.Validating, active.Phase);
            Assert.Equal(runtime.Metadata, active.Metadata);
            Assert.Equal(2, runtime.RuntimeFamilies!.Count);
            Assert.All(
                runtime.RuntimeFamilies,
                static family => Assert.Equal(
                    DiagnosticsAvailability.NotApplicable,
                    family.Value.ActiveMaintenance.Availability));

            disposal = client.DisposeAsync().AsTask();
            await Task.Yield();
            Assert.False(disposal.IsCompleted);
            releaseValidation.Set();
            RestoreResult result = await validation;
            Assert.True(result.ValidateOnly);
            await disposal;
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(active.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);
            _ = transaction;
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseValidation.Set();
            if (validation is not null)
            {
                try { await validation; } catch { }
            }
            if (disposal is not null)
            {
                try { await disposal; } catch { }
            }
            else
            {
                await client.DisposeAsync();
            }
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientFullRestore_ProjectsExactPhasesAndEagerlyCachesOneReplacementOpen()
    {
        string databasePath = CreateDatabasePath("maintenance-full-restore");
        string sourcePath = CreateDatabasePath("maintenance-full-restore-source");
        var phases = new List<MaintenanceOperationPhase>();
        var reopening = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseReopen = new ManualResetEventSlim(initialState: false);
        using var events = new LifecycleRecorder();
        var configuredOptions = CreateOptions("maintenance-full-restore");
        var openedOptions = new List<DatabaseOptions>();
        int openCount = 0;
        await using var client = new EngineTransportClient(
            databasePath,
            async (path, options, ct) =>
            {
                Interlocked.Increment(ref openCount);
                lock (openedOptions)
                    openedOptions.Add(options);
                return await Database.OpenAsync(path, options, ct);
            },
            configuredOptions);
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        try
        {
            await CreateSnapshotWithTableAsync(
                sourcePath,
                "restored_items",
                Ct);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE original_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO original_items VALUES (1, 'original')",
                Ct)).Error);
            CSharpDbRuntimeDiagnosticsState originalState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            long originalEpoch = originalState.CounterEpoch;

            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
                if (phase == MaintenanceOperationPhase.Reopening &&
                    reopening.TrySetResult())
                {
                    releaseReopen.Wait(Ct);
                }
            };

            Task<RestoreResult> restore = Task.Run(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct),
                Ct);
            await reopening.Task.WaitAsync(Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> active =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            MaintenanceOperationSnapshot operation = Assert.IsType<
                MaintenanceOperationSnapshot>(
                active.Aggregate.ActiveMaintenance.Value);
            Assert.Equal(MaintenanceOperationKind.Restore, operation.Kind);
            Assert.Equal(
                MaintenanceOperationPhase.Reopening,
                operation.Phase);
            Assert.Equal(active.Metadata, operation.Metadata);
            Assert.Null(MaintenanceRuntimeDiagnostics.TryGet(
                client.CurrentRuntimeDiagnosticsState));

            releaseReopen.Set();
            RestoreResult result = await restore;
            Assert.False(result.ValidateOnly);
            Assert.Equal(Path.GetFullPath(databasePath), result.DestinationPath);

            SqlExecutionResult restored = await client.ExecuteSqlAsync(
                "SELECT value FROM restored_items WHERE id = 1",
                Ct);
            Assert.Null(restored.Error);
            Assert.Equal("restored", Assert.Single(restored.Rows!)[0]);
            Assert.Equal(2, Volatile.Read(ref openCount));
            DatabaseOptions[] capturedOptions;
            lock (openedOptions)
                capturedOptions = openedOptions.ToArray();
            Assert.Equal(2, capturedOptions.Length);
            Assert.All(
                capturedOptions,
                options => Assert.Same(
                    configuredOptions.StorageEngineOptions,
                    options.StorageEngineOptions));

            CSharpDbRuntimeDiagnosticsState replacementState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(originalState, replacementState);
            Assert.Equal(
                originalState.ServerInstanceId,
                replacementState.ServerInstanceId);
            Assert.Equal(originalEpoch + 1, replacementState.CounterEpoch);

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(operation.OperationId, terminal.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, terminal.Outcome);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(operation.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycle.Outcome);

            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Copying,
                MaintenanceOperationPhase.Staging,
                MaintenanceOperationPhase.Validating,
                MaintenanceOperationPhase.Replacing,
                MaintenanceOperationPhase.Reopening,
                MaintenanceOperationPhase.Completed);
            Assert.DoesNotContain(
                MaintenanceOperationPhase.ReopenPending,
                recorded);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseReopen.Set();
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_PrivateMemoryIsRejectedBeforeDetachAndLeavesCacheUsable()
    {
        string sourcePath = await CreateValidSnapshotAsync();
        using var events = new LifecycleRecorder();
        await using var client = CreateMemoryClient(
            ":memory:maintenance-full-restore-rejected",
            CreateOptions("maintenance-full-restore-rejected"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE memory_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO memory_items VALUES (1, 'retained')",
                Ct)).Error);
            Database cached = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);

            CSharpDbClientException failure =
                await Assert.ThrowsAsync<CSharpDbClientException>(
                    () => client.RestoreAsync(
                        new RestoreRequest { SourcePath = sourcePath },
                        Ct));
            Assert.Contains(
                "file-backed",
                failure.Message,
                StringComparison.OrdinalIgnoreCase);

            Assert.Same(
                cached,
                Assert.IsType<Database>(await client.TryGetDatabaseAsync(Ct)));
            Assert.Same(state, client.CurrentRuntimeDiagnosticsState);
            SqlExecutionResult retained = await client.ExecuteSqlAsync(
                "SELECT value FROM memory_items WHERE id = 1",
                Ct);
            Assert.Null(retained.Error);
            Assert.Equal("retained", Assert.Single(retained.Rows!)[0]);

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, terminal.Outcome);
            Assert.Equal("client_configuration", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, lifecycle.Outcome);
        }
        finally
        {
            DeleteDatabaseArtifacts(sourcePath);
        }
    }

    [Fact]
    public async Task FullRestore_ReplacementOpenFailureRollsBackAndAdoptsOriginalOnce()
    {
        string databasePath = CreateDatabasePath("maintenance-restore-rollback");
        string sourcePath = CreateDatabasePath(
            "maintenance-restore-rollback-source");
        var phases = new List<MaintenanceOperationPhase>();
        using var events = new LifecycleRecorder();
        int openCount = 0;
        await using var client = new EngineTransportClient(
            databasePath,
            async (path, options, ct) =>
            {
                int currentOpen = Interlocked.Increment(ref openCount);
                if (currentOpen == 2)
                {
                    throw new IOException(
                        "injected replacement reopen failure");
                }

                return await Database.OpenAsync(path, options, ct);
            },
            CreateOptions("maintenance-restore-rollback"));

        try
        {
            await CreateSnapshotWithTableAsync(
                sourcePath,
                "replacement_items",
                Ct);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE original_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO original_items VALUES (1, 'original')",
                Ct)).Error);
            CSharpDbRuntimeDiagnosticsState originalState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            long originalCounterEpoch = originalState.CounterEpoch;
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            await Assert.ThrowsAsync<IOException>(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct));

            Assert.Equal(3, Volatile.Read(ref openCount));
            SqlExecutionResult retained = await client.ExecuteSqlAsync(
                "SELECT value FROM original_items WHERE id = 1",
                Ct);
            Assert.Null(retained.Error);
            Assert.Equal("original", Assert.Single(retained.Rows!)[0]);
            Assert.Equal(3, Volatile.Read(ref openCount));
            CSharpDbRuntimeDiagnosticsState recoveredState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            Assert.NotSame(originalState, recoveredState);
            Assert.Equal(
                originalState.ServerInstanceId,
                recoveredState.ServerInstanceId);
            Assert.Equal(
                originalCounterEpoch + 1,
                recoveredState.CounterEpoch);

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Failed, terminal.Outcome);
            Assert.Equal("csharpdb.io", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Failed, lifecycle.Outcome);
            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Replacing,
                MaintenanceOperationPhase.Reopening,
                MaintenanceOperationPhase.RollingBack,
                MaintenanceOperationPhase.Reopening,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_DetachDisposalFailureReopensOriginalWithoutFamilyReset()
    {
        string databasePath = CreateDatabasePath("maintenance-dispose-recovery");
        string sourcePath = await CreateValidSnapshotAsync();
        using var events = new LifecycleRecorder();
        var phases = new List<MaintenanceOperationPhase>();
        int openCount = 0;
        await using var client = new EngineTransportClient(
            databasePath,
            async (path, options, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenAsync(path, options, ct);
            },
            CreateOptions("maintenance-dispose-recovery"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE retained_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO retained_items VALUES (1, 'retained')",
                Ct)).Error);
            Database detached = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            CSharpDbRuntimeDiagnosticsState originalState = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);
            long originalCounterEpoch = originalState.CounterEpoch;
            EngineTransportClient.DisposeExclusiveDatabaseForTests =
                async database =>
                {
                    await database.DisposeAsync();
                    if (ReferenceEquals(database, detached))
                    {
                        throw new IOException(
                            "injected exclusive detach disposal failure");
                    }
                };
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };

            await Assert.ThrowsAsync<IOException>(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct));
            EngineTransportClient.DisposeExclusiveDatabaseForTests = null;

            Assert.Same(originalState, client.CurrentRuntimeDiagnosticsState);
            Assert.Equal(
                originalCounterEpoch,
                originalState.CounterEpoch);
            Assert.Equal(2, Volatile.Read(ref openCount));
            Database recovered = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            Assert.NotSame(detached, recovered);
            Assert.Equal(2, Volatile.Read(ref openCount));
            SqlExecutionResult retained = await client.ExecuteSqlAsync(
                "SELECT value FROM retained_items WHERE id = 1",
                Ct);
            Assert.Null(retained.Error);
            Assert.Equal("retained", Assert.Single(retained.Rows!)[0]);
            Assert.Equal(2, Volatile.Read(ref openCount));

            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Failed, terminal.Outcome);
            Assert.Equal("csharpdb.io", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Failed, lifecycle.Outcome);
            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            AssertSubsequence(
                recorded,
                MaintenanceOperationPhase.Queued,
                MaintenanceOperationPhase.AcquiringAccess,
                MaintenanceOperationPhase.Reopening,
                MaintenanceOperationPhase.Completed);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            EngineTransportClient.DisposeExclusiveDatabaseForTests = null;
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_LazyPrePonrFailureDoesNotOpenDestination()
    {
        string databasePath = CreateDatabasePath("maintenance-lazy-restore");
        string missingSourcePath = CreateDatabasePath(
            "maintenance-lazy-restore-missing");
        int openCount = 0;
        await using var client = new EngineTransportClient(
            databasePath,
            async (path, options, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenAsync(path, options, ct);
            },
            CreateOptions("maintenance-lazy-restore"));
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(
            client.CurrentRuntimeDiagnosticsState);

        try
        {
            await Assert.ThrowsAsync<FileNotFoundException>(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = missingSourcePath },
                    Ct));

            Assert.Equal(0, Volatile.Read(ref openCount));
            Assert.Same(state, client.CurrentRuntimeDiagnosticsState);
            Assert.Equal(0, state.CounterEpoch);
            MaintenanceRuntimeDiagnosticsCapture capture =
                GetClientRegistry(client).Capture();
            Assert.Empty(capture.Active);
            Assert.Equal(
                CSharpDbOperationOutcome.Failed,
                Assert.Single(capture.Recent).Outcome);
        }
        finally
        {
            DeleteDatabaseArtifacts(missingSourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_ActiveTransactionIsRejectedBeforeFamilyOrCacheMutation()
    {
        string databasePath = CreateDatabasePath("maintenance-restore-busy");
        string sourcePath = await CreateValidSnapshotAsync();
        var phases = new List<MaintenanceOperationPhase>();
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-restore-busy"));
        TransactionSessionInfo transaction = await client.BeginTransactionAsync(Ct);
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(
            client.CurrentRuntimeDiagnosticsState);

        try
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                lock (phases)
                    phases.Add(phase);
            };
            await Assert.ThrowsAsync<CSharpDbClientException>(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct));

            Assert.Same(state, client.CurrentRuntimeDiagnosticsState);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, terminal.Outcome);
            Assert.Equal("csharpdb.busy", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, lifecycle.Outcome);
            MaintenanceOperationPhase[] recorded;
            lock (phases)
                recorded = phases.ToArray();
            Assert.Contains(MaintenanceOperationPhase.Queued, recorded);
            Assert.DoesNotContain(
                MaintenanceOperationPhase.AcquiringAccess,
                recorded);

            SqlExecutionResult transactionQuery =
                await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT 1",
                    Ct);
            Assert.Null(transactionQuery.Error);
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            await client.RollbackTransactionAsync(
                transaction.TransactionId,
                Ct);
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_ActiveReaderIsRejectedAndRestoresExactCachedHandle()
    {
        string databasePath = CreateDatabasePath("maintenance-restore-reader");
        string sourcePath = await CreateValidSnapshotAsync();
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-restore-reader"));

        try
        {
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE reader_items (id INTEGER PRIMARY KEY, value TEXT)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO reader_items VALUES (1, 'retained')",
                Ct)).Error);
            Database cached = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                client.CurrentRuntimeDiagnosticsState);

            using (Database.ReaderSession reader = cached.CreateReaderSession())
            {
                await Assert.ThrowsAsync<CSharpDbClientException>(
                    () => client.RestoreAsync(
                        new RestoreRequest { SourcePath = sourcePath },
                        Ct));
                Assert.Same(
                    cached,
                    Assert.IsType<Database>(
                        await client.TryGetDatabaseAsync(Ct)));
            }

            Assert.Same(state, client.CurrentRuntimeDiagnosticsState);
            SqlExecutionResult retained = await client.ExecuteSqlAsync(
                "SELECT value FROM reader_items WHERE id = 1",
                Ct);
            Assert.Null(retained.Error);
            Assert.Equal("retained", Assert.Single(retained.Rows!)[0]);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                GetClientRegistry(client).Capture().Recent);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, terminal.Outcome);
            Assert.Equal("csharpdb.busy", terminal.Error!.Code);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Rejected, lifecycle.Outcome);
        }
        finally
        {
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task FullRestore_DisposeWinsQueuedExclusiveLockWithoutMutationOrResurrection()
    {
        string databasePath = CreateDatabasePath("maintenance-restore-dispose");
        string sourcePath = await CreateValidSnapshotAsync();
        var openEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var queued = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseQueued = new ManualResetEventSlim(
            initialState: false);
        using var releaseCompleted = new ManualResetEventSlim(
            initialState: false);
        using var events = new LifecycleRecorder();
        int openCount = 0;
        var client = new EngineTransportClient(
            databasePath,
            async (path, options, ct) =>
            {
                if (Interlocked.Increment(ref openCount) == 1)
                {
                    openEntered.TrySetResult();
                    await allowOpen.Task.WaitAsync(ct);
                }

                return await Database.OpenAsync(path, options, ct);
            },
            CreateOptions("maintenance-restore-dispose"));
        Task<Database?>? initialOpen = null;
        Task? release = null;
        Task? disposal = null;
        Task<RestoreResult>? restore = null;

        try
        {
            initialOpen = client.TryGetDatabaseAsync(
                CancellationToken.None).AsTask();
            await openEntered.Task.WaitAsync(Ct);
            release = client.ReleaseCachedDatabaseAsync(Ct).AsTask();
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = phase =>
            {
                if (phase == MaintenanceOperationPhase.Queued &&
                    queued.TrySetResult())
                {
                    releaseQueued.Wait(Ct);
                }
                if (phase == MaintenanceOperationPhase.Completed &&
                    completed.TrySetResult())
                {
                    releaseCompleted.Wait(Ct);
                }
            };
            restore = Task.Run(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct),
                Ct);
            await queued.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            MaintenanceRuntimeDiagnostics registry =
                GetClientRegistry(client);
            MaintenanceRuntimeRecord active = Assert.Single(
                registry.Capture().Active);

            disposal = client.DisposeAsync().AsTask();
            releaseQueued.Set();
            allowOpen.TrySetResult();
            await release.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            await completed.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            MaintenanceRuntimeDiagnosticsCapture terminalCapture =
                registry.Capture();
            Assert.Empty(terminalCapture.Active);
            MaintenanceRuntimeRecord terminal = Assert.Single(
                terminalCapture.Recent);
            Assert.Equal(active.Context.OperationId, terminal.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Failed, terminal.Outcome);

            releaseCompleted.Set();
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => restore);
            await disposal.WaitAsync(TimeSpan.FromSeconds(10), Ct);
            _ = await initialOpen;
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.RestoreCompleted.Name));
            Assert.Equal(active.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Failed, lifecycle.Outcome);
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => client.RestoreAsync(
                    new RestoreRequest { SourcePath = sourcePath },
                    Ct));
        }
        finally
        {
            MaintenanceRuntimeDiagnostics.PhaseChangedForTests = null;
            releaseQueued.Set();
            allowOpen.TrySetResult();
            releaseCompleted.Set();
            if (restore is not null)
            {
                try { await restore; } catch { }
            }
            if (release is not null)
            {
                try { await release; } catch { }
            }
            if (initialOpen is not null)
            {
                try { _ = await initialOpen; } catch { }
            }
            if (disposal is not null)
            {
                try { await disposal; } catch { }
            }
            else
            {
                await client.DisposeAsync();
            }
            DeleteDatabaseArtifacts(sourcePath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task DedicatedMaintenanceCollections_EmptySupportDoesNotCreateRegistry()
    {
        await using var client = CreateMemoryClient(
            ":memory:maintenance-empty-dedicated",
            CreateOptions("maintenance-empty-dedicated"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot> active =
            (await diagnostics.GetActiveMaintenanceOperationsAsync(3, Ct)).Aggregate;
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot> recent =
            (await diagnostics.GetRecentMaintenanceOperationsAsync(3, Ct)).Aggregate;
        RuntimeDiagnosticsSnapshot summary =
            (await diagnostics.GetRuntimeDiagnosticsAsync(Ct)).Aggregate;

        Assert.Equal(DiagnosticsAvailability.Available, active.Metadata.Availability);
        Assert.Empty(active.Records!);
        Assert.Equal(8, active.Capacity);
        Assert.Null(active.Retention);
        Assert.False(active.IsTruncated);
        Assert.Equal(DiagnosticsAvailability.Available, recent.Metadata.Availability);
        Assert.Empty(recent.Records!);
        Assert.Equal(8, recent.Capacity);
        Assert.Equal(TimeSpan.FromMinutes(10), recent.Retention);
        Assert.False(recent.IsTruncated);
        Assert.Equal(
            DiagnosticsAvailability.NotApplicable,
            summary.ActiveMaintenance.Availability);
        Assert.Null(GetClientRegistryOrNull(client));
        Assert.Null(MaintenanceRuntimeDiagnostics.TryGet(
            client.CurrentRuntimeDiagnosticsState));
    }

    [Fact]
    public async Task RuntimeDiagnostics_RetriesPromotionAndPublishesOneEpoch()
    {
        await using var client = CreateMemoryClient(
            ":memory:runtime-epoch-retry",
            CreateOptions("runtime-epoch-retry"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(client.CurrentRuntimeDiagnosticsState);
        long initialEpoch = state.CounterEpoch;
        int attempts = 0;
        EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = _ =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
                state.AdvanceCounterEpoch();
        };

        try
        {
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> topology =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);

            Assert.Equal(2, Volatile.Read(ref attempts));
            Assert.Equal(initialEpoch + 1, topology.Metadata.CounterEpoch);
            Assert.Equal(state.CounterEpoch, topology.Metadata.CounterEpoch);
            Assert.Equal(topology.Metadata, topology.Aggregate.Queries.Value!.Metadata);
            Assert.Equal(topology.Metadata, topology.Aggregate.Connections.Value!.Metadata);
        }
        finally
        {
            EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = null;
        }
    }

    [Fact]
    public async Task WalDiagnostics_RetriesPromotionAndReprojectsNestedDetail()
    {
        await using var client = CreateMemoryClient(
            ":memory:wal-epoch-retry",
            CreateOptions("wal-epoch-retry"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        Database database = Assert.IsType<Database>(
            await client.TryGetDatabaseAsync(Ct));
        Assert.Null((await client.ExecuteSqlAsync(
            "CREATE TABLE epoch_retry_items (id INTEGER PRIMARY KEY)",
            Ct)).Error);
        Assert.Null((await client.ExecuteSqlAsync(
            "INSERT INTO epoch_retry_items VALUES (1)",
            Ct)).Error);
        await database.CheckpointAsync(Ct);
        DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot> baseline =
            (await diagnostics.GetWalDiagnosticsAsync(Ct)).Aggregate;
        Assert.NotNull(baseline.Value?.Checkpoint.Value);
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(client.CurrentRuntimeDiagnosticsState);
        int attempts = 0;
        EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = _ =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
                state.AdvanceCounterEpoch();
        };

        try
        {
            DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot> result =
                (await diagnostics.GetWalDiagnosticsAsync(Ct)).Aggregate;

            Assert.Equal(2, Volatile.Read(ref attempts));
            WalRuntimeDiagnosticsSnapshot wal = Assert.IsType<
                WalRuntimeDiagnosticsSnapshot>(result.Value);
            CheckpointDiagnosticsSnapshot checkpoint = Assert.IsType<
                CheckpointDiagnosticsSnapshot>(wal.Checkpoint.Value);
            Assert.Equal(state.CounterEpoch, result.Metadata.CounterEpoch);
            Assert.Equal(result.Metadata, wal.Metadata);
            Assert.Equal(result.Metadata, checkpoint.Metadata);
            if (wal.Recovery.Value is { } recovery)
                Assert.Equal(result.Metadata, recovery.Metadata);
        }
        finally
        {
            EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = null;
        }
    }

    [Fact]
    public async Task MaintenanceCollection_RepeatedPromotionFailsIdentitySafe()
    {
        await using var client = CreateMemoryClient(
            ":memory:maintenance-epoch-churn",
            CreateOptions("maintenance-epoch-churn"));
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(client.CurrentRuntimeDiagnosticsState);
        long initialEpoch = state.CounterEpoch;
        int attempts = 0;
        EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = _ =>
        {
            Interlocked.Increment(ref attempts);
            state.AdvanceCounterEpoch();
        };

        try
        {
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
                MaintenanceOperationSnapshot>> topology = await diagnostics
                    .GetActiveMaintenanceOperationsAsync(1, Ct);

            Assert.Equal(3, Volatile.Read(ref attempts));
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                topology.Metadata.Availability);
            Assert.Equal(DiagnosticsScope.Aggregate, topology.Metadata.Scope);
            Assert.Equal(initialEpoch + 2, topology.Metadata.CounterEpoch);
            Assert.Null(topology.Aggregate.Records);
            Assert.Null(topology.RuntimeFamilies);
        }
        finally
        {
            EngineTransportClient.RuntimeDiagnosticsCaptureCompletedForTests = null;
        }
    }

    [Fact]
    public async Task DisabledPathDoesNotCreateRegistryAndPublicMethodGroupsRemainCompatible()
    {
        string databasePath = CreateDatabasePath("maintenance-disabled");
        string backupPath = CreateDatabasePath("maintenance-disabled-backup");
        await using var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions());
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);

        try
        {
            BackupResult backup = await client.BackupAsync(
                new BackupRequest { DestinationPath = backupPath },
                Ct);
            Assert.True(backup.DatabaseFileBytes > 0);
            RestoreResult validation = await client.RestoreAsync(
                new RestoreRequest
                {
                    SourcePath = backupPath,
                    ValidateOnly = true,
                },
                Ct);
            Assert.True(validation.ValidateOnly);
            RestoreResult restored = await client.RestoreAsync(
                new RestoreRequest { SourcePath = backupPath },
                Ct);
            Assert.False(restored.ValidateOnly);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE disabled_parents (id INTEGER PRIMARY KEY)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE disabled_children (id INTEGER PRIMARY KEY, parent_id INTEGER)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE INDEX idx_disabled_children_parent ON disabled_children(parent_id)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO disabled_parents VALUES (1)",
                Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync(
                "INSERT INTO disabled_children VALUES (10, 1)",
                Ct)).Error);
            await client.CheckpointAsync(Ct);
            ReindexResult reindex = await client.ReindexAsync(
                new ReindexRequest
                {
                    Scope = ReindexScope.Index,
                    Name = "idx_disabled_children_parent",
                },
                Ct);
            Assert.Equal(1, reindex.RebuiltIndexCount);
            ForeignKeyMigrationResult migration =
                await client.MigrateForeignKeysAsync(
                    new ForeignKeyMigrationRequest
                    {
                        ValidateOnly = true,
                        Constraints =
                        [
                            new ForeignKeyMigrationConstraintSpec
                            {
                                TableName = "disabled_children",
                                ColumnName = "parent_id",
                                ReferencedTableName = "disabled_parents",
                                ReferencedColumnName = "id",
                            },
                        ],
                    },
                    Ct);
            Assert.True(migration.Succeeded);
            _ = await client.VacuumAsync(Ct);
            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(Ct));
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
                await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
                activeMaintenance = (await diagnostics
                    .GetActiveMaintenanceOperationsAsync(2, Ct)).Aggregate;
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>
                recentMaintenance = (await diagnostics
                    .GetRecentMaintenanceOperationsAsync(2, Ct)).Aggregate;
            DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot> storage =
                (await diagnostics.GetStorageDiagnosticsAsync(Ct)).Aggregate;
            DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot> wal =
                (await diagnostics.GetWalDiagnosticsAsync(Ct)).Aggregate;
            Assert.Equal(
                DiagnosticsAvailability.Disabled,
                runtime.Aggregate.ActiveMaintenance.Availability);
            Assert.Equal(DiagnosticsAvailability.Disabled, activeMaintenance.Metadata.Availability);
            Assert.Equal(DiagnosticsAvailability.Disabled, recentMaintenance.Metadata.Availability);
            Assert.Equal(DiagnosticsAvailability.Disabled, storage.Metadata.Availability);
            Assert.Equal(DiagnosticsAvailability.Disabled, wal.Metadata.Availability);
            Assert.Null(activeMaintenance.Records);
            Assert.Null(recentMaintenance.Records);
            Assert.Null(storage.Value);
            Assert.Null(wal.Value);
            Assert.Null(GetClientRegistryOrNull(client));
            Assert.Null(client.CurrentRuntimeDiagnosticsState);
            CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
                CSharpDbRuntimeDiagnosticsState>(
                typeof(EngineTransportClient)
                    .GetField(
                        "_disabledRuntimeDiagnosticsState",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(client));
            Assert.Null(GetRuntimeComponents(state));
            Assert.Equal(0, GetPrivateField<int>(
                client,
                "_activeMaintenanceLifetimes"));
            Assert.Null(GetPrivateField<TaskCompletionSource?>(
                client,
                "_maintenanceLifetimesDrained"));

            Func<Database, string, string, bool, CancellationToken,
                ValueTask<DatabaseBackupResult>> backupMethod =
                DatabaseBackupCoordinator.BackupAsync;
            Func<string, CancellationToken, ValueTask<DatabaseRestoreResult>>
                validationMethod =
                    DatabaseBackupCoordinator.ValidateRestoreSourceAsync;
            Func<BackupRequest, CancellationToken, Task<BackupResult>>
                clientBackupMethod = client.BackupAsync;
            Func<CancellationToken, ValueTask> databaseCheckpointMethod =
                database.CheckpointAsync;
            Func<string, DatabaseReindexRequest, CancellationToken,
                ValueTask<DatabaseReindexResult>> reindexMethod =
                DatabaseMaintenanceCoordinator.ReindexAsync;
            Func<string, CancellationToken, ValueTask<DatabaseVacuumResult>>
                vacuumMethod = DatabaseMaintenanceCoordinator.VacuumAsync;
            Func<string, DatabaseForeignKeyMigrationRequest,
                CancellationToken,
                ValueTask<DatabaseForeignKeyMigrationResult>> migrationMethod =
                DatabaseMaintenanceCoordinator.MigrateForeignKeysAsync;
            Func<CancellationToken, Task> clientCheckpointMethod =
                client.CheckpointAsync;
            Func<ReindexRequest, CancellationToken, Task<ReindexResult>>
                clientReindexMethod = client.ReindexAsync;
            Func<CancellationToken, Task<VacuumResult>> clientVacuumMethod =
                client.VacuumAsync;
            Func<ForeignKeyMigrationRequest, CancellationToken,
                Task<ForeignKeyMigrationResult>> clientMigrationMethod =
                client.MigrateForeignKeysAsync;
            ICSharpDbClient publicClient = client;
            Func<CancellationToken, Task> publicCheckpointMethod =
                publicClient.CheckpointAsync;
            Func<ReindexRequest, CancellationToken, Task<ReindexResult>>
                publicReindexMethod = publicClient.ReindexAsync;
            Func<CancellationToken, Task<VacuumResult>> publicVacuumMethod =
                publicClient.VacuumAsync;
            Func<ForeignKeyMigrationRequest, CancellationToken,
                Task<ForeignKeyMigrationResult>> publicMigrationMethod =
                publicClient.MigrateForeignKeysAsync;
            GC.KeepAlive(backupMethod);
            GC.KeepAlive(validationMethod);
            GC.KeepAlive(clientBackupMethod);
            GC.KeepAlive(databaseCheckpointMethod);
            GC.KeepAlive(reindexMethod);
            GC.KeepAlive(vacuumMethod);
            GC.KeepAlive(migrationMethod);
            GC.KeepAlive(clientCheckpointMethod);
            GC.KeepAlive(clientReindexMethod);
            GC.KeepAlive(clientVacuumMethod);
            GC.KeepAlive(clientMigrationMethod);
            GC.KeepAlive(publicCheckpointMethod);
            GC.KeepAlive(publicReindexMethod);
            GC.KeepAlive(publicVacuumMethod);
            GC.KeepAlive(publicMigrationMethod);
        }
        finally
        {
            DeleteDatabaseArtifacts(backupPath);
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task ClientPathNormalizationFailure_TerminalizesExactlyOnceSafely()
    {
        const string canary = "maintenance-normalization-secret";
        string databasePath = CreateDatabasePath("maintenance-normalization");
        using var events = new LifecycleRecorder();
        await using var client = new EngineTransportClient(
            databasePath,
            CreateOptions("maintenance-normalization"));

        try
        {
            await Assert.ThrowsAnyAsync<ArgumentException>(
                () => client.BackupAsync(
                    new BackupRequest
                    {
                        DestinationPath = "\0" + canary,
                    },
                    Ct));

            MaintenanceRuntimeDiagnosticsCapture capture =
                GetClientRegistry(client).Capture();
            Assert.Empty(capture.Active);
            MaintenanceRuntimeRecord terminal = Assert.Single(capture.Recent);
            Assert.Equal(CSharpDbOperationOutcome.Failed, terminal.Outcome);
            Assert.Equal("invalid_argument", terminal.Error!.Code);
            Assert.DoesNotContain(
                canary,
                string.Join(
                    '|',
                    terminal.Error.Code,
                    terminal.Error.ErrorType,
                    terminal.Error.PublicDetail),
                StringComparison.Ordinal);
            CSharpDbLifecycleCompletedEvent lifecycle = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(terminal.Context.OperationId, lifecycle.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Failed, lifecycle.Outcome);
        }
        finally
        {
            DeleteDatabaseArtifacts(databasePath);
        }
    }

    [Fact]
    public async Task DroppedRuntimeFamily_MarksAggregateMaintenanceCoverageTruncated()
    {
        await using var client = CreateMemoryClient(
            ":memory:maintenance-dropped",
            CreateOptions("maintenance-dropped"));
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<
            CSharpDbRuntimeDiagnosticsState>(client.CurrentRuntimeDiagnosticsState);
        Type leaseType = typeof(EngineTransportClient).GetNestedType(
            "RuntimeDiagnosticsFamilyLease",
            BindingFlags.NonPublic)!;
        object lease = Activator.CreateInstance(
            leaseType,
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            args:
            [
                client,
                state,
                new[] { state },
                Array.Empty<CSharpDbRuntimeDiagnosticsState>(),
                1,
            ],
            culture: null)!;

        try
        {
            var topology = Assert.IsType<
                DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
                typeof(EngineTransportClient)
                    .GetMethod(
                        "CreateRuntimeDiagnosticsTopology",
                        BindingFlags.Instance | BindingFlags.NonPublic)!
                    .Invoke(client, [lease]));
            Assert.Equal(DiagnosticsScope.Aggregate, topology.Metadata.Scope);
            Assert.True(topology.Metadata.RecordsTruncated);
            Assert.Equal(
                DiagnosticsAvailability.Unavailable,
                topology.Aggregate.ActiveMaintenance.Availability);
            Assert.Equal(1, topology.DroppedRuntimeFamilyCount);
        }
        finally
        {
            ((IDisposable)lease).Dispose();
        }
    }

    [Fact]
    public void CheckpointOverlay_PreservesAuthoritativeFaultedPhaseOverNewerIdle()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        CheckpointDiagnosticsSnapshot faulted = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Faulted,
            CheckpointOrigin.Manual,
            started,
            completedAtUtc: started.AddSeconds(2),
            elapsed: TimeSpan.FromSeconds(2));
        CheckpointDiagnosticsSnapshot newerIdle = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Idle,
            CheckpointOrigin.Backup,
            started.AddSeconds(1),
            completedAtUtc: started.AddSeconds(4),
            elapsed: TimeSpan.FromSeconds(3));

        CheckpointDiagnosticsSnapshot merged = MergeCheckpointDetails(
            faulted,
            newerIdle,
            CheckpointPhase.Faulted,
            metadata);

        Assert.Equal(CheckpointPhase.Faulted, merged.Phase);
        Assert.Equal(CheckpointOrigin.Manual, merged.Origin);
        Assert.Equal(faulted.LastStartedAtUtc, merged.LastStartedAtUtc);
        Assert.Equal(faulted.LastElapsed, merged.LastElapsed);
        Assert.Equal(newerIdle.LastSuccessfulAtUtc, merged.LastSuccessfulAtUtc);
        Assert.Equal(faulted.LastFailedAtUtc, merged.LastFailedAtUtc);
        Assert.Equal(2, merged.AttemptCount);
        Assert.Equal(1, merged.SuccessCount);
        Assert.Equal(1, merged.FailureCount);
    }

    [Fact]
    public void RecoveryOverlay_ActiveRepresentativeIsOldestAcrossPhases()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        WalRecoveryDiagnosticsSnapshot olderScanning = CreateActiveRecovery(
            metadata,
            "11111111111111111111111111111111",
            WalRecoveryPhase.Scanning,
            started);
        WalRecoveryDiagnosticsSnapshot newerCheckpointing = CreateActiveRecovery(
            metadata,
            "00000000000000000000000000000000",
            WalRecoveryPhase.Checkpointing,
            started.AddSeconds(1));
        WalRecoveryDiagnosticsSnapshot selected = SelectRecoveryDetail(
            olderScanning,
            newerCheckpointing,
            out bool competingActive);

        Assert.Same(olderScanning, selected);
        Assert.True(competingActive);
    }

    [Fact]
    public void RecoveryOverlay_TerminalTieUsesLatestStartThenHigherId()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DateTimeOffset completed = started.AddSeconds(5);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        WalRecoveryDiagnosticsSnapshot earlierHigherId = CreateTerminalRecovery(
            metadata,
            "ffffffffffffffffffffffffffffffff",
            started,
            completed);
        WalRecoveryDiagnosticsSnapshot laterLowerId = CreateTerminalRecovery(
            metadata,
            "00000000000000000000000000000000",
            started.AddSeconds(1),
            completed);

        Assert.Same(
            laterLowerId,
            SelectRecoveryDetail(
                earlierHigherId,
                laterLowerId,
                out bool forwardCompeting));
        Assert.Same(
            laterLowerId,
            SelectRecoveryDetail(
                laterLowerId,
                earlierHigherId,
                out bool reverseCompeting));
        Assert.False(forwardCompeting);
        Assert.False(reverseCompeting);

        WalRecoveryDiagnosticsSnapshot exactLowerId = CreateTerminalRecovery(
            metadata,
            "11111111111111111111111111111111",
            started,
            completed);
        WalRecoveryDiagnosticsSnapshot exactHigherId = CreateTerminalRecovery(
            metadata,
            "22222222222222222222222222222222",
            started,
            completed);
        Assert.Same(
            exactHigherId,
            SelectRecoveryDetail(exactLowerId, exactHigherId, out _));
        Assert.Same(
            exactHigherId,
            SelectRecoveryDetail(exactHigherId, exactLowerId, out _));
    }

    [Fact]
    public void CheckpointOverlay_EqualClockTiePrefersEstablishedUnit()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DateTimeOffset completed = started.AddSeconds(5);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        CheckpointDiagnosticsSnapshot established = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Faulted,
            CheckpointOrigin.Manual,
            started,
            completed,
            elapsed: TimeSpan.FromSeconds(3),
            CheckpointRetentionReason.ActiveReaders);
        CheckpointDiagnosticsSnapshot overlay = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Faulted,
            CheckpointOrigin.Manual,
            started,
            completed,
            elapsed: TimeSpan.FromSeconds(4),
            CheckpointRetentionReason.NewerCommits);

        CheckpointDiagnosticsSnapshot merged = MergeCheckpointDetails(
            established,
            overlay,
            CheckpointPhase.Faulted,
            metadata);

        Assert.Equal(established.LastElapsed, merged.LastElapsed);
        Assert.Equal(established.RetentionReason, merged.RetentionReason);
    }

    [Fact]
    public void CheckpointOverlay_HigherCoarsePhaseWithoutDetailRemainsAuthoritative()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        CheckpointDiagnosticsSnapshot idle = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Idle,
            CheckpointOrigin.Manual,
            started,
            started.AddSeconds(1),
            TimeSpan.FromSeconds(1));
        StorageRuntimeDiagnosticsCapture established = CreateWalCapture(
            metadata,
            CheckpointPhase.Idle,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(idle),
            logicalBytes: 41);
        StorageRuntimeDiagnosticsCapture overlay = CreateWalCapture(
            metadata,
            CheckpointPhase.Faulted,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            logicalBytes: 99);

        StorageRuntimeDiagnosticsCapture merged = OverlayStorageDetails(
            established,
            overlay,
            metadata);

        WalRuntimeDiagnosticsSnapshot wal = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(merged.Wal.Value);
        Assert.Equal(CheckpointPhase.Faulted, wal.CheckpointPhase);
        Assert.Equal(DiagnosticsAvailability.Unavailable, wal.Checkpoint.Availability);
        Assert.Equal(41, wal.LogicalBytes);
        Assert.True(merged.FieldsTruncated);
    }

    [Fact]
    public void WalDurabilityScalars_SurviveDirectReprojectionAndDetailOverlay()
    {
        DateTimeOffset capturedAt = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(capturedAt);
        DiagnosticsSection<CheckpointDiagnosticsSnapshot> noCheckpoint =
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable);
        StorageRuntimeDiagnosticsCapture established = CreateWalCapture(
            metadata,
            CheckpointPhase.Idle,
            noCheckpoint,
            logicalBytes: 41);
        WalRuntimeDiagnosticsSnapshot establishedWal = established.Wal.Value! with
        {
            FlushCount = 3,
            FlushedCommitCount = 5,
            DurableFlushCount = 7,
            LastSuccessfulDurableFlushAtUtc = capturedAt.AddSeconds(-2),
            GroupCommitBatchCount = 2,
            GroupCommitCount = 4,
            LastSuccessfulGroupCommitAtUtc = capturedAt.AddSeconds(-1),
        };
        established = established with
        {
            Wal = DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                establishedWal),
        };

        DiagnosticsSnapshotMetadata projectedMetadata =
            CreateDiagnosticsMetadata(capturedAt.AddSeconds(1));
        WalRuntimeDiagnosticsSnapshot reprojected = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(typeof(EngineTransportClient)
                .GetMethod(
                    "ReprojectWalSnapshot",
                    BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, [establishedWal, projectedMetadata]));

        Assert.Equal(projectedMetadata, reprojected.Metadata);
        AssertWalDurabilityScalars(reprojected, capturedAt);

        StorageRuntimeDiagnosticsCapture overlay = CreateWalCapture(
            metadata,
            CheckpointPhase.Idle,
            noCheckpoint,
            logicalBytes: 99);
        overlay = overlay with
        {
            Wal = DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                overlay.Wal.Value! with
                {
                    FlushCount = 8,
                    FlushedCommitCount = 9,
                    DurableFlushCount = 10,
                    LastSuccessfulDurableFlushAtUtc = capturedAt,
                    GroupCommitBatchCount = 3,
                    GroupCommitCount = 6,
                    LastSuccessfulGroupCommitAtUtc = capturedAt,
                }),
        };

        StorageRuntimeDiagnosticsCapture merged = OverlayStorageDetails(
            established,
            overlay,
            metadata);

        AssertWalDurabilityScalars(merged.Wal.Value!, capturedAt);
    }

    [Fact]
    public void CheckpointOverlay_FaultedCoarseWithActiveDetailOmitsHybridDetail()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        CheckpointDiagnosticsSnapshot faulted = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Faulted,
            CheckpointOrigin.Manual,
            started,
            started.AddSeconds(1),
            TimeSpan.FromSeconds(1));
        var active = new CheckpointDiagnosticsSnapshot(
            metadata,
            new OpaqueDiagnosticsId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            CheckpointPhase.Copying,
            CheckpointOrigin.Backup,
            started.AddSeconds(2),
            TimeSpan.FromSeconds(1),
            completedPageCount: 1,
            totalPageCount: 2,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: started.AddSeconds(2),
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.FromSeconds(1),
            activeCount: 1,
            attemptCount: 1,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null);
        StorageRuntimeDiagnosticsCapture established = CreateWalCapture(
            metadata,
            CheckpointPhase.Faulted,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(faulted),
            logicalBytes: 41);
        StorageRuntimeDiagnosticsCapture overlay = CreateWalCapture(
            metadata,
            CheckpointPhase.Copying,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(active),
            logicalBytes: 99);

        StorageRuntimeDiagnosticsCapture merged = OverlayStorageDetails(
            established,
            overlay,
            metadata);

        WalRuntimeDiagnosticsSnapshot wal = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(merged.Wal.Value);
        Assert.Equal(CheckpointPhase.Faulted, wal.CheckpointPhase);
        Assert.Equal(DiagnosticsAvailability.Unavailable, wal.Checkpoint.Availability);
        Assert.Equal(41, wal.LogicalBytes);
        Assert.True(merged.FieldsTruncated);
    }

    [Fact]
    public void CheckpointOverlay_InactiveRequestedNeverReusesPriorAttemptDetail()
    {
        DateTimeOffset started = new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata metadata = CreateDiagnosticsMetadata(started);
        CheckpointDiagnosticsSnapshot legacyRequested = CreateTerminalCheckpoint(
            metadata,
            CheckpointPhase.Requested,
            CheckpointOrigin.Manual,
            started,
            started.AddSeconds(1),
            TimeSpan.FromSeconds(1));
        StorageRuntimeDiagnosticsCapture established = CreateWalCapture(
            metadata,
            CheckpointPhase.Requested,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                legacyRequested),
            logicalBytes: 41);
        StorageRuntimeDiagnosticsCapture overlay = CreateWalCapture(
            metadata,
            CheckpointPhase.Idle,
            DiagnosticsSection<CheckpointDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            logicalBytes: 99);

        StorageRuntimeDiagnosticsCapture merged = OverlayStorageDetails(
            established,
            overlay,
            metadata);

        WalRuntimeDiagnosticsSnapshot wal = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(merged.Wal.Value);
        Assert.Equal(CheckpointPhase.Requested, wal.CheckpointPhase);
        Assert.Equal(DiagnosticsAvailability.Unavailable, wal.Checkpoint.Availability);
        Assert.Equal(41, wal.LogicalBytes);
        Assert.True(merged.FieldsTruncated);
    }

    private static StorageRuntimeDiagnosticsCapture OverlayStorageDetails(
        StorageRuntimeDiagnosticsCapture established,
        StorageRuntimeDiagnosticsCapture overlay,
        DiagnosticsSnapshotMetadata metadata)
        => Assert.IsType<StorageRuntimeDiagnosticsCapture>(
            typeof(EngineTransportClient)
                .GetMethod(
                    "OverlayStorageRuntimeDetails",
                    BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(null, [established, overlay, metadata]));

    private static void AssertWalDurabilityScalars(
        WalRuntimeDiagnosticsSnapshot wal,
        DateTimeOffset capturedAt)
    {
        Assert.Equal(5, wal.FlushedCommitCount);
        Assert.Equal(7, wal.DurableFlushCount);
        Assert.Equal(capturedAt.AddSeconds(-2), wal.LastSuccessfulDurableFlushAtUtc);
        Assert.Equal(2, wal.GroupCommitBatchCount);
        Assert.Equal(4, wal.GroupCommitCount);
        Assert.Equal(capturedAt.AddSeconds(-1), wal.LastSuccessfulGroupCommitAtUtc);
    }

    private static StorageRuntimeDiagnosticsCapture CreateWalCapture(
        DiagnosticsSnapshotMetadata metadata,
        CheckpointPhase phase,
        DiagnosticsSection<CheckpointDiagnosticsSnapshot> checkpoint,
        long logicalBytes)
    {
        var wal = new WalRuntimeDiagnosticsSnapshot(
            metadata,
            logicalBytes,
            AllocatedBytes: logicalBytes,
            CommittedFrameBytes: 0,
            RetainedBytes: 0,
            FrameCount: 0,
            FlushCount: 0,
            BytesWritten: 0,
            PendingCommitCount: 0,
            phase,
            LastSuccessfulFlushAtUtc: null,
            LastSuccessfulCheckpointAtUtc: checkpoint.Value?.LastSuccessfulAtUtc,
            LastError: checkpoint.Value?.LastError)
        {
            Checkpoint = checkpoint,
        };
        return new StorageRuntimeDiagnosticsCapture(
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(wal));
    }

    private static CheckpointDiagnosticsSnapshot MergeCheckpointDetails(
        CheckpointDiagnosticsSnapshot established,
        CheckpointDiagnosticsSnapshot overlay,
        CheckpointPhase authoritativePhase,
        DiagnosticsSnapshotMetadata metadata)
        => Assert.IsType<CheckpointDiagnosticsSnapshot>(
            typeof(EngineTransportClient)
                .GetMethod(
                    "MergeCheckpointDetails",
                    BindingFlags.Static | BindingFlags.NonPublic)!
                .Invoke(
                    null,
                    [established, overlay, authoritativePhase, metadata]));

    private static CheckpointDiagnosticsSnapshot CreateTerminalCheckpoint(
        DiagnosticsSnapshotMetadata metadata,
        CheckpointPhase phase,
        CheckpointOrigin origin,
        DateTimeOffset startedAtUtc,
        DateTimeOffset completedAtUtc,
        TimeSpan elapsed,
        CheckpointRetentionReason retentionReason =
            CheckpointRetentionReason.None)
    {
        bool failed = phase == CheckpointPhase.Faulted;
        return new CheckpointDiagnosticsSnapshot(
            metadata,
            operationId: null,
            phase,
            origin,
            startedAtUtc: null,
            elapsed: null,
            completedPageCount: null,
            totalPageCount: null,
            phase == CheckpointPhase.Idle
                ? CheckpointRetentionReason.None
                : retentionReason,
            lastStartedAtUtc: startedAtUtc,
            lastSuccessfulAtUtc: failed ? null : completedAtUtc,
            lastFailedAtUtc: failed ? completedAtUtc : null,
            lastElapsed: elapsed,
            activeCount: 0,
            attemptCount: 1,
            successCount: failed ? 0 : 1,
            failureCount: failed ? 1 : 0,
            canceledCount: 0,
            lastError: failed
                ? SafeErrorProjector.Project(SafeErrorKind.DatabaseIo)
                : null);
    }

    private static WalRecoveryDiagnosticsSnapshot CreateActiveRecovery(
        DiagnosticsSnapshotMetadata metadata,
        string operationId,
        WalRecoveryPhase phase,
        DateTimeOffset startedAtUtc)
        => new(
            metadata,
            new OpaqueDiagnosticsId(operationId),
            phase,
            startedAtUtc,
            completedAtUtc: null,
            elapsed: TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Unknown,
            scannedFrameCount: 0,
            scannedBytes: 0,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.Unknown,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null);

    private static WalRecoveryDiagnosticsSnapshot CreateTerminalRecovery(
        DiagnosticsSnapshotMetadata metadata,
        string operationId,
        DateTimeOffset startedAtUtc,
        DateTimeOffset completedAtUtc)
        => new(
            metadata,
            new OpaqueDiagnosticsId(operationId),
            WalRecoveryPhase.Completed,
            startedAtUtc,
            completedAtUtc,
            completedAtUtc - startedAtUtc,
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 0,
            scannedBytes: 0,
            recoveredFrameCount: 0,
            recoveredBytes: 0,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null);

    private static WalRecoveryDiagnosticsSnapshot SelectRecoveryDetail(
        WalRecoveryDiagnosticsSnapshot established,
        WalRecoveryDiagnosticsSnapshot overlay,
        out bool competingActive)
    {
        object?[] arguments = [established, overlay, false];
        WalRecoveryDiagnosticsSnapshot selected = Assert.IsType<
            WalRecoveryDiagnosticsSnapshot>(
                typeof(EngineTransportClient)
                    .GetMethod(
                        "SelectRecoveryDetail",
                        BindingFlags.Static | BindingFlags.NonPublic)!
                    .Invoke(null, arguments));
        competingActive = Assert.IsType<bool>(arguments[2]);
        return selected;
    }

    private static DiagnosticsSnapshotMetadata CreateDiagnosticsMetadata(
        DateTimeOffset capturedAtUtc)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            capturedAtUtc,
            "0123456789abcdef0123456789abcdef",
            counterEpoch: 1,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Engine,
            "checkpoint-overlay",
            recordsTruncated: false,
            fieldsTruncated: false);

    private static MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation Start(
        MaintenanceRuntimeDiagnostics registry,
        CSharpDbOperationContext context,
        MaintenanceOperationPhase phase)
        => Assert.IsType<
            MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation>(
            registry.TryStart(
                context,
                MaintenanceOperationKind.Backup,
                phase));

    private static CSharpDbOperationContext CreateContext(TimeProvider clock)
        => CSharpDbOperationContext.CreateRequest(
            CSharpDbOperationClass.Backup,
            CSharpDB.Observability.CSharpDbTransport.Direct,
            "maintenance-registry",
            timeProvider: clock);

    private static DatabaseOptions CreateOptions(string alias)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = alias,
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = false,
                    SlowQueries = false,
                },
                History = new CSharpDbHistoryOptions
                {
                    ActiveQueryCapacity = 16,
                    RecentQueryCapacity = 16,
                    RecentOperationCapacity = 8,
                    Retention = TimeSpan.FromMinutes(10),
                },
            },
        };

    private static EngineTransportClient CreateMemoryClient(
        string displayName,
        DatabaseOptions options)
        => new(
            displayName,
            static async (_, runtimeOptions, ct) =>
                await Database.OpenInMemoryAsync(runtimeOptions, ct),
            options);

    private static MaintenanceRuntimeDiagnostics GetClientRegistry(
        EngineTransportClient client)
        => Assert.IsType<MaintenanceRuntimeDiagnostics>(
            GetClientRegistryOrNull(client));

    private static MaintenanceRuntimeDiagnostics? GetClientRegistryOrNull(
        EngineTransportClient client)
        => (MaintenanceRuntimeDiagnostics?)typeof(EngineTransportClient)
            .GetField(
                "_maintenanceRuntimeDiagnostics",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(client);

    private static StorageRuntimeDiagnostics.Registration
        GetStorageRuntimeRegistration(Database database)
        => Assert.IsType<StorageRuntimeDiagnostics.Registration>(
            typeof(Database).GetField(
                    "_storageRuntimeDiagnosticsRegistration",
                    BindingFlags.Instance | BindingFlags.NonPublic)!
                .GetValue(database));

    private static object? GetRuntimeComponents(
        CSharpDbRuntimeDiagnosticsState state)
        => typeof(CSharpDbRuntimeDiagnosticsState)
            .GetField("_components", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(state);

    private static T GetPrivateField<T>(
        EngineTransportClient client,
        string fieldName)
        => (T)typeof(EngineTransportClient)
            .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(client)!;

    private static async Task<string> CreateValidSnapshotAsync()
    {
        string path = CreateDatabasePath("maintenance-source");
        await using Database database = await Database.OpenInMemoryAsync(Ct);
        await database.SaveToFileAsync(path, Ct);
        return path;
    }

    private static async Task CreateSnapshotWithTableAsync(
        string path,
        string tableName,
        CancellationToken ct)
    {
        await using Database database = await Database.OpenAsync(path, ct);
        await database.ExecuteAsync(
            $"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, value TEXT)",
            ct);
        await database.ExecuteAsync(
            $"INSERT INTO {tableName} VALUES (1, 'restored')",
            ct);
        await database.CheckpointAsync(ct);
    }

    private static string CreateDatabasePath(string prefix)
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_{prefix}_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseArtifacts(string path)
    {
        foreach (string candidate in new[]
                 {
                     path,
                     path + ".wal",
                     path + ".manifest.json",
                 })
        {
            try
            {
                if (File.Exists(candidate))
                    File.Delete(candidate);
            }
            catch
            {
                // Best-effort cleanup for isolated test artifacts.
            }
        }
    }

    private static void AssertSubsequence(
        IReadOnlyList<MaintenanceOperationPhase> actual,
        params MaintenanceOperationPhase[] expected)
    {
        int index = 0;
        foreach (MaintenanceOperationPhase phase in actual)
        {
            if (index < expected.Length && phase == expected[index])
                index++;
        }

        Assert.Equal(expected.Length, index);
    }

    private class ManualTimeProvider : TimeProvider
    {
        private long _timestamp;

        internal bool ThrowTimestamps { get; set; }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);

        public override long GetTimestamp()
            => ThrowTimestamps
                ? throw new InvalidOperationException("clock failure")
                : Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan elapsed)
            => Interlocked.Add(ref _timestamp, elapsed.Ticks);
    }

    private sealed class BlockingTimeProvider : ManualTimeProvider
    {
        private readonly ManualResetEventSlim _release = new(initialState: true);
        private TaskCompletionSource? _blocked;
        private int _blockNextTimestamp;

        public override long GetTimestamp()
        {
            if (Interlocked.Exchange(ref _blockNextTimestamp, 0) != 0)
            {
                Volatile.Read(ref _blocked)?.TrySetResult();
                _release.Wait();
            }

            return base.GetTimestamp();
        }

        internal void BlockNextTimestamp()
        {
            _release.Reset();
            Volatile.Write(
                ref _blocked,
                new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously));
            Volatile.Write(ref _blockNextTimestamp, 1);
        }

        internal Task WaitUntilBlockedAsync()
            => Volatile.Read(ref _blocked)?.Task ??
               throw new InvalidOperationException("No timestamp read is blocked.");

        internal void Release() => _release.Set();
    }

    private sealed class LifecycleRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private readonly object _gate = new();
        private readonly List<KeyValuePair<string, CSharpDbLifecycleCompletedEvent>>
            _events = [];
        private readonly IDisposable _subscription;

        internal LifecycleRecorder()
        {
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name => name is
                    "CSharpDB.Backup.Completed" or
                    "CSharpDB.Restore.Completed" or
                    "CSharpDB.Checkpoint.Completed" or
                    "CSharpDB.Maintenance.Completed");
        }

        internal CSharpDbLifecycleCompletedEvent[] Events(string name)
        {
            lock (_gate)
            {
                return _events
                    .Where(item => string.Equals(
                        item.Key,
                        name,
                        StringComparison.Ordinal))
                    .Select(static item => item.Value)
                    .ToArray();
            }
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbLifecycleCompletedEvent payload)
                return;

            lock (_gate)
            {
                _events.Add(new KeyValuePair<
                    string,
                    CSharpDbLifecycleCompletedEvent>(value.Key, payload));
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }
}

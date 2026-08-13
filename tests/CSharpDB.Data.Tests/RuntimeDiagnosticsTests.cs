using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class RuntimeDiagnosticsTests : IAsyncLifetime
{
    private readonly List<string> _paths = [];
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        CSharpDbConnectionPool.BeforeFirstPhysicalOpenForTest = null;
        CSharpDbConnectionPool.BeforeOpenSessionGateForTest = null;
        await CSharpDbConnection.ClearAllPoolsAsync();
        foreach (string path in _paths)
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task PooledContributor_ReportsTruthfulSessionReaderTransactionAndIdleState()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability = CreateObservability("pool_runtime");
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 2,
            observability,
            clock);

        PooledDatabaseSession session = await pool.OpenSessionAsync(Ct);
        DataConnectionDiagnosticsRawSnapshot opened =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(DataConnectionOwnerKind.Pooled, opened.OwnerKind);
        Assert.Equal("pool_runtime", opened.DatabaseAlias);
        Assert.Equal(2, opened.PoolCapacity);
        Assert.Equal(1, opened.AvailableSlots);
        Assert.Equal(0, opened.WaiterCount);
        Assert.Equal(1, opened.ActiveLogicalSessions);
        Assert.Equal(ConnectionPoolLifecycleState.Enabled, opened.PoolState);
        Assert.Single(opened.Sessions);
        Assert.Equal(DiagnosticsSessionState.Idle, opened.Sessions[0].State);
        Assert.True(CSharpDbDiagnostics.IsValidOpaqueIdentifier(
            opened.Sessions[0].SessionId.Value));

        await session.BeginTransactionAsync(Ct);
        clock.Advance(TimeSpan.FromSeconds(7));
        DataConnectionDiagnosticsRawSnapshot transaction =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(1, transaction.ActiveTransactions);
        Assert.Equal(TimeSpan.FromSeconds(7), transaction.OldestTransactionAge);
        Assert.Equal(
            transaction.Sessions[0].SessionId,
            transaction.TransactionOwnerSessionId);
        Assert.True(transaction.Sessions[0].HasActiveTransaction);
        Assert.Equal(DiagnosticsSessionState.Transaction, transaction.Sessions[0].State);
        await session.CommitAsync(Ct);

        await using (QueryResult setup = await session.ExecuteAsync(
                         "CREATE TABLE runtime_reader (id INTEGER PRIMARY KEY);",
                         Ct))
        {
        }
        await using (QueryResult insert = await session.ExecuteAsync(
                         "INSERT INTO runtime_reader VALUES (1);",
                         Ct))
        {
        }

        QueryResult reader = await session.ExecuteAsync(
            "SELECT id FROM runtime_reader;",
            Ct);
        DataConnectionDiagnosticsRawSnapshot reading =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(1, reading.ActiveReaders);
        Assert.True(reading.Sessions[0].HasActiveReader);
        Assert.Equal(DiagnosticsSessionState.SnapshotReader, reading.Sessions[0].State);

        await reader.DisposeAsync();
        DataConnectionDiagnosticsRawSnapshot afterReader =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(0, afterReader.ActiveReaders);
        Assert.False(afterReader.Sessions[0].HasActiveReader);

        await session.DisposeAsync();
        DataConnectionDiagnosticsRawSnapshot idle =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(0, idle.ActiveLogicalSessions);
        Assert.Equal(2, idle.AvailableSlots);
        Assert.Equal(1, idle.WarmEngineIdleCount);
        Assert.Empty(idle.Sessions);

        await pool.DisableAsync();
        DataConnectionDiagnosticsRawSnapshot retired =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(ConnectionPoolLifecycleState.Retired, retired.PoolState);
        Assert.Equal(1, retired.RetiredPoolCount);
        Assert.Equal(1, retired.DisabledPoolCount);
    }

    [Fact]
    public async Task PoolAdmission_WaiterGaugeCoversOnlyBlockedWaitAndClearsOnCancellation()
    {
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 1,
            CreateObservability("pool_waiters"));
        PooledDatabaseSession owner = await pool.OpenSessionAsync(Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task<PooledDatabaseSession> blocked = pool
            .OpenSessionAsync(cancellation.Token)
            .AsTask();

        DataConnectionDiagnosticsRawSnapshot? waiting = null;
        for (int attempt = 0; attempt < 100; attempt++)
        {
            waiting = await pool.CaptureRuntimeDiagnosticsAsync(4, Ct);
            if (waiting?.WaiterCount == 1)
                break;
            await Task.Yield();
        }

        DataConnectionDiagnosticsRawSnapshot blockedSnapshot = AssertSnapshot(waiting);
        Assert.Equal(1, blockedSnapshot.WaiterCount);
        Assert.Equal(0, blockedSnapshot.AvailableSlots);
        Assert.Equal(1, blockedSnapshot.ActiveLogicalSessions);

        for (int attempt = 0; attempt < 50; attempt++)
        {
            DataConnectionDiagnosticsRawSnapshot heldWaiterSnapshot =
                AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(4, Ct));
            Assert.Equal(1, heldWaiterSnapshot.WaiterCount);
            Assert.Equal(0, heldWaiterSnapshot.AvailableSlots);
            Assert.Equal(1, heldWaiterSnapshot.ActiveLogicalSessions);
            AssertConsistent(heldWaiterSnapshot);
            await Task.Yield();
        }

        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
        DataConnectionDiagnosticsRawSnapshot canceled =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(4, Ct));
        Assert.Equal(0, canceled.WaiterCount);
        Assert.Equal(0, canceled.AvailableSlots);
        Assert.Equal(1, canceled.ActiveLogicalSessions);

        Task<PooledDatabaseSession> admittedTask = pool.OpenSessionAsync(Ct).AsTask();
        for (int attempt = 0; attempt < 100; attempt++)
        {
            DataConnectionDiagnosticsRawSnapshot? snapshot =
                await pool.CaptureRuntimeDiagnosticsAsync(4, Ct);
            if (snapshot?.WaiterCount == 1)
                break;
            await Task.Yield();
        }

        await owner.DisposeAsync();
        PooledDatabaseSession admitted = await admittedTask;
        DataConnectionDiagnosticsRawSnapshot afterAdmission =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(4, Ct));
        Assert.Equal(0, afterAdmission.WaiterCount);
        Assert.Equal(0, afterAdmission.AvailableSlots);
        Assert.Equal(1, afterAdmission.ActiveLogicalSessions);
        AssertConsistent(afterAdmission);
        await admitted.DisposeAsync();
        await pool.DisableAsync();
    }

    [Fact]
    public async Task DirectContributor_TracksUnobservedOverloadsReaderAndTransactionAge()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 0, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability = CreateObservability("direct_runtime");
        Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions { ObservabilityOptions = observability },
            Ct);
        await using var session = new DirectDatabaseSession(
            database,
            observabilityOptionsSnapshot: observability,
            diagnosticsTimeProvider: clock);

        await session.BeginTransactionAsync(Ct);
        clock.Advance(TimeSpan.FromSeconds(3));
        DataConnectionDiagnosticsRawSnapshot transaction =
            AssertSnapshot(await session.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(DataConnectionOwnerKind.Direct, transaction.OwnerKind);
        Assert.Equal(1, transaction.ActiveTransactions);
        Assert.Equal(TimeSpan.FromSeconds(3), transaction.OldestTransactionAge);
        Assert.Equal(
            transaction.Sessions[0].SessionId,
            transaction.TransactionOwnerSessionId);
        await session.RollbackAsync(Ct);

        await using (QueryResult setup = await session.ExecuteAsync(
                         "CREATE TABLE direct_reader (id INTEGER PRIMARY KEY);",
                         Ct))
        {
        }
        QueryResult reader = await session.ExecuteAsync(
            "SELECT id FROM direct_reader;",
            Ct);
        DataConnectionDiagnosticsRawSnapshot reading =
            AssertSnapshot(await session.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(1, reading.ActiveReaders);
        Assert.True(reading.Sessions[0].HasActiveReader);
        await reader.DisposeAsync();
    }

    [Fact]
    public async Task DirectTransactionAge_UsesMonotonicTimeAcrossWallClockCorrections()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 30, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability =
            CreateObservability("direct_monotonic_age");
        Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions { ObservabilityOptions = observability },
            Ct);
        var session = new DirectDatabaseSession(
            database,
            observabilityOptionsSnapshot: observability,
            diagnosticsTimeProvider: clock);

        try
        {
            await AssertTransactionAgeUsesMonotonicTimeAsync(
                session,
                session,
                clock,
                DataConnectionOwnerKind.Direct);
        }
        finally
        {
            await session.DisposeAsync();
        }
        DataConnectionDiagnosticsRawSnapshot disposed =
            AssertSnapshot(await session.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(0, disposed.ActiveTransactions);
        Assert.Null(disposed.OldestTransactionAge);
    }

    [Fact]
    public async Task PooledTransactionAge_UsesMonotonicTimeAcrossWallClockCorrections()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 14, 30, 0, TimeSpan.Zero));
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 1,
            CreateObservability("pool_monotonic_age"),
            clock);
        PooledDatabaseSession session = await pool.OpenSessionAsync(Ct);

        try
        {
            await AssertTransactionAgeUsesMonotonicTimeAsync(
                session,
                pool,
                clock,
                DataConnectionOwnerKind.Pooled);
        }
        finally
        {
            await pool.DisableAsync();
            await session.DisposeAsync();
            await pool.Retirement;
        }
        DataConnectionDiagnosticsRawSnapshot retired =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(ConnectionPoolLifecycleState.Retired, retired.PoolState);
        Assert.Equal(0, retired.ActiveTransactions);
        Assert.Null(retired.OldestTransactionAge);
    }

    [Fact]
    public async Task NamedSharedTransactionAge_UsesMonotonicTimeAcrossWallClockCorrections()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 15, 30, 0, TimeSpan.Zero));
        CSharpDbObservabilityOptions observability =
            CreateObservability("shared_monotonic_age");
        var configuredOptions = new DatabaseOptions
        {
            ObservabilityOptions = observability,
        };
        using IDisposable runtimeState =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(
                observability,
                clock);
        DatabaseOptions runtimeOptions =
            DataObservabilityOptionsSnapshot.WithRuntimeDiagnosticsStateForTest(
                configuredOptions,
                runtimeState);
        var host = new SharedMemoryDatabaseHost(
            $"monotonic-{Guid.NewGuid():N}",
            configuredOptions,
            runtimeOptions);
        SharedMemoryDatabaseSession session = await host.OpenSessionAsync(
            loadFromPath: null,
            configuredOptions,
            Ct);

        try
        {
            await AssertTransactionAgeUsesMonotonicTimeAsync(
                session,
                host,
                clock,
                DataConnectionOwnerKind.SharedMemory);
        }
        finally
        {
            await host.DisableAsync();
            await session.DisposeAsync();
            await host.Retirement;
        }
        DataConnectionDiagnosticsRawSnapshot retired =
            AssertSnapshot(await host.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(ConnectionPoolLifecycleState.Retired, retired.PoolState);
        Assert.Equal(0, retired.ActiveTransactions);
        Assert.Null(retired.OldestTransactionAge);
    }

    [Fact]
    public async Task PrivateMemoryDirectSession_IsRegisteredOnlyForItsActiveLifetime()
    {
        DatabaseOptions options = new()
        {
            ObservabilityOptions = CreateObservability("private_direct_runtime"),
        };
        var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);

        DataConnectionDiagnosticsRawSnapshot active = await FindContributorAsync(
            "private_direct_runtime",
            DataConnectionOwnerKind.Direct);
        OpaqueDiagnosticsId contributorId = active.ContributorId;
        Assert.Equal(1, active.ActiveLogicalSessions);

        await connection.DisposeAsync();
        DataRuntimeDiagnosticsRegistrySnapshot afterDispose =
            await DataRuntimeDiagnosticsRegistry.CaptureAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                maximumSessionRecordsPerContributor: 8,
                Ct);
        Assert.DoesNotContain(afterDispose.Contributors, snapshot =>
            snapshot.ContributorId == contributorId);
    }

    [Fact]
    public async Task ThrowingRuntimeClock_NeverChangesPoolOperationsOrSnapshotAvailability()
    {
        var clock = new ThrowAfterFirstTimeProvider(
            new DateTimeOffset(2026, 8, 10, 14, 0, 0, TimeSpan.Zero));
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 1,
            CreateObservability("throwing_clock"),
            clock);

        PooledDatabaseSession session = await pool.OpenSessionAsync(Ct);
        await session.BeginTransactionAsync(Ct);
        DataConnectionDiagnosticsRawSnapshot activeTransaction =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(1, activeTransaction.ActiveTransactions);
        Assert.Null(activeTransaction.OldestTransactionAge);
        await session.RollbackAsync(Ct);
        await using (QueryResult result = await session.ExecuteAsync(
                         "SELECT 1;",
                         Ct))
        {
        }

        DataConnectionDiagnosticsRawSnapshot snapshot =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(2, Ct));
        Assert.Equal(clock.InitialUtcNow, snapshot.SnapshotAtUtc);
        Assert.Single(snapshot.Sessions);
        Assert.Equal(clock.InitialUtcNow, snapshot.Sessions[0].CreatedAtUtc);

        await session.DisposeAsync();
        await pool.DisableAsync();
    }

    [Fact]
    public async Task PoolContributor_PublishesPoisonedOwnerWithoutLeakingInternalOwnerId()
    {
        CSharpDbObservabilityOptions observability = CreateObservability("poisoned_pool");
        Database database = await Database.OpenInMemoryAsync(
            new DatabaseOptions { ObservabilityOptions = observability },
            Ct);
        var pool = new CSharpDbConnectionPool(
            CreatePoolKey(maxPoolSize: 2),
            maxPoolSize: 2,
            _ => ValueTask.FromResult(database),
            observability);
        PooledDatabaseSession owner = await pool.OpenSessionAsync(Ct);
        PooledDatabaseSession observer = await pool.OpenSessionAsync(Ct);
        await owner.BeginTransactionAsync(Ct);

        await database.RollbackAsync(Ct);
        await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(
            () => owner.CommitAsync(Ct).AsTask());

        DataConnectionDiagnosticsRawSnapshot poisoned =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        Assert.Equal(ConnectionPoolLifecycleState.Poisoned, poisoned.PoolState);
        Assert.Equal(1, poisoned.PoisonedPoolCount);
        Assert.Equal(1, poisoned.DisabledPoolCount);
        Assert.Equal(1, poisoned.ActiveTransactions);
        Assert.NotNull(poisoned.TransactionOwnerSessionId);
        Assert.DoesNotContain(
            poisoned.TransactionOwnerSessionId!.Value,
            new[] { "1", "2" });

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => owner.DisposeAsync().AsTask());
        await observer.DisposeAsync();
    }

    [Fact]
    public async Task RegistrySnapshots_AreBoundedAndNeverContainDataSourcePaths()
    {
        string firstPath = CreatePath("super-secret-alpha");
        string secondPath = CreatePath("super-secret-beta");
        DatabaseOptions options = new()
        {
            ObservabilityOptions = CreateObservability("safe_alias"),
        };
        string firstConnectionString =
            $"Data Source={firstPath};Pooling=true;Max Pool Size=2";
        string secondConnectionString =
            $"Data Source={secondPath};Pooling=true;Max Pool Size=2";
        await using var first = new CSharpDbConnection(firstConnectionString, options);
        await using var second = new CSharpDbConnection(secondConnectionString, options);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        DataRuntimeDiagnosticsRegistrySnapshot full =
            await DataRuntimeDiagnosticsRegistry.CaptureAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                maximumSessionRecordsPerContributor: 8,
                Ct);
        Assert.Contains(full.Contributors, snapshot =>
            snapshot.DatabaseAlias == "safe_alias" &&
            snapshot.OwnerKind == DataConnectionOwnerKind.Pooled);
        string json = JsonSerializer.Serialize(full);
        Assert.DoesNotContain(firstPath, json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secondPath, json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(firstConnectionString, json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secondConnectionString, json, StringComparison.OrdinalIgnoreCase);

        string[] propertyNames = typeof(DataConnectionDiagnosticsRawSnapshot)
            .GetProperties()
            .Select(static property => property.Name)
            .Concat(typeof(DataSessionDiagnosticsRawSnapshot)
                .GetProperties()
                .Select(static property => property.Name))
            .ToArray();
        Assert.DoesNotContain("DataSource", propertyNames);
        Assert.DoesNotContain("ConnectionString", propertyNames);
        Assert.DoesNotContain("Path", propertyNames);
        Assert.DoesNotContain("TransactionId", propertyNames);

        DataRuntimeDiagnosticsRegistrySnapshot bounded =
            await DataRuntimeDiagnosticsRegistry.CaptureAsync(
                maximumContributorRecords: 1,
                maximumSessionRecordsPerContributor: 1,
                Ct);
        Assert.Single(bounded.Contributors);
        Assert.True(bounded.IsTruncated);
        Assert.Equal(0, bounded.DroppedCount);
    }

    [Fact]
    public async Task SharedMemoryContributor_TracksLogicalSessionsAndTransactionOwner()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        DatabaseOptions options = new()
        {
            ObservabilityOptions = CreateObservability("shared_runtime"),
        };
        await using var first = new CSharpDbConnection(connectionString, options);
        await using var second = new CSharpDbConnection(connectionString, options);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        DataConnectionDiagnosticsRawSnapshot opened = await FindContributorAsync(
            "shared_runtime",
            DataConnectionOwnerKind.SharedMemory);
        Assert.Null(opened.PoolCapacity);
        Assert.Equal(2, opened.ActiveLogicalSessions);
        Assert.Equal(2, opened.Sessions.Count);

        DataRuntimeDiagnosticsRegistrySnapshot boundedRegistry =
            await DataRuntimeDiagnosticsRegistry.CaptureAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                maximumSessionRecordsPerContributor: 1,
                Ct);
        DataConnectionDiagnosticsRawSnapshot bounded = Assert.Single(
            boundedRegistry.Contributors,
            snapshot => snapshot.DatabaseAlias == "shared_runtime" &&
                        snapshot.OwnerKind == DataConnectionOwnerKind.SharedMemory);
        Assert.Equal(2, bounded.ActiveLogicalSessions);
        Assert.Single(bounded.Sessions);
        Assert.Equal(0, bounded.DroppedSessionCount);
        Assert.True(bounded.SessionsTruncated);

        await using var transaction = await first.BeginTransactionAsync(Ct);
        DataConnectionDiagnosticsRawSnapshot activeTransaction = await FindContributorAsync(
            "shared_runtime",
            DataConnectionOwnerKind.SharedMemory);
        Assert.Equal(1, activeTransaction.ActiveTransactions);
        Assert.NotNull(activeTransaction.TransactionOwnerSessionId);
        Assert.Single(activeTransaction.Sessions, static session =>
            session.HasActiveTransaction &&
            session.State == DiagnosticsSessionState.Transaction);
        await transaction.RollbackAsync(Ct);
    }

    [Fact]
    public async Task ConcurrentCheckoutReleaseAndSnapshot_RemainInternallyConsistent()
    {
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 8,
            CreateObservability("pool_stress"));
        Task[] workers = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(async () =>
            {
                for (int iteration = 0; iteration < 75; iteration++)
                {
                    PooledDatabaseSession session = await pool.OpenSessionAsync(Ct);
                    await Task.Yield();
                    await session.DisposeAsync();
                }
            }, Ct))
            .ToArray();
        Task allWorkers = Task.WhenAll(workers);

        while (!allWorkers.IsCompleted)
        {
            DataConnectionDiagnosticsRawSnapshot? snapshot =
                await pool.CaptureRuntimeDiagnosticsAsync(8, Ct);
            if (snapshot is not null)
                AssertConsistent(snapshot);
            await Task.Yield();
        }
        await allWorkers;

        DataConnectionDiagnosticsRawSnapshot completed =
            AssertSnapshot(await pool.CaptureRuntimeDiagnosticsAsync(8, Ct));
        AssertConsistent(completed);
        Assert.Equal(0, completed.ActiveLogicalSessions);
        Assert.Equal(8, completed.AvailableSlots);
        await pool.DisableAsync();
    }

    [Fact]
    public async Task DisabledContributor_HasNoRuntimeSnapshot()
    {
        CSharpDbConnectionPool pool = CreatePool(
            maxPoolSize: 1,
            observability: null);
        Assert.Null(await pool.CaptureRuntimeDiagnosticsAsync(1, Ct));
        PooledDatabaseSession session = await pool.OpenSessionAsync(Ct);
        Assert.Null(await pool.CaptureRuntimeDiagnosticsAsync(1, Ct));
        await session.DisposeAsync();
        await pool.DisableAsync();
    }

    private CSharpDbConnectionPool CreatePool(
        int maxPoolSize,
        CSharpDbObservabilityOptions? observability,
        TimeProvider? clock = null)
    {
        Database? database = null;
        return new CSharpDbConnectionPool(
            CreatePoolKey(maxPoolSize),
            maxPoolSize,
            async cancellationToken => database ??= await Database.OpenInMemoryAsync(
                new DatabaseOptions { ObservabilityOptions = observability },
                cancellationToken),
            observability,
            clock);
    }

    private static PoolKey CreatePoolKey(int maxPoolSize)
        => new(
            DataSource: $"diagnostic-test-{Guid.NewGuid():N}",
            MaxPoolSize: maxPoolSize,
            EffectiveOpenMode: CSharpDbEmbeddedOpenMode.Direct,
            EffectiveStoragePreset: null,
            EffectiveAdaptiveQueryReoptimization: false,
            ExplicitDirectDatabaseOptions: null,
            ExplicitHybridDatabaseOptions: null);

    private static CSharpDbObservabilityOptions CreateObservability(string alias)
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
            },
        };

    private string CreatePath(string marker)
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-{marker}-{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static DataConnectionDiagnosticsRawSnapshot AssertSnapshot(
        DataConnectionDiagnosticsRawSnapshot? snapshot)
        => Assert.IsType<DataConnectionDiagnosticsRawSnapshot>(snapshot);

    private static void AssertConsistent(DataConnectionDiagnosticsRawSnapshot snapshot)
    {
        Assert.Equal(snapshot.ActiveLogicalSessions, snapshot.Sessions.Count);
        Assert.InRange(snapshot.AvailableSlots ?? 0, 0, snapshot.PoolCapacity ?? 0);
        Assert.True(snapshot.WaiterCount >= 0);
        Assert.InRange(snapshot.ActiveTransactions, 0, 1);
        Assert.True(
            snapshot.ActiveReaders >=
            snapshot.Sessions.Count(static session => session.HasActiveReader));
        if (snapshot.TransactionOwnerSessionId is not null)
        {
            Assert.Contains(snapshot.Sessions, session =>
                session.SessionId == snapshot.TransactionOwnerSessionId &&
                session.HasActiveTransaction);
        }
    }

    private static async ValueTask AssertTransactionAgeUsesMonotonicTimeAsync(
        ICSharpDbSession session,
        IDataRuntimeDiagnosticsContributor contributor,
        ManualTimeProvider clock,
        DataConnectionOwnerKind expectedOwnerKind)
    {
        await session.BeginTransactionAsync(Ct);

        clock.AdvanceMonotonic(TimeSpan.FromSeconds(4));
        clock.AdjustWallClock(TimeSpan.FromDays(10));
        DataConnectionDiagnosticsRawSnapshot forwardCorrection =
            AssertSnapshot(await contributor.CaptureRuntimeDiagnosticsAsync(4, Ct));
        Assert.Equal(expectedOwnerKind, forwardCorrection.OwnerKind);
        Assert.Equal(TimeSpan.FromSeconds(4), forwardCorrection.OldestTransactionAge);

        clock.AdvanceMonotonic(TimeSpan.FromSeconds(5));
        clock.AdjustWallClock(TimeSpan.FromDays(-20));
        DataConnectionDiagnosticsRawSnapshot backwardCorrection =
            AssertSnapshot(await contributor.CaptureRuntimeDiagnosticsAsync(4, Ct));
        Assert.Equal(TimeSpan.FromSeconds(9), backwardCorrection.OldestTransactionAge);
        Assert.True(backwardCorrection.SnapshotAtUtc < forwardCorrection.SnapshotAtUtc);

        clock.AdjustMonotonic(TimeSpan.FromSeconds(-20));
        DataConnectionDiagnosticsRawSnapshot regressedMonotonicClock =
            AssertSnapshot(await contributor.CaptureRuntimeDiagnosticsAsync(4, Ct));
        Assert.Null(regressedMonotonicClock.OldestTransactionAge);
        Assert.Equal(1, regressedMonotonicClock.ActiveTransactions);
    }

    private static async ValueTask<DataConnectionDiagnosticsRawSnapshot> FindContributorAsync(
        string alias,
        DataConnectionOwnerKind ownerKind)
    {
        DataRuntimeDiagnosticsRegistrySnapshot registry =
            await DataRuntimeDiagnosticsRegistry.CaptureAsync(
                CSharpDbObservabilityOptions.MaximumActiveOperationCapacity,
                maximumSessionRecordsPerContributor: 32,
                Ct);
        return Assert.Single(registry.Contributors, snapshot =>
            snapshot.DatabaseAlias == alias && snapshot.OwnerKind == ownerKind);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _utcTicks = utcNow.UtcTicks;
        private long _timestampTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow()
            => new(Volatile.Read(ref _utcTicks), TimeSpan.Zero);

        public override long GetTimestamp()
            => Volatile.Read(ref _timestampTicks);

        internal void Advance(TimeSpan elapsed)
        {
            Interlocked.Add(ref _utcTicks, elapsed.Ticks);
            Interlocked.Add(ref _timestampTicks, elapsed.Ticks);
        }

        internal void AdjustWallClock(TimeSpan adjustment)
            => Interlocked.Add(ref _utcTicks, adjustment.Ticks);

        internal void AdvanceMonotonic(TimeSpan elapsed)
            => Interlocked.Add(ref _timestampTicks, elapsed.Ticks);

        internal void AdjustMonotonic(TimeSpan adjustment)
            => Interlocked.Add(ref _timestampTicks, adjustment.Ticks);
    }

    private sealed class ThrowAfterFirstTimeProvider(DateTimeOffset initialUtcNow) : TimeProvider
    {
        private int _calls;

        internal DateTimeOffset InitialUtcNow { get; } = initialUtcNow;

        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Increment(ref _calls) == 1)
                return InitialUtcNow;

            throw new InvalidOperationException("Injected clock failure.");
        }

        public override long GetTimestamp()
            => throw new InvalidOperationException("Injected timestamp failure.");
    }
}

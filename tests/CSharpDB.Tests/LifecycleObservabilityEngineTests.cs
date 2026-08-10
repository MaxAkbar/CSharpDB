using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class LifecycleObservabilityEngineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DatabaseOpenAndClose_UseOperationClockCorrelationAndOneOutcome()
    {
        string databasePath = CreateDatabasePath();
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 14, 0, 0, TimeSpan.Zero));
        OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();
        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Maintenance,
            CSharpDbTransport.Direct,
            DatabaseAlias,
            sessionId,
            timeProvider: clock);
        DatabaseOptions options = CreateOptions(new AdvancingStorageEngineFactory(clock));
        using var events = new LifecycleEventRecorder();
        using IDisposable scope = CSharpDbOperationScope.Enter(parent);
        Database? database = null;

        try
        {
            database = await Database.OpenAsync(databasePath, options, Ct);

            CSharpDbLifecycleCompletedEvent opened = Assert.Single(
                events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, opened.Outcome);
            Assert.Equal(TimeSpan.FromSeconds(2), opened.Duration);
            Assert.Equal(clock.GetUtcNow(), opened.CompletedAtUtc);
            Assert.Equal(parent.OperationId, opened.Context.ParentOperationId);
            Assert.Equal(sessionId, opened.Context.SessionId);
            Assert.Equal(CSharpDbTransport.Direct, opened.Context.Transport);
            Assert.Empty(events.Events(CSharpDbLogEvents.DatabaseClosed.Name));

            await database.DisposeAsync();
            database = null;

            CSharpDbLifecycleCompletedEvent closed = Assert.Single(
                events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, closed.Outcome);
            Assert.Equal(clock.GetUtcNow(), closed.CompletedAtUtc);
            Assert.Equal(parent.OperationId, closed.Context.ParentOperationId);
            Assert.NotEqual(opened.Context.OperationId, closed.Context.OperationId);
        }
        finally
        {
            if (database is not null)
                await database.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DatabaseOpenFailureAndCancellation_EmitSafeOutcomesWithoutClose()
    {
        const string canary = "lifecycle-open-secret-7f3a";
        string databasePath = Path.Combine(Path.GetTempPath(), $"{canary}-{Guid.NewGuid():N}.db");
        using var events = new LifecycleEventRecorder();

        DatabaseOptions failureOptions = CreateOptions(
            new TerminalStorageEngineFactory(
                new InvalidOperationException($"failure at {databasePath}")));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => Database.OpenAsync(databasePath, failureOptions, Ct).AsTask());

        CSharpDbLifecycleCompletedEvent failed = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
        Assert.Equal(CSharpDbOperationOutcome.Failed, failed.Outcome);
        Assert.NotNull(failed.Error);
        Assert.Empty(events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
        AssertCanaryAbsent(failed, canary, databasePath);

        events.Clear();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cancellation.Cancel();
        DatabaseOptions canceledOptions = CreateOptions(new TerminalStorageEngineFactory());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Database.OpenAsync(
                databasePath,
                canceledOptions,
                cancellation.Token).AsTask());

        CSharpDbLifecycleCompletedEvent canceled = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
        Assert.Equal(CSharpDbOperationOutcome.Canceled, canceled.Outcome);
        Assert.NotNull(canceled.Error);
        Assert.Empty(events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
    }

    [Fact]
    public async Task ExplicitTransactions_CommitRollbackAndDisposeExactlyOnce()
    {
        using var events = new LifecycleEventRecorder();
        await using Database database = await Database.OpenInMemoryAsync(CreateOptions(), Ct);
        await ExecuteNonQueryAsync(
            database,
            "CREATE TABLE lifecycle_items (id INTEGER PRIMARY KEY, value INTEGER)");
        events.Clear();

        await database.BeginTransactionAsync(Ct);
        await ExecuteNonQueryAsync(database, "INSERT INTO lifecycle_items VALUES (1, 10)");
        await database.CommitAsync(Ct);
        AssertSucceededTransaction(events);

        events.Clear();
        await database.BeginTransactionAsync(Ct);
        await ExecuteNonQueryAsync(database, "INSERT INTO lifecycle_items VALUES (2, 20)");
        await database.RollbackAsync(Ct);
        AssertSucceededTransaction(events);

        events.Clear();
        await using (WriteTransaction transaction = await database.BeginWriteTransactionAsync(Ct))
        {
            await using QueryResult result = await transaction.ExecuteAsync(
                "INSERT INTO lifecycle_items VALUES (3, 30)",
                Ct);
            await transaction.CommitAsync(Ct);
        }
        AssertSucceededTransaction(events);

        events.Clear();
        await using (WriteTransaction transaction = await database.BeginWriteTransactionAsync(Ct))
        {
            await using QueryResult result = await transaction.ExecuteAsync(
                "INSERT INTO lifecycle_items VALUES (4, 40)",
                Ct);
        }
        AssertSucceededTransaction(events);
    }

    [Fact]
    public async Task WriteTransactionFailureAndCancellation_EmitOneSafeTerminalOutcome()
    {
        const string canary = "lifecycle-transaction-secret-2c91";
        using var events = new LifecycleEventRecorder();
        await using Database database = await Database.OpenInMemoryAsync(CreateOptions(), Ct);
        await ExecuteNonQueryAsync(
            database,
            "CREATE TABLE lifecycle_failures (id INTEGER PRIMARY KEY, value TEXT)");
        await ExecuteNonQueryAsync(
            database,
            $"INSERT INTO lifecycle_failures VALUES (1, '{canary}')");
        events.Clear();

        await using (WriteTransaction transaction = await database.BeginWriteTransactionAsync(Ct))
        {
            await Assert.ThrowsAsync<CSharpDbException>(
                () => transaction.ExecuteAsync(
                    "INSERT INTO lifecycle_failures VALUES (1, 'duplicate')",
                    Ct).AsTask());
            await Assert.ThrowsAsync<CSharpDbException>(() => transaction.CommitAsync(Ct).AsTask());
        }

        CSharpDbLifecycleCompletedEvent failed = Assert.Single(
            events.Events(CSharpDbLogEvents.TransactionCompleted.Name));
        Assert.Equal(CSharpDbOperationOutcome.Failed, failed.Outcome);
        Assert.NotNull(failed.Error);
        AssertCanaryAbsent(failed, canary);

        events.Clear();
        await using (WriteTransaction transaction = await database.BeginWriteTransactionAsync(Ct))
        {
            await using QueryResult result = await transaction.ExecuteAsync(
                "INSERT INTO lifecycle_failures VALUES (2, 'cancel-me')",
                Ct);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => transaction.CommitAsync(cancellation.Token).AsTask());
        }

        CSharpDbLifecycleCompletedEvent canceled = Assert.Single(
            events.Events(CSharpDbLogEvents.TransactionCompleted.Name));
        Assert.Equal(CSharpDbOperationOutcome.Canceled, canceled.Outcome);
        Assert.NotNull(canceled.Error);
    }

    [Fact]
    public async Task CheckpointAndBackup_EmitExactlyOneSuccessAndSafeFailure()
    {
        const string canary = "lifecycle-backup-secret-8b42";
        string databasePath = CreateDatabasePath();
        string backupPath = CreateDatabasePath();
        string failureDestination = Path.Combine(
            Path.GetTempPath(),
            $"{canary}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(failureDestination);
        using var events = new LifecycleEventRecorder();
        Database? database = null;

        try
        {
            database = await Database.OpenAsync(databasePath, CreateOptions(), Ct);
            await ExecuteNonQueryAsync(
                database,
                "CREATE TABLE lifecycle_backup_items (id INTEGER PRIMARY KEY)");
            await ExecuteNonQueryAsync(database, "INSERT INTO lifecycle_backup_items VALUES (1)");
            events.Clear();

            await database.CheckpointAsync(Ct);
            CSharpDbLifecycleCompletedEvent checkpoint = Assert.Single(
                events.Events(CSharpDbLogEvents.CheckpointCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, checkpoint.Outcome);

            events.Clear();
            _ = await DatabaseBackupCoordinator.BackupAsync(
                database,
                databasePath,
                backupPath,
                withManifest: false,
                Ct);
            CSharpDbLifecycleCompletedEvent backup = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, backup.Outcome);

            events.Clear();
            await Assert.ThrowsAnyAsync<Exception>(
                () => DatabaseBackupCoordinator.BackupAsync(
                    database,
                    databasePath,
                    failureDestination,
                    withManifest: false,
                    Ct).AsTask());
            CSharpDbLifecycleCompletedEvent failedBackup = Assert.Single(
                events.Events(CSharpDbLogEvents.BackupCompleted.Name));
            Assert.Equal(CSharpDbOperationOutcome.Failed, failedBackup.Outcome);
            Assert.NotNull(failedBackup.Error);
            AssertCanaryAbsent(failedBackup, canary, failureDestination);
        }
        finally
        {
            if (database is not null)
                await database.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
            DeleteDatabaseFiles(backupPath);
            if (Directory.Exists(failureDestination))
                Directory.Delete(failureDestination, recursive: true);
        }
    }

    [Fact]
    public async Task DisabledSuppressedAndThrowingListeners_DoNotChangeEngineBehavior()
    {
        using var events = new LifecycleEventRecorder();
        Database disabled = await Database.OpenInMemoryAsync(Ct);
        await disabled.CheckpointAsync(Ct);
        await disabled.DisposeAsync();
        Assert.Empty(events.AllEvents());

        using (CSharpDbOperationScope.SuppressDiagnostics())
        {
            Database suppressed = await Database.OpenInMemoryAsync(CreateOptions(), Ct);
            await suppressed.CheckpointAsync(Ct);
            await suppressed.DisposeAsync();
        }
        Assert.Empty(events.AllEvents());

        var throwingObserver = new ThrowingLifecycleObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            throwingObserver,
            LifecycleEventRecorder.IsLifecycleEvent);
        Database observed = await Database.OpenInMemoryAsync(CreateOptions(), Ct);
        await observed.CheckpointAsync(Ct);
        await observed.DisposeAsync();
        Assert.True(throwingObserver.AttemptCount >= 3);
    }

    private const string DatabaseAlias = "lifecycle-tests";

    private static DatabaseOptions CreateOptions(IStorageEngineFactory? storageEngineFactory = null)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = DatabaseAlias,
                Logging = new CSharpDbLoggingOptions { Enabled = true },
            },
            StorageEngineFactory = storageEngineFactory ?? new DefaultStorageEngineFactory(),
        };

    private static async ValueTask ExecuteNonQueryAsync(Database database, string sql)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, Ct);
    }

    private static void AssertSucceededTransaction(LifecycleEventRecorder events)
    {
        CSharpDbLifecycleCompletedEvent transaction = Assert.Single(
            events.Events(CSharpDbLogEvents.TransactionCompleted.Name));
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, transaction.Outcome);
        Assert.Null(transaction.Error);
    }

    private static void AssertCanaryAbsent(
        CSharpDbLifecycleCompletedEvent payload,
        params string[] canaries)
    {
        string json = JsonSerializer.Serialize(
            payload,
            CSharpDbObservabilityJsonContext.Default.CSharpDbLifecycleCompletedEvent);
        foreach (string canary in canaries)
            Assert.DoesNotContain(canary, json, StringComparison.OrdinalIgnoreCase);
    }

    private static string CreateDatabasePath()
        => Path.Combine(Path.GetTempPath(), $"csharpdb_lifecycle_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseFiles(string databasePath)
    {
        foreach (string path in new[]
                 {
                     databasePath,
                     databasePath + ".wal",
                     databasePath + ".manifest.json",
                 })
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private sealed class LifecycleEventRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private readonly object _gate = new();
        private readonly List<KeyValuePair<string, CSharpDbLifecycleCompletedEvent>> _events = [];
        private readonly IDisposable _subscription;

        internal LifecycleEventRecorder()
        {
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                IsLifecycleEvent);
        }

        internal static bool IsLifecycleEvent(string name)
            => name is
                "CSharpDB.Database.Opened" or
                "CSharpDB.Database.Closed" or
                "CSharpDB.Transaction.Completed" or
                "CSharpDB.Checkpoint.Completed" or
                "CSharpDB.Recovery.Completed" or
                "CSharpDB.Backup.Completed" or
                "CSharpDB.Restore.Completed" or
                "CSharpDB.Maintenance.Completed";

        internal CSharpDbLifecycleCompletedEvent[] Events(string name)
        {
            lock (_gate)
            {
                return _events
                    .Where(item => string.Equals(item.Key, name, StringComparison.Ordinal))
                    .Select(static item => item.Value)
                    .ToArray();
            }
        }

        internal CSharpDbLifecycleCompletedEvent[] AllEvents()
        {
            lock (_gate)
                return _events.Select(static item => item.Value).ToArray();
        }

        internal void Clear()
        {
            lock (_gate)
                _events.Clear();
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbLifecycleCompletedEvent payload)
                return;

            lock (_gate)
                _events.Add(new KeyValuePair<string, CSharpDbLifecycleCompletedEvent>(value.Key, payload));
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }

    private sealed class ThrowingLifecycleObserver : IObserver<KeyValuePair<string, object?>>
    {
        private int _attemptCount;

        internal int AttemptCount => Volatile.Read(ref _attemptCount);

        public void OnNext(KeyValuePair<string, object?> value)
        {
            Interlocked.Increment(ref _attemptCount);
            throw new InvalidOperationException("throwing-lifecycle-listener");
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }

    private sealed class TerminalStorageEngineFactory(Exception? failure = null) : IStorageEngineFactory
    {
        public ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            throw failure ?? new InvalidOperationException("A terminal failure was required.");
        }
    }

    private sealed class AdvancingStorageEngineFactory(ManualTimeProvider clock) : IStorageEngineFactory
    {
        private readonly DefaultStorageEngineFactory _inner = new();

        public async ValueTask<StorageEngineContext> OpenAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
        {
            clock.Advance(TimeSpan.FromSeconds(2));
            return await _inner.OpenAsync(filePath, options, ct);
        }

        public ValueTask<StorageEngineContext> CreateNewAsync(
            string filePath,
            StorageEngineOptions options,
            CancellationToken ct = default)
            => _inner.CreateNewAsync(filePath, options, ct);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
        {
            _utcNow = _utcNow.Add(duration);
            Interlocked.Add(ref _timestamp, duration.Ticks);
        }
    }
}

using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using Microsoft.Extensions.Logging;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ReentrantDiagnosticLoggerTests
{
    [Fact]
    public async Task QueryLogger_ReenteringClient_RunsAfterSerializationLockIsReleased()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-observability-reentrant-{Guid.NewGuid():N}.db");
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "reentrant-test",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = true,
                Queries = true,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.QueryCompleted,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            Task<CSharpDB.Client.Models.SqlExecutionResult> execution =
                client.ExecuteSqlAsync(
                    "SELECT 1;",
                    TestContext.Current.CancellationToken);
            Task completed = await Task.WhenAny(
                execution,
                Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));

            Assert.Same(execution, completed);
            Assert.Null((await execution).Error);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteIfExists(databasePath);
            DeleteIfExists(databasePath + ".wal");
            DeleteIfExists(databasePath + "-wal");
            DeleteIfExists(databasePath + "-shm");
        }
    }

    [Fact]
    public async Task FirstGetInfoLifecycleLogger_ReenteringClient_RunsAfterLockRelease()
    {
        string databasePath = NewDatabasePath("first-info");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseOpened,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            DatabaseInfo info = await CompleteWithinAsync(
                client.GetInfoAsync(TestContext.Current.CancellationToken));

            Assert.Equal(databasePath, info.DataSource);
            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task UnlockedFirstDatabaseOpenListener_ReenteringClient_IsDirectAndCorrelated()
    {
        string databasePath = NewDatabasePath("unlocked-open");
        var options = CreateLifecycleOnlyOptions();
        var observer = new ReentrantLifecycleObserver(
            CSharpDbLogEvents.DatabaseOpened.Name,
            static client => client.GetInfoAsync(CancellationToken.None));
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.DatabaseOpened.Name);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            observer.Client = client;

            Database? database = await CompleteWithinAsync(
                client.TryGetDatabaseAsync(TestContext.Current.CancellationToken).AsTask());
            await CompleteWithinAsync(observer.ReentryCompleted);

            Assert.NotNull(database);
            CSharpDbLifecycleCompletedEvent opened = Assert.IsType<CSharpDbLifecycleCompletedEvent>(
                observer.Payload);
            Assert.Equal(ObservabilityTransport.Direct, opened.Context.Transport);
            Assert.NotNull(opened.Context.SessionId);
            Assert.Equal("reentrant-lifecycle", opened.Context.DatabaseAlias);
            Assert.Equal(1, observer.MatchingEventCount);
            Assert.Equal(1, observer.ReentryCount);
            Assert.Null(observer.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task HttpCorrelationFirstOpenListener_ReenteringClient_IsBufferedAndCorrelated()
    {
        string databasePath = NewDatabasePath("http-open");
        var options = CreateLifecycleOnlyOptions();
        OpaqueDiagnosticsId hostSessionId = OpaqueDiagnosticsId.Create();
        var observer = new ReentrantLifecycleObserver(
            CSharpDbLogEvents.DatabaseOpened.Name,
            static client => client.GetInfoAsync(CancellationToken.None));
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.DatabaseOpened.Name);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            observer.Client = client;

            using (CSharpDbOperationScope.EnterTransport(
                       ObservabilityTransport.Http,
                       hostSessionId))
            {
                _ = await CompleteWithinAsync(
                    client.GetInfoAsync(TestContext.Current.CancellationToken));
            }

            CSharpDbLifecycleCompletedEvent opened = Assert.IsType<CSharpDbLifecycleCompletedEvent>(
                observer.Payload);
            Assert.Equal(ObservabilityTransport.Http, opened.Context.Transport);
            Assert.Equal(hostSessionId, opened.Context.SessionId);
            Assert.Equal(1, observer.MatchingEventCount);
            Assert.Equal(1, observer.ReentryCount);
            Assert.Null(observer.ReentryError);
            Assert.Equal(
                ObservabilityTransport.Embedded,
                CSharpDbOperationScope.CurrentTransport);
            Assert.Null(CSharpDbOperationScope.CurrentSessionId);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task BackupLifecycleLogger_ReenteringClient_RunsAfterLockRelease()
    {
        string databasePath = NewDatabasePath("backup-source");
        string backupPath = NewDatabasePath("backup-copy");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.BackupCompleted,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            BackupResult result = await CompleteWithinAsync(client.BackupAsync(
                new BackupRequest
                {
                    DestinationPath = backupPath,
                    WithManifest = false,
                },
                TestContext.Current.CancellationToken));

            Assert.Equal(Path.GetFullPath(backupPath), result.DestinationPath);
            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
            DeleteDatabaseFiles(backupPath);
        }
    }

    [Fact]
    public async Task CheckpointLifecycleLogger_ReenteringClient_IsDirectAndSafe()
    {
        string databasePath = NewDatabasePath("checkpoint");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.CheckpointCompleted,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            await CompleteWithinAsync(
                client.CheckpointAsync(TestContext.Current.CancellationToken));

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseLifecycleLogger_ReenteringClient_RunsAfterLockRelease()
    {
        string databasePath = NewDatabasePath("cached-release");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseClosed,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            await CompleteWithinAsync(
                client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask());

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DisposeLifecycleLogger_ReenteringLockTakingMethod_DoesNotDeadlock()
    {
        string databasePath = NewDatabasePath("dispose");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseClosed,
            static async client =>
            {
                try
                {
                    _ = await client.BeginTransactionAsync(CancellationToken.None);
                }
                catch (ObjectDisposedException)
                {
                    // Reentry must acquire the client lock before observing the
                    // expected disposing state.
                }
            });
        using var loggerFactory = new ReentrantLoggerFactory(logger);
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = options });

        try
        {
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            Task outerDispose = client.DisposeAsync().AsTask();
            await CompleteWithinAsync(
                Task.WhenAll(outerDispose, logger.ReentryCompleted));

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DisposeLifecycleLogger_CrossThreadAndDisposeReentry_DoesNotDeadlock()
    {
        string databasePath = NewDatabasePath("dispose-cross-thread");
        var options = CreateLifecycleOnlyOptions();
        int nestedDisposeCalls = 0;
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseClosed,
            client =>
            {
                _ = client.DisposeAsync();
                Interlocked.Increment(ref nestedDisposeCalls);
                return Task.Run(async () =>
                {
                    try
                    {
                        _ = await client.GetInfoAsync(CancellationToken.None);
                    }
                    catch (ObjectDisposedException)
                    {
                        // The cross-thread call may observe either the client
                        // or its already-closed database as disposed.
                    }
                });
            });
        using var loggerFactory = new ReentrantLoggerFactory(logger);
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = options });

        try
        {
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            Task outerDispose = client.DisposeAsync().AsTask();
            await CompleteWithinAsync(
                Task.WhenAll(outerDispose, logger.ReentryCompleted));

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Equal(1, Volatile.Read(ref nestedDisposeCalls));
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DisposeLifecycleLogger_DeliversDatabaseClosedBeforeOuterCompletion()
    {
        string databasePath = NewDatabasePath("dispose-delivery-order");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseClosed,
            static _ => Task.CompletedTask);
        using var loggerFactory = new ReentrantLoggerFactory(logger);
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = options });

        try
        {
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            await CompleteWithinAsync(client.DisposeAsync().AsTask());

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DisposeLifecycleLogger_AwaitingDisposeReentry_DoesNotSelfCycle()
    {
        string databasePath = NewDatabasePath("dispose-await-reentry");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.DatabaseClosed,
            static client => client.DisposeAsync().AsTask());
        using var loggerFactory = new ReentrantLoggerFactory(logger);
        var client = new EngineTransportClient(
            databasePath,
            new DatabaseOptions { ObservabilityOptions = options });

        try
        {
            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            Task outerDispose = client.DisposeAsync().AsTask();
            await CompleteWithinAsync(
                Task.WhenAll(outerDispose, logger.ReentryCompleted));

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task TransactionCompletionLifecycleLogger_ReenteringClient_RunsAfterFinalization()
    {
        string databasePath = NewDatabasePath("transaction");
        var options = CreateLifecycleOnlyOptions();
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.TransactionCompleted,
            static client => client.GetInfoAsync(CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            await client.CreateProcedureAsync(
                new ProcedureDefinition
                {
                    Name = "internal_transaction_controls",
                    BodySql = "SELECT 1;",
                    Parameters = [],
                    IsEnabled = true,
                    CreatedUtc = DateTime.UtcNow,
                    UpdatedUtc = DateTime.UtcNow,
                },
                TestContext.Current.CancellationToken);
            ProcedureExecutionResult procedure = await client.ExecuteProcedureAsync(
                "internal_transaction_controls",
                new Dictionary<string, object?>(),
                TestContext.Current.CancellationToken);

            Assert.True(procedure.Succeeded);
            Assert.Equal(0, logger.MatchingEventCount);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            await CompleteWithinAsync(client.CommitTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken));

            Assert.Equal(1, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task TransactionCursorTerminalLogger_ReenteringSession_RunsAfterGateRelease()
    {
        string databasePath = NewDatabasePath("transaction-cursor");
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "reentrant-cursor",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = true,
                Queries = true,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };
        string? transactionId = null;
        var logger = new ReentrantLogger(
            CSharpDbLogEventIds.QueryCompleted,
            client => client.ExecuteInTransactionAsync(
                transactionId!,
                "SELECT 2",
                CancellationToken.None));
        using var loggerFactory = new ReentrantLoggerFactory(logger);

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                new DatabaseOptions { ObservabilityOptions = options });
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE cursor_reentry (id INTEGER PRIMARY KEY); " +
                "INSERT INTO cursor_reentry VALUES (1);",
                TestContext.Current.CancellationToken)).Error);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            transactionId = transaction.TransactionId;
            logger.Client = client;
            using var bridge = new CSharpDbDiagnosticLoggerBridge(loggerFactory, options);

            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            await using ForwardOnlyQueryCursor cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT id FROM cursor_reentry",
                    TestContext.Current.CancellationToken));

            List<object?[]> rows = await CompleteWithinAsync(
                cursor.ReadNextAsync(10, TestContext.Current.CancellationToken).AsTask());

            Assert.Single(rows);
            Assert.Equal(2, logger.MatchingEventCount);
            Assert.Equal(1, logger.ReentryCount);
            Assert.Null(logger.ReentryError);

            await client.RollbackTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ListenerSubscribingDuringOpen_IsExcludedFromBoundarySnapshot()
    {
        string databasePath = NewDatabasePath("late-open-listener");
        var options = CreateLifecycleOnlyOptions();
        var observer = new ReentrantLifecycleObserver(
            CSharpDbLogEvents.DatabaseOpened.Name,
            static client => client.GetInfoAsync(CancellationToken.None));
        IDisposable? subscription = null;
        EngineTransportClient? client = null;

        try
        {
            client = new EngineTransportClient(
                databasePath,
                async (path, ct) =>
                {
                    subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                        observer,
                        static name => name == CSharpDbLogEvents.DatabaseOpened.Name);
                    return await Database.OpenAsync(
                        path,
                        new DatabaseOptions { ObservabilityOptions = options },
                        ct);
                },
                new DatabaseOptions { ObservabilityOptions = options });
            observer.Client = client;

            _ = await CompleteWithinAsync(
                client.GetInfoAsync(TestContext.Current.CancellationToken));

            Assert.Equal(0, observer.MatchingEventCount);
            Assert.Equal(0, observer.ReentryCount);
            Assert.Null(observer.ReentryError);
        }
        finally
        {
            subscription?.Dispose();
            if (client is not null)
                await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task CanceledFirstOpenWaiter_PreservesOneLockSafeDatabaseOpenedEvent()
    {
        string databasePath = NewDatabasePath("canceled-open-waiter");
        var options = CreateLifecycleOnlyOptions();
        var openEntered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOpen = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var firstCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        var observer = new ReentrantLifecycleObserver(
            CSharpDbLogEvents.DatabaseOpened.Name,
            static client => client.GetInfoAsync(CancellationToken.None));
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.DatabaseOpened.Name);
        EngineTransportClient? client = null;

        try
        {
            client = new EngineTransportClient(
                databasePath,
                async (path, ct) =>
                {
                    openEntered.TrySetResult();
                    await releaseOpen.Task;
                    return await Database.OpenAsync(
                        path,
                        new DatabaseOptions { ObservabilityOptions = options },
                        ct);
                },
                new DatabaseOptions { ObservabilityOptions = options });
            observer.Client = client;

            Task<DatabaseInfo> first = client.GetInfoAsync(firstCancellation.Token);
            await openEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            // The pending waiter registers its lock-lifetime hold before it
            // begins waiting for the first caller's client lock.
            Task<DatabaseInfo> second = client.GetInfoAsync(
                TestContext.Current.CancellationToken);
            firstCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);

            releaseOpen.TrySetResult();
            DatabaseInfo info = await CompleteWithinAsync(second);

            Assert.Equal(databasePath, info.DataSource);
            Assert.Equal(1, observer.MatchingEventCount);
            Assert.Equal(1, observer.ReentryCount);
            Assert.Null(observer.ReentryError);
        }
        finally
        {
            releaseOpen.TrySetResult();
            if (client is not null)
                await client.DisposeAsync();
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ConfiguredOperationalLogging_CreatesDirectBoundaryWithoutListener()
    {
        string databasePath = NewDatabasePath("no-listener");
        var options = CreateLifecycleOnlyOptions();
        ObservabilityTransport? transportDuringOpen = null;
        OpaqueDiagnosticsId? sessionDuringOpen = null;

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                async (path, ct) =>
                {
                    transportDuringOpen = CSharpDbOperationScope.CurrentTransport;
                    sessionDuringOpen = CSharpDbOperationScope.CurrentSessionId;
                    return await Database.OpenAsync(
                        path,
                        new DatabaseOptions { ObservabilityOptions = options },
                        ct);
                },
                new DatabaseOptions { ObservabilityOptions = options });

            _ = await client.GetInfoAsync(TestContext.Current.CancellationToken);

            Assert.Equal(ObservabilityTransport.Direct, transportDuringOpen);
            Assert.NotNull(sessionDuringOpen);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    private static CSharpDbObservabilityOptions CreateLifecycleOnlyOptions()
        => new()
        {
            Enabled = true,
            DatabaseAlias = "reentrant-lifecycle",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = true,
                Queries = false,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };

    private static string NewDatabasePath(string scenario)
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-observability-reentrant-{scenario}-{Guid.NewGuid():N}.db");

    private static async Task CompleteWithinAsync(Task operation)
    {
        Task completed = await Task.WhenAny(
            operation,
            Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Same(operation, completed);
        await operation;
    }

    private static async Task<T> CompleteWithinAsync<T>(Task<T> operation)
    {
        Task completed = await Task.WhenAny(
            operation,
            Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Same(operation, completed);
        return await operation;
    }

    private static void DeleteDatabaseFiles(string path)
    {
        DeleteIfExists(path);
        DeleteIfExists(path + ".wal");
        DeleteIfExists(path + "-wal");
        DeleteIfExists(path + "-shm");
        DeleteIfExists(path + ".manifest.json");
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
        }
    }

    private sealed class ReentrantLoggerFactory(ReentrantLogger logger) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => logger;

        public void Dispose()
        {
        }
    }

    private sealed class ReentrantLogger(
        int targetEventId,
        Func<EngineTransportClient, Task> reentry) : ILogger
    {
        private int _matchingEventCount;
        private int _reentryStarted;
        private int _reentryCount;
        private readonly TaskCompletionSource _reentryCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal EngineTransportClient? Client { get; set; }
        internal int MatchingEventCount => Volatile.Read(ref _matchingEventCount);
        internal int ReentryCount => Volatile.Read(ref _reentryCount);
        internal Exception? ReentryError { get; private set; }
        internal Task ReentryCompleted => _reentryCompleted.Task;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id != targetEventId)
                return;

            Interlocked.Increment(ref _matchingEventCount);
            if (Interlocked.Exchange(ref _reentryStarted, 1) != 0)
                return;

            try
            {
                reentry(Client!).GetAwaiter().GetResult();
                Interlocked.Increment(ref _reentryCount);
            }
            catch (Exception reentryError)
            {
                ReentryError = reentryError;
            }
            finally
            {
                _reentryCompleted.TrySetResult();
            }
        }
    }

    private sealed class ReentrantLifecycleObserver(
        string targetEventName,
        Func<EngineTransportClient, Task> reentry) :
        IObserver<KeyValuePair<string, object?>>
    {
        private int _matchingEventCount;
        private int _reentryCount;
        private readonly TaskCompletionSource _reentryCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        internal EngineTransportClient? Client { get; set; }
        internal object? Payload { get; private set; }
        internal int MatchingEventCount => Volatile.Read(ref _matchingEventCount);
        internal int ReentryCount => Volatile.Read(ref _reentryCount);
        internal Exception? ReentryError { get; private set; }
        internal Task ReentryCompleted => _reentryCompleted.Task;

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (!string.Equals(value.Key, targetEventName, StringComparison.Ordinal))
                return;

            Interlocked.Increment(ref _matchingEventCount);
            Payload = value.Value;
            try
            {
                reentry(Client!).GetAwaiter().GetResult();
                Interlocked.Increment(ref _reentryCount);
            }
            catch (Exception reentryError)
            {
                ReentryError = reentryError;
            }
            finally
            {
                _reentryCompleted.TrySetResult();
            }
        }
    }
}

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Client.Pipelines;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Pipelines.Models;
using CSharpDB.Sql;
using Microsoft.Extensions.Logging;
using DiagnosticTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ClientObservabilityTests
{
    private static CancellationToken TestCancellationToken
        => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DirectClient_SingleScriptAndProcedure_EmitOneCorrelatedHierarchy()
    {
        string databasePath = CreateDatabasePath();
        var options = CreateOptions(SqlTextCaptureMode.Normalized);
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name =>
                name == CSharpDbLogEvents.QueryCompleted.Name ||
                name == CSharpDbLogEvents.QueryFailed.Name ||
                name == CSharpDbLogEvents.QueryCanceled.Name);

        try
        {
            await using var client = new EngineTransportClient(databasePath, options);

            Assert.Null((await client.ExecuteSqlAsync("SELECT 1;", TestCancellationToken)).Error);
            CSharpDbQueryCompletedEvent single = Assert.Single(observer.CompletedEvents);
            Assert.Equal(CSharpDbOperationClass.Query, single.Context.OperationClass);
            Assert.Equal(CSharpDbOperationRole.Root, single.Context.Role);
            Assert.Equal(DiagnosticTransport.Direct, single.Context.Transport);
            Assert.NotNull(single.Context.SessionId);
            Assert.Null(single.Context.ParentOperationId);

            observer.Clear();
            Assert.Null((await client.ExecuteSqlAsync(
                """
                CREATE TABLE observed_script (id INTEGER PRIMARY KEY, value TEXT);
                INSERT INTO observed_script VALUES (1, 'seed');
                SELECT value FROM observed_script WHERE id = 1;
                """,
                TestCancellationToken)).Error);

            CSharpDbQueryCompletedEvent script = Assert.Single(
                observer.CompletedEvents,
                static item => item.Context.OperationClass == CSharpDbOperationClass.Script);
            Assert.Equal(CSharpDbOperationRole.Request, script.Context.Role);
            CSharpDbQueryCompletedEvent[] scriptStatements = observer.CompletedEvents
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(3, scriptStatements.Length);
            Assert.All(
                scriptStatements,
                item => Assert.Equal(script.Context.OperationId, item.Context.ParentOperationId));

            await client.CreateProcedureAsync(
                new ProcedureDefinition
                {
                    Name = "ObservedProcedure",
                    BodySql = """
                        INSERT INTO observed_script VALUES (@id, @value);
                        SELECT value FROM observed_script WHERE id = @id;
                        """,
                    Parameters =
                    [
                        new ProcedureParameterDefinition
                        {
                            Name = "id",
                            Type = CSharpDB.Client.Models.DbType.Integer,
                            Required = true,
                        },
                        new ProcedureParameterDefinition
                        {
                            Name = "value",
                            Type = CSharpDB.Client.Models.DbType.Text,
                            Required = true,
                        },
                    ],
                    IsEnabled = true,
                    CreatedUtc = DateTime.UtcNow,
                    UpdatedUtc = DateTime.UtcNow,
                },
                TestCancellationToken);

            observer.Clear();
            const string boundSecret = "BoundCustomer42Secret";
            ProcedureExecutionResult procedureResult = await client.ExecuteProcedureAsync(
                "ObservedProcedure",
                new Dictionary<string, object?>
                {
                    ["id"] = 2L,
                    ["value"] = boundSecret,
                },
                TestCancellationToken);
            Assert.True(procedureResult.Succeeded);

            CSharpDbQueryCompletedEvent procedure = Assert.Single(
                observer.CompletedEvents,
                static item => item.Context.OperationClass == CSharpDbOperationClass.Procedure);
            CSharpDbQueryCompletedEvent[] procedureStatements = observer.CompletedEvents
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(2, procedureStatements.Length);
            Assert.All(
                procedureStatements,
                item => Assert.Equal(procedure.Context.OperationId, item.Context.ParentOperationId));
            Assert.All(
                observer.CompletedEvents,
                item => Assert.DoesNotContain(
                    boundSecret,
                    item.CapturedSqlText ?? string.Empty,
                    StringComparison.Ordinal));

            QueryFingerprint expectedInsert = SqlQueryFingerprintProvider.Instance.CreateFingerprint(
                "INSERT INTO observed_script VALUES (@id, @value);",
                TestCancellationToken);
            Assert.Contains(
                procedureStatements,
                item => item.Context.QueryFingerprint == expectedInsert);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DirectClient_MalformedAndEmptyScripts_EmitOneSafeParentOutcome()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.Normalized));
            const string secret = "MalformedScriptSecret91";

            SqlExecutionResult malformed = await client.ExecuteSqlAsync(
                $"SELECT '{secret}",
                TestCancellationToken);

            Assert.NotNull(malformed.Error);
            CSharpDbQueryFailedEvent failed = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>());
            Assert.Equal(CSharpDbOperationClass.Script, failed.Context.OperationClass);
            Assert.Equal(CSharpDbOperationRole.Request, failed.Context.Role);
            Assert.Null(failed.CapturedSqlText);
            Assert.DoesNotContain(
                secret,
                Assert.IsType<SafeErrorProjection>(failed.Error).PublicDetail,
                StringComparison.Ordinal);
            Assert.Single(observer.TerminalEvents);

            observer.Clear();
            SqlExecutionResult empty = await client.ExecuteSqlAsync(
                "  -- intentionally empty\n  ",
                TestCancellationToken);

            Assert.Null(empty.Error);
            CSharpDbQueryCompletedEvent completed = Assert.Single(
                observer.Events<CSharpDbQueryCompletedEvent>());
            Assert.Equal(CSharpDbOperationClass.Script, completed.Context.OperationClass);
            Assert.Equal(CSharpDbOperationRole.Request, completed.Context.Role);
            Assert.Equal(0, completed.RowsProduced);
            Assert.Equal(0, completed.RowsAffected);
            Assert.Single(observer.TerminalEvents);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DirectClient_QueuedCancellation_ReportsMeasuredQueueAndOneTerminal()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.Normalized);
        Database database = await Database.OpenInMemoryAsync(options, TestCancellationToken);
        var openRequested = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOpen = new TaskCompletionSource<Database>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));
        var client = new EngineTransportClient(
            ":memory:queued-observability",
            (_, _) => OpenAsync(),
            options,
            observabilityTimeProvider: clock);
        Task<SqlExecutionResult>? lockHolder = null;

        async Task<Database> OpenAsync()
        {
            openRequested.TrySetResult();
            return await releaseOpen.Task;
        }

        try
        {
            lockHolder = client.ExecuteSqlAsync("SELECT 1", TestCancellationToken);
            await openRequested.Task.WaitAsync(TestCancellationToken);

            using var queuedCancellation = new CancellationTokenSource();
            Task<SqlExecutionResult> queued = client.ExecuteSqlAsync(
                "SELECT 2",
                queuedCancellation.Token);
            clock.Advance(TimeSpan.FromMilliseconds(250));
            queuedCancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);

            CSharpDbQueryCanceledEvent canceled = Assert.Single(
                observer.Events<CSharpDbQueryCanceledEvent>());
            Assert.Equal(CSharpDbOperationClass.Query, canceled.Context.OperationClass);
            Assert.Equal(CSharpDbOperationRole.Root, canceled.Context.Role);
            Assert.Equal(DiagnosticTransport.Direct, canceled.Context.Transport);
            Assert.NotNull(canceled.Context.SessionId);
            Assert.Equal(TimeSpan.FromMilliseconds(250), canceled.TotalDuration);
            Assert.Equal(TimeSpan.FromMilliseconds(250), canceled.QueueDuration);
            Assert.Equal(TimeSpan.Zero, canceled.ExecutionAndConsumptionDuration);
            Assert.Single(observer.TerminalEvents);
        }
        finally
        {
            releaseOpen.TrySetResult(database);
            if (lockHolder is not null)
            {
                try
                {
                    await lockHolder;
                }
                catch
                {
                }
            }

            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task DirectClient_QueuedLateSubscriber_BeginsWithNextQuery()
    {
        TimeSpan threshold = TimeSpan.FromMinutes(1);
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 30, 0, TimeSpan.Zero));
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        options.ObservabilityOptions!.Logging.SlowQueries = true;
        options.ObservabilityOptions.Logging.SlowQueryThreshold = threshold;
        options.ObservabilityOptions.LongRunningQueryThreshold = threshold;
        Database database = await Database.OpenInMemoryAsync(options, TestCancellationToken);
        var openRequested = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOpen = new TaskCompletionSource<Database>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var observer = new WarmupEventObserver();
        var client = new EngineTransportClient(
            ":memory:queued-listener-snapshot",
            (_, _) => OpenAsync(),
            options,
            observabilityTimeProvider: clock);
        Task<SqlExecutionResult>? lockHolder = null;

        async Task<Database> OpenAsync()
        {
            openRequested.TrySetResult();
            return await releaseOpen.Task;
        }

        try
        {
            lockHolder = client.ExecuteSqlAsync("SELECT 1", TestCancellationToken);
            await openRequested.Task.WaitAsync(TestCancellationToken);
            using var lateCancellation = new CancellationTokenSource();
            Task<SqlExecutionResult> startedBeforeSubscription =
                client.ExecuteSqlAsync("SELECT 2", lateCancellation.Token);

            using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                observer,
                static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));
            clock.Advance(threshold);
            CSharpDbRuntimeDiagnosticsState runtimeState =
                Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
                    client.CurrentRuntimeDiagnosticsState);
            QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
                runtimeState,
                startSweepTimer: false);

            Assert.Equal(2, registry.SweepLongRunningQueries());
            Assert.Empty(observer.Events<CSharpDbLongRunningQueryEvent>(
                CSharpDbLogEvents.LongRunningQuery.Name));

            lateCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => startedBeforeSubscription);
            Assert.Empty(observer.EventsStartingWith("CSharpDB.Query."));

            using var nextCancellation = new CancellationTokenSource();
            Task<SqlExecutionResult> startedAfterSubscription =
                client.ExecuteSqlAsync("SELECT 3", nextCancellation.Token);
            clock.Advance(threshold);

            Assert.Equal(1, registry.SweepLongRunningQueries());
            Assert.Single(observer.Events<CSharpDbLongRunningQueryEvent>(
                CSharpDbLogEvents.LongRunningQuery.Name));

            nextCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => startedAfterSubscription);
            Assert.Single(observer.Events<CSharpDbQueryCanceledEvent>(
                CSharpDbLogEvents.QueryCanceled.Name));
        }
        finally
        {
            releaseOpen.TrySetResult(database);
            if (lockHolder is not null)
            {
                try
                {
                    await lockHolder;
                }
                catch
                {
                }
            }
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task DirectClient_ScriptParent_SumsEverySelectAndMutation()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.None));
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE script_totals (id INTEGER PRIMARY KEY, value INTEGER); " +
                "INSERT INTO script_totals VALUES (1, 10), (2, 20);",
                TestCancellationToken)).Error);
            observer.Clear();

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                "SELECT * FROM script_totals ORDER BY id; " +
                "UPDATE script_totals SET value = 30 WHERE id = 1; " +
                "SELECT * FROM script_totals WHERE id = 1;",
                TestCancellationToken);

            Assert.Null(result.Error);
            CSharpDbQueryCompletedEvent parent = Assert.Single(
                observer.CompletedEvents,
                static item => item.Context.OperationClass == CSharpDbOperationClass.Script);
            Assert.Equal(3, parent.RowsProduced);
            Assert.Equal(1, parent.RowsAffected);
            Assert.InRange(parent.QueueDuration, TimeSpan.Zero, parent.TotalDuration);
            CSharpDbQueryCompletedEvent[] children = observer.CompletedEvents
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(3, children.Length);
            Assert.All(children, static child => Assert.Equal(TimeSpan.Zero, child.QueueDuration));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DirectClient_PrimaryKeyAndCursorShortcuts_PreserveBoundaryUntilTerminal()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.None));
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE shortcut_items (id INTEGER PRIMARY KEY, value TEXT); " +
                "INSERT INTO shortcut_items VALUES (1, 'one');",
                TestCancellationToken)).Error);
            observer.Clear();

            Dictionary<string, object?>? row = await client.GetRowByPkAsync(
                "shortcut_items",
                "id",
                1L,
                TestCancellationToken);
            Assert.NotNull(row);
            CSharpDbQueryCompletedEvent primaryKey = Assert.Single(observer.CompletedEvents);
            Assert.Equal(DiagnosticTransport.Direct, primaryKey.Context.Transport);
            Assert.NotNull(primaryKey.Context.SessionId);
            Assert.Equal(1, primaryKey.RowsProduced);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestCancellationToken);
            observer.Clear();
            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            ForwardOnlyQueryCursor cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT * FROM shortcut_items ORDER BY id",
                    TestCancellationToken));
            try
            {
                Assert.Empty(observer.TerminalEvents);
                Assert.Single(await cursor.ReadNextAsync(10, TestCancellationToken));

                CSharpDbQueryCompletedEvent cursorCompleted = Assert.Single(observer.CompletedEvents);
                Assert.Equal(DiagnosticTransport.Direct, cursorCompleted.Context.Transport);
                Assert.NotNull(cursorCompleted.Context.SessionId);
                Assert.Equal(1, cursorCompleted.RowsProduced);
            }
            finally
            {
                await cursor.DisposeAsync();
            }

            await client.RollbackTransactionAsync(transaction.TransactionId, TestCancellationToken);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task PublicDirectCursor_PreservesBoundaryThroughExhaustionAndDisposal()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using ICSharpDbClient contract = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    DataSource = databasePath,
                    DirectDatabaseOptions = CreateOptions(SqlTextCaptureMode.None),
                });
            var client = Assert.IsType<CSharpDbClient>(contract);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE public_cursor_items (id INTEGER PRIMARY KEY); " +
                "INSERT INTO public_cursor_items VALUES (1);",
                TestCancellationToken)).Error);
            observer.Clear();

            await using (ForwardOnlyQueryCursor exhausted =
                         Assert.IsType<ForwardOnlyQueryCursor>(
                             await client.TryOpenForwardOnlyQueryCursorAsync(
                                 "SELECT id FROM public_cursor_items",
                                 TestCancellationToken)))
            {
                Assert.Empty(observer.TerminalEvents);
                Assert.Single(await exhausted.ReadNextAsync(10, TestCancellationToken));

                CSharpDbQueryCompletedEvent terminal = Assert.Single(observer.CompletedEvents);
                Assert.Equal(DiagnosticTransport.Direct, terminal.Context.Transport);
                Assert.NotNull(terminal.Context.SessionId);
                Assert.Equal(1, terminal.RowsProduced);
            }

            Assert.Single(observer.TerminalEvents);
            OpaqueDiagnosticsId sessionId = observer.TerminalEvents[0].Context.SessionId!;
            observer.Clear();

            ForwardOnlyQueryCursor disposed = Assert.IsType<ForwardOnlyQueryCursor>(
                await client.TryOpenForwardOnlyQueryCursorAsync(
                    "SELECT id FROM public_cursor_items",
                    TestCancellationToken));
            Assert.Empty(observer.TerminalEvents);
            await disposed.DisposeAsync();

            CSharpDbQueryCompletedEvent disposedTerminal = Assert.Single(observer.CompletedEvents);
            Assert.Equal(DiagnosticTransport.Direct, disposedTerminal.Context.Transport);
            Assert.Equal(sessionId, disposedTerminal.Context.SessionId);
            Assert.Equal(0, disposedTerminal.RowsProduced);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task PublicDirectCursor_OpenFailure_EmitsOneCorrelatedTerminal()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        await using var client = new EngineTransportClient(
            databasePath,
            static (_, _) => Task.FromException<Database>(
                new InvalidOperationException("injected open failure")),
            CreateOptions(SqlTextCaptureMode.None));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await client.TryOpenForwardOnlyQueryCursorAsync(
                "SELECT 1",
                TestCancellationToken));

        CSharpDbQueryFailedEvent failed = Assert.Single(
            observer.Events<CSharpDbQueryFailedEvent>());
        Assert.Equal(DiagnosticTransport.Direct, failed.Context.Transport);
        Assert.NotNull(failed.Context.SessionId);
        Assert.Equal(TimeSpan.Zero, failed.QueueDuration);
        Assert.Single(observer.TerminalEvents);
    }

    [Fact]
    public async Task PublicDirectCursor_OpenCancellation_EmitsOneCorrelatedTerminal()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        await using var client = new EngineTransportClient(
            databasePath,
            static (_, _) => Task.FromException<Database>(
                new OperationCanceledException()),
            CreateOptions(SqlTextCaptureMode.None));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await client.TryOpenForwardOnlyQueryCursorAsync(
                "SELECT 1",
                TestCancellationToken));

        CSharpDbQueryCanceledEvent canceled = Assert.Single(
            observer.Events<CSharpDbQueryCanceledEvent>());
        Assert.Equal(DiagnosticTransport.Direct, canceled.Context.Transport);
        Assert.NotNull(canceled.Context.SessionId);
        Assert.Equal(TimeSpan.Zero, canceled.QueueDuration);
        Assert.Single(observer.TerminalEvents);
    }

    [Fact]
    public async Task DirectTransaction_QueuedCancellationAndReentrantCompletion_AreOnceOnly()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 13, 0, 0, TimeSpan.Zero));
        string databasePath = CreateDatabasePath();
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                (path, token) => Database.OpenAsync(path, options, token).AsTask(),
                options,
                observabilityTimeProvider: clock);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE transaction_queue_items (id INTEGER PRIMARY KEY)",
                TestCancellationToken)).Error);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestCancellationToken);
            Assert.Null((await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "INSERT INTO transaction_queue_items VALUES (1)",
                TestCancellationToken)).Error);

            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            ForwardOnlyQueryCursor cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT * FROM transaction_queue_items",
                    TestCancellationToken));
            try
            {
                observer.Clear();
                using var queuedCancellation = new CancellationTokenSource();
                Task<SqlExecutionResult> queued = client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT 42",
                    queuedCancellation.Token);
                clock.Advance(TimeSpan.FromMilliseconds(300));
                queuedCancellation.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
                CSharpDbQueryCanceledEvent canceled = Assert.Single(
                    observer.Events<CSharpDbQueryCanceledEvent>());
                Assert.Equal(DiagnosticTransport.Direct, canceled.Context.Transport);
                Assert.NotNull(canceled.Context.SessionId);
                Assert.Equal(TimeSpan.FromMilliseconds(300), canceled.TotalDuration);
                Assert.Equal(TimeSpan.FromMilliseconds(300), canceled.QueueDuration);
                Assert.Equal(TimeSpan.Zero, canceled.ExecutionAndConsumptionDuration);
                Assert.Single(observer.TerminalEvents);
            }
            finally
            {
                await cursor.DisposeAsync();
            }

            observer.Clear();
            var reentrant = new ReentrantTransactionObserver(
                client,
                transaction.TransactionId);
            using IDisposable reentrantSubscription =
                CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                    reentrant,
                    static name => name == CSharpDbLogEvents.QueryCompleted.Name);

            Task<SqlExecutionResult> execution = client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "SELECT 1",
                TestCancellationToken);
            Task completed = await Task.WhenAny(
                execution,
                Task.Delay(TimeSpan.FromSeconds(10), TestCancellationToken));

            Assert.Same(execution, completed);
            Assert.Null((await execution).Error);
            Assert.Equal(1, reentrant.ReentryCount);
            Assert.Null(reentrant.ReentryError);
            CSharpDbQueryTerminalEvent[] successful = observer.TerminalEvents.ToArray();
            Assert.Equal(2, successful.Length);
            Assert.All(successful, item =>
            {
                Assert.Equal(CSharpDbOperationOutcome.Succeeded, item.Outcome);
                Assert.Equal(DiagnosticTransport.Direct, item.Context.Transport);
                Assert.NotNull(item.Context.SessionId);
            });
            Assert.Equal(
                2,
                successful.Select(static item => item.Context.OperationId).Distinct().Count());

            await client.RollbackTransactionAsync(
                transaction.TransactionId,
                TestCancellationToken);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task InvalidTransactionLookup_ReportsZeroQueueForQueryAndCursor()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.None));

            await Assert.ThrowsAsync<CSharpDbClientException>(() =>
                client.ExecuteInTransactionAsync(
                    "missing-transaction",
                    "SELECT 1",
                    TestCancellationToken));
            CSharpDbQueryFailedEvent queryFailure = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>());
            Assert.Equal(TimeSpan.Zero, queryFailure.QueueDuration);
            Assert.Equal(
                queryFailure.TotalDuration,
                queryFailure.ExecutionAndConsumptionDuration);

            observer.Clear();
            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            await Assert.ThrowsAsync<CSharpDbClientException>(async () =>
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    "missing-transaction",
                    "SELECT 1",
                    TestCancellationToken));
            CSharpDbQueryFailedEvent cursorFailure = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>());
            Assert.Equal(TimeSpan.Zero, cursorFailure.QueueDuration);
            Assert.Equal(
                cursorFailure.TotalDuration,
                cursorFailure.ExecutionAndConsumptionDuration);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ScriptAndProcedureFailureParents_RetainCompletedChildTotals()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.None));
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE partial_totals (id INTEGER PRIMARY KEY);",
                TestCancellationToken)).Error);
            observer.Clear();

            SqlExecutionResult script = await client.ExecuteSqlAsync(
                "SELECT 1; " +
                "INSERT INTO partial_totals VALUES (1); " +
                "SELECT FROM partial_totals;",
                TestCancellationToken);

            Assert.NotNull(script.Error);
            CSharpDbQueryFailedEvent scriptParent = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>(),
                static item => item.Context.OperationClass == CSharpDbOperationClass.Script);
            Assert.Equal(1, scriptParent.RowsProduced);
            Assert.Equal(1, scriptParent.RowsAffected);

            await client.CreateProcedureAsync(
                new ProcedureDefinition
                {
                    Name = "partial_failure",
                    BodySql =
                        "SELECT 1; " +
                        "INSERT INTO partial_totals VALUES (2); " +
                        "SELECT FROM partial_totals;",
                    Parameters = [],
                    IsEnabled = true,
                    CreatedUtc = DateTime.UtcNow,
                    UpdatedUtc = DateTime.UtcNow,
                },
                TestCancellationToken);
            observer.Clear();

            ProcedureExecutionResult procedure = await client.ExecuteProcedureAsync(
                "partial_failure",
                new Dictionary<string, object?>(),
                TestCancellationToken);

            Assert.False(procedure.Succeeded);
            CSharpDbQueryFailedEvent procedureParent = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>(),
                static item => item.Context.OperationClass == CSharpDbOperationClass.Procedure);
            Assert.Equal(1, procedureParent.RowsProduced);
            Assert.Equal(1, procedureParent.RowsAffected);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ScriptCancellationParent_RetainsCompletedChildTotals()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using var executionCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(TestCancellationToken);
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None)
            .ConfigureFunctions(functions => functions.AddScalar(
                "CancelObservedScript",
                1,
                new CSharpDB.Primitives.DbScalarFunctionOptions(
                    CSharpDB.Primitives.DbType.Integer,
                    IsDeterministic: false),
                (_, arguments) =>
                {
                    executionCancellation.Cancel();
                    return arguments[0];
                }));
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(databasePath, options);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE cancellation_items (id INTEGER PRIMARY KEY); " +
                "INSERT INTO cancellation_items VALUES (1);",
                TestCancellationToken)).Error);
            observer.Clear();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.ExecuteSqlAsync(
                    "SELECT CancelObservedScript(1); " +
                    "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM cancellation_items;",
                    executionCancellation.Token));

            CSharpDbQueryCanceledEvent parent = Assert.Single(
                observer.Events<CSharpDbQueryCanceledEvent>(),
                static item => item.Context.OperationClass == CSharpDbOperationClass.Script);
            Assert.Equal(1, parent.RowsProduced);
            Assert.Equal(0, parent.RowsAffected);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task DirectClient_InvalidPrimaryKeyShortcut_EmitsOneSafeTerminal()
    {
        const string canary = @"PkNormalizationSecret77_C:\private\row.db";
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.Normalized));
            string invalidTableName = canary + new string('x', 1_024);

            await Assert.ThrowsAsync<CSharpDbClientException>(() => client.GetRowByPkAsync(
                invalidTableName,
                "id",
                1L,
                TestCancellationToken));

            CSharpDbQueryFailedEvent failed = Assert.Single(
                observer.Events<CSharpDbQueryFailedEvent>());
            Assert.Equal(CSharpDbOperationClass.Query, failed.Context.OperationClass);
            Assert.Equal(CSharpDbOperationRole.Root, failed.Context.Role);
            Assert.Equal(DiagnosticTransport.Direct, failed.Context.Transport);
            Assert.NotNull(failed.Context.SessionId);
            Assert.Null(failed.Context.QueryFingerprint);
            Assert.Null(failed.CapturedSqlText);
            Assert.DoesNotContain(canary, failed.Error!.PublicDetail, StringComparison.Ordinal);
            string serialized = JsonSerializer.Serialize(
                failed,
                CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
            Assert.DoesNotContain(canary, serialized, StringComparison.Ordinal);
            Assert.DoesNotContain(@"C:\private", serialized, StringComparison.Ordinal);
            Assert.Single(observer.TerminalEvents);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectOptionsSnapshot_EnabledToDisabledBeforeFirstOpen_StaysEnabled(
        bool privateMemory)
    {
        string databasePath = CreateDatabasePath();
        string dataSource = privateMemory ? ":memory:" : databasePath;
        var configured = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            DatabaseAlias = "snapshot-enabled",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = true,
                Queries = true,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };
        var observer = new WarmupEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name =>
                name == CSharpDbLogEvents.DatabaseOpened.Name ||
                name == CSharpDbLogEvents.QueryCompleted.Name);

        try
        {
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    DataSource = dataSource,
                    DirectDatabaseOptions = new DatabaseOptions
                    {
                        ObservabilityOptions = configured,
                    },
                });

            configured.Enabled = false;
            configured.DatabaseAlias = "mutated-disabled";
            configured.Logging.Enabled = false;
            configured.Logging.Queries = false;

            Assert.Null((await client.ExecuteSqlAsync(
                "SELECT 1",
                TestCancellationToken)).Error);

            CSharpDbLifecycleCompletedEvent opened = Assert.Single(
                observer.Events<CSharpDbLifecycleCompletedEvent>(
                    CSharpDbLogEvents.DatabaseOpened.Name));
            CSharpDbQueryCompletedEvent query = Assert.Single(
                observer.Events<CSharpDbQueryCompletedEvent>(
                    CSharpDbLogEvents.QueryCompleted.Name));
            Assert.Equal("snapshot-enabled", opened.Context.DatabaseAlias);
            Assert.Equal("snapshot-enabled", query.Context.DatabaseAlias);
            Assert.Equal(DiagnosticTransport.Direct, opened.Context.Transport);
            Assert.Equal(DiagnosticTransport.Direct, query.Context.Transport);
            Assert.NotNull(opened.Context.SessionId);
            Assert.Equal(opened.Context.SessionId, query.Context.SessionId);
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectOptionsSnapshot_DisabledToEnabledBeforeFirstOpen_StaysDisabled(
        bool privateMemory)
    {
        string databasePath = CreateDatabasePath();
        string dataSource = privateMemory ? ":memory:" : databasePath;
        var configured = new CSharpDbObservabilityOptions
        {
            Enabled = false,
            DatabaseAlias = "snapshot-disabled",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                Queries = false,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.None,
            },
        };
        var observer = new WarmupEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name =>
                name == CSharpDbLogEvents.DatabaseOpened.Name ||
                name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    DataSource = dataSource,
                    DirectDatabaseOptions = new DatabaseOptions
                    {
                        ObservabilityOptions = configured,
                    },
                });

            configured.Enabled = true;
            configured.DatabaseAlias = "mutated-enabled";
            configured.Logging.Enabled = true;
            configured.Logging.Queries = true;

            Assert.Null((await client.ExecuteSqlAsync(
                "SELECT 1",
                TestCancellationToken)).Error);

            Assert.Empty(observer.Events<CSharpDbLifecycleCompletedEvent>(
                CSharpDbLogEvents.DatabaseOpened.Name));
            Assert.Empty(observer.EventsStartingWith("CSharpDB.Query."));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task FirstGetInfo_EmitsDatabaseOpenedWithoutInternalCatalogQueries()
    {
        string databasePath = CreateDatabasePath();
        var observer = new WarmupEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name =>
                name == CSharpDbLogEvents.DatabaseOpened.Name ||
                name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using var client = new EngineTransportClient(
                databasePath,
                CreateOptions(SqlTextCaptureMode.Normalized));

            DatabaseInfo info = await client.GetInfoAsync(TestCancellationToken);

            Assert.Equal(0, info.ProcedureCount);
            Assert.Equal(0, info.SavedQueryCount);
            CSharpDbLifecycleCompletedEvent opened = Assert.Single(
                observer.Events<CSharpDbLifecycleCompletedEvent>(
                    CSharpDbLogEvents.DatabaseOpened.Name));
            Assert.Equal(CSharpDbOperationOutcome.Succeeded, opened.Outcome);
            Assert.Equal("client-test", opened.Context.DatabaseAlias);
            Assert.Empty(observer.EventsStartingWith("CSharpDB.Query."));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task PipelineRun_EmitsOneParentAndOnlyUserSqlChildren()
    {
        string databasePath = CreateDatabasePath();
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions
            {
                DataSource = databasePath,
                DirectDatabaseOptions = CreateOptions(SqlTextCaptureMode.Normalized),
            });
            Assert.Null((await client.ExecuteSqlAsync(
                """
                CREATE TABLE pipeline_source (id INTEGER PRIMARY KEY);
                CREATE TABLE pipeline_destination (id INTEGER PRIMARY KEY, payload TEXT);
                INSERT INTO pipeline_source VALUES (1);
                """,
                TestCancellationToken)).Error);
            observer.Clear();

            const string secret = "PipelineValueSecret42";
            using var activity = new Activity("pipeline-observability-test");
            activity.SetIdFormat(ActivityIdFormat.W3C);
            activity.Start();

            PipelineRunResult result = await new CSharpDbPipelineRunner(client).RunPackageAsync(
                new PipelinePackageDefinition
                {
                    Name = "observed-pipeline",
                    Version = "1.0.0",
                    Source = new PipelineSourceDefinition
                    {
                        Kind = PipelineSourceKind.SqlQuery,
                        QueryText = $"SELECT id, '{secret}' AS payload FROM pipeline_source;",
                    },
                    Destination = new PipelineDestinationDefinition
                    {
                        Kind = PipelineDestinationKind.CSharpDbTable,
                        TableName = "pipeline_destination",
                    },
                    Options = new PipelineExecutionOptions
                    {
                        BatchSize = 10,
                        ErrorMode = PipelineErrorMode.FailFast,
                    },
                },
                ct: TestCancellationToken);

            Assert.Equal(PipelineRunStatus.Succeeded, result.Status);
            CSharpDbQueryTerminalEvent[] events = observer.TerminalEvents.ToArray();
            Assert.Equal(3, events.Length);
            CSharpDbQueryTerminalEvent pipeline = Assert.Single(
                events,
                static item => item.Context.OperationClass == CSharpDbOperationClass.Pipeline);
            Assert.Equal(CSharpDbOperationRole.Request, pipeline.Context.Role);
            Assert.True(pipeline.Context.CountsAsRequest);
            Assert.False(pipeline.Context.CountsAsStatement);
            Assert.Equal(DiagnosticTransport.Direct, pipeline.Context.Transport);
            Assert.Null(pipeline.Context.ParentOperationId);
            Assert.NotNull(pipeline.Context.TraceId);

            CSharpDbQueryTerminalEvent[] statements = events
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Statement)
                .ToArray();
            Assert.Equal(2, statements.Length);
            Assert.All(statements, statement =>
            {
                Assert.Equal(pipeline.Context.OperationId, statement.Context.ParentOperationId);
                Assert.Equal(pipeline.Context.TraceId, statement.Context.TraceId);
                Assert.True(statement.Context.CountsAsStatement);
                Assert.False(statement.Context.CountsAsRequest);
                Assert.Equal(DiagnosticTransport.Direct, statement.Context.Transport);
                Assert.NotNull(statement.Context.QueryFingerprint);
            });
            Assert.Equal(1, events.Count(static item => item.Context.CountsAsRequest));
            Assert.Equal(2, events.Count(static item => item.Context.CountsAsStatement));
            Assert.Equal(events.Length, events.Select(static item => item.Context.OperationId).Distinct().Count());
            Assert.All(events, item => Assert.DoesNotContain(
                secret,
                item.CapturedSqlText ?? string.Empty,
                StringComparison.Ordinal));
        }
        finally
        {
            DeleteDatabaseFiles(databasePath);
        }
    }

    [Fact]
    public async Task ShardedFanOut_EmitsOneCoordinatorAndInternalAttemptsExactlyOnce()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_client_observability_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            var options = new CSharpDbShardingOptions
            {
                Keyspace = "observed",
                MapVersion = 1,
                VirtualBucketCount = 2,
                Shards =
                [
                    new CSharpDbShardDefinition
                    {
                        ShardId = "shard-0",
                        DataSource = Path.Combine(directory, "shard-0.db"),
                    },
                    new CSharpDbShardDefinition
                    {
                        ShardId = "shard-1",
                        DataSource = Path.Combine(directory, "shard-1.db"),
                    },
                ],
                BucketRanges =
                [
                    new CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 0,
                        EndBucketExclusive = 1,
                        ShardId = "shard-0",
                    },
                    new CSharpDbShardBucketRange
                    {
                        StartBucketInclusive = 1,
                        EndBucketExclusive = 2,
                        ShardId = "shard-1",
                    },
                ],
                DirectDatabaseOptions = CreateOptions(SqlTextCaptureMode.Normalized),
            };
            await using CSharpDbShardedClient client =
                await CSharpDbShardedClient.CreateAsync(options, ct: TestCancellationToken);
            Assert.All(
                await client.ExecuteSqlOnAllShardsAsync(
                    "CREATE TABLE fanout_items (id INTEGER PRIMARY KEY);",
                    TestCancellationToken),
                static item => Assert.Null(item.Error));
            observer.Clear();

            const string secret = "FanOutLiteralSecret77";
            using var activity = new Activity("fanout-observability-test");
            activity.SetIdFormat(ActivityIdFormat.W3C);
            activity.Start();
            IReadOnlyList<CSharpDbShardSqlExecutionResult> results =
                await client.ExecuteReadOnlySqlOnAllShardsAsync(
                    $"SELECT '{secret}' FROM fanout_items;",
                    TestCancellationToken);

            Assert.Equal(2, results.Count);
            Assert.All(results, static item => Assert.Null(item.Error));
            CSharpDbQueryTerminalEvent[] successful = observer.TerminalEvents.ToArray();
            Assert.Equal(3, successful.Length);
            CSharpDbQueryTerminalEvent coordinator = Assert.Single(
                successful,
                static item => item.Context.Role == CSharpDbOperationRole.Root);
            Assert.Equal(DiagnosticTransport.Sharded, coordinator.Context.Transport);
            Assert.True(coordinator.Context.CountsAsRequest);
            Assert.True(coordinator.Context.CountsAsStatement);
            Assert.NotNull(coordinator.Context.TraceId);

            CSharpDbQueryTerminalEvent[] attempts = successful
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Internal)
                .ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.All(attempts, attempt =>
            {
                Assert.Equal(coordinator.Context.OperationId, attempt.Context.ParentOperationId);
                Assert.Equal(coordinator.Context.TraceId, attempt.Context.TraceId);
                Assert.Equal(DiagnosticTransport.Direct, attempt.Context.Transport);
                Assert.False(attempt.Context.CountsAsRequest);
                Assert.False(attempt.Context.CountsAsStatement);
                Assert.Equal(coordinator.Context.QueryFingerprint, attempt.Context.QueryFingerprint);
            });
            Assert.Equal(1, successful.Count(static item => item.Context.CountsAsRequest));
            Assert.Equal(1, successful.Count(static item => item.Context.CountsAsStatement));
            Assert.Equal(successful.Length, successful.Select(static item => item.Context.OperationId).Distinct().Count());
            Assert.All(successful, item => Assert.DoesNotContain(
                secret,
                item.CapturedSqlText ?? string.Empty,
                StringComparison.Ordinal));

            observer.Clear();
            IReadOnlyList<CSharpDbShardSqlExecutionResult> failedResults =
                await client.ExecuteReadOnlySqlOnAllShardsAsync(
                    $"SELECT '{secret}' FROM missing_fanout_table;",
                    TestCancellationToken);
            Assert.All(failedResults, static item => Assert.NotNull(item.Error));
            CSharpDbQueryFailedEvent[] failed = observer.Events<CSharpDbQueryFailedEvent>();
            Assert.Equal(3, failed.Length);
            Assert.Equal(3, failed.Select(static item => item.Context.OperationId).Distinct().Count());
            Assert.Single(failed, static item => item.Context.Role == CSharpDbOperationRole.Root);
            Assert.Equal(2, failed.Count(static item => item.Context.Role == CSharpDbOperationRole.Internal));
            Assert.All(failed, item =>
            {
                Assert.DoesNotContain(secret, item.CapturedSqlText ?? string.Empty, StringComparison.Ordinal);
                Assert.DoesNotContain(
                    secret,
                    Assert.IsType<SafeErrorProjection>(item.Error).PublicDetail,
                    StringComparison.Ordinal);
            });
            Assert.Equal(3, observer.TerminalEvents.Count);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ShardedOptionsSnapshot_EnabledToDisabled_PreservesCoordinatorHierarchy()
    {
        string directory = CreateShardedDatabaseDirectory();
        var configured = CreateObservabilityOptions(
            enabled: true,
            databaseAlias: "sharded-snapshot-enabled");
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using CSharpDbShardedClient client = await CSharpDbShardedClient.CreateAsync(
                CreateObservedShardingOptions(directory, configured),
                ct: TestCancellationToken);
            observer.Clear();

            configured.Enabled = false;
            configured.DatabaseAlias = "mutated-disabled";
            configured.Logging.Enabled = false;
            configured.Logging.Queries = false;

            IReadOnlyList<CSharpDbShardSqlExecutionResult> results =
                await client.ExecuteReadOnlySqlOnAllShardsAsync(
                    "SELECT 1;",
                    TestCancellationToken);

            Assert.All(results, static result => Assert.Null(result.Error));
            CSharpDbQueryTerminalEvent[] events = observer.TerminalEvents.ToArray();
            Assert.Equal(3, events.Length);
            CSharpDbQueryTerminalEvent coordinator = Assert.Single(
                events,
                static item => item.Context.Role == CSharpDbOperationRole.Root);
            Assert.Equal("sharded-snapshot-enabled", coordinator.Context.DatabaseAlias);
            Assert.Equal(DiagnosticTransport.Sharded, coordinator.Context.Transport);
            Assert.True(coordinator.Context.CountsAsRequest);
            Assert.True(coordinator.Context.CountsAsStatement);

            CSharpDbQueryTerminalEvent[] attempts = events
                .Where(static item => item.Context.Role == CSharpDbOperationRole.Internal)
                .ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.All(attempts, attempt =>
            {
                Assert.Equal(coordinator.Context.OperationId, attempt.Context.ParentOperationId);
                Assert.Equal(coordinator.Context.TraceId, attempt.Context.TraceId);
                Assert.Equal(coordinator.Context.QueryFingerprint, attempt.Context.QueryFingerprint);
                Assert.Equal(DiagnosticTransport.Direct, attempt.Context.Transport);
                Assert.False(attempt.Context.CountsAsRequest);
                Assert.False(attempt.Context.CountsAsStatement);
            });
            Assert.Equal(events.Length, events.Select(static item => item.Context.OperationId).Distinct().Count());
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ShardedOptionsSnapshot_DisabledToEnabled_RemainsDisabledAcrossHierarchy()
    {
        string directory = CreateShardedDatabaseDirectory();
        var configured = CreateObservabilityOptions(
            enabled: false,
            databaseAlias: "sharded-snapshot-disabled");
        var observer = new DiagnosticEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));

        try
        {
            await using CSharpDbShardedClient client = await CSharpDbShardedClient.CreateAsync(
                CreateObservedShardingOptions(directory, configured),
                ct: TestCancellationToken);
            observer.Clear();

            configured.Enabled = true;
            configured.DatabaseAlias = "mutated-enabled";
            configured.Logging.Enabled = true;
            configured.Logging.Queries = true;

            IReadOnlyList<CSharpDbShardSqlExecutionResult> results =
                await client.ExecuteReadOnlySqlOnAllShardsAsync(
                    "SELECT 1;",
                    TestCancellationToken);

            Assert.All(results, static result => Assert.Null(result.Error));
            Assert.Empty(observer.TerminalEvents);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task FirstMigrationHistoryRead_SuppressesSchemaDiagnostics_AndAllowsReentry()
    {
        string directory = CreateShardedDatabaseDirectory();
        var configured = CreateObservabilityOptions(
            enabled: true,
            databaseAlias: "sharded-catalog-schema");
        CSharpDbShardingOptions options = CreateObservedShardingOptions(directory, configured);
        options.Catalog = new CSharpDbShardCatalogOptions
        {
            DataSource = Path.Combine(directory, "master-catalog.db"),
        };
        var observer = new ReentrantShardCatalogObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name =>
                name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal) ||
                name == CSharpDbLogEvents.CheckpointCompleted.Name);

        try
        {
            await using CSharpDbShardedClient client = await CSharpDbShardedClient.CreateAsync(
                options,
                ct: TestCancellationToken);
            observer.Arm(client);

            IReadOnlyList<CSharpDbShardMigrationHistoryEntry> history = await client
                .GetShardMigrationHistoryAsync(TestCancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(10), TestCancellationToken);

            Assert.Empty(history);
            Assert.Equal(1, observer.ReentryCount);
            Assert.Null(observer.ReentryError);
            Assert.NotNull(observer.ReentryResult);
            Assert.Empty(observer.ReentryResult!);
            Assert.Empty(observer.Events(CSharpDbLogEvents.CheckpointCompleted.Name));

            CSharpDbQueryTerminalEvent[] queryEvents = observer.QueryEvents;
            Assert.Equal(2, queryEvents.Length);
            Assert.All(queryEvents, query =>
            {
                Assert.DoesNotContain(
                    "CREATE TABLE",
                    query.CapturedSqlText ?? string.Empty,
                    StringComparison.OrdinalIgnoreCase);
                Assert.Contains(
                    "_shard_migration_history",
                    query.CapturedSqlText ?? string.Empty,
                    StringComparison.OrdinalIgnoreCase);
            });
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void LoggerBridge_UsesTypedEvents_RedactsErrors_AndContainsProviderFailures()
    {
        var options = CreateOptions(SqlTextCaptureMode.None).ObservabilityOptions!;
        options.Logging.SlowQueries = true;
        var logger = new CapturingLogger();
        using var bridge = new CSharpDbDiagnosticLoggerBridge(
            new SingleLoggerFactory(logger),
            options);

        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            DiagnosticTransport.Direct,
            "client-test",
            queryFingerprint: SqlQueryFingerprintProvider.Instance.CreateFingerprint(
                "SELECT 'Customer42Canary'",
                TestCancellationToken));
        SafeErrorProjection safeError = SafeErrorProjector.Project(
            new InvalidOperationException(
                "Password=Customer42Canary;Data Source=C:\\private\\secret.db"));

        CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.QueryFailed,
            () => new CSharpDbQueryFailedEvent(
                context,
                context.GetUtcNow(),
                TimeSpan.Zero,
                timeToFirstResult: null,
                TimeSpan.Zero,
                TimeSpan.Zero,
                rowsProduced: 0,
                rowsAffected: 0,
                safeError));
        CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.LongRunningQuery,
            () => new CSharpDbLongRunningQueryEvent(
                context,
                context.GetUtcNow(),
                elapsed: TimeSpan.FromSeconds(1),
                longRunningQueryThreshold: TimeSpan.FromSeconds(1),
                QueryExecutionPhase.Executing));
        CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.RawSqlCaptureEnabled,
            static () => new CSharpDbRawSqlCaptureEnabledEvent(
                "client-test",
                SqlTextCaptureMode.Raw));

        Assert.Contains(logger.Entries, item => item.EventId.Id == CSharpDbLogEventIds.QueryFailed);
        Assert.Contains(
            logger.Entries,
            item => item.EventId.Id == CSharpDbLogEventIds.LongRunningQuery &&
                    item.Level == LogLevel.Warning);
        Assert.Contains(
            logger.Entries,
            item => item.EventId.Id == CSharpDbLogEventIds.RawSqlCaptureEnabled &&
                    item.Level == LogLevel.Warning);
        string rendered = string.Join(Environment.NewLine, logger.Entries.Select(static item => item.Message));
        Assert.DoesNotContain("Customer42Canary", rendered, StringComparison.Ordinal);
        Assert.DoesNotContain("C:\\private", rendered, StringComparison.Ordinal);
        Assert.All(logger.Entries, static item => Assert.Null(item.Exception));

        using var throwingBridge = new CSharpDbDiagnosticLoggerBridge(
            new SingleLoggerFactory(new ThrowingLogger()),
            options);
        Exception? failure = Record.Exception(() =>
            CSharpDbDiagnostics.EventPublisher.Publish(
                CSharpDbLogEvents.QueryCompleted,
                () => new CSharpDbQueryCompletedEvent(
                    context,
                    context.GetUtcNow(),
                    TimeSpan.Zero,
                    timeToFirstResult: null,
                    TimeSpan.Zero,
                    TimeSpan.Zero,
                    rowsProduced: 0,
                    rowsAffected: 0)));
        Assert.Null(failure);
    }

    private static DatabaseOptions CreateOptions(SqlTextCaptureMode captureMode)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "client-test",
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = true,
                    SlowQueries = false,
                    SqlText = captureMode,
                },
            },
        };

    private static CSharpDbObservabilityOptions CreateObservabilityOptions(
        bool enabled,
        string databaseAlias)
        => new()
        {
            Enabled = enabled,
            DatabaseAlias = databaseAlias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = enabled,
                Queries = enabled,
                SlowQueries = false,
                SqlText = SqlTextCaptureMode.Normalized,
            },
        };

    private static CSharpDbShardingOptions CreateObservedShardingOptions(
        string directory,
        CSharpDbObservabilityOptions observabilityOptions)
        => new()
        {
            Keyspace = "observed",
            MapVersion = 1,
            VirtualBucketCount = 2,
            Shards =
            [
                new CSharpDbShardDefinition
                {
                    ShardId = "shard-0",
                    DataSource = Path.Combine(directory, "shard-0.db"),
                },
                new CSharpDbShardDefinition
                {
                    ShardId = "shard-1",
                    DataSource = Path.Combine(directory, "shard-1.db"),
                },
            ],
            BucketRanges =
            [
                new CSharpDbShardBucketRange
                {
                    StartBucketInclusive = 0,
                    EndBucketExclusive = 1,
                    ShardId = "shard-0",
                },
                new CSharpDbShardBucketRange
                {
                    StartBucketInclusive = 1,
                    EndBucketExclusive = 2,
                    ShardId = "shard-1",
                },
            ],
            DirectDatabaseOptions = new DatabaseOptions
            {
                ObservabilityOptions = observabilityOptions,
            },
        };

    private static string CreateShardedDatabaseDirectory()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_client_observability_shards_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static string CreateDatabasePath()
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_client_observability_{Guid.NewGuid():N}.db");

    private static void DeleteDatabaseFiles(string databasePath)
    {
        foreach (string suffix in new[] { string.Empty, ".wal", "-wal" })
        {
            string path = databasePath + suffix;
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private sealed class DiagnosticEventObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<CSharpDbQueryTerminalEvent> _terminal = new();

        internal IReadOnlyList<CSharpDbQueryCompletedEvent> CompletedEvents
            => _terminal.OfType<CSharpDbQueryCompletedEvent>().ToArray();

        internal IReadOnlyList<CSharpDbQueryTerminalEvent> TerminalEvents
            => _terminal.ToArray();

        internal T[] Events<T>()
            where T : CSharpDbQueryTerminalEvent
            => _terminal.OfType<T>().ToArray();

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is CSharpDbQueryTerminalEvent terminal)
                _terminal.Enqueue(terminal);
        }

        internal void Clear()
        {
            while (_terminal.TryDequeue(out _))
            {
            }
        }
    }

    private sealed class WarmupEventObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<KeyValuePair<string, object?>> _events = new();

        internal T[] Events<T>(string name)
            where T : class
            => _events
                .Where(item => string.Equals(item.Key, name, StringComparison.Ordinal))
                .Select(static item => item.Value)
                .OfType<T>()
                .ToArray();

        internal KeyValuePair<string, object?>[] EventsStartingWith(string prefix)
            => _events
                .Where(item => item.Key.StartsWith(prefix, StringComparison.Ordinal))
                .ToArray();

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
            => _events.Enqueue(value);
    }

    private sealed class ReentrantShardCatalogObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<KeyValuePair<string, object?>> _events = new();
        private CSharpDbShardedClient? _client;
        private int _reentryStarted;

        internal int ReentryCount { get; private set; }
        internal Exception? ReentryError { get; private set; }
        internal IReadOnlyList<CSharpDbShardMigrationHistoryEntry>? ReentryResult { get; private set; }
        internal CSharpDbQueryTerminalEvent[] QueryEvents
            => _events.Select(static item => item.Value).OfType<CSharpDbQueryTerminalEvent>().ToArray();

        internal void Arm(CSharpDbShardedClient client)
        {
            _client = client;
            while (_events.TryDequeue(out _))
            {
            }
        }

        internal KeyValuePair<string, object?>[] Events(string name)
            => _events.Where(item => string.Equals(item.Key, name, StringComparison.Ordinal)).ToArray();

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            _events.Enqueue(value);
            CSharpDbShardedClient? client = _client;
            if (client is null ||
                value.Value is not CSharpDbQueryTerminalEvent ||
                Interlocked.Exchange(ref _reentryStarted, 1) != 0)
            {
                return;
            }

            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                ReentryResult = client.GetShardMigrationHistoryAsync(timeout.Token)
                    .GetAwaiter()
                    .GetResult();
                ReentryCount++;
            }
            catch (Exception exception)
            {
                ReentryError = exception;
            }
        }
    }

    private sealed class SingleLoggerFactory(ILogger logger) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => logger;

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        internal List<LogEntry> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add(new LogEntry(
                logLevel,
                eventId,
                formatter(state, exception),
                exception));
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => throw new InvalidOperationException("scope provider failed");

        public bool IsEnabled(LogLevel logLevel)
            => throw new InvalidOperationException("provider failed");

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("provider failed");
    }

    private sealed record LogEntry(
        LogLevel Level,
        EventId EventId,
        string Message,
        Exception? Exception);

    private sealed class ReentrantTransactionObserver(
        EngineTransportClient client,
        string transactionId) : IObserver<KeyValuePair<string, object?>>
    {
        private int _reentryStarted;

        internal int ReentryCount { get; private set; }
        internal Exception? ReentryError { get; private set; }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbQueryCompletedEvent ||
                Interlocked.Exchange(ref _reentryStarted, 1) != 0)
            {
                return;
            }

            try
            {
                SqlExecutionResult result = client.ExecuteInTransactionAsync(
                    transactionId,
                    "SELECT 2",
                    CancellationToken.None).GetAwaiter().GetResult();
                if (result.Error is not null)
                    throw new InvalidOperationException("Reentrant query failed.");
                ReentryCount++;
            }
            catch (Exception exception)
            {
                ReentryError = exception;
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
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

using System.Runtime.ExceptionServices;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class LifecycleLockSafetyTests : IAsyncLifetime
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(10);
    private readonly List<string> _databasePaths = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        foreach (string path in _databasePaths)
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task FirstPooledOpen_ListenerCanSynchronouslyReenterSamePool()
    {
        string path = CreateDatabasePath();
        string connectionString = $"Data Source={path};Pooling=true;Max Pool Size=2";
        DatabaseOptions options = CreateOptions("pooled-open-lock");
        using var events = new ReentrantLifecycleRecorder(
            CSharpDbLogEvents.DatabaseOpened.Name,
            async cancellationToken =>
            {
                await using var reentered = new CSharpDbConnection(connectionString, options);
                await reentered.OpenAsync(cancellationToken);
            });

        await using var connection = new CSharpDbConnection(connectionString, options);
        await connection.OpenAsync(Ct).WaitAsync(OperationTimeout, Ct);
        await events.WaitForReentryAsync(Ct);

        CSharpDbLifecycleCompletedEvent opened = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
        AssertDirectLifecycle(opened, "pooled-open-lock");
    }

    [Fact]
    public async Task FirstPooledOpen_UsesPoolSnapshotWhenMismatchedPlanWinsGate()
    {
        string path = CreateDatabasePath();
        string connectionString =
            $"Data Source={path};Pooling=true;Max Pool Size=3";
        DatabaseOptions options = CreateOptions("pooled-authoritative-open");
        using var firstPaused = new ManualResetEventSlim();
        using var releaseFirst = new ManualResetEventSlim();
        int gateAttempts = 0;
        CSharpDbConnectionPool.BeforeOpenSessionGateForTest = () =>
        {
            if (Interlocked.Increment(ref gateAttempts) != 1)
                return;

            firstPaused.Set();
            releaseFirst.Wait();
        };
        using var events = new ReentrantLifecycleRecorder(
            CSharpDbLogEvents.DatabaseOpened.Name,
            async cancellationToken =>
            {
                await using var reentered = new CSharpDbConnection(connectionString, options);
                await reentered.OpenAsync(cancellationToken);
            });
        await using var first = new CSharpDbConnection(connectionString, options);
        await using var winner = new CSharpDbConnection(connectionString, options);
        Task firstOpen = Task.Run(() => first.OpenAsync(Ct), Ct);

        try
        {
            Assert.True(await Task.Run(
                () => firstPaused.Wait(TimeSpan.FromSeconds(5))));

            // The first plan created the pool with enabled observability. A
            // second plan now sees the caller-mutated disabled configuration,
            // but the pool's immutable runtime snapshot remains authoritative.
            options.ObservabilityOptions!.Enabled = false;
            await winner.OpenAsync(Ct).WaitAsync(OperationTimeout, Ct);
            await events.WaitForReentryAsync(Ct);

            releaseFirst.Set();
            await firstOpen.WaitAsync(OperationTimeout, Ct);

            CSharpDbLifecycleCompletedEvent opened = Assert.Single(
                events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
            AssertDirectLifecycle(opened, "pooled-authoritative-open");
        }
        finally
        {
            CSharpDbConnectionPool.BeforeOpenSessionGateForTest = null;
            releaseFirst.Set();
            await firstOpen;
        }
    }

    [Fact]
    public async Task FirstSharedMemoryOpen_ListenerCanSynchronouslyReenterSameHost()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        DatabaseOptions options = CreateOptions("shared-open-lock");
        using var events = new ReentrantLifecycleRecorder(
            CSharpDbLogEvents.DatabaseOpened.Name,
            async cancellationToken =>
            {
                await using var reentered = new CSharpDbConnection(connectionString, options);
                await reentered.OpenAsync(cancellationToken);
            });

        await using var connection = new CSharpDbConnection(connectionString, options);
        await connection.OpenAsync(Ct).WaitAsync(OperationTimeout, Ct);
        await events.WaitForReentryAsync(Ct);

        CSharpDbLifecycleCompletedEvent opened = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
        AssertDirectLifecycle(opened, "shared-open-lock");
    }

    [Fact]
    public async Task ClearPool_CloseListenerCanReenterRegistry_AfterOptionsMutation()
    {
        string path = CreateDatabasePath();
        string connectionString = $"Data Source={path};Pooling=true;Max Pool Size=1";
        DatabaseOptions options = CreateOptions("pooled-clear-lock");
        using var events = new ReentrantLifecycleRecorder(
            CSharpDbLogEvents.DatabaseClosed.Name,
            async cancellationToken =>
            {
                await using var reentered = new CSharpDbConnection(connectionString, options);
                await reentered.OpenAsync(cancellationToken);
            });

        await using (var connection = new CSharpDbConnection(connectionString, options))
        {
            await connection.OpenAsync(Ct);
            await connection.CloseAsync();
        }

        CSharpDbLifecycleCompletedEvent opened = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));

        // The engine snapshots observability during open. Mutating the caller's
        // object must not cause the pool to skip the matching close boundary.
        options.ObservabilityOptions!.Enabled = false;
        options.ObservabilityOptions.Logging.Enabled = false;

        await CSharpDbConnection.ClearPoolAsync(connectionString)
            .AsTask()
            .WaitAsync(OperationTimeout, Ct);
        await events.WaitForReentryAsync(Ct);

        CSharpDbLifecycleCompletedEvent closed = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
        AssertDirectLifecycle(closed, "pooled-clear-lock");
        Assert.NotEqual(opened.Context.SessionId, closed.Context.SessionId);
    }

    [Fact]
    public async Task ClearSharedMemory_CloseListenerCanSynchronouslyReenterRegistry()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        DatabaseOptions options = CreateOptions("shared-clear-lock");
        using var events = new ReentrantLifecycleRecorder(
            CSharpDbLogEvents.DatabaseClosed.Name,
            async cancellationToken =>
            {
                await using var reentered = new CSharpDbConnection(connectionString, options);
                await reentered.OpenAsync(cancellationToken);
            });

        await using (var connection = new CSharpDbConnection(connectionString, options))
        {
            await connection.OpenAsync(Ct);
            await connection.CloseAsync();
        }

        await CSharpDbConnection.ClearPoolAsync(connectionString)
            .AsTask()
            .WaitAsync(OperationTimeout, Ct);
        await events.WaitForReentryAsync(Ct);

        CSharpDbLifecycleCompletedEvent closed = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
        AssertDirectLifecycle(closed, "shared-clear-lock");
    }

    [Fact]
    public async Task DirectConnection_OpenAndCloseReuseOpaqueSessionCorrelation()
    {
        DatabaseOptions options = CreateOptions("direct-lifecycle-session");
        using var events = new ReentrantLifecycleRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);

        await connection.OpenAsync(Ct);
        await connection.CloseAsync();

        CSharpDbLifecycleCompletedEvent opened = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseOpened.Name));
        CSharpDbLifecycleCompletedEvent closed = Assert.Single(
            events.Events(CSharpDbLogEvents.DatabaseClosed.Name));
        AssertDirectLifecycle(opened, "direct-lifecycle-session");
        AssertDirectLifecycle(closed, "direct-lifecycle-session");
        Assert.Equal(opened.Context.SessionId, closed.Context.SessionId);
    }

    [Fact]
    public Task PooledTransactions_CommitAndRollbackListenersCanSynchronouslyReenter()
        => AssertTransactionListenersCanReenterAsync(sharedMemory: false);

    [Fact]
    public Task SharedMemoryTransactions_CommitAndRollbackListenersCanSynchronouslyReenter()
        => AssertTransactionListenersCanReenterAsync(sharedMemory: true);

    private async Task AssertTransactionListenersCanReenterAsync(bool sharedMemory)
    {
        string databaseAlias = sharedMemory
            ? "shared-transaction-lock"
            : "pooled-transaction-lock";
        string connectionString;
        if (sharedMemory)
        {
            connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        }
        else
        {
            string path = CreateDatabasePath();
            connectionString =
                $"Data Source={path};Pooling=true;Max Pool Size=2";
        }

        DatabaseOptions options = CreateOptions(databaseAlias);
        await using var owner = new CSharpDbConnection(connectionString, options);
        await using var reentrant = new CSharpDbConnection(connectionString, options);
        await owner.OpenAsync(Ct);
        await reentrant.OpenAsync(Ct);
        using var events = new ReentrantTransactionRecorder(
            expectedReentries: 2,
            async cancellationToken =>
            {
                await using var command = reentrant.CreateCommand();
                command.CommandText = "SELECT 1";
                Assert.NotNull(await command.ExecuteScalarAsync(cancellationToken));
            });

        await using (var transaction = await owner.BeginTransactionAsync(Ct))
            await transaction.CommitAsync(Ct).WaitAsync(OperationTimeout, Ct);

        await using (var transaction = await owner.BeginTransactionAsync(Ct))
            await transaction.RollbackAsync(Ct).WaitAsync(OperationTimeout, Ct);

        await events.WaitForReentriesAsync(Ct);

        CSharpDbLifecycleCompletedEvent[] completed = events.Events();
        Assert.Equal(2, completed.Length);
        Assert.All(
            completed,
            transaction => AssertDirectTransaction(transaction, databaseAlias));
        Assert.Equal(
            completed[0].Context.SessionId,
            completed[1].Context.SessionId);
    }

    private string CreateDatabasePath()
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_lifecycle_lock_{Guid.NewGuid():N}.db");
        _databasePaths.Add(path);
        return path;
    }

    private static DatabaseOptions CreateOptions(string databaseAlias)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = databaseAlias,
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = false,
                    SlowQueries = false,
                },
            },
        };

    private static void AssertDirectLifecycle(
        CSharpDbLifecycleCompletedEvent lifecycleEvent,
        string databaseAlias)
    {
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, lifecycleEvent.Outcome);
        Assert.Equal(CSharpDbOperationClass.Database, lifecycleEvent.Context.OperationClass);
        Assert.Equal(CSharpDbTransport.Direct, lifecycleEvent.Context.Transport);
        Assert.Equal(databaseAlias, lifecycleEvent.Context.DatabaseAlias);
        OpaqueDiagnosticsId sessionId = Assert.IsType<OpaqueDiagnosticsId>(
            lifecycleEvent.Context.SessionId);
        Assert.Equal(32, sessionId.Value.Length);
    }

    private static void AssertDirectTransaction(
        CSharpDbLifecycleCompletedEvent transactionEvent,
        string databaseAlias)
    {
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, transactionEvent.Outcome);
        Assert.Null(transactionEvent.Error);
        Assert.Equal(
            CSharpDbOperationClass.Transaction,
            transactionEvent.Context.OperationClass);
        Assert.Equal(CSharpDbTransport.Direct, transactionEvent.Context.Transport);
        Assert.Equal(databaseAlias, transactionEvent.Context.DatabaseAlias);
        OpaqueDiagnosticsId sessionId = Assert.IsType<OpaqueDiagnosticsId>(
            transactionEvent.Context.SessionId);
        Assert.Equal(32, sessionId.Value.Length);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class ReentrantLifecycleRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private static readonly TimeSpan ReentryTimeout = TimeSpan.FromSeconds(5);
        private readonly object _gate = new();
        private readonly List<(string Name, CSharpDbLifecycleCompletedEvent Event)> _events = [];
        private readonly string? _reentryEventName;
        private readonly Func<CancellationToken, Task>? _reentry;
        private readonly TaskCompletionSource _reentryCompleted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IDisposable _subscription;
        private Exception? _reentryError;
        private int _reentryStarted;

        internal ReentrantLifecycleRecorder(
            string? reentryEventName = null,
            Func<CancellationToken, Task>? reentry = null)
        {
            _reentryEventName = reentryEventName;
            _reentry = reentry;
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name =>
                    name == CSharpDbLogEvents.DatabaseOpened.Name ||
                    name == CSharpDbLogEvents.DatabaseClosed.Name);
        }

        internal CSharpDbLifecycleCompletedEvent[] Events(string eventName)
        {
            lock (_gate)
            {
                return _events
                    .Where(item => item.Name == eventName)
                    .Select(static item => item.Event)
                    .ToArray();
            }
        }

        internal async Task WaitForReentryAsync(CancellationToken cancellationToken)
        {
            await _reentryCompleted.Task.WaitAsync(OperationTimeout, cancellationToken);
            if (_reentryError is not null)
                ExceptionDispatchInfo.Capture(_reentryError).Throw();
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbLifecycleCompletedEvent lifecycleEvent)
                return;

            lock (_gate)
                _events.Add((value.Key, lifecycleEvent));

            if (_reentry is null ||
                value.Key != _reentryEventName ||
                Interlocked.Exchange(ref _reentryStarted, 1) != 0)
            {
                return;
            }

            try
            {
                using var timeout = new CancellationTokenSource(ReentryTimeout);
                _reentry(timeout.Token)
                    .WaitAsync(ReentryTimeout)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception exception)
            {
                _reentryError = exception;
            }
            finally
            {
                _reentryCompleted.TrySetResult();
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

    private sealed class ReentrantTransactionRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private static readonly TimeSpan ReentryTimeout = TimeSpan.FromSeconds(5);
        private readonly object _gate = new();
        private readonly List<CSharpDbLifecycleCompletedEvent> _events = [];
        private readonly int _expectedReentries;
        private readonly Func<CancellationToken, Task> _reentry;
        private readonly TaskCompletionSource _reentriesCompleted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IDisposable _subscription;
        private Exception? _reentryError;
        private int _reentries;

        internal ReentrantTransactionRecorder(
            int expectedReentries,
            Func<CancellationToken, Task> reentry)
        {
            _expectedReentries = expectedReentries;
            _reentry = reentry;
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name => name == CSharpDbLogEvents.TransactionCompleted.Name);
        }

        internal CSharpDbLifecycleCompletedEvent[] Events()
        {
            lock (_gate)
                return [.. _events];
        }

        internal async Task WaitForReentriesAsync(
            CancellationToken cancellationToken)
        {
            await _reentriesCompleted.Task.WaitAsync(
                OperationTimeout,
                cancellationToken);
            if (_reentryError is not null)
                ExceptionDispatchInfo.Capture(_reentryError).Throw();
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Key != CSharpDbLogEvents.TransactionCompleted.Name ||
                value.Value is not CSharpDbLifecycleCompletedEvent transactionEvent)
            {
                return;
            }

            lock (_gate)
                _events.Add(transactionEvent);

            try
            {
                using var timeout = new CancellationTokenSource(ReentryTimeout);
                _reentry(timeout.Token)
                    .WaitAsync(ReentryTimeout)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception exception)
            {
                _reentryError ??= exception;
            }
            finally
            {
                if (Interlocked.Increment(ref _reentries) >= _expectedReentries)
                    _reentriesCompleted.TrySetResult();
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

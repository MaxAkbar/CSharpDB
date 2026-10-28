using System.Collections.Concurrent;
using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ClientTransactionAdmissionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task CancellationAtCompletionHandoff_RestoresSessionForRollback()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        var options = new DatabaseOptions().ConfigureFunctions(functions => functions.AddScalar(
            "HoldForCompletion", 0, new DbScalarFunctionOptions(DbType.Integer), (_, _) =>
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout, Ct)) throw new TimeoutException("Query was not released.");
                return DbValue.FromInteger(1);
            }));
        await using var client = EngineTransportClient.CreatePrivateMemory("completion-handoff", null, options);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var context = new QueuedContext();
        string id = (await client.BeginTransactionAsync(Ct)).TransactionId;
        Task query = Task.Run(() => client.ExecuteInTransactionAsync(id, "SELECT HoldForCompletion();", Ct), Ct);
        Task completion = Task.CompletedTask;
        try
        {
            await entered.Task.WaitAsync(Timeout, Ct);
            SynchronizationContext? previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(context);
            try { completion = client.CommitTransactionAsync(id, cancel.Token); }
            finally { SynchronizationContext.SetSynchronizationContext(previous); }
            release.Set();
            await query.WaitAsync(Timeout, Ct);
            // The semaphore has handed off, but the completion continuation
            // cannot run until we release it. Cancellation must still win.
            await context.Posted.WaitAsync(Timeout, Ct);
            cancel.Cancel();
            await context.DrainUntilAsync(completion);
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion);
            Assert.Equal(cancel.Token, error.CancellationToken);
            await client.RollbackTransactionAsync(id, Ct);
        }
        finally
        {
            release.Set();
            cancel.Cancel();
            await context.DrainUntilAsync(completion);
            await query.WaitAsync(Timeout, Ct);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingSchemaBegin_DoesNotBlockCompletion(bool commit)
    {
        await using var client = EngineTransportClient.CreatePrivateMemory("admission-completion", null, null);
        using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        string first = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await client.ExecuteInTransactionAsync(first, "CREATE TABLE admitted (id INTEGER PRIMARY KEY);", Ct);
        Task<CSharpDB.Client.Models.TransactionSessionInfo> pending = client.BeginTransactionAsync(cancelBegin.Token);
        Task completion = Task.CompletedTask;
        try
        {
            Assert.False(pending.IsCompleted);
            completion = commit ? client.CommitTransactionAsync(first, Ct) : client.RollbackTransactionAsync(first, Ct);
            await completion.WaitAsync(Timeout, Ct);
            string second = (await pending.WaitAsync(Timeout, Ct)).TransactionId;
            await client.RollbackTransactionAsync(second, Ct);
            Assert.Equal(commit, (await client.GetTableNamesAsync(Ct)).Contains("admitted"));
        }
        finally
        {
            cancelBegin.Cancel();
            await ObserveCanceledBeginAsync(pending);
            await completion.WaitAsync(Timeout, Ct);
        }
    }

    [Fact]
    public async Task Disposal_CancelsPendingSchemaBegin_AndDrainsItBeforeClosingOwner()
    {
        var client = EngineTransportClient.CreatePrivateMemory("admission-disposal", null, null);
        using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task<CSharpDB.Client.Models.TransactionSessionInfo>? pending = null;
        Task disposal = Task.CompletedTask;
        try
        {
            string first = (await client.BeginTransactionAsync(Ct)).TransactionId;
            await client.ExecuteInTransactionAsync(first, "CREATE TABLE disposed_admission (id INTEGER);", Ct);
            pending = client.BeginTransactionAsync(cancelBegin.Token);
            Assert.False(pending.IsCompleted);
            disposal = client.DisposeAsync().AsTask();
            await disposal.WaitAsync(Timeout, Ct);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
        }
        finally
        {
            cancelBegin.Cancel();
            if (pending is not null) await ObserveCanceledBeginAsync(pending);
            await disposal.WaitAsync(Timeout, Ct);
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task CanceledPendingSchemaBegin_PreservesCallerTokenAndOwnerTransaction()
    {
        await using var client = EngineTransportClient.CreatePrivateMemory("admission-cancellation", null, null);
        using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        string first = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await client.ExecuteInTransactionAsync(first, "CREATE TABLE retained_admission (id INTEGER);", Ct);
        Task pending = client.BeginTransactionAsync(cancelBegin.Token);
        Assert.False(pending.IsCompleted);
        cancelBegin.Cancel();
        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(cancelBegin.Token, error.CancellationToken);
        await client.CommitTransactionAsync(first, Ct).WaitAsync(Timeout, Ct);
        string next = (await client.BeginTransactionAsync(Ct)).TransactionId;
        await client.RollbackTransactionAsync(next, Ct);
        Assert.Contains("retained_admission", await client.GetTableNamesAsync(Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnpublishedAdmission_IsRolledBackWhenCancellationOrDisposalWins(bool dispose)
    {
        var client = EngineTransportClient.CreatePrivateMemory("admission-publication-race", null, null);
        using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task<CSharpDB.Client.Models.TransactionSessionInfo>? pending = null;
        Task disposal = Task.CompletedTask;
        SemaphoreSlim gate = ClientGate(client);
        bool gateHeld = false;
        try
        {
            await client.GetInfoAsync(Ct);
            Database database = (await client.TryGetDatabaseAsync(Ct))!;
            await using var schemaOwner = await database.BeginWriteTransactionAsync(Ct);
            await using (var result = await schemaOwner.ExecuteAsync("CREATE TABLE publication_race (id INTEGER);", Ct)) { }
            pending = client.BeginTransactionAsync(cancelBegin.Token);
            Assert.False(pending.IsCompleted);
            using var gateDeadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            gateDeadline.CancelAfter(Timeout);
            await gate.WaitAsync(gateDeadline.Token);
            gateHeld = true;
            if (dispose) disposal = client.DisposeAsync().AsTask();
            await schemaOwner.RollbackAsync(Ct);
            await schemaOwner.DisposeAsync();
            await WaitUntilAsync(() => database.ActiveReaderCount == 1);
            // The only snapshot now belongs to the admitted but unpublished
            // begin. The held gate prevents it from registering its session.
            if (!dispose)
            {
                cancelBegin.Cancel();
                await WaitUntilAsync(() => database.ActiveReaderCount == 0);
            }
            gate.Release();
            gateHeld = false;
            if (dispose)
            {
                await disposal.WaitAsync(Timeout, Ct);
                await Assert.ThrowsAsync<ObjectDisposedException>(() => pending);
            }
            else
            {
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
                Assert.Equal(cancelBegin.Token, error.CancellationToken);
                string next = (await client.BeginTransactionAsync(Ct)).TransactionId;
                await client.RollbackTransactionAsync(next, Ct);
            }
        }
        finally
        {
            cancelBegin.Cancel();
            if (gateHeld) gate.Release();
            if (pending is not null) await ObserveCanceledBeginAsync(pending);
            await disposal.WaitAsync(Timeout, Ct);
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task PendingAdmission_PreventsCachedOwnerReleaseAndExclusiveMaintenance()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_admission_owner_{Guid.NewGuid():N}.db");
        try
        {
            await using var client = new EngineTransportClient(path);
            using var cancelBegin = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            await client.GetInfoAsync(Ct);
            Database database = (await client.TryGetDatabaseAsync(Ct))!;
            await using var schemaOwner = await database.BeginWriteTransactionAsync(Ct);
            await using (var result = await schemaOwner.ExecuteAsync("CREATE TABLE external_schema_owner (id INTEGER);", Ct)) { }
            Task pending = client.BeginTransactionAsync(cancelBegin.Token);
            try
            {
                Assert.False(pending.IsCompleted);
                using var operationDeadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
                operationDeadline.CancelAfter(Timeout);
                await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ReleaseCachedDatabaseAsync(operationDeadline.Token).AsTask());
                var error = await Assert.ThrowsAnyAsync<Exception>(() => client.OpenExclusiveSessionAsync(operationDeadline.Token).AsTask());
                Assert.Contains("Finish active transactions", error.Message, StringComparison.Ordinal);
                Assert.Same(database, await client.TryGetDatabaseAsync(Ct));
            }
            finally
            {
                cancelBegin.Cancel();
                await ObserveCanceledBeginAsync(pending);
                await schemaOwner.RollbackAsync(Ct);
            }
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".wal");
        }
    }

    private static SemaphoreSlim ClientGate(EngineTransportClient client) =>
        (SemaphoreSlim)typeof(EngineTransportClient).GetField("_lock", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        deadline.CancelAfter(Timeout);
        while (!condition()) await Task.Delay(1, deadline.Token);
    }

    private static async Task ObserveCanceledBeginAsync(Task pending)
    {
        try { await pending.WaitAsync(Timeout, Ct); }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
    }

    private sealed class QueuedContext : SynchronizationContext
    {
        private readonly ConcurrentQueue<(SendOrPostCallback Callback, object? State)> _callbacks = new();
        private readonly TaskCompletionSource _posted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Posted => _posted.Task;
        public override void Post(SendOrPostCallback callback, object? state)
        {
            _callbacks.Enqueue((callback, state));
            _posted.TrySetResult();
        }

        public async Task DrainUntilAsync(Task task)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            deadline.CancelAfter(Timeout);
            while (!task.IsCompleted)
            {
                while (_callbacks.TryDequeue(out var callback))
                {
                    SynchronizationContext? previous = Current;
                    SetSynchronizationContext(this);
                    try { callback.Callback(callback.State); }
                    finally { SetSynchronizationContext(previous); }
                }
                if (!task.IsCompleted) await Task.Delay(1, deadline.Token);
            }
        }
    }
}

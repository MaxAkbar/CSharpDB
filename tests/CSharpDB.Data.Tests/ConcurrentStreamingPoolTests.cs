using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class ConcurrentStreamingPoolTests
{
    private const int Capacity = 8;

    [Fact(Timeout = 45000)]
    public async Task EightMixedRentals_ShareOneCapacityLimit_AndReuseEachOthersSessions()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await SeedAsync(host, ct);
        Rental[] rentals = [];
        try
        {
            rentals = await OpenEightAsync(host, reverseApis: false, ct);
            Assert.Equal(Capacity, SessionCount(host));
            await Task.WhenAll(rentals.Select(async (rental, id) =>
            {
                for (int read = 0; read < 16; read++)
                    Assert.Equal(0L, await rental.ScalarAsync($"SELECT value FROM stream_concurrent WHERE id = {id}", ct));
            }));

            // Both APIs must wait on the same eight slots. Cancellation of a
            // capacity waiter must neither create a stream nor consume a slot.
            using (var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                Task<Rental> sdkWaiter = Rental.OpenAsync(host, ado: false, cancellation.Token);
                Assert.False(sdkWaiter.IsCompleted);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sdkWaiter);
            }
            await using (var canceledAdo = new CSharpDbConnection(ConnectionString(host)))
            {
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
                Task open = canceledAdo.OpenAsync(cancellation.Token);
                Assert.False(open.IsCompleted);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => open);
                Assert.Equal(ConnectionState.Closed, canceledAdo.State);
            }
            Assert.Equal(Capacity, SessionCount(host));

            Task<Rental> adoWaitingForSdk = Rental.OpenAsync(host, ado: true, ct);
            Assert.False(adoWaitingForSdk.IsCompleted);
            await rentals[0].DisposeAsync(); // The only available slot and owner are from the SDK.
            await using var ado = await adoWaitingForSdk.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(0L, await ado.ScalarAsync("SELECT value FROM stream_concurrent WHERE id = 0", ct));

            Task<Rental> sdkWaitingForAdo = Rental.OpenAsync(host, ado: false, ct);
            Assert.False(sdkWaitingForAdo.IsCompleted);
            await rentals[1].DisposeAsync();
            await using var sdk = await sdkWaitingForAdo.WaitAsync(TimeSpan.FromSeconds(10), ct);
            Assert.Equal(0L, await sdk.ScalarAsync("SELECT value FROM stream_concurrent WHERE id = 1", ct));
            Assert.Equal(Capacity, SessionCount(host));
            Assert.All(host.Snapshot(), request => Assert.Equal("HTTP/2", request.Key.Protocol));
        }
        finally
        {
            await DisposeAsync(rentals);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    [Fact(Timeout = 45000)]
    public async Task EightMixedTransactions_IsolateWrites_AndResetAbandonedStateBeforeReuse()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await SeedAsync(host, ct);
        await using var observer = host.CreateClient(CSharpDbGrpcMode.Native);
        // Existing rows in distinct tables occupy independent write pages. The
        // engine detects page conflicts, so distinct keys on one small table
        // would not be a valid assumption for concurrent successful commits.
        for (int id = 0; id < Capacity; id++)
            Assert.Null((await observer.ExecuteSqlAsync(
                $"CREATE TABLE stream_tx_{id} (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO stream_tx_{id} VALUES (0, 0)", ct)).Error);
        Rental[] rentals = [];
        try
        {
            rentals = await OpenEightAsync(host, reverseApis: false, ct);
            await Task.WhenAll(rentals.Select(rental => rental.BeginAsync(ct)));
            await Task.WhenAll(rentals.Select((rental, id) =>
                rental.ExecuteAsync($"UPDATE stream_tx_{id} SET value = {100 + id} WHERE id = 0", ct)));

            // Every snapshot contains exactly its own write, while ordinary
            // readers still see the committed zeros. No commit ordering is
            // needed to establish this isolation boundary.
            await Task.WhenAll(rentals.Select(async (rental, id) =>
            {
                Assert.Equal(100L + id, await rental.ScalarAsync($"SELECT value FROM stream_tx_{id} WHERE id = 0", ct));
                Assert.Equal(0L, await rental.ScalarAsync($"SELECT value FROM stream_tx_{(id + 1) % Capacity} WHERE id = 0", ct));
            }));
            for (int id = 0; id < Capacity; id++)
                Assert.Equal(0L, (await observer.ExecuteSqlAsync($"SELECT value FROM stream_tx_{id} WHERE id = 0", ct)).Rows![0][0]);

            // Both APIs commit, explicitly roll back, and abandon a transaction.
            // Finish the other sessions while the abandoned ones pin snapshots.
            await Task.WhenAll(rentals.Select((rental, id) => (id % 3) switch
            {
                0 => rental.CommitAsync(ct),
                1 => rental.RollbackAsync(ct),
                _ => Task.CompletedTask,
            }));
            await DisposeAsync(rentals);

            rentals = await OpenEightAsync(host, reverseApis: true, ct);
            await Task.WhenAll(rentals.Select(async rental =>
            {
                for (int id = 0; id < Capacity; id++)
                    Assert.Equal(id % 3 == 0 ? 100L + id : 0L,
                        await rental.ScalarAsync($"SELECT value FROM stream_tx_{id} WHERE id = 0", ct));
                // A new transaction must be possible on every returned owner.
                await rental.BeginAsync(ct);
                await rental.RollbackAsync(ct);
            }));
            Assert.Equal(Capacity, SessionCount(host));
        }
        finally
        {
            await DisposeAsync(rentals);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    [Fact(Timeout = 45000)]
    public async Task CancelingOneActiveStream_DoesNotReplayOrInterruptTheOtherSeven()
    {
        var ct = TestContext.Current.CancellationToken;
        var gate = new CancellationGate();
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, gate));
        await SeedAsync(host, ct);
        Rental[] rentals = [];
        try
        {
            rentals = await OpenEightAsync(host, reverseApis: false, ct);
            await rentals[0].BeginAsync(ct);
            await rentals[0].ExecuteAsync("UPDATE stream_concurrent SET value = 99 WHERE id = 0", ct);

            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task<SqlExecutionResult> active = rentals[0].Sdk!.ExecuteSqlAsync(CancellationGate.BlockedSql, cancellation.Token);
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            await Task.WhenAll(rentals.Skip(1).Select(async (rental, index) =>
            {
                for (int read = 0; read < 16; read++)
                    Assert.Equal(0L, await rental.ScalarAsync($"SELECT value FROM stream_concurrent WHERE id = {index + 1}", ct));
            }));
            Assert.False(active.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.WaitAsync(TimeSpan.FromSeconds(10), ct));
            await gate.RolledBack.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            await rentals[0].DisposeAsync();

            // The seven healthy owners are still rented, so only the canceled
            // owner can be replaced. Its uncommitted value must have vanished.
            await using var replacement = await Rental.OpenAsync(host, ado: true, ct);
            Assert.Equal(0L, await replacement.ScalarAsync("SELECT value FROM stream_concurrent WHERE id = 0", ct));
            await Task.WhenAll(rentals.Skip(1).Select(async rental =>
                Assert.Equal(0L, await rental.ScalarAsync("SELECT SUM(value) FROM stream_concurrent", ct))));
            Assert.Equal(1, Volatile.Read(ref gate.BlockedCalls));
            Assert.Equal(Capacity + 1, SessionCount(host));
        }
        finally
        {
            gate.Release.TrySetResult();
            await DisposeAsync(rentals);
            await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri);
        }
    }

    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    {
        Transport = CSharpDbTransport.Grpc,
        Endpoint = host.NativeEndpoint.AbsoluteUri,
        GrpcMode = CSharpDbGrpcMode.Native,
        GrpcCallMode = CSharpDbGrpcCallMode.Streaming,
    };

    private static string ConnectionString(GrpcSocketDaemonFixture host)
        => $"Transport=Grpc;Endpoint={host.NativeEndpoint};Grpc Mode=Native;Grpc Call Mode=Streaming;Pooling=true;Max Pool Size={Capacity}";

    private static long SessionCount(GrpcSocketDaemonFixture host)
        => host.Snapshot().Where(p => p.Key.Method.EndsWith("/CommandSession", StringComparison.Ordinal)).Sum(p => p.Value);

    private static async Task SeedAsync(GrpcSocketDaemonFixture host, CancellationToken ct)
    {
        await using var observer = host.CreateClient(CSharpDbGrpcMode.Native);
        string rows = string.Join(", ", Enumerable.Range(0, Capacity).Select(id => $"({id}, 0)"));
        Assert.Null((await observer.ExecuteSqlAsync(
            $"CREATE TABLE stream_concurrent (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO stream_concurrent VALUES {rows}", ct)).Error);
    }

    private static async Task<Rental[]> OpenEightAsync(GrpcSocketDaemonFixture host, bool reverseApis, CancellationToken ct)
    {
        Task<Rental>[] tasks = Enumerable.Range(0, Capacity)
            .Select(id => Rental.OpenAsync(host, ado: (id % 2 == 1) != reverseApis, ct)).ToArray();
        try { return await Task.WhenAll(tasks); }
        catch
        {
            await DisposeAsync(tasks.Where(task => task.IsCompletedSuccessfully).Select(task => task.Result));
            throw;
        }
    }

    private static Task DisposeAsync(IEnumerable<Rental> rentals)
        => Task.WhenAll(rentals.Select(rental => rental.DisposeAsync().AsTask()));

    private sealed class Rental : IAsyncDisposable
    {
        public ICSharpDbClient? Sdk { get; private init; }
        private CSharpDbConnection? Connection { get; init; }
        private string? _transactionId;
        private DbTransaction? _transaction;
        private int _disposed;

        public static async Task<Rental> OpenAsync(GrpcSocketDaemonFixture host, bool ado, CancellationToken ct)
        {
            if (!ado) return new Rental { Sdk = await CSharpDbClient.RentAsync(Options(host), Capacity, ct) };
            var connection = new CSharpDbConnection(ConnectionString(host));
            try { await connection.OpenAsync(ct); return new Rental { Connection = connection }; }
            catch { await connection.DisposeAsync(); throw; }
        }

        public async Task BeginAsync(CancellationToken ct)
        {
            if (Sdk is not null) _transactionId = (await Sdk.BeginTransactionAsync(ct)).TransactionId;
            else _transaction = await Connection!.BeginTransactionAsync(ct);
        }

        public async Task ExecuteAsync(string sql, CancellationToken ct)
        {
            if (Sdk is not null) Assert.Null((await ExecuteSdkAsync(sql, ct)).Error);
            else
            {
                using var command = CreateCommand(sql);
                await command.ExecuteNonQueryAsync(ct);
            }
        }

        public async Task<long> ScalarAsync(string sql, CancellationToken ct)
        {
            if (Sdk is null)
            {
                using var command = CreateCommand(sql);
                return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
            }
            var result = await ExecuteSdkAsync(sql, ct);
            Assert.Null(result.Error);
            return Assert.IsType<long>(Assert.Single(Assert.Single(result.Rows!)));
        }

        public async Task CommitAsync(CancellationToken ct)
        {
            if (Sdk is not null) await Sdk.CommitTransactionAsync(_transactionId!, ct);
            else await _transaction!.CommitAsync(ct);
            _transactionId = null;
            if (_transaction is not null) await _transaction.DisposeAsync();
            _transaction = null;
        }

        public async Task RollbackAsync(CancellationToken ct)
        {
            if (Sdk is not null) await Sdk.RollbackTransactionAsync(_transactionId!, ct);
            else await _transaction!.RollbackAsync(ct);
            _transactionId = null;
            if (_transaction is not null) await _transaction.DisposeAsync();
            _transaction = null;
        }

        private Task<SqlExecutionResult> ExecuteSdkAsync(string sql, CancellationToken ct)
            => _transactionId is null ? Sdk!.ExecuteSqlAsync(sql, ct) : Sdk!.ExecuteInTransactionAsync(_transactionId, sql, ct);

        private DbCommand CreateCommand(string sql)
        {
            var command = Connection!.CreateCommand();
            command.CommandText = sql;
            command.Transaction = _transaction;
            return command;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            // Returning the rental/connection is deliberately responsible for
            // rollback, rather than explicitly disposing an abandoned transaction.
            if (Sdk is not null) await Sdk.DisposeAsync();
            else await Connection!.DisposeAsync();
            if (_transaction is not null) await _transaction.DisposeAsync();
        }
    }

    private static void Decorate(IServiceCollection services, CancellationGate gate)
    {
        var original = services.Last(descriptor => descriptor.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var proxy = DispatchProxy.Create<ICSharpDbClient, GatedClient>();
            var state = (GatedClient)(object)proxy;
            state.Inner = (ICSharpDbClient)original.ImplementationFactory!(provider);
            state.Gate = gate;
            return proxy;
        }));
    }

    public sealed class CancellationGate
    {
        public const string BlockedSql = "SELECT 7001";
        public int BlockedCalls;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RolledBack { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class GatedClient : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public CancellationGate Gate = null!;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var method = targetMethod!;
            if (method.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && (string)args![0]! == CancellationGate.BlockedSql)
                return BlockAsync((CancellationToken)args[1]!);
            object? result;
            try { result = method.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
            return method.Name == nameof(ICSharpDbClient.RollbackTransactionAsync)
                ? ObserveRollbackAsync((Task)result!) : result;
        }

        private async Task<SqlExecutionResult> BlockAsync(CancellationToken ct)
        {
            Interlocked.Increment(ref Gate.BlockedCalls);
            Gate.Entered.TrySetResult();
            await Gate.Release.Task.WaitAsync(ct);
            return await Inner.ExecuteSqlAsync(CancellationGate.BlockedSql, ct);
        }

        private async Task ObserveRollbackAsync(Task rollback)
        {
            await rollback;
            Gate.RolledBack.TrySetResult();
        }
    }
}

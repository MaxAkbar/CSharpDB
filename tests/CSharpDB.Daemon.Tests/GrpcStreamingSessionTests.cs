extern alias CSharpDbApi;

using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.ExceptionServices;
using CSharpDbHostReadinessCoordinator = CSharpDbApi::CSharpDB.Api.CSharpDbHostReadinessCoordinator;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcStreamingSessionTests
{
    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host, CSharpDbGrpcCallMode mode = CSharpDbGrpcCallMode.Streaming,
        string? apiKey = null, CSharpDbRouteContext? route = null) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native, GrpcCallMode = mode,
        Endpoint = host.NativeEndpoint.AbsoluteUri, ApiKey = apiKey, RouteContext = route,
    };

    [Fact(Timeout = 30000)]
    public async Task PooledCommands_ReuseOneStream_PreserveTransactionsAndUnaryMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        try
        {
            var options = Options(host);
            await using (var client = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE stream_test (id INTEGER PRIMARY KEY, value DECIMAL(10,2)); INSERT INTO stream_test VALUES (1, 12.34)", ct)).Error);
                var tx = await client.BeginTransactionAsync(ct);
                Assert.Null((await client.ExecuteInTransactionAsync(tx.TransactionId, "UPDATE stream_test SET value = 99.50 WHERE id = 1", ct)).Error);
                await client.RollbackTransactionAsync(tx.TransactionId, ct);
                Assert.Equal(12.34m, (await client.ExecuteSqlAsync("SELECT value FROM stream_test WHERE id = 1", ct)).Rows![0][0]);
                tx = await client.BeginTransactionAsync(ct);
                Assert.Null((await client.ExecuteInTransactionAsync(tx.TransactionId, "UPDATE stream_test SET value = 22.50 WHERE id = 1", ct)).Error);
                await client.CommitTransactionAsync(tx.TransactionId, ct);
                Assert.Contains("stream_test", await client.GetTableNamesAsync(ct));
            }
            await using (var again = await CSharpDbClient.RentAsync(options, 1, ct))
                Assert.Equal(22.50m, (await again.ExecuteSqlAsync("SELECT value FROM stream_test WHERE id = 1", ct)).Rows![0][0]);
            Assert.Equal(1, Count(host, "CommandSession"));
            Assert.Equal(1, Count(host, "GetTableNames"));
            Assert.Equal(0, Count(host, "ExecuteSql"));
            Assert.Equal(0, Count(host, "GetInfo"));
            await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
            await Task.WhenAll(host.ConnectionClosures).WaitAsync(TimeSpan.FromSeconds(5), ct);
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task AbandonedTransaction_IsRolledBackBeforeNextRental()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        try
        {
            var options = Options(host);
            await using (var first = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                Assert.Null((await first.ExecuteSqlAsync("CREATE TABLE stream_abandon (id INTEGER PRIMARY KEY)", ct)).Error);
                var tx = await first.BeginTransactionAsync(ct);
                Assert.Null((await first.ExecuteInTransactionAsync(tx.TransactionId, "INSERT INTO stream_abandon VALUES (1)", ct)).Error);
            }
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(0L, (await next.ExecuteSqlAsync("SELECT COUNT(*) FROM stream_abandon", ct)).Rows![0][0]);
            Assert.Equal(1, Count(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task ConcurrentCalls_AreOrdered_AndStaleRentalIsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        try
        {
            var options = Options(host);
            var client = await CSharpDbClient.RentAsync(options, 1, ct);
            var reads = Enumerable.Range(0, 32).Select(i => client.ExecuteSqlAsync($"SELECT {i}", ct)).ToArray();
            var results = await Task.WhenAll(reads);
            for (int i = 0; i < results.Length; i++) Assert.Equal((long)i, results[i].Rows![0][0]);
            await client.DisposeAsync();
            await Assert.ThrowsAsync<ObjectDisposedException>(() => client.ExecuteSqlAsync("SELECT 42", ct));
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(42L, (await next.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
            Assert.Equal(1, Count(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task CallModes_HaveSeparatePoolFamilies()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        try
        {
            await using var unary = await CSharpDbClient.RentAsync(Options(host, CSharpDbGrpcCallMode.Unary), 1, ct);
            await using var stream = await CSharpDbClient.RentAsync(Options(host), 1, ct);
            Assert.Equal(1, Count(host, "GetInfo"));
            Assert.Equal(1, Count(host, "CommandSession"));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task AuthenticationAndRoute_AreBoundToTheSession()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl { BlockFirstUnroutedGetInfo = true };
        await using var host = new GrpcSocketDaemonFixture("session-key", services => Decorate(services, control));
        using var startupDeadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        startupDeadline.CancelAfter(TimeSpan.FromSeconds(15));
        try
        {
            await control.StartupInfoEntered.Task.WaitAsync(startupDeadline.Token);
            Assert.False(control.Readiness.IsReady);
            control.Routes.Clear(); // Reproduce the old clear before startup GetInfo records its route.
            Assert.Empty(control.Routes);
            control.StartupInfoRelease.TrySetResult();
            await control.StartupInfoRecorded.Task.WaitAsync(startupDeadline.Token);
            Assert.Null(Assert.Single(control.Routes)); // The known startup probe can arrive after that clear.
            await control.WaitForReadyAsync(startupDeadline.Token);
            Assert.True(control.Readiness.IsReady);
            control.Routes.Clear(); // Initialization has completed; no startup probe can now race this clear.

            var route = new CSharpDbRouteContext { Keyspace = "tenant", Key = "42" };
            await using var client = CSharpDbClient.Create(Options(host, apiKey: "session-key", route: route));
            Assert.Equal(42L, (await client.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
            Assert.NotEmpty(control.Routes);
            Assert.All(control.Routes, actual => { Assert.Equal(route.Keyspace, actual?.Keyspace); Assert.Equal(route.Key, actual?.Key); });
            await using var rejected = CSharpDbClient.Create(Options(host, apiKey: "wrong-key"));
            await Assert.ThrowsAsync<CSharpDbClientException>(() => rejected.GetInfoAsync(ct));
            Assert.Equal(0, Count(host, "GetInfo"));
            Assert.Equal(2, Count(host, "CommandSession"));
        }
        finally
        {
            control.StartupInfoRelease.TrySetResult();
            if (control.StartupInfoEntered.Task.IsCompleted)
                // Finish released server work even if the test token was canceled.
                await control.StartupInfoFinished.Task.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None);
        }
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnsupportedVersionOrDuplicateRequestId_ClosesWithoutExecutingSql(bool duplicate)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        using var channel = GrpcChannel.ForAddress(host.NativeEndpoint);
        using var call = new CSharpDbRpc.CSharpDbRpcClient(channel).CommandSession(cancellationToken: ct);
        await call.RequestStream.WriteAsync(new CommandSessionRequest { Hello = new() { ProtocolVersion = duplicate ? 1u : 2u } }, ct);
        if (duplicate)
        {
            Assert.True(await call.ResponseStream.MoveNext(ct));
            await call.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 1, GetInfo = new() }, ct);
            Assert.True(await call.ResponseStream.MoveNext(ct));
            await call.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 1, ExecuteSql = new() { Sql = "SELECT 42" } }, ct);
        }
        var error = await Assert.ThrowsAsync<RpcException>(() => call.ResponseStream.MoveNext(ct));
        Assert.Equal(duplicate ? StatusCode.InvalidArgument : StatusCode.Unimplemented, error.StatusCode);
    }

    [Fact(Timeout = 30000)]
    public async Task RawSessions_CannotUseAnotherSessionsTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        using var channel = GrpcChannel.ForAddress(host.NativeEndpoint);
        var rpc = new CSharpDbRpc.CSharpDbRpcClient(channel);
        using var first = rpc.CommandSession(cancellationToken: ct);
        using var second = rpc.CommandSession(cancellationToken: ct);
        foreach (var call in new[] { first, second })
        {
            await call.RequestStream.WriteAsync(new CommandSessionRequest { Hello = new() { ProtocolVersion = 1 } }, ct);
            Assert.True(await call.ResponseStream.MoveNext(ct));
        }
        await first.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 1, BeginTransaction = new() }, ct);
        Assert.True(await first.ResponseStream.MoveNext(ct));
        string id = first.ResponseStream.Current.Transaction.TransactionId;
        await second.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 1, ExecuteInTransaction = new() { TransactionId = id, Sql = "SELECT 42" } }, ct);
        Assert.True(await second.ResponseStream.MoveNext(ct));
        Assert.Equal((int)StatusCode.PermissionDenied, second.ResponseStream.Current.Error.StatusCode);
        await first.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 2, RollbackTransaction = new() { TransactionId = id } }, ct);
        Assert.True(await first.ResponseStream.MoveNext(ct));
        Assert.NotNull(first.ResponseStream.Current.Completed);
    }

    [Theory(Timeout = 30000)]
    [InlineData("BeginTransactionAsync")]
    [InlineData("CommitTransactionAsync")]
    public async Task LostTransactionResponse_IsNotReplayed_AndLateBeginIsCleaned(string method)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl { BlockAfter = method };
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, control));
        var options = Options(host);
        try
        {
            var client = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE stream_lost (id INTEGER PRIMARY KEY)", ct)).Error);
            string? transaction = null;
            if (method == nameof(ICSharpDbClient.CommitTransactionAsync))
            {
                transaction = (await client.BeginTransactionAsync(ct)).TransactionId;
                Assert.Null((await client.ExecuteInTransactionAsync(transaction, "INSERT INTO stream_lost VALUES (1)", ct)).Error);
            }
            using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task operation = transaction is null ? client.BeginTransactionAsync(cancel.Token) : client.CommitTransactionAsync(transaction, cancel.Token);
            await control.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation.WaitAsync(TimeSpan.FromSeconds(5), ct));
            control.Release.TrySetResult();
            if (transaction is null) await control.RolledBack.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            await client.DisposeAsync();
            await using var next = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(transaction is null ? 0L : 1L, (await next.ExecuteSqlAsync("SELECT COUNT(*) FROM stream_lost", ct)).Rows![0][0]);
            Assert.Equal(1, control.Count(method));
            Assert.Equal(2, Count(host, "CommandSession"));
        }
        finally { control.Release.TrySetResult(); await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Fact(Timeout = 30000)]
    public async Task CanceledQueueWaiter_DoesNotCancelActiveCommand()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl { BlockBeforeSql = true };
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, control));
        try
        {
            await using var client = await CSharpDbClient.RentAsync(Options(host), 1, ct);
            var active = client.ExecuteSqlAsync("SELECT 42", ct);
            await control.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var waiting = client.GetInfoAsync(cancel.Token);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
            Assert.False(active.IsCompleted);
            control.Release.TrySetResult();
            Assert.Equal(42L, (await active.WaitAsync(TimeSpan.FromSeconds(5), ct)).Rows![0][0]);
        }
        finally { control.Release.TrySetResult(); await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RawDisconnect_RollsBackSessionTransactions(bool graceful)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl();
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, control));
        using var channel = GrpcChannel.ForAddress(host.NativeEndpoint);
        using var call = new CSharpDbRpc.CSharpDbRpcClient(channel).CommandSession(cancellationToken: ct);
        await call.RequestStream.WriteAsync(new CommandSessionRequest { Hello = new() { ProtocolVersion = 1 } }, ct);
        Assert.True(await call.ResponseStream.MoveNext(ct));
        await call.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 1, BeginTransaction = new() }, ct);
        Assert.True(await call.ResponseStream.MoveNext(ct));
        await call.RequestStream.WriteAsync(new CommandSessionRequest { RequestId = 2, ExecuteInTransaction = new()
            { TransactionId = call.ResponseStream.Current.Transaction.TransactionId, Sql = "CREATE TABLE disconnected (id INTEGER PRIMARY KEY)" } }, ct);
        Assert.True(await call.ResponseStream.MoveNext(ct));
        Assert.Null(call.ResponseStream.Current.Error);
        if (graceful)
        {
            await call.RequestStream.CompleteAsync();
            Assert.False(await call.ResponseStream.MoveNext(ct));
        }
        else call.Dispose();
        await control.RolledBack.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        await using var verify = host.CreateClient(CSharpDbGrpcMode.Native);
        Assert.DoesNotContain("disconnected", await verify.GetTableNamesAsync(ct));
        Assert.Equal(1, control.Count(nameof(ICSharpDbClient.BeginTransactionAsync)));
        Assert.Equal(1, control.Count(nameof(ICSharpDbClient.RollbackTransactionAsync)));
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActiveCancellationOrOwnerDisposal_ClosesSessionAndRollsBack(bool dispose)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl();
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, control));
        await using var client = CSharpDbClient.Create(Options(host));
        await client.BeginTransactionAsync(ct);
        control.BlockBeforeSql = true;
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var active = client.ExecuteSqlAsync("SELECT 42", cancel.Token);
        await control.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        if (dispose) await client.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5), ct);
        else cancel.Cancel();
        await Assert.ThrowsAnyAsync<Exception>(() => active.WaitAsync(TimeSpan.FromSeconds(5), ct));
        await control.RolledBack.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.Equal(1, Count(host, "CommandSession"));
        Assert.Equal(1, control.Count(nameof(ICSharpDbClient.ExecuteSqlAsync)));
        if (!dispose) await Assert.ThrowsAsync<CSharpDbClientException>(() => client.GetInfoAsync(ct));
    }

    [Theory(Timeout = 30000)]
    [InlineData("id")]
    [InlineData("type")]
    [InlineData("error")]
    public async Task MalformedResponse_DiscardsSessionWithoutReplay(string corruption)
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl();
        await using var host = new GrpcSocketDaemonFixture(configureServices: services =>
        {
            Decorate(services, control);
            services.AddSingleton(new CorruptSessionInterceptor(corruption));
            services.AddGrpc(options => options.Interceptors.Add<CorruptSessionInterceptor>());
        });
        var options = Options(host);
        try
        {
            await using (var client = await CSharpDbClient.RentAsync(options, 1, ct))
                await Assert.ThrowsAsync<CSharpDbClientException>(() => client.ExecuteSqlAsync("SELECT 42", ct));
            Assert.Equal(1, control.Count(nameof(ICSharpDbClient.ExecuteSqlAsync)));
            await using var replacement = await CSharpDbClient.RentAsync(options, 1, ct);
            Assert.Equal(42L, (await replacement.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
            Assert.Equal(2, Count(host, "CommandSession"));
            Assert.Equal(2, control.Count(nameof(ICSharpDbClient.ExecuteSqlAsync)));
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Theory]
    [InlineData(CSharpDbTransport.Direct, CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    [InlineData(CSharpDbTransport.Grpc, CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Streaming)]
    [InlineData(CSharpDbTransport.Grpc, CSharpDbGrpcMode.Native, (CSharpDbGrpcCallMode)42)]
    public void InvalidStreamingConfiguration_IsRejected(CSharpDbTransport transport, CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
        => Assert.Throws<CSharpDbClientConfigurationException>(() => CSharpDbClient.Create(new()
        { Transport = transport, Endpoint = "http://localhost", GrpcMode = mode, GrpcCallMode = calls }));

    [Fact(Timeout = 30000)]
    public async Task CommandError_PreservesDatabaseCode_AndDirectSessionRemainsUsable()
    {
        var ct = TestContext.Current.CancellationToken;
        var control = new SessionControl { SqlError = new CSharpDB.Primitives.CSharpDbException(CSharpDB.Primitives.ErrorCode.Busy, "test busy") };
        await using var host = new GrpcSocketDaemonFixture(configureServices: services => Decorate(services, control));
        await using var client = CSharpDbClient.Create(Options(host));
        var error = await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(() => client.ExecuteSqlAsync("SELECT 42", ct));
        Assert.Equal(CSharpDB.Primitives.ErrorCode.Busy, error.Code);
        var rpc = Assert.IsType<RpcException>(error.InnerException);
        Assert.Equal(StatusCode.Aborted, rpc.StatusCode);
        Assert.Equal("Busy", rpc.Trailers.GetValue("csharpdb-error-code"));
        control.SqlError = null;
        Assert.Equal(42L, (await client.ExecuteSqlAsync("SELECT 42", ct)).Rows![0][0]);
        Assert.Equal(1, Count(host, "CommandSession"));
    }

    private static long Count(GrpcSocketDaemonFixture host, string method)
        => host.Snapshot().Where(x => x.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(x => x.Value);

    private static void Decorate(IServiceCollection services, SessionControl control)
    {
        var original = services.Last(x => x.ServiceType == typeof(ICSharpDbClient));
        services.Replace(ServiceDescriptor.Singleton<ICSharpDbClient>(provider =>
        {
            var real = (ICSharpDbClient)original.ImplementationFactory!(provider);
            var proxy = DispatchProxy.Create<ICSharpDbClient, SessionProxy>();
            var state = (SessionProxy)(object)proxy;
            state.Inner = real; state.Control = control;
            state.Route = provider.GetRequiredService<ICSharpDbRouteContextAccessor>();
            control.Readiness = provider.GetRequiredService<CSharpDbHostReadinessCoordinator>();
            return proxy;
        }));
    }

    public sealed class SessionControl
    {
        public string? BlockAfter { get; init; }
        public bool BlockBeforeSql { get; set; }
        public Exception? SqlError { get; set; }
        public bool BlockFirstUnroutedGetInfo { get; init; }
        public int StartupInfoIntercepted;
        public CSharpDbHostReadinessCoordinator Readiness { get; set; } = null!;
        public TaskCompletionSource StartupInfoEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource StartupInfoRelease { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource StartupInfoRecorded { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource StartupInfoFinished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RolledBack { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ConcurrentQueue<CSharpDbRouteContext?> Routes { get; } = new();
        public ConcurrentDictionary<string, int> Calls { get; } = new();
        public int Count(string method) => Calls.GetValueOrDefault(method);

        public async Task WaitForReadyAsync(CancellationToken ct)
        {
            try
            {
                while (!Readiness.IsReady)
                {
                    ct.ThrowIfCancellationRequested();
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException error) when (ct.IsCancellationRequested)
            {
                throw new TimeoutException($"Host readiness did not complete; last state: {Readiness.Snapshot}", error);
            }
        }
    }

    public sealed class CorruptSessionInterceptor(string corruption) : Interceptor
    {
        private readonly string _corruption = corruption;
        private int _corrupted;
        public override Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest> reader,
            IServerStreamWriter<TResponse> writer, ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
            => continuation(reader, new CorruptWriter<TResponse>(writer, this), context);

        private sealed class CorruptWriter<T>(IServerStreamWriter<T> inner, CorruptSessionInterceptor owner) : IServerStreamWriter<T>
        {
            public WriteOptions? WriteOptions { get => inner.WriteOptions; set => inner.WriteOptions = value; }
            public Task WriteAsync(T message)
            {
                if (message is CommandSessionResponse { Result: not null } response && Interlocked.Exchange(ref owner._corrupted, 1) == 0)
                {
                    response = response.Clone();
                    switch (owner._corruption)
                    {
                        case "id": response.RequestId++; break;
                        case "type": response.Completed = new Empty(); break;
                        case "error": response.Error = new() { StatusCode = 0, Detail = "invalid" }; break;
                    }
                    return inner.WriteAsync((T)(object)response);
                }
                return inner.WriteAsync(message);
            }
        }
    }

    public class SessionProxy : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public SessionControl Control = null!;
        public ICSharpDbRouteContextAccessor Route = null!;
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var method = targetMethod!;
            Control.Calls.AddOrUpdate(method.Name, 1, (_, count) => count + 1);
            if (method.Name == nameof(ICSharpDbClient.GetInfoAsync) && Control.BlockFirstUnroutedGetInfo &&
                Route.Current is null && Interlocked.CompareExchange(ref Control.StartupInfoIntercepted, 1, 0) == 0)
                return BlockStartupInfoAsync((CancellationToken)args![0]!);
            if (method.Name is nameof(ICSharpDbClient.GetInfoAsync) or nameof(ICSharpDbClient.ExecuteSqlAsync))
                Control.Routes.Enqueue(Route.Current);
            if (method.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && Control.SqlError is { } sqlError) throw sqlError;
            if (method.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && Control.BlockBeforeSql)
                return BlockSqlAsync((string)args![0]!, (CancellationToken)args[1]!);
            object? result;
            try { result = method.Invoke(Inner, args); }
            catch (TargetInvocationException error) when (error.InnerException is not null)
            { ExceptionDispatchInfo.Capture(error.InnerException).Throw(); throw; }
            if (method.Name == Control.BlockAfter)
                return result is Task<TransactionSessionInfo> begin ? AfterBeginAsync(begin) : AfterAsync((Task)result!);
            if (method.Name == nameof(ICSharpDbClient.RollbackTransactionAsync)) return ObserveRollbackAsync((Task)result!);
            return result;
        }
        private async Task<DatabaseInfo> BlockStartupInfoAsync(CancellationToken ct)
        {
            Control.StartupInfoEntered.TrySetResult();
            try
            {
                await Control.StartupInfoRelease.Task.WaitAsync(ct);
                Control.Routes.Enqueue(Route.Current);
                Control.StartupInfoRecorded.TrySetResult();
                return await Inner.GetInfoAsync(ct);
            }
            finally { Control.StartupInfoFinished.TrySetResult(); }
        }
        private async Task<SqlExecutionResult> BlockSqlAsync(string sql, CancellationToken ct)
        { Control.Entered.TrySetResult(); await Control.Release.Task.WaitAsync(ct); return await Inner.ExecuteSqlAsync(sql, ct); }
        private async Task<TransactionSessionInfo> AfterBeginAsync(Task<TransactionSessionInfo> task)
        { var result = await task; Control.Entered.TrySetResult(); await Control.Release.Task; return result; }
        private async Task AfterAsync(Task task)
        { await task; Control.Entered.TrySetResult(); await Control.Release.Task; }
        private async Task ObserveRollbackAsync(Task task)
        { await task; Control.RolledBack.TrySetResult(); }
    }
}

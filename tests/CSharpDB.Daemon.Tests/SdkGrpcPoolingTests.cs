using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Empty = Google.Protobuf.WellKnownTypes.Empty;

namespace CSharpDB.Daemon.Tests;

public sealed class SdkGrpcPoolingTests : IAsyncLifetime
{
    private GrpcSocketDaemonFixture _host = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private CSharpDbClientOptions Options(CSharpDbGrpcMode mode = CSharpDbGrpcMode.Auto, HttpClient? httpClient = null) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = mode,
        Endpoint = _host.EndpointFor(mode).AbsoluteUri, HttpClient = httpClient,
    };

    public ValueTask InitializeAsync()
    {
        _host = new GrpcSocketDaemonFixture();
        return ValueTask.CompletedTask;
    }
    public async ValueTask DisposeAsync()
    {
        await CSharpDbClient.ClearPoolAsync(_host.CompatibilityEndpoint.AbsoluteUri);
        await CSharpDbClient.ClearPoolAsync(_host.NativeEndpoint.AbsoluteUri);
        await _host.DisposeAsync();
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task RentDispose_ReusesChannel_ButDisposedRentalCannotIssueAnyRpc(CSharpDbGrpcMode mode)
    {
        var first = await CSharpDbClient.RentAsync(Options(mode), 1, Ct);
        AssertRead(await first.ExecuteSqlAsync("SELECT 42", Ct));
        await first.DisposeAsync();
        await using var second = await CSharpDbClient.RentAsync(Options(mode), 1, Ct);
        Assert.NotSame(first, second);
        AssertRead(await second.ExecuteSqlAsync("SELECT 42", Ct));
        Assert.Equal(1, Count("GetInfo"));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.ExecuteSqlAsync("SELECT 42", Ct));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.GetTableNamesAsync(Ct));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.GetInfoAsync(Ct));
        await first.DisposeAsync();
        Assert.Equal(2, Count("ExecuteSql"));
        Assert.Equal(0, Count("GetTableNames"));
    }

    [Fact]
    public async Task ReusedOptions_PreserveCapacityIsolationAndRetirement()
    {
        var options = Options();
        await using var single = await CSharpDbClient.RentAsync(options, 1, Ct);
        await using var first = await CSharpDbClient.RentAsync(options, 2, Ct);
        await using var second = await CSharpDbClient.RentAsync(options, 2, Ct);
        Assert.Equal(3, Count("GetInfo"));
        using var cancellation = new CancellationTokenSource();
        var waiting = CSharpDbClient.RentAsync(options, 1, cancellation.Token).AsTask();
        Assert.False(waiting.IsCompleted);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
        await using var fresh = await CSharpDbClient.RentAsync(options, 1, Ct);
        AssertRead(await fresh.ExecuteSqlAsync("SELECT 42", Ct));
        AssertRead(await single.ExecuteSqlAsync("SELECT 42", Ct));
        Assert.Equal(4, Count("GetInfo"));
    }

    [Fact]
    public async Task PoolCapacity_WaitsCancels_AndClearWakesWaiterWithoutClosingActiveRental()
    {
        await using var owner = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task<ICSharpDbClient> canceled = CSharpDbClient.RentAsync(Options(), 1, cancellation.Token).AsTask();
        Assert.False(canceled.IsCompleted);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
        Task<ICSharpDbClient> waiting = CSharpDbClient.RentAsync(Options(), 1, Ct).AsTask();
        Assert.False(waiting.IsCompleted);
        await CSharpDbClient.ClearPoolAsync(_host.CompatibilityEndpoint.AbsoluteUri);
        await using var admitted = await waiting.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        AssertRead(await owner.ExecuteSqlAsync("SELECT 42", Ct));
        Assert.Equal(2, Count("GetInfo"));
    }

    [Fact]
    public async Task Dispose_RollsBackUnfinishedTransaction_BeforeNextRental()
    {
        var owner = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        Assert.Null((await owner.ExecuteSqlAsync("CREATE TABLE sdk_tx (id INTEGER PRIMARY KEY)", Ct)).Error);
        var transaction = await owner.BeginTransactionAsync(Ct);
        Assert.Null((await owner.ExecuteInTransactionAsync(transaction.TransactionId, "INSERT INTO sdk_tx VALUES (1)", Ct)).Error);
        await owner.DisposeAsync();
        await using var next = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        var result = await next.ExecuteSqlAsync("SELECT COUNT(*) FROM sdk_tx", Ct);
        Assert.Null(result.Error);
        Assert.Equal(0L, result.Rows![0][0]);
        await Assert.ThrowsAsync<InvalidOperationException>(() => next.CommitTransactionAsync(transaction.TransactionId, Ct));
        Assert.Equal(1, Count("RollbackTransaction"));
        Assert.Equal(0, Count("CommitTransaction"));
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task Commit_CompletesTransaction_WithoutDisposalRollback()
    {
        await using (var client = await CSharpDbClient.RentAsync(Options(), 1, Ct))
        {
            var transaction = await client.BeginTransactionAsync(Ct);
            await client.CommitTransactionAsync(transaction.TransactionId, Ct);
        }
        Assert.Equal(1, Count("CommitTransaction"));
        Assert.Equal(0, Count("RollbackTransaction"));
    }

    [Fact]
    public async Task ForeignTransactionId_IsRejected_WhileBothRentalsAreLive()
    {
        await using var first = await CSharpDbClient.RentAsync(Options(), 2, Ct);
        await using var second = await CSharpDbClient.RentAsync(Options(), 2, Ct);
        var transaction = await first.BeginTransactionAsync(Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.ExecuteInTransactionAsync(transaction.TransactionId, "SELECT 42", Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.RollbackTransactionAsync(transaction.TransactionId, Ct));
        Assert.Equal(0, Count("ExecuteInTransaction"));
        Assert.Equal(0, Count("RollbackTransaction"));
        await first.RollbackTransactionAsync(transaction.TransactionId, Ct);
    }

    [Fact]
    public async Task CanceledRpc_DiscardsConnection_AndDoesNotReplaySql()
    {
        var first = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.ExecuteSqlAsync("SELECT 42", cancellation.Token));
        await first.DisposeAsync();
        long before = Count("ExecuteSql");
        await using var second = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        Assert.Equal(2, Count("GetInfo"));
        Assert.Equal(before, Count("ExecuteSql"));
    }

    [Theory]
    [InlineData("CommitTransaction")]
    [InlineData("RollbackTransaction")]
    public async Task FailedTransactionRpc_IsNotReplayed_AndConnectionIsDiscarded(string failedMethod)
    {
        using var handler = new ControlledHandler();
        using var httpClient = new HttpClient(handler);
        var options = Options(CSharpDbGrpcMode.Native, httpClient);
        var first = await CSharpDbClient.RentAsync(options, 1, Ct);
        var transaction = await first.BeginTransactionAsync(Ct);
        handler.FailMethod = failedMethod;
        if (failedMethod == "CommitTransaction")
            await Assert.ThrowsAsync<CSharpDbClientException>(() => first.CommitTransactionAsync(transaction.TransactionId, Ct));
        else await Assert.ThrowsAsync<CSharpDbClientException>(() => first.RollbackTransactionAsync(transaction.TransactionId, Ct));
        handler.FailMethod = null;
        await first.DisposeAsync();
        Assert.Equal(1, handler.Failures);
        Assert.Equal(0, Count(failedMethod));
        Assert.Equal(failedMethod == "CommitTransaction" ? 1 : 0, Count("RollbackTransaction"));
        if (failedMethod == "RollbackTransaction")
        {
            // Test teardown explicitly releases the server transaction whose
            // rollback was intercepted. The rental itself did not replay it.
            await using var cleanup = CSharpDbClient.Create(options);
            await cleanup.RollbackTransactionAsync(transaction.TransactionId, Ct);
        }
        await using var next = await CSharpDbClient.RentAsync(options, 1, Ct);
        Assert.Equal(2, Count("GetInfo"));
    }

    [Theory]
    [InlineData("ExecuteSql")]
    [InlineData("BeginTransaction")]
    public async Task Disposal_DrainsOutstandingRpc_AndResetsTransactionThatCompletesDuringDisposal(string blockedMethod)
    {
        using var handler = new ControlledHandler { BlockMethod = blockedMethod };
        using var httpClient = new HttpClient(handler);
        var options = Options(CSharpDbGrpcMode.Native, httpClient);
        var first = await CSharpDbClient.RentAsync(options, 1, Ct);
        Task operation = blockedMethod == "ExecuteSql"
            ? first.ExecuteSqlAsync("SELECT 42", Ct) : first.BeginTransactionAsync(Ct);
        await handler.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Task disposed = first.DisposeAsync().AsTask();
        Task alsoDisposed = first.DisposeAsync().AsTask();
        Assert.Same(disposed, alsoDisposed);
        Task<ICSharpDbClient> waiting = CSharpDbClient.RentAsync(options, 1, Ct).AsTask();
        Assert.False(disposed.IsCompleted);
        Assert.False(waiting.IsCompleted);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => first.GetInfoAsync(Ct));
        handler.Resume.SetResult();
        await operation.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await disposed.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await alsoDisposed;
        await using var next = await waiting.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(1, Count("GetInfo"));
        Assert.Equal(blockedMethod == "BeginTransaction" ? 1 : 0, Count("RollbackTransaction"));
        AssertRead(await next.ExecuteSqlAsync("SELECT 42", Ct));
    }

    [Fact]
    public async Task RentedClient_PreservesMetadataAndDefinitionCapabilities()
    {
        await using var client = await CSharpDbClient.RentAsync(Options(), 1, Ct);
        var catalog = Assert.IsAssignableFrom<ICSharpDbDefinitionCatalogReader>(client);
        Assert.NotNull(await catalog.ReadDefinitionCatalogAsync(ct: Ct));
        Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(client);
        Assert.NotNull(await client.GetTableNamesAsync(Ct));
    }

    [Fact]
    public async Task Rent_ValidatesConfigurationEvenWhenPoolIsWarm()
    {
        await using (var client = await CSharpDbClient.RentAsync(Options(), 1, Ct)) { }
        await Assert.ThrowsAsync<CSharpDbClientConfigurationException>(() => CSharpDbClient.RentAsync(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, Endpoint = _host.CompatibilityEndpoint.AbsoluteUri, DataSource = "forbidden.db",
        }, 1, Ct).AsTask());
        await Assert.ThrowsAsync<CSharpDbClientConfigurationException>(() => CSharpDbClient.RentAsync(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Http, Endpoint = _host.CompatibilityEndpoint.AbsoluteUri,
        }, 1, Ct).AsTask());
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => CSharpDbClient.RentAsync(Options(), 0, Ct).AsTask());
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task IdleRetirement_RequiresAllRentalsReturned_AndClosesTheFamily()
    {
        var pool = new GrpcClientPool(Options(), 1);
        try
        {
            var client = await pool.TryRentAsync(Ct);
            Assert.NotNull(client);
            Assert.False(pool.TryRetireIdle(TimeSpan.Zero));
            await client.DisposeAsync();
            Assert.True(pool.TryRetireIdle(TimeSpan.Zero));
            Assert.Null(await pool.TryRentAsync(Ct));
        }
        finally { await pool.RetireAsync(); }
    }

    [Fact]
    public async Task ConcurrentReads_OnOneRental_CompleteBeforeConnectionReturnsToPool()
    {
        var first = await CSharpDbClient.RentAsync(Options(CSharpDbGrpcMode.Native), 1, Ct);
        Task<SqlExecutionResult>[] reads = Enumerable.Range(0, 16).Select(_ => first.ExecuteSqlAsync("SELECT 42", Ct)).ToArray();
        Task disposed = first.DisposeAsync().AsTask();
        foreach (var result in await Task.WhenAll(reads)) AssertRead(result);
        await disposed;
        await using var next = await CSharpDbClient.RentAsync(Options(CSharpDbGrpcMode.Native), 1, Ct);
        AssertRead(await next.ExecuteSqlAsync("SELECT 42", Ct));
        Assert.Equal(1, Count("GetInfo"));
        Assert.Equal(17, Count("ExecuteSql"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletedRental_DisposeOnce_AndAllDisposersAwaitReturn(bool delayed)
    {
        await using var owner = new GrpcTransportClient(_host.CompatibilityEndpoint);
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int returns = 0;
        var lease = new GrpcClientLease(owner, (client, healthy) =>
        {
            Assert.Same(owner, client);
            Assert.True(healthy);
            Interlocked.Increment(ref returns);
            return delayed ? new ValueTask(returned.Task) : ValueTask.CompletedTask;
        });
        Task first = lease.DisposeAsync().AsTask();
        Task second = lease.DisposeAsync().AsTask();
        Assert.Same(first, second);
        Assert.Equal(!delayed, first.IsCompletedSuccessfully);
        Assert.Equal(1, returns);
        returned.SetResult();
        await Task.WhenAll(first, second);
        await lease.DisposeAsync();
        Assert.Equal(1, returns);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletedRental_ReturnFailure_IsSharedWithoutRetry(bool asynchronous)
    {
        await using var owner = new GrpcTransportClient(_host.CompatibilityEndpoint);
        var failure = new InvalidOperationException("Injected return failure");
        var returned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int returns = 0;
        var lease = new GrpcClientLease(owner, (_, _) =>
        {
            returns++;
            if (!asynchronous) throw failure;
            return new ValueTask(returned.Task);
        });
        Task first = lease.DisposeAsync().AsTask();
        Task second = lease.DisposeAsync().AsTask();
        if (asynchronous) returned.SetException(failure);
        Assert.Same(first, second);
        Assert.Same(failure, await Record.ExceptionAsync(() => first));
        Assert.Same(failure, await Record.ExceptionAsync(() => second));
        Assert.Equal(1, returns);
    }

    [Fact]
    public async Task RentalInterceptor_ForwardsMetadataStatusAndDisposal_WithoutEagerHeaders()
    {
        await using var owner = new GrpcTransportClient(_host.CompatibilityEndpoint);
        var lease = new GrpcClientLease(owner, (_, _) => ValueTask.CompletedTask);
        var headers = new Metadata { { "test-header", "header" } };
        var trailers = new Metadata { { "test-trailer", "trailer" } };
        int headerReads = 0, callDisposals = 0;
        var marshaller = Marshallers.Create<Empty>(_ => [], _ => new Empty());
        var context = new ClientInterceptorContext<Empty, Empty>(
            new Method<Empty, Empty>(MethodType.Unary, "test", "Read", marshaller, marshaller),
            "localhost", new CallOptions());
        var upstream = new AsyncUnaryCall<Empty>(Task.FromResult(new Empty()),
            _ => { headerReads++; return Task.FromResult(headers); },
            _ => Status.DefaultSuccess, _ => trailers, _ => callDisposals++, new object());
        var call = lease.AsyncUnaryCall(new Empty(), context, (_, _) => upstream);
        Assert.NotNull(await call.ResponseAsync);
        Assert.Equal(0, headerReads);
        Assert.Same(headers, await call.ResponseHeadersAsync);
        Assert.Equal(1, headerReads);
        Assert.Equal(Status.DefaultSuccess, call.GetStatus());
        Assert.Same(trailers, call.GetTrailers());
        call.Dispose();
        Assert.Equal(1, callDisposals);
        await lease.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => lease.AsyncUnaryCall(new Empty(), context, (_, _) => upstream));
    }

    [Fact]
    public async Task DisposalDeadline_DiscardsConnectionWithOutstandingRpc()
    {
        using var handler = new ControlledHandler { BlockMethod = "ExecuteSql" };
        using var httpClient = new HttpClient(handler);
        var options = Options(CSharpDbGrpcMode.Native, httpClient);
        var first = await CSharpDbClient.RentAsync(options, 1, Ct);
        Task operation = first.ExecuteSqlAsync("SELECT 42", Ct);
        await handler.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await first.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(12), Ct);
        await Assert.ThrowsAnyAsync<Exception>(() => operation.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        await using var next = await CSharpDbClient.RentAsync(options, 1, Ct);
        Assert.Equal(2, Count("GetInfo"));
        Assert.Equal(0, Count("ExecuteSql"));
    }

    [Fact]
    public async Task AuthenticatedRentals_DoNotReuseConnectionsAcrossDifferentCredentials()
    {
        await using var secure = new GrpcSocketDaemonFixture(apiKey: "pool-test-key");
        CSharpDbClientOptions AuthOptions(string key) => new()
        {
            Transport = CSharpDbTransport.Grpc, Endpoint = secure.CompatibilityEndpoint.AbsoluteUri, ApiKey = key,
        };
        try
        {
            await Assert.ThrowsAsync<CSharpDbClientException>(() => CSharpDbClient.RentAsync(AuthOptions("wrong-key"), 1, Ct).AsTask());
            await using (var first = await CSharpDbClient.RentAsync(AuthOptions("pool-test-key"), 1, Ct))
                AssertRead(await first.ExecuteSqlAsync("SELECT 42", Ct));
            await Assert.ThrowsAsync<CSharpDbClientException>(() => CSharpDbClient.RentAsync(AuthOptions("wrong-key"), 1, Ct).AsTask());
            await using (var next = await CSharpDbClient.RentAsync(AuthOptions("pool-test-key"), 1, Ct))
                AssertRead(await next.ExecuteSqlAsync("SELECT 42", Ct));
            Assert.Equal(3, secure.Snapshot().Where(pair => pair.Key.Method.EndsWith("/GetInfo", StringComparison.Ordinal)).Sum(pair => pair.Value));
        }
        finally { await CSharpDbClient.ClearPoolAsync(secure.CompatibilityEndpoint.AbsoluteUri); }
    }

    private long Count(string method) => _host.Snapshot().Where(pair => pair.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(pair => pair.Value);
    private static void AssertRead(SqlExecutionResult result)
    {
        Assert.Null(result.Error);
        Assert.Equal(42L, result.Rows![0][0]);
    }

    private sealed class ControlledHandler() : DelegatingHandler(new SocketsHttpHandler())
    {
        public string? FailMethod { get; set; }
        public string? BlockMethod { get; init; }
        public int Failures { get; private set; }
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            string method = request.RequestUri!.Segments[^1];
            if (method == FailMethod) { Failures++; throw new HttpRequestException("Injected RPC failure"); }
            if (method == BlockMethod) { Entered.TrySetResult(); await Resume.Task.WaitAsync(cancellationToken); }
            return await base.SendAsync(request, cancellationToken);
        }
    }
}

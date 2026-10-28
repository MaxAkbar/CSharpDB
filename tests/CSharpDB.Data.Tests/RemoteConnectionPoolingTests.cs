using System.Data;
using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class RemoteConnectionPoolingTests : IAsyncLifetime
{
    private GrpcSocketDaemonFixture _host = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private string ConnectionString => $"Transport=Grpc;Endpoint={_host.CompatibilityEndpoint};Pooling=true;Max Pool Size=1";

    public ValueTask InitializeAsync()
    {
        _host = new GrpcSocketDaemonFixture();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearPoolAsync(ConnectionString);
        await CSharpDbClient.ClearPoolAsync(_host.NativeEndpoint.AbsoluteUri);
        await _host.DisposeAsync();
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 3)]
    public async Task OpenReadClose_ValidatesOnlyNewPhysicalClients(bool pooling, int expectedOpens)
    {
        string cs = ConnectionString.Replace("Pooling=true", $"Pooling={pooling}");
        for (int i = 0; i < 3; i++)
        {
            await using var connection = new CSharpDbConnection(cs);
            await connection.OpenAsync(Ct);
            Assert.Equal(42, await ScalarAsync(connection));
        }
        Assert.Equal(expectedOpens, Count("GetInfo"));
        Assert.Equal(3, Count("ExecuteSql"));
        Assert.All(_host.Snapshot(), pair => Assert.Equal("HTTP/1.1", pair.Key.Protocol));
    }

    [Fact]
    public async Task WarmConfiguration_StillRejectsEmbeddedOptionsAndHonorsCanceledOpen()
    {
        string cs = ConnectionString;
        await using (var first = new CSharpDbConnection(cs))
            await first.OpenAsync(Ct);

        await using var invalid = new CSharpDbConnection(cs)
        {
            DirectDatabaseOptions = new CSharpDB.Engine.DatabaseOptions(),
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => invalid.OpenAsync(Ct));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await using var canceled = new CSharpDbConnection(cs);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.OpenAsync(cancellation.Token));
        Assert.Equal(ConnectionState.Closed, canceled.State);
        await using var next = new CSharpDbConnection(cs);
        await next.OpenAsync(Ct);
        Assert.Equal(42, await ScalarAsync(next));
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task WarmConfiguration_FollowsChangedConnectionStringAndPoolRetirement()
    {
        string cs = ConnectionString;
        await using (var warm = new CSharpDbConnection(cs))
            await warm.OpenAsync(Ct);
        await CSharpDbClient.ClearPoolAsync(_host.CompatibilityEndpoint.AbsoluteUri);
        await using var connection = new CSharpDbConnection(cs);
        await connection.OpenAsync(Ct);
        Assert.Equal(42, await ScalarAsync(connection));
        await connection.CloseAsync();
        connection.ConnectionString = $"Transport=Grpc;Grpc Mode=Native;Endpoint={_host.NativeEndpoint};Pooling=true;Max Pool Size=1";
        await connection.OpenAsync(Ct);
        Assert.Equal(42, await ScalarAsync(connection));
        Assert.Equal(3, Count("GetInfo"));
        Assert.Contains(_host.Snapshot(), pair => pair.Key.Protocol == "HTTP/2" && pair.Key.Method.EndsWith("/ExecuteSql"));
    }

    [Fact]
    public async Task Capacity_WaitsUntilReturn_AndCancellationDoesNotLeakSlot()
    {
        await using var owner = new CSharpDbConnection(ConnectionString);
        await owner.OpenAsync(Ct);
        await using var canceled = new CSharpDbConnection(ConnectionString);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        Task canceledOpen = canceled.OpenAsync(cancellation.Token);
        Assert.False(canceledOpen.IsCompleted);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledOpen);
        Assert.Equal(ConnectionState.Closed, canceled.State);

        await using var waiter = new CSharpDbConnection(ConnectionString);
        Task waitingOpen = waiter.OpenAsync(Ct);
        Assert.False(waitingOpen.IsCompleted);
        Assert.Equal(1, Count("GetInfo"));
        await owner.CloseAsync();
        await waitingOpen.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(42, await ScalarAsync(waiter));
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task Close_RollsBackBeforeReuse_AndOldTransactionCannotAffectNewLease()
    {
        await using var connection = new CSharpDbConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await NonQueryAsync(connection, "CREATE TABLE pool_tx (id INTEGER PRIMARY KEY)");
        var transaction = await connection.BeginTransactionAsync(Ct);
        await NonQueryAsync(connection, "INSERT INTO pool_tx VALUES (1)");
        await connection.CloseAsync();
        Assert.Equal(1, Count("RollbackTransaction"));
        await connection.OpenAsync(Ct);
        Assert.Equal(0L, await ScalarAsync(connection, "SELECT COUNT(*) FROM pool_tx"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Ct));
        await using var nextTransaction = await connection.BeginTransactionAsync(Ct);
        await NonQueryAsync(connection, "INSERT INTO pool_tx VALUES (2)");
        await nextTransaction.CommitAsync(Ct);
        Assert.Equal(1L, await ScalarAsync(connection, "SELECT COUNT(*) FROM pool_tx"));
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task ClearPool_RetiresActiveLease_AndWakesWaiterIntoNewPool()
    {
        await using var owner = new CSharpDbConnection(ConnectionString);
        await owner.OpenAsync(Ct);
        await using var waiter = new CSharpDbConnection(ConnectionString);
        Task waitingOpen = waiter.OpenAsync(Ct);
        Assert.False(waitingOpen.IsCompleted);
        await CSharpDbConnection.ClearPoolAsync(ConnectionString);
        await waitingOpen.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(2, Count("GetInfo"));
        Assert.Equal(42, await ScalarAsync(owner));
        await owner.CloseAsync();
        await waiter.CloseAsync();
        await using var next = new CSharpDbConnection(ConnectionString);
        await next.OpenAsync(Ct);
        Assert.Equal(42, await ScalarAsync(next));
        Assert.Equal(2, Count("GetInfo"));
    }

    [Fact]
    public async Task ClearAllPools_DiscardsIdleClient()
    {
        await using var connection = new CSharpDbConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await connection.CloseAsync();
        await CSharpDbConnection.ClearAllPoolsAsync();
        await connection.OpenAsync(Ct);
        Assert.Equal(2, Count("GetInfo"));
    }

    [Fact]
    public async Task SdkAndAdoNet_RentTheSamePool_AndBothClearApisRetireIt()
    {
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, Endpoint = _host.CompatibilityEndpoint.AbsoluteUri,
        };
        await using (var sdk = await CSharpDbClient.RentAsync(options, 1, Ct))
            Assert.Null((await sdk.ExecuteSqlAsync("SELECT 42", Ct)).Error);
        await using (var ado = new CSharpDbConnection(ConnectionString))
        {
            await ado.OpenAsync(Ct);
            Assert.Equal(42, await ScalarAsync(ado));
        }
        await using (var sdk = await CSharpDbClient.RentAsync(options, 1, Ct))
            Assert.Null((await sdk.ExecuteSqlAsync("SELECT 42", Ct)).Error);
        Assert.Equal(1, Count("GetInfo"));

        await CSharpDbClient.ClearPoolAsync(options.Endpoint);
        await using (var ado = new CSharpDbConnection(ConnectionString)) await ado.OpenAsync(Ct);
        Assert.Equal(2, Count("GetInfo"));
        await CSharpDbConnection.ClearPoolAsync(ConnectionString);
        await using (var sdk = await CSharpDbClient.RentAsync(options, 1, Ct)) { }
        Assert.Equal(3, Count("GetInfo"));
    }

    [Fact]
    public async Task CanceledRpc_DiscardsClient_WithoutReplayingCommand()
    {
        await using var connection = new CSharpDbConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 42";
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cancellation.Token));
        await connection.CloseAsync();
        long requestsBefore = Count("ExecuteSql");
        await connection.OpenAsync(Ct);
        Assert.Equal(2, Count("GetInfo"));
        Assert.Equal(requestsBefore, Count("ExecuteSql"));
        Assert.Equal(42, await ScalarAsync(connection));
    }

    [Fact]
    public async Task SqlErrorResponse_DoesNotDiscardHealthyChannel()
    {
        await using var connection = new CSharpDbConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await Assert.ThrowsAsync<CSharpDbDataException>(() => NonQueryAsync(connection,
            "CREATE TEMP TABLE requires_transaction (id INTEGER)"));
        Assert.Equal(42, await ScalarAsync(connection));
        await connection.CloseAsync();
        await connection.OpenAsync(Ct);
        Assert.Equal(1, Count("GetInfo"));
    }

    [Fact]
    public async Task TransportFailure_ReleasesFailedOpenSlot_AndDiscardsFailedQueryClient()
    {
        using var handler = new FaultHandler { Fail = true };
        using var httpClient = new HttpClient(handler);
        string cs = $"Transport=Grpc;Grpc Mode=Native;Endpoint={_host.NativeEndpoint};Pooling=true;Max Pool Size=1";
        await using var connection = new CSharpDbConnection(cs, httpClient);
        await Assert.ThrowsAsync<CSharpDbClientException>(() => connection.OpenAsync(Ct));
        Assert.Equal(ConnectionState.Closed, connection.State);
        handler.Fail = false;
        await connection.OpenAsync(Ct).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        handler.Fail = true;
        await Assert.ThrowsAsync<CSharpDbClientException>(() => ScalarAsync(connection));
        await connection.CloseAsync();
        handler.Fail = false;
        await connection.OpenAsync(Ct);
        Assert.Equal(2, Count("GetInfo"));
        Assert.Equal(0, Count("ExecuteSql"));
        Assert.Equal(42, await ScalarAsync(connection));
    }

    [Fact]
    public async Task PoolKeys_IsolateCredentialsRoutesAndCapacity()
    {
        CSharpDbClientOptions Options(string? key = null, string? route = null, string header = "X-CSharpDB-Api-Key") => new()
        {
            Transport = CSharpDbTransport.Grpc, Endpoint = _host.CompatibilityEndpoint.AbsoluteUri,
            ApiKey = key, ApiKeyHeaderName = header,
            RouteContext = route is null ? null : new CSharpDbRouteContext { Keyspace = "tenant", Key = route },
        };
        async Task Rent(CSharpDbClientOptions options, int capacity = 1)
        {
            await using var session = await CSharpDbClient.RentAsync(options, capacity, Ct);
        }
        await Rent(Options());
        await Rent(Options());
        Assert.Equal(1, Count("GetInfo"));
        await Rent(Options("key-a"));
        await Rent(Options("key-b"));
        await Rent(Options("key-a", header: "X-Other-Key"));
        await Rent(Options(route: "a"));
        await Rent(Options(route: "b"));
        await Rent(Options(), 2);
        Assert.Equal(7, Count("GetInfo"));
        await CSharpDbConnection.ClearPoolAsync(ConnectionString);
        await Rent(Options("key-a"));
        await Rent(Options(route: "a"));
        Assert.Equal(9, Count("GetInfo"));
    }

    [Fact]
    public async Task SuppliedHttpClients_AreIsolated_AndRemainCallerOwnedAfterClear()
    {
        using var firstClient = new HttpClient();
        using var secondClient = new HttpClient();
        string cs = $"Transport=Grpc;Grpc Mode=Native;Endpoint={_host.NativeEndpoint};Pooling=true;Max Pool Size=1";
        // A warmed plan for the default handler must not capture later callers
        // that supply their own HTTP client using the very same string object.
        await using (var warm = new CSharpDbConnection(cs))
            await warm.OpenAsync(Ct);
        foreach (HttpClient client in new[] { firstClient, firstClient, secondClient })
        {
            await using var connection = new CSharpDbConnection(cs, client);
            await connection.OpenAsync(Ct);
            Assert.Equal(42, await ScalarAsync(connection));
        }
        Assert.Equal(3, Count("GetInfo"));
        await using (var auto = new CSharpDbConnection(cs.Replace("Grpc Mode=Native", "Grpc Mode=Auto"), firstClient))
        {
            await auto.OpenAsync(Ct);
            Assert.Equal(42, await ScalarAsync(auto));
        }
        Assert.Equal(4, Count("GetInfo"));
        await CSharpDbConnection.ClearPoolAsync(cs);
        using var response = await firstClient.GetAsync(_host.CompatibilityEndpoint, Ct);
        Assert.True((int)response.StatusCode > 0);
    }

    [Theory]
    [InlineData("Transport=Http;Endpoint=http://localhost;Pooling=true")]
    [InlineData("Transport=Grpc;Endpoint=http://localhost;Pooling=true;Max Pool Size=0")]
    public async Task Pooling_RejectsUnsupportedTransportAndInvalidCapacity(string connectionString)
    {
        await using var connection = new CSharpDbConnection(connectionString);
        await Assert.ThrowsAnyAsync<Exception>(() => connection.OpenAsync(Ct));
        Assert.Equal(ConnectionState.Closed, connection.State);
        Assert.Equal(0, Count("GetInfo"));
    }

    private long Count(string method) => _host.Snapshot()
        .Where(pair => pair.Key.Method.EndsWith("/" + method, StringComparison.Ordinal)).Sum(pair => pair.Value);

    private static async Task<object?> ScalarAsync(CSharpDbConnection connection, string sql = "SELECT CAST(42 AS INTEGER)")
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private static async Task NonQueryAsync(CSharpDbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private sealed class FaultHandler : DelegatingHandler
    {
        public bool Fail { get; set; }
        public FaultHandler() : base(new SocketsHttpHandler()) { }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Fail ? throw new HttpRequestException("Injected transport failure") : base.SendAsync(request, cancellationToken);
    }
}

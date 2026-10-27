using System.Net;
using CSharpDB.Client;
using CSharpDB.Testing;
using Grpc.Net.Client.Web;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcHandlerLifetimeTests : IAsyncLifetime
{
    private const string ApiKey = "handler-lifetime-test-key";
    private GrpcSocketDaemonFixture _host = null!;
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        _host = new GrpcSocketDaemonFixture(ApiKey);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbClient.ClearPoolAsync(_host.CompatibilityEndpoint.AbsoluteUri);
        await CSharpDbClient.ClearPoolAsync(_host.NativeEndpoint.AbsoluteUri);
        await _host.DisposeAsync();
    }

    private CSharpDbClientOptions Options(CSharpDbGrpcMode mode, string apiKey = ApiKey, HttpClient? httpClient = null) => new()
    {
        Transport = CSharpDbTransport.Grpc,
        GrpcMode = mode,
        Endpoint = _host.EndpointFor(mode).AbsoluteUri,
        ApiKey = apiKey,
        HttpClient = httpClient,
    };

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task DirectDispose_ClosesEstablishedTransport(CSharpDbGrpcMode mode)
    {
        var client = CSharpDbClient.Create(Options(mode));
        try
        {
            Assert.NotNull(await client.GetInfoAsync(Ct));
            Task closed = Assert.Single(_host.ConnectionClosures);
            Assert.False(closed.IsCompleted);
            await client.DisposeAsync();
            await closed.WaitAsync(TimeSpan.FromSeconds(3), Ct);
        }
        finally { await client.DisposeAsync(); }
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task PoolClear_ClosesReturnedTransport(CSharpDbGrpcMode mode)
    {
        var options = Options(mode);
        await using (var client = await CSharpDbClient.RentAsync(options, 1, Ct)) { }
        Task closed = Assert.Single(_host.ConnectionClosures);
        Assert.False(closed.IsCompleted);
        await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
        await closed.WaitAsync(TimeSpan.FromSeconds(3), Ct);
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task PoolClear_PreservesActiveTransport_UntilRentalReturns(CSharpDbGrpcMode mode)
    {
        var options = Options(mode);
        await using var client = await CSharpDbClient.RentAsync(options, 1, Ct);
        Task closed = Assert.Single(_host.ConnectionClosures);
        await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
        Assert.False(closed.IsCompleted);
        Assert.NotNull(await client.GetInfoAsync(Ct));
        await client.DisposeAsync();
        await closed.WaitAsync(TimeSpan.FromSeconds(3), Ct);
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task FailedWarmup_ClosesRejectedTransport(CSharpDbGrpcMode mode)
    {
        await Assert.ThrowsAsync<CSharpDbClientException>(() =>
            CSharpDbClient.RentAsync(Options(mode, "wrong-key"), 1, Ct).AsTask());
        await Assert.Single(_host.ConnectionClosures).WaitAsync(TimeSpan.FromSeconds(3), Ct);
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto)]
    [InlineData(CSharpDbGrpcMode.Native)]
    public async Task FaultedRental_ClosesDiscardedTransport(CSharpDbGrpcMode mode)
    {
        await using var client = await CSharpDbClient.RentAsync(Options(mode), 1, Ct);
        Task closed = Assert.Single(_host.ConnectionClosures);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.GetInfoAsync(cancellation.Token));
        await client.DisposeAsync();
        await closed.WaitAsync(TimeSpan.FromSeconds(3), Ct);
    }

    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, false)]
    [InlineData(CSharpDbGrpcMode.Auto, true)]
    [InlineData(CSharpDbGrpcMode.Native, false)]
    [InlineData(CSharpDbGrpcMode.Native, true)]
    public async Task CallerHttpClient_RemainsUsable_AfterClientDisposeAndPoolClear(CSharpDbGrpcMode mode, bool pooled)
    {
        HttpMessageHandler transport = mode == CSharpDbGrpcMode.Auto
            ? new CompatibilityVersionHandler()
            : new SocketsHttpHandler();
        using var httpClient = new HttpClient(transport);
        var options = Options(mode, httpClient: httpClient);
        var client = pooled
            ? await CSharpDbClient.RentAsync(options, 1, Ct)
            : CSharpDbClient.Create(options);
        try
        {
            Assert.NotNull(await client.GetInfoAsync(Ct));
            await client.DisposeAsync();
            await CSharpDbClient.ClearPoolAsync(options.Endpoint!);
            Task closed = Assert.Single(_host.ConnectionClosures);
            Assert.False(closed.IsCompleted);
            using var request = new HttpRequestMessage(HttpMethod.Get, _host.EndpointFor(mode))
            {
                Version = mode == CSharpDbGrpcMode.Auto ? HttpVersion.Version11 : HttpVersion.Version20,
                VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            };
            using var response = await httpClient.SendAsync(request, Ct);
            Assert.Equal(request.Version, response.Version);
            httpClient.Dispose();
            await closed.WaitAsync(TimeSpan.FromSeconds(3), Ct);
        }
        finally { await client.DisposeAsync(); }
    }

    private sealed class CompatibilityVersionHandler()
        : DelegatingHandler(new GrpcWebHandler(GrpcWebMode.GrpcWeb, new SocketsHttpHandler()))
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            request.Version = HttpVersion.Version11;
            request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            return base.SendAsync(request, ct);
        }
    }
}

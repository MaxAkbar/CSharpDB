using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Daemon.Tests;

public sealed class NativeGrpcSocketTests
{
    [Theory]
    [InlineData(CSharpDbGrpcMode.Auto, "HTTP/1.1", "application/grpc-web")]
    [InlineData(CSharpDbGrpcMode.Native, "HTTP/2", "application/grpc")]
    public async Task SqlAndTransactions_UseExpectedWireProtocol(
        CSharpDbGrpcMode mode, string protocol, string contentType)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await using var client = host.CreateClient(mode);
        Assert.Null((await client.ExecuteSqlAsync(
            "CREATE TABLE native_test (id INTEGER PRIMARY KEY, amount DECIMAL(10,2)); INSERT INTO native_test VALUES (1, 12.34);", ct)).Error);
        var transaction = await client.BeginTransactionAsync(ct);
        Assert.Null((await client.ExecuteInTransactionAsync(transaction.TransactionId,
            "UPDATE native_test SET amount = 99.50 WHERE id = 1", ct)).Error);
        await client.RollbackTransactionAsync(transaction.TransactionId, ct);
        var result = await client.ExecuteSqlAsync("SELECT amount FROM native_test WHERE id = 1", ct);
        Assert.Null(result.Error);
        Assert.Equal(12.34m, Assert.Single(Assert.Single(result.Rows!)));
        Assert.All(host.Snapshot(), pair =>
        {
            Assert.Equal(protocol, pair.Key.Protocol);
            Assert.StartsWith(contentType, pair.Key.ContentType, StringComparison.Ordinal);
            if (mode == CSharpDbGrpcMode.Native)
                Assert.DoesNotContain("grpc-web", pair.Key.ContentType, StringComparison.Ordinal);
        });
        Assert.NotEmpty(host.Snapshot());
    }

    [Fact]
    public async Task NativeMode_DoesNotFallBackToHttp1()
    {
        await using var host = new GrpcSocketDaemonFixture();
        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            GrpcMode = CSharpDbGrpcMode.Native,
            Endpoint = host.CompatibilityEndpoint.AbsoluteUri,
        });
        await Assert.ThrowsAsync<CSharpDbClientException>(
            () => client.GetInfoAsync(TestContext.Current.CancellationToken));
        Assert.Empty(host.Snapshot());
    }

    [Fact]
    public async Task NativeMode_CustomHttpClientRemainsCallerOwned()
    {
        await using var host = new GrpcSocketDaemonFixture();
        using var httpClient = new HttpClient { DefaultRequestVersion = System.Net.HttpVersion.Version11 };
        await using (var client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            GrpcMode = CSharpDbGrpcMode.Native,
            Endpoint = host.NativeEndpoint.AbsoluteUri,
            HttpClient = httpClient,
        }))
            Assert.NotNull(await client.GetInfoAsync(TestContext.Current.CancellationToken));
        using var request = new HttpRequestMessage(HttpMethod.Get, host.NativeEndpoint)
        {
            Version = System.Net.HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        using var response = await httpClient.SendAsync(request, TestContext.Current.CancellationToken);
        Assert.Equal(System.Net.HttpVersion.Version20, response.Version);
        Assert.All(host.Snapshot(), pair => Assert.Equal("HTTP/2", pair.Key.Protocol));
    }

    [Fact]
    public async Task NativeMode_ApiKeyAuthorizationStillApplies()
    {
        await using var host = new GrpcSocketDaemonFixture("socket-test-key");
        await using var authorized = host.CreateClient(CSharpDbGrpcMode.Native, "socket-test-key");
        Assert.NotNull(await authorized.GetInfoAsync(TestContext.Current.CancellationToken));
        await using var unauthorized = host.CreateClient(CSharpDbGrpcMode.Native);
        await Assert.ThrowsAsync<CSharpDbClientException>(
            () => unauthorized.GetInfoAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(CSharpDbTransport.Direct, "test.db")]
    [InlineData(CSharpDbTransport.Http, "http://localhost")]
    public void NativeMode_RejectsOtherTransports(CSharpDbTransport transport, string endpoint)
        => Assert.Throws<CSharpDbClientConfigurationException>(() => CSharpDbClient.Create(
            new CSharpDbClientOptions { Transport = transport, Endpoint = endpoint, GrpcMode = CSharpDbGrpcMode.Native }));

    [Fact]
    public void UnknownMode_IsRejected()
        => Assert.Throws<CSharpDbClientConfigurationException>(() => CSharpDbClient.Create(
            new CSharpDbClientOptions { Transport = CSharpDbTransport.Grpc, Endpoint = "http://localhost", GrpcMode = (CSharpDbGrpcMode)42 }));
}

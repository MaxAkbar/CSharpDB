using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AdminRoutedGrpcClientTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RoutedClients_PreserveNativeStreamingMode(bool capture)
    {
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://127.0.0.1:5821",
            GrpcMode = CSharpDbGrpcMode.Native,
            GrpcCallMode = CSharpDbGrpcCallMode.Streaming,
        };
        await using var holder = CreateHolder(options);

        await CheckRoutedModeAsync(holder, capture, expectedStreaming: true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RoutedClients_KeepUnaryAsTheDefault(bool capture)
    {
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc,
            Endpoint = "http://127.0.0.1:5821",
            GrpcMode = CSharpDbGrpcMode.Native,
        };
        await using var holder = CreateHolder(options);

        await CheckRoutedModeAsync(holder, capture, expectedStreaming: false);
    }

    private static DatabaseClientHolder CreateHolder(CSharpDbClientOptions options)
        => new(
            CSharpDbClient.Create(options),
            shardAdmin: null,
            baseClientOptions: options,
            hostDatabaseOptions: new AdminHostDatabaseOptions(),
            functions: DbFunctionRegistry.Create(_ => { }));

    private static async Task CheckRoutedModeAsync(
        DatabaseClientHolder holder, bool capture, bool expectedStreaming)
    {
        var route = new CSharpDbRouteContext { Keyspace = "tenants", Key = "one" };
        if (capture)
        {
            await using DatabaseClientHolder.ClientLease lease = holder.CaptureClient(route);
            Assert.Equal(expectedStreaming,
                Assert.IsAssignableFrom<IScalarProjectionClient>(lease.Client).SupportsScalarProjection);
        }
        else
        {
            await using ICSharpDbClient client = holder.CreateRouteBoundClient(route);
            Assert.Equal(expectedStreaming,
                Assert.IsAssignableFrom<IScalarProjectionClient>(client).SupportsScalarProjection);
        }
    }
}

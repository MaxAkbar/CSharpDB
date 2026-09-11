using System.Net;
using CSharpDB.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Daemon.Tests;

public sealed class DefinitionCatalogGrpcTests
{
    [Fact]
    public async Task Catalog_EmbeddedAndGrpcParityWithPaginationAndNativeDefinitions()
    {
        string path = Path.Combine(Path.GetTempPath(), $"definitions-grpc-{Guid.NewGuid():N}.db"); var ct = TestContext.Current.CancellationToken;
        try
        {
            var expected = await DefinitionCatalogTransportContract.SeedAsync(path, ct);
            await using var factory = new Factory(path);
            using var http = new HttpClient(factory.Server.CreateHandler()) { BaseAddress = new Uri("http://localhost"), DefaultRequestVersion = HttpVersion.Version20, DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact };
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { Transport = CSharpDbTransport.Grpc, Endpoint = "http://localhost", HttpClient = http });
            await DefinitionCatalogTransportContract.VerifyAsync(client, expected, ct);
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm", path + ".archive.csdbtable" }) if (File.Exists(file)) File.Delete(file); }
    }
    private sealed class Factory(string path) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development"); builder.ConfigureServices(s => s.AddHostedService<Shutdown>());
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string,string?> { ["ConnectionStrings:CSharpDB"] = "Data Source=" + path }));
        }
    }
    private sealed class Shutdown(ICSharpDbClient client) : IHostedService
    {
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public async Task StopAsync(CancellationToken ct) => await client.DisposeAsync();
    }
}

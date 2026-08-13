using System.Net;
using CSharpDB.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Daemon.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class PrometheusHostTests
{
    private const string ApiKey = "daemon-prometheus-test-key";

    [Fact]
    public async Task RestDisabledDaemon_StillServesProtectedMetricsAndGrpc()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-daemon-prometheus-{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestDaemonFactory(databasePath);
            using HttpClient httpClient = factory.CreateClient();
            using var scrapeRequest = new HttpRequestMessage(
                HttpMethod.Get,
                "/metrics");
            scrapeRequest.Headers.TryAddWithoutValidation(
                "X-CSharpDB-Api-Key",
                ApiKey);

            using HttpResponseMessage scrape = await httpClient.SendAsync(
                scrapeRequest,
                Ct);
            using HttpResponseMessage rest = await httpClient.GetAsync(
                "/api/info",
                Ct);

            Assert.Equal(HttpStatusCode.OK, scrape.StatusCode);
            Assert.Equal(HttpStatusCode.NotFound, rest.StatusCode);

            using var grpcHttpClient = new HttpClient(
                factory.Server.CreateHandler())
            {
                BaseAddress = new Uri("http://localhost"),
                DefaultRequestVersion = HttpVersion.Version20,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
            };
            await using ICSharpDbClient grpcClient = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    Transport = CSharpDbTransport.Grpc,
                    Endpoint = "http://localhost",
                    HttpClient = grpcHttpClient,
                    ApiKey = ApiKey,
                });

            Assert.False(string.IsNullOrWhiteSpace(
                (await grpcClient.GetInfoAsync(Ct)).DataSource));
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    private sealed class TestDaemonFactory(string databasePath) :
        WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.UseSetting("CSharpDB:Observability:Enabled", "true");
            builder.UseSetting(
                "CSharpDB:Observability:Prometheus:Enabled",
                "true");
            builder.ConfigureServices(services =>
                services.AddHostedService<TestDaemonClientShutdown>());
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["ConnectionStrings:CSharpDB"] =
                            $"Data Source={databasePath}",
                        ["CSharpDB:Daemon:EnableRestApi"] = "false",
                        ["CSharpDB:Daemon:Security:Mode"] = "ApiKey",
                        ["CSharpDB:Daemon:Security:ApiKey"] = ApiKey,
                        ["CSharpDB:Observability:Enabled"] = "true",
                        ["CSharpDB:Observability:Prometheus:Enabled"] = "true",
                    }));
        }
    }

    private sealed class TestDaemonClientShutdown(ICSharpDbClient client) :
        IHostedService
    {
        private int _stopped;

        public Task StartAsync(CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 0)
                await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}

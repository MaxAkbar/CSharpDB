extern alias CSharpDbApi;

using System.Net;
using System.Net.Http.Json;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Health.V1;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using CSharpDbHealthHostExtensions =
    CSharpDbApi::CSharpDB.Api.CSharpDbHealthHostExtensions;
using CSharpDbHostReadinessCoordinator =
    CSharpDbApi::CSharpDB.Api.CSharpDbHostReadinessCoordinator;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcHealthHostTests
{
    private const string ApiKey = "health-test-secret";
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task HealthIsAvailableWithoutRestAndBypassesApiKey()
    {
        string dbPath = NewTempDbPath();
        try
        {
            await using var factory = new HealthDaemonFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:Daemon:EnableRestApi"] = "false",
                    ["CSharpDB:Daemon:Security:Mode"] = "ApiKey",
                    ["CSharpDB:Daemon:Security:ApiKey"] = ApiKey,
                });
            using HttpClient client = factory.CreateDefaultClient();

            await WaitUntilReadyAsync(client);

            using HttpResponseMessage live = await client.GetAsync(
                "/health/live",
                Ct);
            using HttpResponseMessage ready = await client.GetAsync(
                "/health/ready",
                Ct);
            using HttpResponseMessage rest = await client.GetAsync(
                "/api/info",
                Ct);

            Assert.Equal(HttpStatusCode.OK, live.StatusCode);
            Assert.Equal(HttpStatusCode.OK, ready.StatusCode);
            Assert.Contains(
                rest.StatusCode,
                new[]
                {
                    HttpStatusCode.NotFound,
                    HttpStatusCode.MethodNotAllowed,
                });
            await AssertMinimalStatusOnlyAsync(live, "healthy");
            await AssertMinimalStatusOnlyAsync(ready, "healthy");

            using GrpcChannel channel = GrpcChannel.ForAddress(
                client.BaseAddress!,
                new GrpcChannelOptions
                {
                    HttpClient = client,
                    DisposeHttpClient = false,
                });
            var health = new Health.HealthClient(channel);
            HealthCheckResponse overall = await health.CheckAsync(
                new HealthCheckRequest { Service = string.Empty },
                cancellationToken: Ct);
            HealthCheckResponse database = await health.CheckAsync(
                new HealthCheckRequest
                {
                    Service = CSharpDbHealthHostExtensions
                        .DatabaseGrpcServiceName,
                },
                cancellationToken: Ct);
            using AsyncServerStreamingCall<HealthCheckResponse> watch =
                health.Watch(
                    new HealthCheckRequest
                    {
                        Service = CSharpDbHealthHostExtensions
                            .DatabaseGrpcServiceName,
                    },
                    cancellationToken: Ct);
            Assert.True(await watch.ResponseStream.MoveNext(Ct));
            RpcException unknown = await Assert.ThrowsAsync<RpcException>(
                () => health.CheckAsync(
                    new HealthCheckRequest { Service = "unknown.service" },
                    cancellationToken: Ct).ResponseAsync);

            Assert.Equal(
                HealthCheckResponse.Types.ServingStatus.Serving,
                overall.Status);
            Assert.Equal(
                HealthCheckResponse.Types.ServingStatus.Serving,
                database.Status);
            Assert.Equal(
                HealthCheckResponse.Types.ServingStatus.Serving,
                watch.ResponseStream.Current.Status);
            Assert.Equal(StatusCode.NotFound, unknown.StatusCode);
        }
        finally
        {
            TryDelete(dbPath);
            TryDelete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseHealthTracksCachedReadinessWithoutResolvingClient()
    {
        string dbPath = NewTempDbPath();
        try
        {
            await using var factory = new HealthDaemonFactory(
                dbPath,
                publishHealthManually: true);
            using HttpClient client = factory.CreateDefaultClient();
            await WaitUntilReadyAsync(client);
            CSharpDbHostReadinessCoordinator readiness = factory.Services
                .GetRequiredService<CSharpDbHostReadinessCoordinator>();
            using GrpcChannel channel = GrpcChannel.ForAddress(
                client.BaseAddress!,
                new GrpcChannelOptions
                {
                    HttpClient = client,
                    DisposeHttpClient = false,
                });
            var health = new Health.HealthClient(channel);
            HealthCheckService healthChecks = factory.Services
                .GetRequiredService<HealthCheckService>();
            IHealthCheckPublisher healthPublisher = Assert.Single(
                factory.Services.GetServices<IHealthCheckPublisher>());

            await PublishHealthAsync(healthChecks, healthPublisher);
            using AsyncServerStreamingCall<HealthCheckResponse> watch =
                health.Watch(
                    new HealthCheckRequest
                    {
                        Service = CSharpDbHealthHostExtensions
                            .DatabaseGrpcServiceName,
                    },
                    // Keep the streaming call bounded if a transition is
                    // ever lost or the publisher fails to notify watchers.
                    deadline: DateTime.UtcNow.AddSeconds(20),
                    cancellationToken: Ct);
            Assert.True(await watch.ResponseStream.MoveNext(Ct));
            Assert.Equal(
                HealthCheckResponse.Types.ServingStatus.Serving,
                watch.ResponseStream.Current.Status);

            IDisposable lease = readiness.EnterNotReady(
                CSharpDB.Observability.CSharpDbReadinessReason
                    .ExclusiveMaintenance);
            try
            {
                await PublishHealthAsync(healthChecks, healthPublisher);
                Assert.True(await watch.ResponseStream.MoveNext(Ct));
                Assert.Equal(
                    HealthCheckResponse.Types.ServingStatus.NotServing,
                    watch.ResponseStream.Current.Status);

                using HttpResponseMessage live = await client.GetAsync(
                    "/health/live",
                    Ct);
                using HttpResponseMessage ready = await client.GetAsync(
                    "/health/ready",
                    Ct);
                HealthCheckResponse database = await health.CheckAsync(
                    new HealthCheckRequest
                    {
                        Service = CSharpDbHealthHostExtensions
                            .DatabaseGrpcServiceName,
                    },
                    cancellationToken: Ct);

                Assert.Equal(HttpStatusCode.OK, live.StatusCode);
                Assert.Equal(
                    HttpStatusCode.ServiceUnavailable,
                    ready.StatusCode);
                Assert.Equal(
                    HealthCheckResponse.Types.ServingStatus.NotServing,
                    database.Status);

                lease.Dispose();
                await PublishHealthAsync(healthChecks, healthPublisher);
                Assert.True(await watch.ResponseStream.MoveNext(Ct));
                Assert.Equal(
                    HealthCheckResponse.Types.ServingStatus.Serving,
                    watch.ResponseStream.Current.Status);
            }
            finally
            {
                lease.Dispose();
            }

            HealthCheckResponse recovered = await health.CheckAsync(
                new HealthCheckRequest
                {
                    Service = CSharpDbHealthHostExtensions
                        .DatabaseGrpcServiceName,
                },
                cancellationToken: Ct);
            Assert.Equal(
                HealthCheckResponse.Types.ServingStatus.Serving,
                recovered.Status);
        }
        finally
        {
            TryDelete(dbPath);
            TryDelete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DisabledHealthMapsNoHttpOrGrpcHealthButProtectsOrdinaryRpc()
    {
        string dbPath = NewTempDbPath();
        try
        {
            await using var factory = new HealthDaemonFactory(
                dbPath,
                new Dictionary<string, string?>
                {
                    ["CSharpDB:Daemon:EnableRestApi"] = "false",
                    ["CSharpDB:Daemon:Security:Mode"] = "ApiKey",
                    ["CSharpDB:Daemon:Security:ApiKey"] = ApiKey,
                    ["CSharpDB:Observability:Health:Enabled"] = "false",
                });
            using HttpClient client = factory.CreateDefaultClient();

            using HttpResponseMessage live = await client.GetAsync(
                "/health/live",
                Ct);
            using HttpResponseMessage ready = await client.GetAsync(
                "/health/ready",
                Ct);
            Assert.Equal(HttpStatusCode.NotFound, live.StatusCode);
            Assert.Equal(HttpStatusCode.NotFound, ready.StatusCode);

            using GrpcChannel channel = GrpcChannel.ForAddress(
                client.BaseAddress!,
                new GrpcChannelOptions
                {
                    HttpClient = client,
                    DisposeHttpClient = false,
                });
            var health = new Health.HealthClient(channel);
            RpcException check = await Assert.ThrowsAsync<RpcException>(
                () => health.CheckAsync(
                    new HealthCheckRequest(),
                    cancellationToken: Ct).ResponseAsync);
            using AsyncServerStreamingCall<HealthCheckResponse> watch =
                health.Watch(
                    new HealthCheckRequest(),
                    deadline: DateTime.UtcNow.AddSeconds(5),
                    cancellationToken: Ct);
            RpcException watchFailure = await Assert.ThrowsAsync<RpcException>(
                () => watch.ResponseStream.MoveNext(Ct));

            var rpc = new CSharpDbRpc.CSharpDbRpcClient(channel);
            RpcException ordinary = await Assert.ThrowsAsync<RpcException>(
                () => rpc.GetInfoAsync(
                    new Empty(),
                    cancellationToken: Ct).ResponseAsync);

            Assert.Equal(StatusCode.Unimplemented, check.StatusCode);
            Assert.Equal(StatusCode.Unimplemented, watchFailure.StatusCode);
            Assert.Equal(StatusCode.Unauthenticated, ordinary.StatusCode);
        }
        finally
        {
            TryDelete(dbPath);
            TryDelete(dbPath + ".wal");
        }
    }

    private static async Task WaitUntilReadyAsync(HttpClient client)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddSeconds(20);
        while (DateTimeOffset.UtcNow < deadline)
        {
            using HttpResponseMessage response = await client.GetAsync(
                "/health/ready",
                Ct);
            if (response.StatusCode == HttpStatusCode.OK)
                return;
            await Task.Delay(25, Ct);
        }

        throw new TimeoutException("The daemon did not become ready.");
    }

    private static async Task PublishHealthAsync(
        HealthCheckService healthChecks,
        IHealthCheckPublisher publisher)
    {
        HealthReport report = await healthChecks.CheckHealthAsync(Ct);
        await publisher.PublishAsync(report, Ct);
    }

    private static async Task AssertMinimalStatusOnlyAsync(
        HttpResponseMessage response,
        string expected)
    {
        Dictionary<string, string>? body = await response.Content
            .ReadFromJsonAsync<Dictionary<string, string>>(
                cancellationToken: Ct);
        KeyValuePair<string, string> value = Assert.Single(body!);
        Assert.Equal("status", value.Key);
        Assert.Equal(expected, value.Value);
    }

    private static string NewTempDbPath()
        => Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_daemon_health_{Guid.NewGuid():N}.db");

    private sealed class HealthDaemonFactory(
        string dbPath,
        IReadOnlyDictionary<string, string?>? extraConfig = null,
        bool publishHealthManually = false) :
        WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            if (publishHealthManually)
                builder.ConfigureLogging(logging => logging.ClearProviders());
            if (extraConfig is not null)
            {
                foreach (KeyValuePair<string, string?> pair in extraConfig)
                {
                    if (pair.Value is not null)
                        builder.UseSetting(pair.Key, pair.Value);
                }
            }

            builder.ConfigureServices(services =>
            {
                services.RemoveAll<ICSharpDbClient>();
                services.AddSingleton<ICSharpDbClient>(_ =>
                    CSharpDbClient.Create(new CSharpDbClientOptions
                    {
                        Transport = CSharpDbTransport.Direct,
                        ConnectionString = $"Data Source={dbPath}",
                    }));
                services.AddHostedService<TestDaemonClientShutdown>();
                services.Configure<HealthCheckPublisherOptions>(options =>
                {
                    if (publishHealthManually)
                    {
                        // Keep the hosted publisher from racing explicit
                        // transition publications in the streaming test.
                        options.Delay = TimeSpan.FromHours(1);
                        options.Period = TimeSpan.FromHours(1);
                    }
                    else
                    {
                        options.Delay = TimeSpan.Zero;
                        options.Period = TimeSpan.FromSeconds(1);
                    }
                });
            });
            builder.ConfigureAppConfiguration((_, configuration) =>
            {
                var values = new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] =
                        $"Data Source={dbPath}",
                };
                if (extraConfig is not null)
                {
                    foreach (KeyValuePair<string, string?> pair in extraConfig)
                        values[pair.Key] = pair.Value;
                }

                configuration.AddInMemoryCollection(values);
            });
        }
    }

    private sealed class TestDaemonClientShutdown(ICSharpDbClient client) :
        IHostedService
    {
        private int _stopped;
        public Task StartAsync(CancellationToken _) => Task.CompletedTask;
        public async Task StopAsync(CancellationToken _)
        {
            if (Interlocked.Exchange(ref _stopped, 1) == 0)
                await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException)
        {
        }
    }
}

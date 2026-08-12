using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using CSharpDB.Client;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Daemon.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ActivityParentageHostTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task GrpcRequest_ParentsOneEngineQueryToInboundServerSpan()
    {
        const string secret = "grpc-trace-secret-7b2d";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-daemon-trace-parentage-{Guid.NewGuid():N}.db");
        using var activities = new FrameworkActivityRecorder();

        try
        {
            await using var factory = new ParentageDaemonFactory(databasePath);
            using var transportClient = new HttpClient(
                factory.Server.CreateHandler())
            {
                BaseAddress = new Uri("http://localhost"),
                DefaultRequestVersion = HttpVersion.Version20,
                DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact,
            };
            await using ICSharpDbClient client = CSharpDbClient.Create(
                new CSharpDbClientOptions
                {
                    Transport = CSharpDB.Client.CSharpDbTransport.Grpc,
                    Endpoint = "http://localhost",
                    HttpClient = transportClient,
                });
            activities.Clear();

            CSharpDB.Client.Models.SqlExecutionResult result =
                await client.ExecuteSqlAsync(
                    $"SELECT '{secret}' AS value",
                    Ct);
            Assert.Null(result.Error);

            Activity query = Assert.Single(
                activities.Stopped,
                static activity =>
                    activity.Source.Name == CSharpDbDiagnostics.ActivitySourceName &&
                    activity.OperationName == "csharpdb.query");
            Activity inbound = Assert.Single(
                activities.Started,
                activity => activity.Kind == ActivityKind.Server &&
                    activity.TraceId == query.TraceId &&
                    activity.SpanId == query.ParentSpanId);

            Assert.Equal(inbound.SpanId, query.ParentSpanId);
            Assert.Equal(inbound.TraceId, query.TraceId);
            Assert.Equal("grpc", Tag(query, "csharpdb.transport"));
            Assert.Equal("succeeded", Tag(query, "csharpdb.operation.outcome"));
            AssertCanariesAbsent(query, secret, databasePath);
            AssertCanariesAbsent(inbound, secret, databasePath);
        }
        finally
        {
            DeleteIfExists(databasePath);
            DeleteIfExists(databasePath + ".wal");
            DeleteIfExists(databasePath + ".manifest.json");
        }
    }

    private static string? Tag(Activity activity, string name)
        => activity.TagObjects.FirstOrDefault(
            item => string.Equals(item.Key, name, StringComparison.Ordinal)).Value
            ?.ToString();

    private static void AssertCanariesAbsent(
        Activity activity,
        params string[] canaries)
    {
        string projection = string.Join(
            "|",
            activity.TagObjects.Select(
                static item => $"{item.Key}={item.Value}"));
        foreach (string canary in canaries)
        {
            Assert.DoesNotContain(
                canary,
                projection,
                StringComparison.OrdinalIgnoreCase);
        }
    }

    private static void DeleteIfExists(string path)
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

    private sealed class ParentageDaemonFactory(string databasePath) :
        WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.UseSetting("CSharpDB:Observability:Enabled", "true");
            builder.UseSetting(
                "CSharpDB:Observability:Logging:Enabled",
                "false");
            builder.UseSetting(
                "CSharpDB:Observability:OpenTelemetry:Enabled",
                "true");
            builder.UseSetting(
                "CSharpDB:Observability:OpenTelemetry:SamplingRatio",
                "1");
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(
                    new Dictionary<string, string?>
                    {
                        ["ConnectionStrings:CSharpDB"] =
                            $"Data Source={databasePath}",
                        ["CSharpDB:Daemon:Security:Mode"] = "None",
                        ["CSharpDB:Observability:Enabled"] = "true",
                        ["CSharpDB:Observability:Logging:Enabled"] = "false",
                        ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
                        ["CSharpDB:Observability:OpenTelemetry:SamplingRatio"] = "1",
                    }));
            builder.ConfigureServices(services =>
            {
                services.AddHostedService<TestDaemonClientShutdown>();
            });
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

    private sealed class FrameworkActivityRecorder : IDisposable
    {
        private readonly ConcurrentQueue<Activity> _started = new();
        private readonly ConcurrentQueue<Activity> _stopped = new();
        private readonly ActivityListener _listener;

        internal FrameworkActivityRecorder()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = static source =>
                    source.Name == CSharpDbDiagnostics.ActivitySourceName ||
                    source.Name.StartsWith(
                        "Microsoft.AspNetCore",
                        StringComparison.Ordinal),
                Sample = static (
                    ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = static (
                    ref ActivityCreationOptions<string> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStarted = activity => _started.Enqueue(activity),
                ActivityStopped = activity => _stopped.Enqueue(activity),
            };
            ActivitySource.AddActivityListener(_listener);
        }

        internal Activity[] Started => _started.ToArray();

        internal Activity[] Stopped => _stopped.ToArray();

        internal void Clear()
        {
            _started.Clear();
            _stopped.Clear();
        }

        public void Dispose() => _listener.Dispose();
    }
}

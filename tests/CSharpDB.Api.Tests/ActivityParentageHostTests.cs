using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http.Json;
using CSharpDB.Api.Dtos;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Api.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ActivityParentageHostTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RestRequest_ParentsOneEngineQueryToInboundServerSpan()
    {
        const string secret = "rest-trace-secret-9f1c";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-trace-parentage-{Guid.NewGuid():N}.db");
        using var activities = new FrameworkActivityRecorder();

        try
        {
            await using var factory = new ParentageApiFactory(databasePath);
            using HttpClient client = factory.CreateClient();
            activities.Clear();

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/api/sql/execute",
                new ExecuteSqlRequest($"SELECT '{secret}' AS value"),
                Ct);
            response.EnsureSuccessStatusCode();

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
            Assert.Equal("http", Tag(query, "csharpdb.transport"));
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

    private sealed class ParentageApiFactory(string databasePath) :
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
                        ["CSharpDB:Observability:Enabled"] = "true",
                        ["CSharpDB:Observability:Logging:Enabled"] = "false",
                        ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
                        ["CSharpDB:Observability:OpenTelemetry:SamplingRatio"] = "1",
                    }));
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

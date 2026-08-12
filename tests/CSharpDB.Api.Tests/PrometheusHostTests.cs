using System.Collections.Concurrent;
using System.Net;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;

namespace CSharpDB.Api.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class PrometheusHostTests
{
    private const string ApiKey = "prometheus-test-key";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisabledExporter_IsNotRegisteredAndEndpointIsNotMapped(
        bool observabilityEnabled)
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: false,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            },
            observabilityEnabled: observabilityEnabled);
        using HttpClient client = app.GetTestClient();

        using HttpResponseMessage response = await client.GetAsync("/metrics", Ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Null(app.Services.GetService<MeterProvider>());
        Assert.Null(app.Services.GetService<TracerProvider>());
        Assert.DoesNotContain(
            app.Services.GetServices<IHostedService>(),
            static service => service.GetType().Assembly.GetName().Name
                ?.StartsWith("OpenTelemetry", StringComparison.Ordinal) == true);
    }

    [Fact]
    public async Task ApiKeyMode_RequiresTheConfiguredKey()
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            });
        using HttpClient client = app.GetTestClient();

        using HttpResponseMessage missing = await client.GetAsync("/metrics", Ct);
        using var wrongRequest = new HttpRequestMessage(HttpMethod.Get, "/metrics");
        wrongRequest.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            "wrong");
        using HttpResponseMessage wrong = await client.SendAsync(wrongRequest, Ct);
        using var validRequest = new HttpRequestMessage(HttpMethod.Get, "/metrics");
        validRequest.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        using HttpResponseMessage valid = await client.SendAsync(validRequest, Ct);

        Assert.Equal(HttpStatusCode.Unauthorized, missing.StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, wrong.StatusCode);
        Assert.Equal(HttpStatusCode.OK, valid.StatusCode);
    }

    [Theory]
    [InlineData("127.0.0.1", false, HttpStatusCode.OK)]
    [InlineData("::1", false, HttpStatusCode.OK)]
    [InlineData("203.0.113.42", false, HttpStatusCode.Forbidden)]
    [InlineData("203.0.113.42", true, HttpStatusCode.OK)]
    public async Task NoneMode_UsesTheActualPeerAddress(
        string remoteAddress,
        bool allowInsecureRemote,
        HttpStatusCode expectedStatus)
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
                security.Mode = CSharpDbRemoteSecurityMode.None,
            allowInsecureRemote: allowInsecureRemote,
            remoteAddress: IPAddress.Parse(remoteAddress));
        using HttpClient client = app.GetTestClient();
        client.DefaultRequestHeaders.TryAddWithoutValidation(
            "X-Forwarded-For",
            "127.0.0.1");

        using HttpResponseMessage response = await client.GetAsync("/metrics", Ct);

        Assert.Equal(expectedStatus, response.StatusCode);
    }

    [Fact]
    public async Task NoneMode_FailsClosedWhenThePeerCannotBeProven()
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
                security.Mode = CSharpDbRemoteSecurityMode.None,
            remoteAddress: IPAddress.Any);
        using HttpClient client = app.GetTestClient();

        using HttpResponseMessage response = await client.GetAsync("/metrics", Ct);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task CustomPathIsProtected_DefaultPathIsNotMapped_AndSuffixIsNotShadowed()
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            },
            prometheusPath: "/internal/metrics",
            configureAppBeforePrometheus: static app =>
                app.MapGet("/internal/metrics/status", () => "ordinary-route"));
        using HttpClient client = app.GetTestClient();
        using var scrapeRequest = new HttpRequestMessage(
            HttpMethod.Get,
            "/internal/metrics");
        scrapeRequest.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);

        using HttpResponseMessage unauthorized = await client.GetAsync(
            "/internal/metrics",
            Ct);
        using HttpResponseMessage scrape = await client.SendAsync(scrapeRequest, Ct);
        using HttpResponseMessage defaultPath = await client.GetAsync("/metrics", Ct);
        using HttpResponseMessage trailingSlash = await client.GetAsync(
            "/internal/metrics/",
            Ct);
        using HttpResponseMessage suffix = await client.GetAsync(
            "/internal/metrics/status",
            Ct);

        Assert.Equal(HttpStatusCode.Unauthorized, unauthorized.StatusCode);
        Assert.Equal(HttpStatusCode.OK, scrape.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, defaultPath.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, trailingSlash.StatusCode);
        Assert.Equal(HttpStatusCode.OK, suffix.StatusCode);
        Assert.Equal("ordinary-route", await suffix.Content.ReadAsStringAsync(Ct));
        Assert.NotNull(app.Services.GetService<MeterProvider>());
        Assert.Null(app.Services.GetService<TracerProvider>());
    }

    [Fact]
    public async Task ScrapeBodyUsesStableSchemaAndClosedLabelSet()
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            });

        await RecordQueriesAsync(app, "SchemaCanary", count: 1);
        string body = await ScrapeAsync(app);

        Assert.Contains("# TYPE csharpdb_requests", body, StringComparison.Ordinal);
        Assert.Contains("# TYPE csharpdb_query_duration", body, StringComparison.Ordinal);
        Assert.Contains(
            "csharpdb_database_alias=\"prometheus-tests\"",
            body,
            StringComparison.Ordinal);
        Assert.Contains(
            "csharpdb_transport=\"direct\"",
            body,
            StringComparison.Ordinal);
        AssertClosedCSharpDbLabelSet(body);
    }

    [Fact]
    public async Task ConcurrentScrapesReturnCompleteCSharpDbSchema()
    {
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            });
        await RecordQueriesAsync(app, "ConcurrentCanary", count: 1);
        using HttpClient client = CreateAuthorizedClient(app);

        Task<string>[] scrapes = Enumerable.Range(0, 8)
            .Select(_ => client.GetStringAsync("/metrics", Ct))
            .ToArray();
        string[] bodies = await Task.WhenAll(scrapes);

        Assert.All(
            bodies,
            body =>
            {
                Assert.Contains(
                    "# TYPE csharpdb_requests",
                    body,
                    StringComparison.Ordinal);
                AssertClosedCSharpDbLabelSet(body);
            });
    }

    [Fact]
    public async Task DistinctQueriesSessionsAndPathsStayBoundedAndPrivate()
    {
        const string secret = "AllAlphaPrometheusSecretCanary";
        const string pathCanary = "PrometheusPrivatePathCanary";
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            });

        await RecordAdversarialQueriesAsync(
            app,
            secret,
            pathCanary,
            count: 32);
        string body = await ScrapeAsync(app);
        string[] requestSeries = GetCSharpDbSampleLines(body)
            .Where(static line => line.StartsWith(
                "csharpdb_requests",
                StringComparison.Ordinal))
            .ToArray();

        Assert.Single(requestSeries);
        Assert.DoesNotContain(secret, body, StringComparison.Ordinal);
        Assert.DoesNotContain(pathCanary, body, StringComparison.Ordinal);
        Assert.DoesNotContain(
            "csharpdb_query_fingerprint=",
            body,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "csharpdb_session_id=",
            body,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "csharpdb_path=",
            body,
            StringComparison.Ordinal);
        Assert.DoesNotContain("trace_id=", body, StringComparison.Ordinal);
        Assert.DoesNotContain("span_id=", body, StringComparison.Ordinal);
        AssertClosedCSharpDbLabelSet(body);
    }

    [Fact]
    public async Task InsecureRemoteOverride_LogsOneStartupWarning()
    {
        var loggerProvider = new CapturingLoggerProvider();
        await using WebApplication app = await StartAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
                security.Mode = CSharpDbRemoteSecurityMode.None,
            allowInsecureRemote: true,
            remoteAddress: IPAddress.Parse("203.0.113.42"),
            loggerProvider: loggerProvider);

        LogEntry warning = Assert.Single(
            loggerProvider.Entries,
            entry => entry.EventId.Id == 7002);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("shared Kestrel listener", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PrometheusPathCannotCollideWithRestOrExistingEndpoints()
    {
        await using WebApplication restCollision = await CreateAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            },
            prometheusPath: "/api/info");
        restCollision.MapCSharpDbRestApi(options =>
            options.MapDevelopmentOpenApi = false);
        Assert.Throws<InvalidOperationException>(
            () => restCollision.MapCSharpDbPrometheusEndpoint());

        await using WebApplication exactCollision = await CreateAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            },
            prometheusPath: "/custom-metrics");
        exactCollision.MapGet("/custom-metrics", () => "collision");
        Assert.Throws<InvalidOperationException>(
            () => exactCollision.MapCSharpDbPrometheusEndpoint());

        await using WebApplication reverseOrder = await CreateAppAsync(
            prometheusEnabled: true,
            configureSecurity: static security =>
            {
                security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                security.ApiKey = ApiKey;
            },
            prometheusPath: "/api/info");
        reverseOrder.MapCSharpDbPrometheusEndpoint();
        Assert.Throws<InvalidOperationException>(
            () => reverseOrder.MapCSharpDbRestApi(options =>
                options.MapDevelopmentOpenApi = false));
    }

    private static async Task<WebApplication> StartAppAsync(
        bool prometheusEnabled,
        Action<CSharpDbApiSecurityOptions> configureSecurity,
        string prometheusPath = "/metrics",
        bool allowInsecureRemote = false,
        IPAddress? remoteAddress = null,
        Action<WebApplication>? configureAppBeforePrometheus = null,
        ILoggerProvider? loggerProvider = null,
        bool? observabilityEnabled = null)
    {
        WebApplication app = await CreateAppAsync(
            prometheusEnabled,
            configureSecurity,
            prometheusPath,
            allowInsecureRemote,
            remoteAddress,
            loggerProvider,
            observabilityEnabled);
        app.UseCSharpDbObservability();
        configureAppBeforePrometheus?.Invoke(app);
        app.MapCSharpDbPrometheusEndpoint();
        await app.StartAsync(Ct);
        return app;
    }

    private static Task<WebApplication> CreateAppAsync(
        bool prometheusEnabled,
        Action<CSharpDbApiSecurityOptions> configureSecurity,
        string prometheusPath = "/metrics",
        bool allowInsecureRemote = false,
        IPAddress? remoteAddress = null,
        ILoggerProvider? loggerProvider = null,
        bool? observabilityEnabled = null)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        if (loggerProvider is not null)
            builder.Logging.AddProvider(loggerProvider);
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] =
                    (observabilityEnabled ?? prometheusEnabled)
                        ? "true"
                        : "false",
                ["CSharpDB:Observability:DatabaseAlias"] =
                    "prometheus-tests",
                ["CSharpDB:Observability:Prometheus:Enabled"] =
                    prometheusEnabled ? "true" : "false",
                ["CSharpDB:Observability:Prometheus:Path"] = prometheusPath,
                ["CSharpDB:Observability:Prometheus:AllowInsecureRemoteAccess"] =
                    allowInsecureRemote ? "true" : "false",
            });
        builder.Services.AddCSharpDbObservability(
            builder.Configuration,
            "prometheus-tests",
            builder.Environment.EnvironmentName);
        builder.Services.AddCSharpDbRestApi(configureSecurity);

        WebApplication app = builder.Build();
        if (remoteAddress is not null)
        {
            app.Use((context, next) =>
            {
                context.Connection.RemoteIpAddress = remoteAddress;
                return next(context);
            });
        }

        return Task.FromResult(app);
    }

    private static async Task RecordQueriesAsync(
        WebApplication app,
        string sqlCanary,
        int count)
    {
        CSharpDbObservabilityOptions observabilityOptions = app.Services
            .GetRequiredService<CSharpDbObservabilityOptions>();
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Direct,
                ConnectionString = "Data Source=:memory:",
                DirectDatabaseOptions = new DatabaseOptions
                {
                    ObservabilityOptions = observabilityOptions,
                },
            });

        for (int index = 0; index < count; index++)
        {
            var result = await client.ExecuteSqlAsync(
                $"SELECT '{sqlCanary}{index}' AS value",
                Ct);
            Assert.Null(result.Error);
        }
    }

    private static async Task RecordAdversarialQueriesAsync(
        WebApplication app,
        string sqlCanary,
        string pathCanary,
        int count)
    {
        CSharpDbObservabilityOptions observabilityOptions = app.Services
            .GetRequiredService<CSharpDbObservabilityOptions>();

        for (int index = 0; index < count; index++)
        {
            string databasePath = Path.Combine(
                Path.GetTempPath(),
                $"{pathCanary}-{index}-{Guid.NewGuid():N}.db");
            try
            {
                await using ICSharpDbClient client = CSharpDbClient.Create(
                    new CSharpDbClientOptions
                    {
                        Transport = ClientTransport.Direct,
                        ConnectionString = $"Data Source={databasePath}",
                        DirectDatabaseOptions = new DatabaseOptions
                        {
                            ObservabilityOptions = observabilityOptions,
                        },
                    });
                using IDisposable sessionScope =
                    CSharpDbOperationScope.EnterTransport(
                        CSharpDB.Observability.CSharpDbTransport.Direct,
                        OpaqueDiagnosticsId.Create());
                var result = await client.ExecuteSqlAsync(
                    $"SELECT '{sqlCanary}{index}\\{pathCanary}{index}' AS value",
                    Ct);
                Assert.Null(result.Error);
            }
            finally
            {
                DeleteIfExists(databasePath);
                DeleteIfExists(databasePath + ".wal");
                DeleteIfExists(databasePath + "-wal");
                DeleteIfExists(databasePath + "-shm");
                DeleteIfExists(databasePath + ".manifest.json");
            }
        }
    }

    private static async Task<string> ScrapeAsync(WebApplication app)
    {
        using HttpClient client = CreateAuthorizedClient(app);
        return await client.GetStringAsync("/metrics", Ct);
    }

    private static HttpClient CreateAuthorizedClient(WebApplication app)
    {
        HttpClient client = app.GetTestClient();
        client.DefaultRequestHeaders.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        return client;
    }

    private static IEnumerable<string> GetCSharpDbSampleLines(string body)
        => body.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(static line =>
                !line.StartsWith('#') &&
                line.StartsWith("csharpdb_", StringComparison.Ordinal));

    private static void AssertClosedCSharpDbLabelSet(string body)
    {
        string[] allowedLabels =
        [
            "csharpdb_database_alias",
            "csharpdb_operation_class",
            "csharpdb_operation_outcome",
            "csharpdb_transport",
            "le",
        ];

        foreach (string line in GetCSharpDbSampleLines(body))
        {
            int openBrace = line.IndexOf('{');
            if (openBrace < 0)
                continue;

            int closeBrace = line.IndexOf('}', openBrace + 1);
            Assert.True(closeBrace > openBrace, $"Malformed metric line: {line}");
            string labels = line[(openBrace + 1)..closeBrace];
            foreach (string label in labels.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                int equals = label.IndexOf('=');
                Assert.True(equals > 0, $"Malformed metric label: {label}");
                Assert.Contains(label[..equals], allowedLabels);
            }
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

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

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        internal ConcurrentQueue<LogEntry> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) =>
            new CapturingLogger(Entries);

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => entries.Enqueue(new LogEntry(
                logLevel,
                eventId,
                formatter(state, exception)));
    }

    private sealed record LogEntry(
        LogLevel Level,
        EventId EventId,
        string Message);
}

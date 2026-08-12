using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Http.Json;
using CSharpDB.Api.Dtos;
using CSharpDB.Api.Middleware;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Api.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class ObservabilityHostTests
{
    [Fact]
    public async Task RestExtensionsWithoutObservabilityRegistration_ServeRequests()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Testing",
            });
        builder.WebHost.UseTestServer();
        builder.Services.AddCSharpDbRestApi();
        builder.Services.AddSingleton<ICSharpDbClient>(_ =>
            CSharpDbClient.Create(new CSharpDbClientOptions
            {
                Transport = CSharpDB.Client.CSharpDbTransport.Direct,
                ConnectionString = "Data Source=:memory:",
            }));

        await using WebApplication app = builder.Build();
        app.MapCSharpDbRestApi();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using HttpClient client = app.GetTestClient();
        using HttpResponseMessage response = await client.GetAsync(
            "/api/not-a-real-route",
            TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public void DisabledHttpScopeReturnsContinuationTaskWithoutAllocatingScope()
    {
        Task continuationTask = Task.CompletedTask;
        int invocationCount = 0;
        ObservabilityTransport observedTransport = ObservabilityTransport.Unknown;
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                invocationCount++;
                observedTransport = CSharpDbOperationScope.CurrentTransport;
                return continuationTask;
            },
            new CSharpDbObservabilityOptions());
        var context = new DefaultHttpContext();

        Task warmup = middleware.InvokeAsync(context);
        Assert.Same(continuationTask, warmup);

        long before = GC.GetAllocatedBytesForCurrentThread();
        Task? returnedTask = null;
        for (int index = 0; index < 256; index++)
            returnedTask = middleware.InvokeAsync(context);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Same(continuationTask, returnedTask);
        Assert.Equal(257, invocationCount);
        Assert.Equal(ObservabilityTransport.Embedded, observedTransport);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void LegacyNullOptionsConstructor_RemainsSourceCompatible()
    {
        Task continuationTask = Task.CompletedTask;
        RequestDelegate next = _ => continuationTask;

        var middleware = new CSharpDbOperationScopeMiddleware(next, null);

        Assert.Same(
            continuationTask,
            middleware.InvokeAsync(new DefaultHttpContext()));
    }

    [Fact]
    public void InvalidConfigurationFailsBeforeDatabaseWarmup()
    {
        IConfiguration configuration = BuildConfiguration(new Dictionary<string, string?>
        {
            ["CSharpDB:Observability:DatabaseAlias"] = @"C:\private\database.db",
        });

        CSharpDbObservabilityOptionsValidationException error = Assert.Throws<
            CSharpDbObservabilityOptionsValidationException>(
            () =>
            {
                var services = new ServiceCollection();
                services.AddSingleton<ILoggerFactory>(
                    new CapturingLoggerFactory());
                services.AddCSharpDbObservability(configuration);
            });

        Assert.Contains(error.Errors, message => message.Contains("DatabaseAlias", StringComparison.Ordinal));
    }

    [Fact]
    public void PreRegisteredOptionsInstanceControlsTelemetryProviderShape()
    {
        IConfiguration configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
                ["CSharpDB:Observability:Prometheus:Enabled"] = "true",
            });
        var authoritativeOptions = new CSharpDbObservabilityOptions();
        var services = new ServiceCollection();
        services.AddSingleton(authoritativeOptions);

        services.AddCSharpDbObservability(configuration);

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Same(
            authoritativeOptions,
            provider.GetRequiredService<CSharpDbObservabilityOptions>());
        Assert.Null(provider.GetService<TracerProvider>());
        Assert.Null(provider.GetService<MeterProvider>());
    }

    [Fact]
    public void RepeatedRegistrationPreservesFirstOptionsAndServiceShape()
    {
        IConfiguration firstConfiguration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
            });
        IConfiguration ignoredConfiguration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:DatabaseAlias"] =
                    @"C:\private\ignored.db",
                ["CSharpDB:Observability:Prometheus:Enabled"] = "true",
            });
        var services = new ServiceCollection();

        IServiceCollection firstResult = services.AddCSharpDbObservability(
            firstConfiguration,
            "CSharpDB.Api",
            "Testing");
        int descriptorCount = services.Count;
        CSharpDbObservabilityOptions firstOptions = Assert.IsType<
            CSharpDbObservabilityOptions>(
            Assert.Single(services, static descriptor =>
                descriptor.ServiceType ==
                    typeof(CSharpDbObservabilityOptions))
                .ImplementationInstance);

        IServiceCollection secondResult = services.AddCSharpDbObservability(
            ignoredConfiguration,
            "ignored-service",
            "ignored-environment");

        Assert.Same(services, firstResult);
        Assert.Same(services, secondResult);
        Assert.Equal(descriptorCount, services.Count);
        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Same(
            firstOptions,
            provider.GetRequiredService<CSharpDbObservabilityOptions>());
        Assert.NotNull(provider.GetService<TracerProvider>());
        Assert.NotNull(provider.GetService<MeterProvider>());
    }

    [Fact]
    public void FactoryRegisteredOptionsPreserveLegacyCoreWithoutTelemetryWiring()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CSharpDbObservabilityOptions>(
            static _ => new CSharpDbObservabilityOptions());

        services.AddCSharpDbObservability(
            BuildConfiguration(new Dictionary<string, string?>()));

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.NotNull(
            provider.GetRequiredService<CSharpDbObservabilityOptions>());
        Assert.False(provider
            .GetRequiredService<CSharpDbObservabilityRegistrationMarker>()
            .HostedExportersConfigured);
        Assert.Null(provider.GetService<TracerProvider>());
        Assert.Null(provider.GetService<MeterProvider>());
    }

    [Fact]
    public async Task FactoryRegisteredPrometheusOptionsFailClearlyAtMap()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton<CSharpDbObservabilityOptions>(
            static _ => new CSharpDbObservabilityOptions
            {
                Enabled = true,
                Prometheus = new CSharpDbPrometheusOptions
                {
                    Enabled = true,
                },
            });
        builder.Services.AddCSharpDbObservability(builder.Configuration);

        await using WebApplication app = builder.Build();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => app.MapCSharpDbPrometheusEndpoint());

        Assert.Contains(
            "factory or type registration",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FactoryRegisteredOpenTelemetryOptionsFailClearlyAtStart()
    {
        var services = new ServiceCollection();
        services.AddSingleton<CSharpDbObservabilityOptions>(
            static _ => new CSharpDbObservabilityOptions
            {
                Enabled = true,
                OpenTelemetry = new CSharpDbOpenTelemetryOptions
                {
                    Enabled = true,
                },
            });
        services.AddCSharpDbObservability(
            BuildConfiguration(new Dictionary<string, string?>()));

        using ServiceProvider provider = services.BuildServiceProvider();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => provider.StartCSharpDbObservability());

        Assert.Contains(
            "factory or type registration",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OptionsReplacementAfterRegistrationFailsBeforeProviderStartup()
    {
        IConfiguration configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
            });
        var services = new ServiceCollection();
        services.AddCSharpDbObservability(configuration);
        services.AddSingleton(new CSharpDbObservabilityOptions());

        using ServiceProvider provider = services.BuildServiceProvider();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => provider.StartCSharpDbObservability());

        Assert.Contains(
            "registered before",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OptionsMutationAfterRegistrationFailsBeforeProviderStartup()
    {
        var options = new CSharpDbObservabilityOptions();
        var services = new ServiceCollection();
        services.AddSingleton(options);
        services.AddCSharpDbObservability(
            BuildConfiguration(new Dictionary<string, string?>()));
        options.Enabled = true;
        options.OpenTelemetry.Enabled = true;

        using ServiceProvider provider = services.BuildServiceProvider();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => provider.StartCSharpDbObservability());

        Assert.Contains(
            "must not be replaced or mutated",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PrometheusMapRejectsMutationEvenWhenItDisablesTheRoute()
    {
        var options = new CSharpDbObservabilityOptions
        {
            Enabled = true,
            Prometheus = new CSharpDbPrometheusOptions
            {
                Enabled = true,
            },
        };
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton(options);
        builder.Services.AddCSharpDbObservability(builder.Configuration);
        options.Prometheus.Enabled = false;

        await using WebApplication app = builder.Build();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            () => app.MapCSharpDbPrometheusEndpoint());

        Assert.Contains(
            "must not be replaced or mutated",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void KeyedOptionsDoNotOverrideTheUnkeyedHostSnapshot()
    {
        IConfiguration configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:DatabaseAlias"] = "unkeyed",
            });
        var keyed = new CSharpDbObservabilityOptions
        {
            DatabaseAlias = "keyed",
        };
        var services = new ServiceCollection();
        services.AddKeyedSingleton("canary", keyed);

        services.AddCSharpDbObservability(configuration);

        using ServiceProvider provider = services.BuildServiceProvider();
        Assert.Equal(
            "unkeyed",
            provider.GetRequiredService<CSharpDbObservabilityOptions>()
                .DatabaseAlias);
        Assert.Same(
            keyed,
            provider.GetRequiredKeyedService<CSharpDbObservabilityOptions>(
                "canary"));
    }

    [Fact]
    public void HostedTelemetryUsesSafeResourceDefaults()
    {
        CSharpDbTelemetryResourceIdentity identity =
            CSharpDbTelemetryExporterHostExtensions.ResolveResourceIdentity(
                new CSharpDbOpenTelemetryResourceOptions(),
                "CSharpDB.Api",
                "Testing");

        Assert.Equal("CSharpDB.Api", identity.ServiceName);
        Assert.Equal("CSharpDB", identity.ServiceNamespace);
        Assert.False(string.IsNullOrWhiteSpace(identity.ServiceVersion));
        Assert.True(CSharpDbDiagnostics.IsValidOpaqueIdentifier(
            identity.ServiceInstanceId));
        Assert.Equal("Testing", identity.DeploymentEnvironment);
    }

    [Fact]
    public void UnsafeHostResourceDefaultsFallBackToBoundedValues()
    {
        string oversized = new('x', 129);
        (string? ServiceName, string? Environment)[] unsafeDefaults =
        [
            (null, null),
            ("", " "),
            (" CSharpDB.Api", "Testing "),
            (@"C:\private\service", "Testing\nCanary"),
            (oversized, oversized),
        ];

        foreach ((string? serviceName, string? environment) in unsafeDefaults)
        {
            CSharpDbTelemetryResourceIdentity identity =
                CSharpDbTelemetryExporterHostExtensions.ResolveResourceIdentity(
                    new CSharpDbOpenTelemetryResourceOptions(),
                    serviceName,
                    environment);

            Assert.Equal("CSharpDB", identity.ServiceName);
            Assert.Equal("Production", identity.DeploymentEnvironment);
        }
    }

    [Fact]
    public void HostedTelemetrySamplerHonorsRatioAndParentDecision()
    {
        Sampler neverSample =
            CSharpDbTelemetryExporterHostExtensions.CreateSampler(0);
        Sampler alwaysSample =
            CSharpDbTelemetryExporterHostExtensions.CreateSampler(1);
        var root = CreateSamplingParameters(default);
        var sampledParent = CreateSamplingParameters(new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded,
            traceState: null,
            isRemote: true));
        var unsampledParent = CreateSamplingParameters(new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.None,
            traceState: null,
            isRemote: true));

        Assert.Equal(SamplingDecision.Drop, neverSample.ShouldSample(root).Decision);
        Assert.Equal(
            SamplingDecision.RecordAndSample,
            alwaysSample.ShouldSample(root).Decision);
        Assert.Equal(
            SamplingDecision.RecordAndSample,
            neverSample.ShouldSample(sampledParent).Decision);
        Assert.Equal(
            SamplingDecision.Drop,
            alwaysSample.ShouldSample(unsampledParent).Decision);
    }

    [Fact]
    public async Task HostedTelemetryProvidersStartBeforeHostAndExportConfiguredResource()
    {
        var exportedActivities = new List<Activity>();
        var exportedMetrics = new List<Metric>();
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:SamplingRatio"] = "1",
            });
        builder.Services.AddCSharpDbObservability(
            builder.Configuration,
            "CSharpDB.Api",
            builder.Environment.EnvironmentName);
        builder.Services.AddOpenTelemetry()
            .WithTracing(tracing =>
                tracing.AddInMemoryExporter(exportedActivities))
            .WithMetrics(metrics =>
                metrics.AddInMemoryExporter(exportedMetrics));

        await using WebApplication app = builder.Build();
        app.UseCSharpDbObservability();

        using (Activity? activity = CSharpDbDiagnostics.ActivitySource.StartActivity(
                   "host-exporter-canary",
                   ActivityKind.Internal))
        {
            Assert.NotNull(activity);
            Assert.Equal("1.0.0", activity.Source.Version);
        }

        using (var meter = new Meter(CSharpDbDiagnostics.MeterName, "1.0.0"))
        {
            Counter<long> counter = meter.CreateCounter<long>(
                "csharpdb.host.warmup.canary");
            counter.Add(1);
            Assert.True(app.Services.GetRequiredService<MeterProvider>().ForceFlush());
        }

        Activity exported = Assert.Single(
            exportedActivities,
            activity => activity.OperationName == "host-exporter-canary");
        Assert.Equal(CSharpDbDiagnostics.ActivitySourceName, exported.Source.Name);
        Assert.Contains(
            exportedMetrics,
            metric => metric.Name == "csharpdb.host.warmup.canary");

        TracerProvider tracerProvider =
            app.Services.GetRequiredService<TracerProvider>();
        Assert.Contains(
            tracerProvider.GetResource().Attributes,
            attribute => attribute.Key == "service.name" &&
                         Equals(attribute.Value, "CSharpDB.Api"));
        Assert.Contains(
            tracerProvider.GetResource().Attributes,
            attribute => attribute.Key == "deployment.environment.name" &&
                         Equals(attribute.Value, "Testing"));

        await app.StartAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task HostedDatabaseQueryExportsParentedTraceAndMetricSchema()
    {
        const string secret = "HostedExporterSecretCanary";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-exporter-{Guid.NewGuid():N}.db");
        var exportedActivities = new List<Activity>();
        var exportedMetrics = new List<Metric>();

        try
        {
            await using var factory = new ExporterApiFactory(
                databasePath,
                exportedActivities,
                exportedMetrics);
            using HttpClient client = factory.CreateClient();
            exportedActivities.Clear();
            exportedMetrics.Clear();

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/api/sql/execute",
                new ExecuteSqlRequest($"SELECT '{secret}' AS value"),
                TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();

            TracerProvider tracerProvider =
                factory.Services.GetRequiredService<TracerProvider>();
            MeterProvider meterProvider =
                factory.Services.GetRequiredService<MeterProvider>();
            Assert.True(tracerProvider.ForceFlush());
            Assert.True(meterProvider.ForceFlush());

            Activity query = Assert.Single(
                exportedActivities,
                static activity =>
                    activity.Source.Name ==
                        CSharpDbDiagnostics.ActivitySourceName &&
                    activity.OperationName == "csharpdb.query");
            Activity inbound = Assert.Single(
                exportedActivities,
                activity => activity.Kind == ActivityKind.Server &&
                    activity.TraceId == query.TraceId &&
                    activity.SpanId == query.ParentSpanId);

            Assert.Equal(ActivityKind.Internal, query.Kind);
            Assert.Equal(ActivityStatusCode.Unset, query.Status);
            Assert.Equal(inbound.TraceId, query.TraceId);
            Assert.Equal(inbound.SpanId, query.ParentSpanId);
            Assert.Equal("query", ActivityTag(query, "csharpdb.operation.class"));
            Assert.Equal("succeeded", ActivityTag(query, "csharpdb.operation.outcome"));
            Assert.Equal("http", ActivityTag(query, "csharpdb.transport"));
            Assert.Equal(
                "api-exporter-test",
                ActivityTag(query, "csharpdb.database.alias"));
            Assert.Equal("1.0.0", query.Source.Version);

            Metric requests = Assert.Single(
                exportedMetrics,
                static metric => metric.Name ==
                    CSharpDbMetricInstrumentNames.Requests);
            Metric queryDuration = Assert.Single(
                exportedMetrics,
                static metric => metric.Name ==
                    CSharpDbMetricInstrumentNames.QueryDuration);
            Assert.Equal(MetricType.LongSum, requests.MetricType);
            Assert.Equal(CSharpDbMetricUnits.Request, requests.Unit);
            Assert.Equal(CSharpDbDiagnostics.MeterName, requests.MeterName);
            Assert.Equal(CSharpDbDiagnostics.InstrumentationVersion, requests.MeterVersion);
            Assert.Equal(MetricType.Histogram, queryDuration.MetricType);
            Assert.Equal(CSharpDbMetricUnits.Seconds, queryDuration.Unit);
            AssertMetricPointHasQueryTags(requests, "api-exporter-test");
            AssertMetricPointHasQueryTags(queryDuration, "api-exporter-test");
            AssertExplicitHistogramBoundaries(
                queryDuration,
                CSharpDbTelemetryExporterHostExtensions
                    .DurationHistogramBoundariesSeconds);

            AssertResourceIdentity(tracerProvider, "CSharpDB.Api", "Testing");
            AssertResourceIdentity(meterProvider, "CSharpDB.Api", "Testing");

            string activityProjection = string.Join(
                "|",
                query.TagObjects.Select(
                    static tag => $"{tag.Key}={tag.Value}"));
            string metricProjection = string.Join(
                "|",
                exportedMetrics.SelectMany(static metric =>
                    MetricTagProjection(metric)));
            Assert.DoesNotContain(secret, activityProjection, StringComparison.Ordinal);
            Assert.DoesNotContain(databasePath, activityProjection, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(secret, metricProjection, StringComparison.Ordinal);
            Assert.DoesNotContain(databasePath, metricProjection, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
            TryDelete(databasePath + "-wal");
            TryDelete(databasePath + "-shm");
            TryDelete(databasePath + ".manifest.json");
        }
    }

    [Fact]
    public async Task HostedTelemetryAppliesRecommendedHistogramViews()
    {
        string[] durationNames =
        [
            CSharpDbMetricInstrumentNames.QueryDuration,
            CSharpDbMetricInstrumentNames.TransactionDuration,
            CSharpDbMetricInstrumentNames.MaintenanceDuration,
            CSharpDbMetricInstrumentNames.CheckpointDuration,
            CSharpDbMetricInstrumentNames.WalRecoveryDuration,
            CSharpDbMetricInstrumentNames.PoolWaitDuration,
        ];
        var exportedMetrics = new List<Metric>();
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
            });
        builder.Services.AddCSharpDbObservability(
            builder.Configuration,
            "CSharpDB.Api",
            builder.Environment.EnvironmentName);
        builder.Services.AddOpenTelemetry()
            .WithMetrics(metrics =>
                metrics.AddInMemoryExporter(exportedMetrics));

        await using WebApplication app = builder.Build();
        app.UseCSharpDbObservability();

        using (var meter = new Meter(
                   CSharpDbDiagnostics.MeterName,
                   CSharpDbDiagnostics.InstrumentationVersion))
        {
            foreach (string durationName in durationNames)
            {
                meter.CreateHistogram<double>(
                        durationName,
                        CSharpDbMetricUnits.Seconds)
                    .Record(0.125);
            }

            meter.CreateHistogram<long>(
                    CSharpDbMetricInstrumentNames.WalCommitBatchSize,
                    CSharpDbMetricUnits.Commit)
                .Record(7);
            Assert.True(app.Services
                .GetRequiredService<MeterProvider>()
                .ForceFlush());
        }

        foreach (string durationName in durationNames)
        {
            Metric duration = Assert.Single(
                exportedMetrics,
                metric => metric.Name == durationName);
            AssertExplicitHistogramBoundaries(
                duration,
                CSharpDbTelemetryExporterHostExtensions
                    .DurationHistogramBoundariesSeconds);
        }

        Metric batchSize = Assert.Single(
            exportedMetrics,
            static metric => metric.Name ==
                CSharpDbMetricInstrumentNames.WalCommitBatchSize);
        AssertExplicitHistogramBoundaries(
            batchSize,
            CSharpDbTelemetryExporterHostExtensions
                .WalCommitBatchSizeBoundaries);
    }

    [Fact]
    public async Task UnavailableOtlpCollectorDoesNotPreventHostStartup()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Enabled"] = "true",
                ["CSharpDB:Observability:OpenTelemetry:Otlp:Enabled"] = "true",
                ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:1",
            });
        builder.Services.AddCSharpDbObservability(
            builder.Configuration,
            "CSharpDB.Api",
            builder.Environment.EnvironmentName);

        await using WebApplication app = builder.Build();

        Exception? activationError = Record.Exception(
            () => app.UseCSharpDbObservability());
        Assert.Null(activationError);

        Exception? error = await Record.ExceptionAsync(
            () => app.StartAsync(TestContext.Current.CancellationToken));

        Assert.Null(error);
        Assert.NotNull(app.Services.GetService<TracerProvider>());
        Assert.NotNull(app.Services.GetService<MeterProvider>());
    }

    [Fact]
    public async Task HttpScopeTagsTransportPreservesActivityAndRestoresAmbientState()
    {
        using var inboundActivity = new Activity("http-inbound");
        inboundActivity.SetIdFormat(ActivityIdFormat.W3C);
        inboundActivity.Start();
        Activity? expectedActivity = Activity.Current;

        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                Assert.Equal(ObservabilityTransport.Http, CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.Current);
                Assert.NotNull(CSharpDbOperationScope.CurrentSessionId);
                Assert.Same(expectedActivity, Activity.Current);
                return Task.CompletedTask;
            },
            CreateEnabledLoggingOptions());

        await middleware.InvokeAsync(new DefaultHttpContext());

        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.Current);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
        Assert.Same(expectedActivity, Activity.Current);
    }

    [Fact]
    public async Task HttpScopeDoesNotSwallowDownstreamFailures()
    {
        var expected = new InvalidOperationException("downstream");
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ => Task.FromException(expected),
            CreateEnabledLoggingOptions());

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(new DefaultHttpContext()));

        Assert.Same(expected, actual);
        Assert.Equal(ObservabilityTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public void DefaultCaptureStartupLogsDoNotExposeConfigurationCanary()
    {
        const string canary = "AllAlphaBearerCapabilitySecret";
        var loggerFactory = new CapturingLoggerFactory();
        using ServiceProvider provider = BuildServices(
            loggerFactory,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["ConnectionStrings:CSharpDB"] = $"Data Source={canary}",
            });

        provider.StartCSharpDbObservability(ObservabilityTransport.Direct);

        LogEntry hostStart = Assert.Single(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.HostStarting);
        Assert.DoesNotContain(canary, hostStart.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            loggerFactory.Entries,
            entry => entry.Message.Contains(canary, StringComparison.Ordinal));
        Assert.DoesNotContain(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.RawSqlCaptureEnabled);
    }

    [Fact]
    public void RawCapturePublishesExactlyOneTypedWarningAfterBridgeSubscription()
    {
        var loggerFactory = new CapturingLoggerFactory();
        using ServiceProvider provider = BuildServices(
            loggerFactory,
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:DatabaseAlias"] = "raw-test",
                ["CSharpDB:Observability:Logging:SqlText"] = "Raw",
            });

        provider.StartCSharpDbObservability(ObservabilityTransport.Direct);

        LogEntry warning = Assert.Single(
            loggerFactory.Entries,
            entry => entry.EventId == CSharpDbLogEventIds.RawSqlCaptureEnabled);
        Assert.Equal(CSharpDbLogEvents.RawSqlCaptureEnabled.Name, warning.EventName);
        Assert.Contains("raw-test", warning.Message, StringComparison.Ordinal);
        Assert.Contains("Raw", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThrowingLoggerCannotPreventHostObservabilityStartup()
    {
        using ServiceProvider provider = BuildServices(
            new ThrowingLoggerFactory(),
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:Logging:SqlText"] = "Raw",
            });

        Exception? error = Record.Exception(
            () => provider.StartCSharpDbObservability(ObservabilityTransport.Direct));

        Assert.Null(error);
    }

    [Fact]
    public async Task ApiProgramPassesBoundOptionsToDirectDatabase()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-observability-{Guid.NewGuid():N}.db");

        try
        {
            await using var factory = new TestApiFactory(databasePath);
            using HttpClient client = factory.CreateClient();

            CSharpDbObservabilityOptions observability =
                factory.Services.GetRequiredService<CSharpDbObservabilityOptions>();
            CSharpDbClientOptions clientOptions =
                factory.Services.GetRequiredService<CSharpDbClientOptions>();

            Assert.True(observability.Enabled);
            Assert.Equal("api-test", observability.DatabaseAlias);
            Assert.Same(observability, clientOptions.DirectDatabaseOptions?.ObservabilityOptions);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task RestQueryLog_UsesHttpCorrelationAndDoesNotExposeSqlOrLiteral()
    {
        const string secret = "RestQueryAllAlphaCanary";
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-api-query-observability-{Guid.NewGuid():N}.db");
        var loggerFactory = new CapturingLoggerFactory();
        var observer = new QueryCompletedObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);

        try
        {
            await using var factory = new TestApiFactory(databasePath, loggerFactory);
            using HttpClient client = factory.CreateClient();
            loggerFactory.Entries.Clear();
            observer.Clear();

            using HttpResponseMessage response = await client.PostAsJsonAsync(
                "/api/sql/execute",
                new ExecuteSqlRequest($"SELECT '{secret}' AS value"),
                TestContext.Current.CancellationToken);
            response.EnsureSuccessStatusCode();

            CSharpDbQueryCompletedEvent completed = Assert.Single(observer.Events);
            Assert.Equal(ObservabilityTransport.Http, completed.Context.Transport);
            Assert.NotNull(completed.Context.TraceId);
            Assert.NotNull(completed.Context.SessionId);
            Assert.Equal(SqlTextCaptureMode.None, completed.SqlTextCaptureMode);
            Assert.Null(completed.CapturedSqlText);

            LogEntry log = Assert.Single(
                loggerFactory.Entries,
                entry => entry.EventId == CSharpDbLogEventIds.QueryCompleted);
            Assert.Equal(CSharpDbLogEvents.QueryCompleted.Name, log.EventName);
            Assert.DoesNotContain(secret, log.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(databasePath, log.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            TryDelete(databasePath);
            TryDelete(databasePath + ".wal");
        }
    }

    private static string? ActivityTag(Activity activity, string key)
        => activity.TagObjects.FirstOrDefault(
            tag => string.Equals(tag.Key, key, StringComparison.Ordinal)).Value
            ?.ToString();

    private static void AssertMetricPointHasQueryTags(
        Metric metric,
        string databaseAlias)
    {
        int matchingPoints = 0;
        foreach (ref readonly MetricPoint point in metric.GetMetricPoints())
        {
            var tags = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, object?> tag in point.Tags)
                tags[tag.Key] = tag.Value?.ToString();

            if (!tags.TryGetValue(
                    CSharpDbMetricTagNames.DatabaseAlias,
                    out string? observedAlias) ||
                !string.Equals(
                    observedAlias,
                    databaseAlias,
                    StringComparison.Ordinal))
            {
                continue;
            }

            matchingPoints++;
            Assert.Equal(4, tags.Count);
            Assert.All(tags.Keys, static key =>
                Assert.True(CSharpDbMetricTagNames.IsAllowed(key)));
            Assert.Equal(
                "query",
                tags[CSharpDbMetricTagNames.OperationClass]);
            Assert.Equal(
                "succeeded",
                tags[CSharpDbMetricTagNames.Outcome]);
            Assert.Equal("http", tags[CSharpDbMetricTagNames.Transport]);
        }

        Assert.Equal(1, matchingPoints);
    }

    private static void AssertExplicitHistogramBoundaries(
        Metric metric,
        IReadOnlyList<double> expected)
    {
        int pointCount = 0;
        var actual = new List<double>();
        foreach (ref readonly MetricPoint point in metric.GetMetricPoints())
        {
            pointCount++;
            foreach (HistogramBucket bucket in point.GetHistogramBuckets())
                actual.Add(bucket.ExplicitBound);
        }

        Assert.Equal(1, pointCount);
        Assert.NotEmpty(actual);
        Assert.True(double.IsPositiveInfinity(actual[^1]));
        actual.RemoveAt(actual.Count - 1);
        Assert.Equal(expected, actual);
    }

    private static void AssertResourceIdentity(
        BaseProvider provider,
        string serviceName,
        string deploymentEnvironment)
    {
        IReadOnlyDictionary<string, object> attributes = provider
            .GetResource()
            .Attributes
            .ToDictionary(
                static attribute => attribute.Key,
                static attribute => attribute.Value,
                StringComparer.Ordinal);
        Assert.Equal(serviceName, attributes["service.name"]);
        Assert.Equal(
            deploymentEnvironment,
            attributes["deployment.environment.name"]);
        Assert.False(string.IsNullOrWhiteSpace(
            attributes["service.version"].ToString()));
        Assert.True(CSharpDbDiagnostics.IsValidOpaqueIdentifier(
            Assert.IsType<string>(attributes["service.instance.id"])));
    }

    private static IReadOnlyList<string> MetricTagProjection(Metric metric)
    {
        var projection = new List<string>();
        foreach (ref readonly MetricPoint point in metric.GetMetricPoints())
        {
            foreach (KeyValuePair<string, object?> tag in point.Tags)
                projection.Add($"{tag.Key}={tag.Value}");
        }

        return projection;
    }

    private static ServiceProvider BuildServices(
        ILoggerFactory loggerFactory,
        IReadOnlyDictionary<string, string?> values)
    {
        IConfiguration configuration = BuildConfiguration(values);
        var services = new ServiceCollection();
        services.AddSingleton(loggerFactory);
        services.AddCSharpDbObservability(configuration);
        return services.BuildServiceProvider();
    }

    private static CSharpDbObservabilityOptions CreateEnabledLoggingOptions()
        => new()
        {
            Enabled = true,
            Logging = new CSharpDbLoggingOptions { Enabled = true },
        };

    private static SamplingParameters CreateSamplingParameters(
        ActivityContext parentContext)
        => new(
            parentContext,
            ActivityTraceId.CreateRandom(),
            "sampler-canary",
            ActivityKind.Internal,
            Array.Empty<KeyValuePair<string, object?>>(),
            Array.Empty<ActivityLink>());

    private static IConfiguration BuildConfiguration(
        IReadOnlyDictionary<string, string?> values)
        => new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();

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

    private sealed class TestApiFactory(
        string databasePath,
        ILoggerFactory? loggerFactory = null) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Development");
            builder.UseSetting("CSharpDB:Observability:Enabled", "true");
            builder.UseSetting(
                "CSharpDB:Observability:DatabaseAlias",
                "api-test");
            builder.UseSetting(
                "CSharpDB:Observability:Logging:Queries",
                "true");
            builder.UseSetting(
                "CSharpDB:Observability:Logging:SlowQueries",
                "false");
            if (loggerFactory is not null)
            {
                builder.ConfigureServices(services =>
                {
                    services.RemoveAll<ILoggerFactory>();
                    services.AddSingleton(loggerFactory);
                });
            }
            builder.ConfigureAppConfiguration((_, configuration) =>
            {
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:CSharpDB"] = $"Data Source={databasePath}",
                    ["CSharpDB:Observability:Enabled"] = "true",
                    ["CSharpDB:Observability:DatabaseAlias"] = "api-test",
                    ["CSharpDB:Observability:Logging:Queries"] = "true",
                    ["CSharpDB:Observability:Logging:SlowQueries"] = "false",
                });
            });
        }
    }

    private sealed class ExporterApiFactory(
        string databasePath,
        ICollection<Activity> exportedActivities,
        ICollection<Metric> exportedMetrics) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.UseSetting("CSharpDB:Observability:Enabled", "true");
            builder.UseSetting(
                "CSharpDB:Observability:DatabaseAlias",
                "api-exporter-test");
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
                        ["CSharpDB:Observability:DatabaseAlias"] =
                            "api-exporter-test",
                        ["CSharpDB:Observability:Logging:Enabled"] = "false",
                        ["CSharpDB:Observability:OpenTelemetry:Enabled"] =
                            "true",
                        ["CSharpDB:Observability:OpenTelemetry:SamplingRatio"] =
                            "1",
                    }));
            builder.ConfigureServices(services =>
                services.AddOpenTelemetry()
                    .WithTracing(tracing =>
                        tracing.AddInMemoryExporter(exportedActivities))
                    .WithMetrics(metrics =>
                        metrics.AddInMemoryExporter(exportedMetrics)));
        }
    }

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public ConcurrentQueue<LogEntry> Entries { get; } = new();

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(Entries);

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger(ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            entries.Enqueue(new LogEntry(eventId.Id, eventId.Name, formatter(state, exception)));
        }
    }

    private sealed class ThrowingLoggerFactory : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new ThrowingLogger();

        public void Dispose()
        {
        }
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            => throw new InvalidOperationException("logger scope failure");

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("logger write failure");
    }

    private sealed record LogEntry(int EventId, string? EventName, string Message);

    private sealed class QueryCompletedObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly ConcurrentQueue<CSharpDbQueryCompletedEvent> _events = new();

        internal IReadOnlyList<CSharpDbQueryCompletedEvent> Events => _events.ToArray();

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is CSharpDbQueryCompletedEvent completed)
                _events.Enqueue(completed);
        }

        internal void Clear() => _events.Clear();
    }
}

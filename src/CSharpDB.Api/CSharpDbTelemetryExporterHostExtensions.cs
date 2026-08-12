using System.Reflection;
using CSharpDB.Api.Security;
using CSharpDB.Observability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace CSharpDB.Api;

internal static class CSharpDbTelemetryExporterHostExtensions
{
    private const string FallbackServiceName = "CSharpDB";
    private static readonly double[] s_durationHistogramBoundariesSeconds =
    [
        0.0005,
        0.001,
        0.0025,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1,
        2.5,
        5,
        10,
        30,
        60,
    ];
    private static readonly double[] s_walCommitBatchSizeBoundaries =
    [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
    ];
    private static readonly string[] s_durationHistogramNames =
    [
        CSharpDbMetricInstrumentNames.QueryDuration,
        CSharpDbMetricInstrumentNames.TransactionDuration,
        CSharpDbMetricInstrumentNames.MaintenanceDuration,
        CSharpDbMetricInstrumentNames.CheckpointDuration,
        CSharpDbMetricInstrumentNames.WalRecoveryDuration,
        CSharpDbMetricInstrumentNames.PoolWaitDuration,
    ];

    internal static IReadOnlyList<double> DurationHistogramBoundariesSeconds =>
        s_durationHistogramBoundariesSeconds;

    internal static IReadOnlyList<double> WalCommitBatchSizeBoundaries =>
        s_walCommitBatchSizeBoundaries;

    internal static IServiceCollection AddCSharpDbTelemetryExporters(
        this IServiceCollection services,
        CSharpDbObservabilityOptions options,
        string? defaultServiceName,
        string? defaultDeploymentEnvironment)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        services.AddOptions<CSharpDbApiSecurityOptions>();
        services.TryAddSingleton<CSharpDbHostRouteRegistry>();

        bool openTelemetryEnabled = options.OpenTelemetry.Enabled;
        bool prometheusEnabled = options.Prometheus.Enabled;
        double samplingRatio = options.OpenTelemetry.SamplingRatio;
        bool consoleEnabled = options.OpenTelemetry.Console.Enabled;
        bool otlpEnabled = options.OpenTelemetry.Otlp.Enabled;
        string prometheusPath = options.Prometheus.Path;
        bool healthEnabled = options.Health.Enabled;
        string livenessPath = options.Health.LivenessPath;
        string readinessPath = options.Health.ReadinessPath;
        if (!openTelemetryEnabled && !prometheusEnabled)
            return services;

        CSharpDbTelemetryResourceIdentity identity = ResolveResourceIdentity(
            options.OpenTelemetry.Resource,
            defaultServiceName,
            defaultDeploymentEnvironment);

        var telemetry = services.AddOpenTelemetry()
            .ConfigureResource(resource =>
            {
                resource.AddService(
                    identity.ServiceName,
                    identity.ServiceNamespace,
                    identity.ServiceVersion,
                    autoGenerateServiceInstanceId: false,
                    serviceInstanceId: identity.ServiceInstanceId);
                resource.AddAttributes(
                [
                    new KeyValuePair<string, object>(
                        "deployment.environment.name",
                        identity.DeploymentEnvironment),
                ]);
            });

        if (openTelemetryEnabled)
        {
            telemetry.WithTracing(tracing =>
            {
                tracing
                    .AddSource(CSharpDbDiagnostics.ActivitySourceName)
                    .SetSampler(CreateSampler(samplingRatio))
                    .AddAspNetCoreInstrumentation(instrumentation =>
                    {
                        instrumentation.Filter = context =>
                            !CSharpDbTelemetryRequestPaths.IsInfrastructurePath(
                                context.Request.Path,
                                prometheusEnabled,
                                prometheusPath,
                                healthEnabled,
                                livenessPath,
                                readinessPath);
                    });

                if (consoleEnabled)
                    tracing.AddConsoleExporter();
                if (otlpEnabled)
                    tracing.AddOtlpExporter();
            });
        }

        if (openTelemetryEnabled || prometheusEnabled)
        {
            telemetry.WithMetrics(metrics =>
            {
                metrics.AddMeter(CSharpDbDiagnostics.MeterName);
                AddRecommendedHistogramViews(metrics);

                if (prometheusEnabled)
                {
                    // Prometheus is a public pull surface. Keep trace/span
                    // exemplars out even if a process-wide OTEL setting would
                    // otherwise enable them.
                    metrics.SetExemplarFilter(ExemplarFilterType.AlwaysOff);
                }

                if (openTelemetryEnabled &&
                    consoleEnabled)
                {
                    metrics.AddConsoleExporter();
                }

                if (openTelemetryEnabled && otlpEnabled)
                    metrics.AddOtlpExporter();

                if (prometheusEnabled)
                {
                    metrics.AddPrometheusExporter(prometheus =>
                    {
                        prometheus.ScrapeEndpointPath = prometheusPath;
                        prometheus.ScopeInfoEnabled = false;
                        prometheus.TargetInfoEnabled = false;
                    });
                }
            });
        }

        return services;
    }

    private static void AddRecommendedHistogramViews(
        MeterProviderBuilder metrics)
    {
        foreach (string instrumentName in s_durationHistogramNames)
        {
            metrics.AddView(
                instrumentName,
                new ExplicitBucketHistogramConfiguration
                {
                    Boundaries = s_durationHistogramBoundariesSeconds,
                });
        }

        metrics.AddView(
            CSharpDbMetricInstrumentNames.WalCommitBatchSize,
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries = s_walCommitBatchSizeBoundaries,
            });
    }

    internal static Sampler CreateSampler(double samplingRatio)
        => new ParentBasedSampler(new TraceIdRatioBasedSampler(samplingRatio));

    internal static CSharpDbTelemetryResourceIdentity ResolveResourceIdentity(
        CSharpDbOpenTelemetryResourceOptions resourceOptions,
        string? defaultServiceName,
        string? defaultDeploymentEnvironment)
    {
        ArgumentNullException.ThrowIfNull(resourceOptions);

        string serviceName = resourceOptions.ServiceName ??
            NormalizeDefault(defaultServiceName, FallbackServiceName);
        return new CSharpDbTelemetryResourceIdentity(
            serviceName,
            resourceOptions.ServiceNamespace,
            resourceOptions.ServiceVersion ??
                TryGetInformationalVersion(),
            resourceOptions.ServiceInstanceId ??
                CSharpDbDiagnostics.CreateServerInstanceId(),
            resourceOptions.DeploymentEnvironment ??
                NormalizeDefault(
                    defaultDeploymentEnvironment,
                    Environments.Production));
    }

    private static string NormalizeDefault(string? value, string fallback)
        => IsSafeResourceValue(value) ? value! : fallback;

    private static bool IsSafeResourceValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 128 ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal))
        {
            return false;
        }

        return !value.Any(static character =>
            char.IsControl(character) || character is '/' or '\\');
    }

    private static string? TryGetInformationalVersion()
    {
        try
        {
            return (Assembly.GetEntryAssembly() ??
                    typeof(CSharpDbTelemetryExporterHostExtensions).Assembly)
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                ?.InformationalVersion;
        }
        catch
        {
            return typeof(CSharpDbTelemetryExporterHostExtensions).Assembly
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                ?.InformationalVersion;
        }
    }
}

internal sealed record CSharpDbTelemetryResourceIdentity(
    string ServiceName,
    string? ServiceNamespace,
    string? ServiceVersion,
    string ServiceInstanceId,
    string DeploymentEnvironment);

internal static class CSharpDbTelemetryRequestPaths
{
    internal static bool IsInfrastructurePath(
        PathString requestPath,
        bool prometheusEnabled,
        string prometheusPath,
        bool healthEnabled,
        string livenessPath,
        string readinessPath)
    {
        if (prometheusEnabled &&
            requestPath.Equals(new PathString(prometheusPath)))
        {
            return true;
        }

        return healthEnabled &&
               (requestPath.Equals(new PathString(livenessPath)) ||
                requestPath.Equals(new PathString(readinessPath)));
    }
}

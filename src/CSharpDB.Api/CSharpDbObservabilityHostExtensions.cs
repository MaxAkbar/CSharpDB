using CSharpDB.Client;
using CSharpDB.Api.Diagnostics;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Api;

/// <summary>
/// Registers and starts the logger bridge shared by the standalone API and
/// daemon hosts. Configuration is bound and validated during host startup so
/// an invalid observability setup cannot reach database warmup.
/// </summary>
public static class CSharpDbObservabilityHostExtensions
{
    public static IServiceCollection AddCSharpDbObservability(
        this IServiceCollection services,
        IConfiguration configuration)
        => services.AddCSharpDbObservability(
            configuration,
            defaultServiceName: null,
            defaultDeploymentEnvironment: null);

    internal static IServiceCollection AddCSharpDbObservability(
        this IServiceCollection services,
        IConfiguration configuration,
        string? defaultServiceName,
        string? defaultDeploymentEnvironment)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        if (services.Any(static descriptor =>
                descriptor.ServiceType ==
                    typeof(CSharpDbObservabilityRegistrationMarker)))
        {
            return services;
        }

        ServiceDescriptor? existingOptionsDescriptor = services
            .LastOrDefault(static descriptor =>
                !descriptor.IsKeyedService &&
                descriptor.ServiceType ==
                    typeof(CSharpDbObservabilityOptions));
        CSharpDbObservabilityOptions? options = existingOptionsDescriptor is null
            ? configuration
                .GetSection(CSharpDbObservabilityOptions.ConfigurationSectionName)
                .Get<CSharpDbObservabilityOptions>()
                ?? new CSharpDbObservabilityOptions()
            : existingOptionsDescriptor.ImplementationInstance as
                CSharpDbObservabilityOptions;
        options?.Validate();

        services.TryAddSingleton<IConfiguration>(configuration);
        if (existingOptionsDescriptor is null)
            services.AddSingleton(options!);
        services.TryAddSingleton<CSharpDbDiagnosticLoggerBridge>();
        services.TryAddSingleton<CSharpDbHostRequestDiagnostics>(serviceProvider =>
        {
            CSharpDbObservabilityOptions options = serviceProvider
                .GetRequiredService<CSharpDbObservabilityOptions>();
            return new CSharpDbHostRequestDiagnostics(
                options.History.ActiveQueryCapacity);
        });
        services.TryAddSingleton<ICSharpDbHostRequestDiagnosticsContributor>(
            serviceProvider => serviceProvider
                .GetRequiredService<CSharpDbHostRequestDiagnostics>());

        bool hostedExportersConfigured = options is not null;
        if (hostedExportersConfigured)
        {
            services.AddCSharpDbTelemetryExporters(
                options!,
                defaultServiceName,
                defaultDeploymentEnvironment);
        }

        services.AddSingleton(
            new CSharpDbObservabilityRegistrationMarker(
                hostedExportersConfigured,
                options,
                options is null
                    ? null
                    : CSharpDbHostedTelemetryOptionsShape.Capture(options)));
        return services;
    }

    public static WebApplication UseCSharpDbObservability(
        this WebApplication app,
        ObservabilityTransport hostTransport = ObservabilityTransport.Direct)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.Services.StartCSharpDbObservability(hostTransport);
        return app;
    }

    /// <summary>
    /// Starts the bridge before database warmup and publishes safe typed host
    /// events. All work is best effort so logging providers cannot prevent the
    /// database host from starting.
    /// </summary>
    public static void StartCSharpDbObservability(
        this IServiceProvider services,
        ObservabilityTransport hostTransport = ObservabilityTransport.Direct)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Keep configuration failures visible and fail the host before warmup.
        CSharpDbObservabilityOptions options =
            services.GetRequiredService<CSharpDbObservabilityOptions>();
        options.Validate();
        services.GetRequiredService<CSharpDbObservabilityRegistrationMarker>()
            .ValidateEffectiveOptions(options);

        try
        {
            // Resolve first: this establishes the DiagnosticListener
            // subscription before either startup event is published.
            _ = services.GetRequiredService<CSharpDbDiagnosticLoggerBridge>();
        }
        catch
        {
            // The logger bridge is optional and cannot prevent provider startup.
        }

        if (!options.Enabled)
            return;

        // Hosted providers are otherwise initialized by app.Start/app.Run,
        // after the API and daemon warm their databases. Resolve the
        // registered providers now so recovery, checkpoint, and query
        // telemetry emitted during warmup has an active listener.
        TryStartTelemetryProviders(services, options);

        try
        {
            CSharpDbDiagnostics.EventPublisher.Publish(
                CSharpDbLogEvents.HostStarting,
                (hostTransport, options.DatabaseAlias),
                static state => new CSharpDbHostStartingEvent(
                    CSharpDbOperationContext.CreateRoot(
                        CSharpDbOperationClass.Database,
                        state.hostTransport,
                        state.DatabaseAlias)));

            if (options.Logging.Enabled && options.Logging.SqlText == SqlTextCaptureMode.Raw)
            {
                CSharpDbDiagnostics.EventPublisher.Publish(
                    CSharpDbLogEvents.RawSqlCaptureEnabled,
                    options.DatabaseAlias,
                    static alias => new CSharpDbRawSqlCaptureEnabledEvent(
                        alias,
                        SqlTextCaptureMode.Raw));
            }
        }
        catch
        {
            // Host startup events are best effort. Configuration was resolved
            // and validated outside this isolation boundary, and provider
            // activation is isolated above.
        }
    }

    private static void TryStartTelemetryProviders(
        IServiceProvider services,
        CSharpDbObservabilityOptions options)
    {
        if (options.OpenTelemetry.Enabled || options.Prometheus.Enabled)
        {
            try
            {
                _ = services.GetService<MeterProvider>();
            }
            catch
            {
                // Exporters are optional and cannot prevent database warmup.
            }
        }

        if (options.OpenTelemetry.Enabled)
        {
            try
            {
                _ = services.GetService<TracerProvider>();
            }
            catch
            {
                // Exporters are optional and cannot prevent database warmup.
            }
        }
    }
}

internal sealed class CSharpDbObservabilityRegistrationMarker(
    bool hostedExportersConfigured,
    CSharpDbObservabilityOptions? authoritativeOptions,
    CSharpDbHostedTelemetryOptionsShape? authoritativeShape)
{
    internal bool HostedExportersConfigured { get; } =
        hostedExportersConfigured;

    internal void ValidateEffectiveOptions(
        CSharpDbObservabilityOptions effectiveOptions)
    {
        if (!HostedExportersConfigured)
        {
            if (effectiveOptions.OpenTelemetry.Enabled ||
                effectiveOptions.Prometheus.Enabled)
            {
                throw new InvalidOperationException(
                    "Hosted OpenTelemetry and Prometheus require " +
                    "configuration-bound or instance-registered " +
                    $"{nameof(CSharpDbObservabilityOptions)}. A legacy factory " +
                    "or type registration cannot determine the exporter " +
                    "service shape during host registration.");
            }

            return;
        }

        if (ReferenceEquals(authoritativeOptions, effectiveOptions) &&
            authoritativeShape ==
                CSharpDbHostedTelemetryOptionsShape.Capture(effectiveOptions))
        {
            return;
        }

        throw new InvalidOperationException(
            $"{nameof(CSharpDbObservabilityOptions)} must be registered before " +
            $"{nameof(CSharpDbObservabilityHostExtensions.AddCSharpDbObservability)} " +
            "and must not be replaced or " +
            "mutated afterward. The effective options disagree with the " +
            "hosted telemetry provider shape established during registration.");
    }
}

internal sealed record CSharpDbHostedTelemetryOptionsShape(
    bool Enabled,
    bool OpenTelemetryEnabled,
    double SamplingRatio,
    string? ServiceName,
    string? ServiceNamespace,
    string? ServiceVersion,
    string? ServiceInstanceId,
    string? DeploymentEnvironment,
    bool OtlpEnabled,
    bool ConsoleEnabled,
    bool PrometheusEnabled,
    string PrometheusPath,
    bool AllowInsecureRemoteAccess,
    bool HealthEnabled,
    string LivenessPath,
    string ReadinessPath,
    TimeSpan ReadinessTimeout)
{
    internal static CSharpDbHostedTelemetryOptionsShape Capture(
        CSharpDbObservabilityOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new CSharpDbHostedTelemetryOptionsShape(
            options.Enabled,
            options.OpenTelemetry.Enabled,
            options.OpenTelemetry.SamplingRatio,
            options.OpenTelemetry.Resource.ServiceName,
            options.OpenTelemetry.Resource.ServiceNamespace,
            options.OpenTelemetry.Resource.ServiceVersion,
            options.OpenTelemetry.Resource.ServiceInstanceId,
            options.OpenTelemetry.Resource.DeploymentEnvironment,
            options.OpenTelemetry.Otlp.Enabled,
            options.OpenTelemetry.Console.Enabled,
            options.Prometheus.Enabled,
            options.Prometheus.Path,
            options.Prometheus.AllowInsecureRemoteAccess,
            options.Health.Enabled,
            options.Health.LivenessPath,
            options.Health.ReadinessPath,
            options.Health.ReadinessTimeout);
    }
}

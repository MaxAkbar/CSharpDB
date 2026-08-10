using CSharpDB.Client;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.TryAddSingleton<IConfiguration>(configuration);
        services.TryAddSingleton<CSharpDbObservabilityOptions>(serviceProvider =>
        {
            CSharpDbObservabilityOptions options = serviceProvider
                .GetRequiredService<IConfiguration>()
                .GetSection(CSharpDbObservabilityOptions.ConfigurationSectionName)
                .Get<CSharpDbObservabilityOptions>()
                ?? new CSharpDbObservabilityOptions();
            options.Validate();
            return options;
        });
        services.TryAddSingleton<CSharpDbDiagnosticLoggerBridge>();
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

        try
        {
            // Resolve first: this establishes the DiagnosticListener
            // subscription before either startup event is published.
            _ = services.GetRequiredService<CSharpDbDiagnosticLoggerBridge>();

            if (!options.Enabled)
                return;

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
            // Host observability is best effort. Configuration was resolved
            // and validated outside this isolation boundary.
        }
    }
}

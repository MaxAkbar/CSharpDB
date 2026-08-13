using CSharpDB.Api.Middleware;
using CSharpDB.Api.Security;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

namespace CSharpDB.Api;

public static class CSharpDbPrometheusHostExtensions
{
    private static readonly EventId InsecureRemoteAccessEvent =
        new(7002, "PrometheusInsecureRemoteAccessEnabled");

    public static WebApplication MapCSharpDbPrometheusEndpoint(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);

        CSharpDbObservabilityOptions options = app.Services
            .GetRequiredService<CSharpDbObservabilityOptions>();
        options.Validate();
        CSharpDbObservabilityRegistrationMarker registration = app.Services
            .GetRequiredService<CSharpDbObservabilityRegistrationMarker>();
        registration.ValidateEffectiveOptions(options);
        if (!options.Prometheus.Enabled)
            return app;

        if (!registration.HostedExportersConfigured)
        {
            throw new InvalidOperationException(
                "Prometheus hosting requires configuration-bound or " +
                "instance-registered CSharpDbObservabilityOptions. A legacy " +
                "factory or type registration cannot determine the exporter " +
                "service shape during host registration.");
        }

        CSharpDbApiSecurityOptions security = app.Services
            .GetRequiredService<IOptions<CSharpDbApiSecurityOptions>>()
            .Value;
        ValidateSecurity(security);

        string path = options.Prometheus.Path;
        CSharpDbHostRouteRegistry registry = app.Services
            .GetRequiredService<CSharpDbHostRouteRegistry>();
        registry.ReserveSubtree(
            "/csharpdb.rpc.CSharpDbRpc",
            "CSharpDB gRPC service");
        registry.ThrowIfCollides(path, "Prometheus");
        ThrowIfEndpointAlreadyUsesPath(app, path);
        registry.ReserveExact(path, "Prometheus");

        var pathString = new PathString(path);
        app.UseWhen(
            context => context.Request.Path.Equals(pathString),
            branch => branch.UseMiddleware<CSharpDbPrometheusAccessMiddleware>());
        app.MapPrometheusScrapingEndpoint(
            path,
            meterProvider: null,
            configureBranchedPipeline: branch =>
                branch.UseMiddleware<CSharpDbPrometheusAccessMiddleware>(),
            optionsName: null);

        if (security.Mode == CSharpDbRemoteSecurityMode.None &&
            options.Prometheus.AllowInsecureRemoteAccess)
        {
            app.Logger.LogWarning(
                InsecureRemoteAccessEvent,
                "CSharpDB Prometheus scraping permits unauthenticated non-loopback peers on the shared Kestrel listener at {PrometheusPath}.",
                path);
        }

        return app;
    }

    private static void ValidateSecurity(CSharpDbApiSecurityOptions security)
    {
        if (!Enum.IsDefined(security.Mode))
        {
            throw new InvalidOperationException(
                "Prometheus requires a defined CSharpDB host security mode.");
        }

        if (security.Mode == CSharpDbRemoteSecurityMode.ApiKey &&
            string.IsNullOrEmpty(security.ApiKey))
        {
            throw new InvalidOperationException(
                "Prometheus API-key security requires a configured API key.");
        }

        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(
            security.ApiKeyHeaderName);
        if (headerName.Any(static character =>
                char.IsControl(character) ||
                char.IsWhiteSpace(character) ||
                character == ':'))
        {
            throw new InvalidOperationException(
                "The Prometheus API-key header name is invalid.");
        }
    }

    private static void ThrowIfEndpointAlreadyUsesPath(
        WebApplication app,
        string path)
    {
        string normalized = path.TrimEnd('/');
        foreach (EndpointDataSource source in
                 ((IEndpointRouteBuilder)app).DataSources)
        {
            foreach (RouteEndpoint endpoint in source.Endpoints
                         .OfType<RouteEndpoint>())
            {
                string? rawText = endpoint.RoutePattern.RawText;
                if (rawText is null)
                    continue;

                string existing = rawText.StartsWith('/')
                    ? rawText.TrimEnd('/')
                    : "/" + rawText.TrimEnd('/');
                if (string.Equals(
                        normalized,
                        existing,
                        StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        "The Prometheus path collides with an existing endpoint route.");
                }
            }
        }
    }
}

using CSharpDB.Api.Diagnostics;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Api.Middleware;

/// <summary>
/// Marks database work initiated by an HTTP request without replacing the
/// inbound ASP.NET Core activity or manufacturing an operation parent.
/// </summary>
public sealed class CSharpDbOperationScopeMiddleware
{
    private readonly RequestDelegate _next;
    private readonly bool _enabled;
    private readonly CSharpDbHostRequestDiagnostics? _requestDiagnostics;
    private readonly PathString _diagnosticsPath;

    public CSharpDbOperationScopeMiddleware(
        RequestDelegate next,
        CSharpDbObservabilityOptions? options = null)
        : this(next, options, serviceProvider: null, diagnosticsPath: null)
    {
    }

    [ActivatorUtilitiesConstructor]
    public CSharpDbOperationScopeMiddleware(
        RequestDelegate next,
        IServiceProvider serviceProvider,
        string? diagnosticsPath)
        : this(
            next,
            serviceProvider.GetService<CSharpDbObservabilityOptions>(),
            serviceProvider,
            diagnosticsPath)
    {
    }

    public CSharpDbOperationScopeMiddleware(
        RequestDelegate next,
        CSharpDbObservabilityOptions? options,
        IServiceProvider serviceProvider)
        : this(next, options, serviceProvider, diagnosticsPath: null)
    {
    }

    private CSharpDbOperationScopeMiddleware(
        RequestDelegate next,
        CSharpDbObservabilityOptions? options,
        IServiceProvider? serviceProvider,
        string? diagnosticsPath)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _enabled = options?.Enabled == true;
        _diagnosticsPath = NormalizeDiagnosticsPath(diagnosticsPath);
        if (_enabled && serviceProvider is not null)
        {
            try
            {
                _requestDiagnostics = serviceProvider
                    .GetService<CSharpDbHostRequestDiagnostics>();
            }
            catch
            {
                // Diagnostics registration must never prevent host startup.
            }
        }
    }

    public Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return _enabled &&
               !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
               !IsDiagnosticsRequest(context.Request.Path, _diagnosticsPath)
            ? InvokeWithScopeAsync(context)
            : _next(context);
    }

    private async Task InvokeWithScopeAsync(HttpContext context)
    {
        IDisposable? scope = null;
        IDisposable? requestLease = null;
        OpaqueDiagnosticsId? sessionId = null;
        try
        {
            sessionId = OpaqueDiagnosticsId.Create();
            requestLease = _requestDiagnostics?.TryBeginRequest(
                sessionId,
                CSharpDbTransport.Http,
                CSharpDbOperationScope.Current?.OperationId);
            scope = CSharpDbOperationScope.EnterTransport(
                CSharpDbTransport.Http,
                sessionId);
        }
        catch
        {
            // Diagnostics context must never affect request execution.
        }

        try
        {
            await _next(context).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                requestLease?.Dispose();
            }
            catch
            {
                // Diagnostics state must never affect request completion.
            }

            try
            {
                scope?.Dispose();
            }
            catch
            {
                // Diagnostics context must never affect request completion.
            }
        }
    }

    private static bool IsDiagnosticsRequest(
        PathString path,
        PathString diagnosticsPath)
        => path.StartsWithSegments(
            diagnosticsPath,
            StringComparison.OrdinalIgnoreCase);

    private static PathString NormalizeDiagnosticsPath(string? diagnosticsPath)
    {
        if (string.IsNullOrWhiteSpace(diagnosticsPath))
            return new PathString("/api/diagnostics");

        string normalized = diagnosticsPath.Trim();
        return new PathString(
            normalized.StartsWith("/", StringComparison.Ordinal)
                ? normalized
                : "/" + normalized);
    }
}

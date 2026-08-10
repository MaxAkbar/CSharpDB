using CSharpDB.Observability;
using Microsoft.AspNetCore.Http;

namespace CSharpDB.Api.Middleware;

/// <summary>
/// Marks database work initiated by an HTTP request without replacing the
/// inbound ASP.NET Core activity or manufacturing an operation parent.
/// </summary>
public sealed class CSharpDbOperationScopeMiddleware
{
    private readonly RequestDelegate _next;
    private readonly bool _loggingEnabled;

    public CSharpDbOperationScopeMiddleware(
        RequestDelegate next,
        CSharpDbObservabilityOptions? options = null)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _loggingEnabled = options?.Enabled == true &&
            options.Logging?.Enabled == true;
    }

    public Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return _loggingEnabled
            ? InvokeWithScopeAsync(context)
            : _next(context);
    }

    private async Task InvokeWithScopeAsync(HttpContext context)
    {
        IDisposable? scope = null;
        try
        {
            scope = CSharpDbOperationScope.EnterTransport(
                CSharpDbTransport.Http,
                OpaqueDiagnosticsId.Create());
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
                scope?.Dispose();
            }
            catch
            {
                // Diagnostics context must never affect request completion.
            }
        }
    }
}

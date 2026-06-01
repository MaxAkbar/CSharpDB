using CSharpDB.Client;
using Microsoft.AspNetCore.Http;

namespace CSharpDB.Api.Middleware;

public sealed class RouteContextMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context, ICSharpDbRouteContextAccessor routeContextAccessor)
    {
        string? keyspace = ReadHeader(context, CSharpDbRouteHeaderNames.Keyspace);
        string? shardKey = ReadHeader(context, CSharpDbRouteHeaderNames.ShardKey);

        if ((keyspace is null) != (shardKey is null))
        {
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
            await context.Response.WriteAsJsonAsync(new
            {
                error = $"Both {CSharpDbRouteHeaderNames.Keyspace} and {CSharpDbRouteHeaderNames.ShardKey} are required for sharded requests.",
            });
            return;
        }

        CSharpDbRouteContext? previous = routeContextAccessor.Current;
        routeContextAccessor.Current = keyspace is null
            ? null
            : new CSharpDbRouteContext
            {
                Keyspace = keyspace,
                Key = shardKey!,
            };

        try
        {
            await next(context);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private static string? ReadHeader(HttpContext context, string headerName)
    {
        if (!context.Request.Headers.TryGetValue(headerName, out var values))
            return null;

        string? value = values.FirstOrDefault();
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}

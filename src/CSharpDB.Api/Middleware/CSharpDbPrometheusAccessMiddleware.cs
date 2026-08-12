using System.Net;
using CSharpDB.Api.Security;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace CSharpDB.Api.Middleware;

public sealed class CSharpDbPrometheusAccessMiddleware(
    RequestDelegate next,
    CSharpDbObservabilityOptions observabilityOptions,
    IOptions<CSharpDbApiSecurityOptions> securityOptions)
{
    private const string UnauthorizedDetail =
        "A valid CSharpDB API key is required for Prometheus metrics.";
    private const string ForbiddenDetail =
        "Prometheus metrics access is not permitted from this endpoint.";

    public Task InvokeAsync(HttpContext context)
    {
        var configuredPath = new PathString(
            observabilityOptions.Prometheus.Path);
        if (!context.Request.Path.Equals(configuredPath))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return Task.CompletedTask;
        }

        CSharpDbApiSecurityOptions security = securityOptions.Value;
        return security.Mode switch
        {
            CSharpDbRemoteSecurityMode.ApiKey => AuthorizeApiKeyAsync(
                context,
                security),
            CSharpDbRemoteSecurityMode.None => AuthorizePeerAsync(context),
            _ => WriteProblemAsync(
                context,
                StatusCodes.Status403Forbidden,
                "Forbidden",
                ForbiddenDetail),
        };
    }

    private Task AuthorizeApiKeyAsync(
        HttpContext context,
        CSharpDbApiSecurityOptions security)
    {
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(
            security.ApiKeyHeaderName);
        string? suppliedApiKey = context.Request.Headers.TryGetValue(
            headerName,
            out var values)
            && values.Count == 1
            ? values[0]
            : null;
        return CSharpDbApiKeyValidator.IsAuthorized(security, suppliedApiKey)
            ? next(context)
            : WriteProblemAsync(
                context,
                StatusCodes.Status401Unauthorized,
                "Unauthorized",
                UnauthorizedDetail);
    }

    private Task AuthorizePeerAsync(HttpContext context)
    {
        IPAddress? remoteAddress = context.Connection.RemoteIpAddress;
        bool allowed = IsProvenRemoteAddress(remoteAddress) &&
            (IPAddress.IsLoopback(remoteAddress!) ||
             observabilityOptions.Prometheus.AllowInsecureRemoteAccess);
        return allowed
            ? next(context)
            : WriteProblemAsync(
                context,
                StatusCodes.Status403Forbidden,
                "Forbidden",
                ForbiddenDetail);
    }

    private static bool IsProvenRemoteAddress(IPAddress? address)
        => address is not null &&
           !address.Equals(IPAddress.Any) &&
           !address.Equals(IPAddress.IPv6Any) &&
           !address.Equals(IPAddress.None) &&
           !address.Equals(IPAddress.IPv6None);

    private static Task WriteProblemAsync(
        HttpContext context,
        int statusCode,
        string title,
        string detail)
    {
        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/problem+json";
        return context.Response.WriteAsJsonAsync(
            new ProblemDetails
            {
                Status = statusCode,
                Title = title,
                Detail = detail,
            },
            context.RequestAborted);
    }
}

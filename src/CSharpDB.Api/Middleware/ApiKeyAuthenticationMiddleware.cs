using CSharpDB.Api.Security;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace CSharpDB.Api.Middleware;

public sealed class ApiKeyAuthenticationMiddleware
{
    private const string UnauthorizedDetail = "A valid CSharpDB API key is required.";

    private readonly RequestDelegate _next;
    private readonly IOptions<CSharpDbApiSecurityOptions> _options;

    public ApiKeyAuthenticationMiddleware(RequestDelegate next, IOptions<CSharpDbApiSecurityOptions> options)
    {
        _next = next;
        _options = options;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        CSharpDbApiSecurityOptions security = _options.Value;
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(security.ApiKeyHeaderName);
        string? suppliedApiKey = context.Request.Headers.TryGetValue(headerName, out var values)
            ? values.FirstOrDefault()
            : null;

        if (CSharpDbApiKeyValidator.IsAuthorized(security, suppliedApiKey))
        {
            await _next(context);
            return;
        }

        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        context.Response.ContentType = "application/problem+json";
        await context.Response.WriteAsJsonAsync(new ProblemDetails
        {
            Status = StatusCodes.Status401Unauthorized,
            Title = "Unauthorized",
            Detail = UnauthorizedDetail,
        });
    }
}

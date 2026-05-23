using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http.HttpResults;

namespace CSharpDB.Admin.Services;

public static class DesktopShellEndpoints
{
    public const string TokenHeaderName = "X-CSharpDB-Desktop-Shell-Token";

    private const string DesktopShellEnabledKey = "CSharpDB:DesktopShell";
    private const string DesktopShellTokenKey = "CSharpDB:DesktopShellToken";

    public static IEndpointRouteBuilder MapCSharpDbDesktopShellEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));

        IConfiguration configuration = endpoints.ServiceProvider.GetRequiredService<IConfiguration>();
        if (!configuration.GetValue<bool>(DesktopShellEnabledKey))
            return endpoints;

        endpoints.MapPost(
                "/_desktop/open-database",
                async Task<Results<Ok<OpenDatabaseResponse>, BadRequest<string>, UnauthorizedHttpResult, StatusCodeHttpResult, NotFound>> (
                    HttpContext context,
                    OpenDatabaseRequest request,
                    DatabaseClientHolder holder) =>
                {
                    if (!IsLoopbackRequest(context))
                        return TypedResults.NotFound();

                    string? configuredToken = configuration[DesktopShellTokenKey];
                    if (string.IsNullOrWhiteSpace(configuredToken))
                        return TypedResults.StatusCode(StatusCodes.Status503ServiceUnavailable);

                    if (!RequestHasValidToken(context, configuredToken))
                        return TypedResults.Unauthorized();

                    if (string.IsNullOrWhiteSpace(request.DatabasePath))
                        return TypedResults.BadRequest("A databasePath value is required.");

                    await holder.SwitchAsync(request.DatabasePath.Trim());

                    return TypedResults.Ok(new OpenDatabaseResponse(holder.DataSource));
                })
            .DisableAntiforgery();

        return endpoints;
    }

    private static bool IsLoopbackRequest(HttpContext context)
    {
        IPAddress? remoteIpAddress = context.Connection.RemoteIpAddress;
        return remoteIpAddress is null || IPAddress.IsLoopback(remoteIpAddress);
    }

    private static bool RequestHasValidToken(HttpContext context, string configuredToken)
    {
        if (!context.Request.Headers.TryGetValue(TokenHeaderName, out var providedValues))
            return false;

        string? provided = providedValues.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(provided))
            return false;

        byte[] expectedBytes = Encoding.UTF8.GetBytes(configuredToken);
        byte[] providedBytes = Encoding.UTF8.GetBytes(provided);

        return expectedBytes.Length == providedBytes.Length &&
               CryptographicOperations.FixedTimeEquals(expectedBytes, providedBytes);
    }

    public sealed record OpenDatabaseRequest(string? DatabasePath);

    public sealed record OpenDatabaseResponse(string DataSource);
}

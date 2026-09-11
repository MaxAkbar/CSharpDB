using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class DefinitionCatalogEndpoints
{
    public static RouteGroupBuilder MapDefinitionCatalogEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/catalog/definitions", async (ICSharpDbClient client, string? continuationToken, int? pageSize, CancellationToken ct) =>
        {
            if (client is not ICSharpDbDefinitionCatalogReader reader)
                return Results.Problem("Definition inspection is unavailable. Upgrade the server.", statusCode: 501);
            try { return Results.Ok(await reader.ReadDefinitionCatalogAsync(string.IsNullOrEmpty(continuationToken) ? null : continuationToken, pageSize ?? 64, ct)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
        });
        return group;
    }
}

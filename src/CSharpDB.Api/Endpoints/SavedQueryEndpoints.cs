using CSharpDB.Api.Dtos;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class SavedQueryEndpoints
{
    public static RouteGroupBuilder MapSavedQueryEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/saved-queries", GetSavedQueries);
        group.MapGet("/saved-queries/{name}", GetSavedQuery);
        group.MapPut("/saved-queries/{name}", UpsertSavedQuery);
        group.MapDelete("/saved-queries/{name}", DeleteSavedQuery);
        return group;
    }

    private static async Task<IResult> GetSavedQueries(ICSharpDbClient db)
    {
        var savedQueries = await db.GetSavedQueriesAsync();
        return Results.Ok(savedQueries);
    }

    private static async Task<IResult> GetSavedQuery(string name, ICSharpDbClient db)
    {
        var savedQuery = await db.GetSavedQueryAsync(name);
        return savedQuery is null
            ? Results.NotFound(new { error = $"Saved query '{name}' not found." })
            : Results.Ok(savedQuery);
    }

    private static async Task<IResult> UpsertSavedQuery(string name, UpsertSavedQueryRequest req, ICSharpDbClient db)
    {
        var existing = await db.GetSavedQueryAsync(name);
        var savedQuery = await db.UpsertSavedQueryAsync(name, req.SqlText);
        string location = $"/api/saved-queries/{Uri.EscapeDataString(savedQuery.Name)}";
        return existing is null
            ? Results.Created(location, savedQuery)
            : Results.Ok(savedQuery);
    }

    private static async Task<IResult> DeleteSavedQuery(string name, ICSharpDbClient db)
    {
        await db.DeleteSavedQueryAsync(name);
        return Results.NoContent();
    }
}

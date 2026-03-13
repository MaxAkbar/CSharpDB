using CSharpDB.Api.Dtos;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class CollectionEndpoints
{
    public static RouteGroupBuilder MapCollectionEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/collections", GetCollectionNames);
        group.MapGet("/collections/{name}/count", GetCollectionCount);
        group.MapGet("/collections/{name}", BrowseCollection);
        group.MapGet("/collections/{name}/document", GetDocument);
        group.MapPut("/collections/{name}/document", PutDocument);
        group.MapDelete("/collections/{name}/document", DeleteDocument);
        return group;
    }

    private static async Task<IResult> GetCollectionNames(ICSharpDbClient db)
    {
        var names = await db.GetCollectionNamesAsync();
        return Results.Ok(names);
    }

    private static async Task<IResult> GetCollectionCount(string name, ICSharpDbClient db)
    {
        int count = await db.GetCollectionCountAsync(name);
        return Results.Ok(new CollectionCountResponse(name, count));
    }

    private static async Task<IResult> BrowseCollection(string name, ICSharpDbClient db, int page = 1, int pageSize = 50)
    {
        var result = await db.BrowseCollectionAsync(name, page, pageSize);
        return Results.Ok(result);
    }

    private static async Task<IResult> GetDocument(string name, string key, ICSharpDbClient db)
    {
        if (string.IsNullOrWhiteSpace(key))
            return Results.BadRequest(new { error = "A collection document key is required." });

        var document = await db.GetDocumentAsync(name, key);
        return document is null
            ? Results.NotFound(new { error = $"Document '{key}' was not found in collection '{name}'." })
            : Results.Ok(new CollectionDocument { Key = key, Document = document.Value });
    }

    private static async Task<IResult> PutDocument(string name, string key, PutDocumentRequest req, ICSharpDbClient db)
    {
        if (string.IsNullOrWhiteSpace(key))
            return Results.BadRequest(new { error = "A collection document key is required." });

        await db.PutDocumentAsync(name, key, req.Document);
        return Results.NoContent();
    }

    private static async Task<IResult> DeleteDocument(string name, string key, ICSharpDbClient db)
    {
        if (string.IsNullOrWhiteSpace(key))
            return Results.BadRequest(new { error = "A collection document key is required." });

        bool deleted = await db.DeleteDocumentAsync(name, key);
        return deleted
            ? Results.NoContent()
            : Results.NotFound(new { error = $"Document '{key}' was not found in collection '{name}'." });
    }
}

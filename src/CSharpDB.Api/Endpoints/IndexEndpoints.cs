using CSharpDB.Api.Dtos;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class IndexEndpoints
{
    public static RouteGroupBuilder MapIndexEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/indexes", GetAllIndexes);
        group.MapPost("/indexes", CreateIndex);
        group.MapPut("/indexes/{name}", UpdateIndex);
        group.MapDelete("/indexes/{name}", DropIndex);

        return group;
    }

    private static async Task<IResult> GetAllIndexes(ICSharpDbClient db)
    {
        var indexes = await db.GetIndexesAsync();
        var response = indexes.Select(i => new IndexResponse(
            i.IndexName, i.TableName, i.Columns, i.IsUnique, i.ColumnCollations)).ToList();
        return Results.Ok(response);
    }

    private static async Task<IResult> CreateIndex(CreateIndexRequest req, ICSharpDbClient db)
    {
        await db.CreateIndexAsync(req.IndexName, req.TableName, req.ColumnName, req.IsUnique, req.Collation);
        return Results.Created($"/api/indexes/{req.IndexName}", await GetIndexResponseAsync(db, req.IndexName));
    }

    private static async Task<IResult> UpdateIndex(string name, UpdateIndexRequest req, ICSharpDbClient db)
    {
        await db.UpdateIndexAsync(name, req.NewIndexName, req.TableName, req.ColumnName, req.IsUnique, req.Collation);
        return Results.Ok(await GetIndexResponseAsync(db, req.NewIndexName));
    }

    private static async Task<IResult> DropIndex(string name, ICSharpDbClient db)
    {
        await db.DropIndexAsync(name);
        return Results.NoContent();
    }

    private static async Task<IndexResponse> GetIndexResponseAsync(ICSharpDbClient db, string indexName)
    {
        var index = (await db.GetIndexesAsync()).Single(i => string.Equals(i.IndexName, indexName, StringComparison.OrdinalIgnoreCase));
        return new IndexResponse(index.IndexName, index.TableName, index.Columns, index.IsUnique, index.ColumnCollations);
    }
}

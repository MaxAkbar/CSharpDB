using CSharpDB.Api.Dtos;
using CSharpDB.Service;

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

    private static async Task<IResult> GetAllIndexes(CSharpDbService db)
    {
        var indexes = await db.GetIndexesAsync();
        var response = indexes.Select(i => new IndexResponse(
            i.IndexName, i.TableName, i.Columns, i.IsUnique)).ToList();
        return Results.Ok(response);
    }

    private static async Task<IResult> CreateIndex(CreateIndexRequest req, CSharpDbService db)
    {
        await db.CreateIndexAsync(req.IndexName, req.TableName, req.ColumnName, req.IsUnique);
        return Results.Created($"/api/indexes/{req.IndexName}", new IndexResponse(
            req.IndexName, req.TableName, [req.ColumnName], req.IsUnique));
    }

    private static async Task<IResult> UpdateIndex(string name, UpdateIndexRequest req, CSharpDbService db)
    {
        await db.UpdateIndexAsync(name, req.NewIndexName, req.TableName, req.ColumnName, req.IsUnique);
        return Results.Ok(new IndexResponse(
            req.NewIndexName, req.TableName, [req.ColumnName], req.IsUnique));
    }

    private static async Task<IResult> DropIndex(string name, CSharpDbService db)
    {
        await db.DropIndexAsync(name);
        return Results.NoContent();
    }
}

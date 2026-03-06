using CSharpDB.Api.Dtos;
using CSharpDB.Service;

namespace CSharpDB.Api.Endpoints;

public static class SchemaEndpoints
{
    public static RouteGroupBuilder MapSchemaEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/info", GetDatabaseInfo);
        return group;
    }

    private static async Task<IResult> GetDatabaseInfo(CSharpDbService db)
    {
        var tables = await db.GetTableNamesAsync();
        var indexes = await db.GetIndexesAsync();
        var views = await db.GetViewsAsync();
        var triggers = await db.GetTriggersAsync();
        var procedures = await db.GetProceduresAsync();

        return Results.Ok(new DatabaseInfoResponse(
            db.DataSource,
            tables.Count,
            indexes.Count,
            views.Count,
            triggers.Count,
            procedures.Count));
    }
}

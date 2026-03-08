using CSharpDB.Api.Dtos;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class SchemaEndpoints
{
    public static RouteGroupBuilder MapSchemaEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/info", GetDatabaseInfo);
        return group;
    }

    private static async Task<IResult> GetDatabaseInfo(ICSharpDbClient db)
    {
        var info = await db.GetInfoAsync();

        return Results.Ok(new DatabaseInfoResponse(
            info.DataSource,
            info.TableCount,
            info.IndexCount,
            info.ViewCount,
            info.TriggerCount,
            info.ProcedureCount));
    }
}

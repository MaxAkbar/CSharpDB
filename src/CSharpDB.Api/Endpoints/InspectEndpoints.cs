using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class InspectEndpoints
{
    public static RouteGroupBuilder MapInspectEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/inspect", InspectStorage);
        group.MapGet("/inspect/wal", InspectWal);
        group.MapGet("/inspect/page/{id}", InspectPage);
        group.MapGet("/inspect/indexes", CheckIndexes);
        return group;
    }

    private static async Task<IResult> InspectStorage(
        ICSharpDbClient db,
        HttpContext context,
        bool includePages = false,
        string? path = null)
    {
        var report = await db.InspectStorageAsync(path, includePages, context.RequestAborted);
        return Results.Ok(report);
    }

    private static async Task<IResult> InspectWal(
        ICSharpDbClient db,
        HttpContext context,
        string? path = null)
    {
        var report = await db.CheckWalAsync(path, context.RequestAborted);
        return Results.Ok(report);
    }

    private static async Task<IResult> InspectPage(
        uint id,
        ICSharpDbClient db,
        HttpContext context,
        bool hex = false,
        string? path = null)
    {
        var report = await db.InspectPageAsync(id, hex, path, context.RequestAborted);
        return Results.Ok(report);
    }

    private static async Task<IResult> CheckIndexes(
        ICSharpDbClient db,
        HttpContext context,
        string? index = null,
        int? sample = null,
        string? path = null)
    {
        var report = await db.CheckIndexesAsync(path, index, sample, context.RequestAborted);
        return Results.Ok(report);
    }
}

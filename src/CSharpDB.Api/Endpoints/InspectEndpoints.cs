using CSharpDB.Service;

namespace CSharpDB.Api.Endpoints;

public static class InspectEndpoints
{
    public static RouteGroupBuilder MapInspectEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/inspect", InspectStorage);
        group.MapGet("/inspect/wal", InspectWal);
        group.MapGet("/inspect/page/{id:uint}", InspectPage);
        group.MapGet("/inspect/indexes", CheckIndexes);
        return group;
    }

    private static async Task<IResult> InspectStorage(
        CSharpDbService db,
        bool includePages = false,
        string? path = null)
    {
        var report = await db.InspectStorageAsync(path, includePages);
        return Results.Ok(report);
    }

    private static async Task<IResult> InspectWal(
        CSharpDbService db,
        string? path = null)
    {
        var report = await db.CheckWalAsync(path);
        return Results.Ok(report);
    }

    private static async Task<IResult> InspectPage(
        uint id,
        CSharpDbService db,
        bool hex = false,
        string? path = null)
    {
        var report = await db.InspectPageAsync(id, hex, path);
        return Results.Ok(report);
    }

    private static async Task<IResult> CheckIndexes(
        CSharpDbService db,
        string? index = null,
        int? sample = null,
        string? path = null)
    {
        var report = await db.CheckIndexesAsync(path, index, sample);
        return Results.Ok(report);
    }
}

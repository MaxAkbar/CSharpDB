using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class MaintenanceEndpoints
{
    public static RouteGroupBuilder MapMaintenanceEndpoints(this RouteGroupBuilder group)
    {
        var maintenance = group.MapGroup("/maintenance");
        maintenance.MapGet("/report", GetReport);
        maintenance.MapPost("/reindex", Reindex);
        maintenance.MapPost("/vacuum", Vacuum);
        return group;
    }

    private static async Task<IResult> GetReport(ICSharpDbClient db)
    {
        var report = await db.GetMaintenanceReportAsync();
        return Results.Ok(report);
    }

    private static async Task<IResult> Reindex(ICSharpDbClient db, ReindexRequest request)
    {
        var result = await db.ReindexAsync(request);
        return Results.Ok(result);
    }

    private static async Task<IResult> Vacuum(ICSharpDbClient db)
    {
        var result = await db.VacuumAsync();
        return Results.Ok(result);
    }
}

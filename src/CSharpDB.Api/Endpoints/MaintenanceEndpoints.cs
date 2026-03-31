using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class MaintenanceEndpoints
{
    public static RouteGroupBuilder MapMaintenanceEndpoints(this RouteGroupBuilder group)
    {
        var maintenance = group.MapGroup("/maintenance");
        maintenance.MapPost("/checkpoint", Checkpoint);
        maintenance.MapPost("/backup", Backup);
        maintenance.MapPost("/restore", Restore);
        maintenance.MapPost("/migrate-foreign-keys", MigrateForeignKeys);
        maintenance.MapGet("/report", GetReport);
        maintenance.MapPost("/reindex", Reindex);
        maintenance.MapPost("/vacuum", Vacuum);
        return group;
    }

    private static async Task<IResult> Checkpoint(ICSharpDbClient db)
    {
        await db.CheckpointAsync();
        return Results.NoContent();
    }

    private static async Task<IResult> Backup(ICSharpDbClient db, BackupRequest request)
    {
        var result = await db.BackupAsync(request);
        return Results.Ok(result);
    }

    private static async Task<IResult> Restore(ICSharpDbClient db, RestoreRequest request)
    {
        var result = await db.RestoreAsync(request);
        return Results.Ok(result);
    }

    private static async Task<IResult> MigrateForeignKeys(ICSharpDbClient db, ForeignKeyMigrationRequest request)
    {
        var result = await db.MigrateForeignKeysAsync(request);
        return Results.Ok(result);
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

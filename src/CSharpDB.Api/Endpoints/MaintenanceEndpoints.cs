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

    private static async Task<IResult> Checkpoint(ICSharpDbClient db, HttpContext context)
    {
        await db.CheckpointAsync(context.RequestAborted);
        return Results.NoContent();
    }

    private static async Task<IResult> Backup(
        ICSharpDbClient db,
        BackupRequest request,
        HttpContext context)
    {
        var result = await db.BackupAsync(request, context.RequestAborted);
        return Results.Ok(result);
    }

    private static async Task<IResult> Restore(
        ICSharpDbClient db,
        RestoreRequest request,
        HttpContext context)
    {
        var result = await db.RestoreAsync(request, context.RequestAborted);
        return Results.Ok(result);
    }

    private static async Task<IResult> MigrateForeignKeys(
        ICSharpDbClient db,
        ForeignKeyMigrationRequest request,
        HttpContext context)
    {
        foreach (ForeignKeyMigrationConstraintSpec constraint in request.Constraints)
        {
            if (!Enum.IsDefined(constraint.OnDelete))
            {
                throw new ArgumentException(
                    $"Unsupported foreign key ON DELETE action '{constraint.OnDelete}'.",
                    nameof(request));
            }

            if (!Enum.IsDefined(constraint.OnUpdate))
            {
                throw new ArgumentException(
                    $"Unsupported foreign key ON UPDATE action '{constraint.OnUpdate}'.",
                    nameof(request));
            }
        }

        var result = await db.MigrateForeignKeysAsync(request, context.RequestAborted);
        return Results.Ok(result);
    }

    private static async Task<IResult> GetReport(ICSharpDbClient db, HttpContext context)
    {
        var report = await db.GetMaintenanceReportAsync(context.RequestAborted);
        return Results.Ok(report);
    }

    private static async Task<IResult> Reindex(
        ICSharpDbClient db,
        ReindexRequest request,
        HttpContext context)
    {
        var result = await db.ReindexAsync(request, context.RequestAborted);
        return Results.Ok(result);
    }

    private static async Task<IResult> Vacuum(ICSharpDbClient db, HttpContext context)
    {
        var result = await db.VacuumAsync(context.RequestAborted);
        return Results.Ok(result);
    }
}

using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using Microsoft.Extensions.Hosting;

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
        if (request.ValidateOnly)
        {
            var validation = await db.RestoreAsync(
                request,
                context.RequestAborted);
            return Results.Ok(validation);
        }

        RestoreResult result = await RunExclusiveAsync(
            db,
            context,
            CSharpDbReadinessReason.RestoreInProgress,
            CSharpDbReadinessReason.ReopenPending,
            recoverAfterAnyFailure: true,
            operation: cancellationToken => db.RestoreAsync(
                request,
                cancellationToken));
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

        if (request.ValidateOnly)
        {
            var validation = await db.MigrateForeignKeysAsync(
                request,
                context.RequestAborted);
            return Results.Ok(validation);
        }

        ForeignKeyMigrationResult result = await RunExclusiveAsync(
            db,
            context,
            CSharpDbReadinessReason.ExclusiveMaintenance,
            CSharpDbReadinessReason.Unavailable,
            recoverAfterAnyFailure: false,
            operation: cancellationToken => db.MigrateForeignKeysAsync(
                request,
                cancellationToken));
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
        ReindexResult result = await RunExclusiveAsync(
            db,
            context,
            CSharpDbReadinessReason.ExclusiveMaintenance,
            CSharpDbReadinessReason.Unavailable,
            recoverAfterAnyFailure: false,
            operation: cancellationToken => db.ReindexAsync(
                request,
                cancellationToken));
        return Results.Ok(result);
    }

    private static async Task<IResult> Vacuum(ICSharpDbClient db, HttpContext context)
    {
        VacuumResult result = await RunExclusiveAsync(
            db,
            context,
            CSharpDbReadinessReason.ExclusiveMaintenance,
            CSharpDbReadinessReason.Unavailable,
            recoverAfterAnyFailure: false,
            operation: db.VacuumAsync);
        return Results.Ok(result);
    }

    private static async Task<T> RunExclusiveAsync<T>(
        ICSharpDbClient db,
        HttpContext context,
        CSharpDbReadinessReason activeReason,
        CSharpDbReadinessReason failureReason,
        bool recoverAfterAnyFailure,
        Func<CancellationToken, Task<T>> operation)
    {
        CSharpDbHostReadinessCoordinator? coordinator = context.RequestServices
            .GetService<CSharpDbHostReadinessCoordinator>();
        CSharpDbObservabilityOptions? healthOptions = context.RequestServices
            .GetService<CSharpDbObservabilityOptions>();
        CancellationToken hostStopping = context.RequestServices
            .GetRequiredService<IHostApplicationLifetime>()
            .ApplicationStopping;
        using IDisposable? lease = coordinator?.EnterNotReady(activeReason);
        try
        {
            T result = await operation(context.RequestAborted)
                .ConfigureAwait(false);

            // Direct exclusive maintenance closes the cached Database. Verify
            // that its lazy reopen succeeds before releasing readiness.
            if (coordinator is not null)
            {
                await VerifyAvailableAsync(
                        db,
                        healthOptions,
                        hostStopping)
                    .ConfigureAwait(false);
            }

            return result;
        }
        catch
        {
            if (coordinator is not null)
            {
                bool recoveryRequired = recoverAfterAnyFailure;
                if (!recoveryRequired)
                {
                    try
                    {
                        await VerifyAvailableAsync(
                                db,
                                healthOptions,
                                hostStopping)
                            .ConfigureAwait(false);
                    }
                    catch
                    {
                        recoveryRequired = true;
                    }
                }

                // Restore can cross its replacement point before failing.
                // Other exclusive failures remain ready when a direct probe
                // proves that the existing database is still available.
                if (recoveryRequired)
                    coordinator.RequestRecovery(failureReason);
            }

            throw;
        }
    }

    private static async Task VerifyAvailableAsync(
        ICSharpDbClient db,
        CSharpDbObservabilityOptions? options,
        CancellationToken hostStopping)
    {
        TimeSpan timeout = options?.Health.ReadinessTimeout ??
            TimeSpan.FromSeconds(2);
        using var attemptCancellation = CancellationTokenSource
            .CreateLinkedTokenSource(hostStopping);
        attemptCancellation.CancelAfter(timeout);
        Task<DatabaseInfo> attempt = db.GetInfoAsync(
            attemptCancellation.Token);
        try
        {
            _ = await attempt
                .WaitAsync(timeout, hostStopping)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            !hostStopping.IsCancellationRequested &&
            attemptCancellation.IsCancellationRequested)
        {
            throw new TimeoutException(
                "CSharpDB database verification exceeded the configured readiness timeout.");
        }
        finally
        {
            if (!attempt.IsCompleted)
            {
                _ = attempt.ContinueWith(
                    static completed => _ = completed.Exception,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously |
                    TaskContinuationOptions.OnlyOnFaulted,
                    TaskScheduler.Default);
            }
        }
    }
}

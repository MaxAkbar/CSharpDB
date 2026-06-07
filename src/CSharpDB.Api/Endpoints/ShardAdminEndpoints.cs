using CSharpDB.Api.Dtos;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class ShardAdminEndpoints
{
    public static RouteGroupBuilder MapShardAdminEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/sharding/map", GetShardMap);
        group.MapPost("/sharding/resolve", ResolveRoute);
        group.MapGet("/sharding/status", GetShardStatus);
        group.MapPost("/sharding/sql/execute-all", ExecuteSqlOnAllShards);
        group.MapPost("/sharding/sql/read-all", ExecuteReadOnlySqlOnAllShards);
        group.MapGet("/sharding/catalog", GetShardCatalog);
        group.MapPost("/sharding/catalog/validate", ValidateShardCatalogUpdate);
        group.MapPost("/sharding/catalog/apply", ApplyShardCatalogUpdate);
        group.MapGet("/sharding/migrations", GetShardMigrationHistory);
        group.MapGet("/sharding/migrations/progress", GetShardMigrationProgress);
        group.MapGet("/sharding/migrations/{migrationId}/progress", GetShardMigrationProgressById);
        group.MapPost("/sharding/migrations/{migrationId}/resume", ResumeShardMigration);
        group.MapPost("/sharding/migrations/{migrationId}/retry", RetryShardMigration);
        group.MapPost("/sharding/migrations/exact-route-key", MigrateExactRouteKey);
        group.MapPost("/sharding/migrations/bucket-range", MigrateBucketRange);
        group.MapPost("/sharding/directory/resolve", ResolveDirectoryEntry);
        group.MapPost("/sharding/directory/reserve", ReserveDirectoryEntry);
        group.MapPost("/sharding/directory/activate", ActivateDirectoryEntry);
        group.MapPost("/sharding/directory/upsert", UpsertDirectoryEntry);
        group.MapPost("/sharding/directory/disable", DisableDirectoryEntry);
        group.MapPost("/sharding/directory/delete", DeleteDirectoryEntry);
        group.MapPost("/sharding/directory/mark-stale", MarkDirectoryEntryStale);
        return group;
    }

    private static async Task<IResult> GetShardMap(ICSharpDbClient db, CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.GetShardMapAsync(ct));
    }

    private static async Task<IResult> ResolveRoute(
        CSharpDbRouteContext routeContext,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.ResolveRouteAsync(routeContext, ct));
    }

    private static async Task<IResult> GetShardStatus(ICSharpDbClient db, CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.GetShardStatusAsync(ct));
    }

    private static async Task<IResult> ExecuteSqlOnAllShards(
        ExecuteSqlRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        IReadOnlyList<CSharpDbShardSqlExecutionResult> results =
            await shardAdmin.ExecuteSqlOnAllShardsAsync(request.Sql, ct);

        return Results.Ok(results.Select(result => new ShardSqlExecutionResultResponse(
            result.ShardId,
            result.Result is null ? null : SqlEndpoints.ToResponse(result.Result),
            result.Error)).ToList());
    }

    private static async Task<IResult> ExecuteReadOnlySqlOnAllShards(
        ExecuteSqlRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        try
        {
            IReadOnlyList<CSharpDbShardSqlExecutionResult> results =
                await shardAdmin.ExecuteReadOnlySqlOnAllShardsAsync(request.Sql, ct);

            return Results.Ok(results.Select(result => new ShardSqlExecutionResultResponse(
                result.ShardId,
                result.Result is null ? null : SqlEndpoints.ToResponse(result.Result),
                result.Error)).ToList());
        }
        catch (CSharpDbClientException ex)
        {
            return Results.BadRequest(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetShardCatalog(ICSharpDbClient db, CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.GetShardCatalogAsync(ct));
    }

    private static async Task<IResult> ValidateShardCatalogUpdate(
        CSharpDbShardCatalogUpdateRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.ValidateShardCatalogUpdateAsync(request, ct));
    }

    private static async Task<IResult> ApplyShardCatalogUpdate(
        CSharpDbShardCatalogUpdateRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardCatalogApplyResult result = await shardAdmin.ApplyShardCatalogUpdateAsync(request, ct);
        return result.Applied ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> MigrateExactRouteKey(
        CSharpDbShardExactKeyMigrationRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardMigrationResult result = await shardAdmin.MigrateExactRouteKeyAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> MigrateBucketRange(
        CSharpDbShardBucketRangeMigrationRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardMigrationResult result = await shardAdmin.MigrateBucketRangeAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> GetShardMigrationHistory(ICSharpDbClient db, CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.GetShardMigrationHistoryAsync(ct));
    }

    private static async Task<IResult> GetShardMigrationProgress(ICSharpDbClient db, CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        return Results.Ok(await shardAdmin.GetShardMigrationProgressAsync(ct));
    }

    private static async Task<IResult> GetShardMigrationProgressById(
        string migrationId,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardMigrationProgress? progress = await shardAdmin.GetShardMigrationProgressAsync(migrationId, ct);
        return progress is null ? Results.NotFound(new { error = $"Shard migration '{migrationId}' was not found." }) : Results.Ok(progress);
    }

    private static async Task<IResult> ResumeShardMigration(
        string migrationId,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardMigrationResult result = await shardAdmin.ResumeShardMigrationAsync(migrationId, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> RetryShardMigration(
        string migrationId,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardAdminClient? shardAdmin = GetShardAdmin(db);
        if (shardAdmin is null)
            return Unsupported();

        CSharpDbShardMigrationResult result = await shardAdmin.RetryShardMigrationAsync(migrationId, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> ResolveDirectoryEntry(
        CSharpDbShardDirectoryResolveRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        return Results.Ok(await directoryClient.ResolveDirectoryEntryAsync(request, ct));
    }

    private static async Task<IResult> ReserveDirectoryEntry(
        CSharpDbShardDirectoryReserveRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.ReserveDirectoryEntryAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> ActivateDirectoryEntry(
        CSharpDbShardDirectoryActivateRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.ActivateDirectoryEntryAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> UpsertDirectoryEntry(
        CSharpDbShardDirectoryUpsertRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.UpsertDirectoryEntryAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> DisableDirectoryEntry(
        CSharpDbShardDirectoryDisableRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.DisableDirectoryEntryAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> DeleteDirectoryEntry(
        CSharpDbShardDirectoryDeleteRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.DeleteDirectoryEntryAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static async Task<IResult> MarkDirectoryEntryStale(
        CSharpDbShardDirectoryMarkStaleRequest request,
        ICSharpDbClient db,
        CancellationToken ct)
    {
        ICSharpDbShardDirectoryClient? directoryClient = GetShardDirectory(db);
        if (directoryClient is null)
            return UnsupportedDirectory();

        CSharpDbShardDirectoryMutationResult result = await directoryClient.MarkDirectoryEntryStaleAsync(request, ct);
        return result.Succeeded ? Results.Ok(result) : Results.BadRequest(result);
    }

    private static ICSharpDbShardAdminClient? GetShardAdmin(ICSharpDbClient db)
        => db as ICSharpDbShardAdminClient;

    private static ICSharpDbShardDirectoryClient? GetShardDirectory(ICSharpDbClient db)
        => db as ICSharpDbShardDirectoryClient;

    private static IResult Unsupported()
        => Results.NotFound(new
        {
            error = "CSharpDB shard-admin APIs are available only when API-level sharding is enabled.",
        });

    private static IResult UnsupportedDirectory()
        => Results.NotFound(new
        {
            error = "CSharpDB shard-directory APIs are available only when API-level sharding is enabled.",
        });
}

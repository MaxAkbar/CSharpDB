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
        group.MapPost("/sharding/migrations/exact-route-key", MigrateExactRouteKey);
        group.MapPost("/sharding/migrations/bucket-range", MigrateBucketRange);
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

    private static ICSharpDbShardAdminClient? GetShardAdmin(ICSharpDbClient db)
        => db as ICSharpDbShardAdminClient;

    private static IResult Unsupported()
        => Results.NotFound(new
        {
            error = "CSharpDB shard-admin APIs are available only when API-level sharding is enabled.",
        });
}

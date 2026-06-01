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

    private static ICSharpDbShardAdminClient? GetShardAdmin(ICSharpDbClient db)
        => db as ICSharpDbShardAdminClient;

    private static IResult Unsupported()
        => Results.NotFound(new
        {
            error = "CSharpDB shard-admin APIs are available only when API-level sharding is enabled.",
        });
}

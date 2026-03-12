using CSharpDB.Api.Dtos;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class TransactionEndpoints
{
    public static RouteGroupBuilder MapTransactionEndpoints(this RouteGroupBuilder group)
    {
        group.MapPost("/transactions", BeginTransaction);
        group.MapPost("/transactions/{id}/execute", ExecuteInTransaction);
        group.MapPost("/transactions/{id}/commit", CommitTransaction);
        group.MapPost("/transactions/{id}/rollback", RollbackTransaction);
        return group;
    }

    private static async Task<IResult> BeginTransaction(ICSharpDbClient db)
    {
        var transaction = await db.BeginTransactionAsync();
        return Results.Ok(transaction);
    }

    private static async Task<IResult> ExecuteInTransaction(string id, ExecuteSqlRequest req, ICSharpDbClient db)
    {
        var result = await db.ExecuteInTransactionAsync(id, req.Sql);
        var response = SqlEndpoints.ToResponse(result);
        return result.Error is not null
            ? Results.BadRequest(response)
            : Results.Ok(response);
    }

    private static async Task<IResult> CommitTransaction(string id, ICSharpDbClient db)
    {
        await db.CommitTransactionAsync(id);
        return Results.NoContent();
    }

    private static async Task<IResult> RollbackTransaction(string id, ICSharpDbClient db)
    {
        await db.RollbackTransactionAsync(id);
        return Results.NoContent();
    }
}

using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class SqlEndpoints
{
    public static RouteGroupBuilder MapSqlEndpoints(this RouteGroupBuilder group)
    {
        group.MapPost("/sql/execute", ExecuteSql);
        return group;
    }

    private static async Task<IResult> ExecuteSql(ExecuteSqlRequest req, ICSharpDbClient db)
    {
        var result = await db.ExecuteSqlAsync(req.Sql);

        List<Dictionary<string, object?>>? namedRows = null;
        if (result.IsQuery && result.ColumnNames is not null && result.Rows is not null)
            namedRows = JsonHelper.RowsToNamedDictionaries(result.ColumnNames, result.Rows);

        var response = new SqlResultResponse(
            result.IsQuery,
            result.ColumnNames,
            namedRows,
            result.RowsAffected,
            result.Error,
            result.Elapsed.TotalMilliseconds);

        return result.Error is not null
            ? Results.BadRequest(response)
            : Results.Ok(response);
    }
}

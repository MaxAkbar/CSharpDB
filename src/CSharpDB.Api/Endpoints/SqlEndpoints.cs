using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Primitives;
using CSharpDB.Sql;

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
        if (TryRejectStatelessTemporaryTableSql(req.Sql, out IResult rejection))
            return rejection;

        var result = await db.ExecuteSqlAsync(req.Sql);
        var response = ToResponse(result);

        return result.Error is not null
            ? Results.BadRequest(response)
            : Results.Ok(response);
    }

    private static bool TryRejectStatelessTemporaryTableSql(string sql, out IResult rejection)
    {
        rejection = null!;

        try
        {
            foreach (string statementSql in SqlScriptSplitter.SplitExecutableStatements(sql))
            {
                Statement statement = Parser.Parse(statementSql);
                if (!SqlStatementClassifier.IsTemporaryTableStatement(statement))
                    continue;

                rejection = Results.BadRequest(new SqlResultResponse(
                    IsQuery: false,
                    ColumnNames: null,
                    ColumnTypes: null,
                    Rows: null,
                    RowsAffected: 0,
                    Error: "Temporary table commands require a transaction session when using stateless HTTP. Use BeginTransaction and ExecuteInTransaction for remote temporary table workflows.",
                    ElapsedMs: 0));
                return true;
            }
        }
        catch (CSharpDbException)
        {
            return false;
        }

        return false;
    }

    internal static SqlResultResponse ToResponse(SqlExecutionResult result)
    {
        List<Dictionary<string, object?>>? namedRows = null;
        if (result.IsQuery && result.ColumnNames is not null && result.Rows is not null)
            namedRows = JsonHelper.RowsToNamedDictionaries(result.ColumnNames, result.Rows);

        return new SqlResultResponse(
            result.IsQuery,
            result.ColumnNames,
            result.ColumnTypes,
            namedRows,
            result.RowsAffected,
            result.Error,
            result.Elapsed.TotalMilliseconds);
    }
}

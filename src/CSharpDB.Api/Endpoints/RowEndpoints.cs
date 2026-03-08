using CSharpDB.Api.Dtos;
using CSharpDB.Api.Helpers;
using CSharpDB.Client;

namespace CSharpDB.Api.Endpoints;

public static class RowEndpoints
{
    public static RouteGroupBuilder MapRowEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/tables/{name}/rows", BrowseRows);
        group.MapGet("/tables/{name}/rows/{pkValue}", GetRowByPk);
        group.MapPost("/tables/{name}/rows", InsertRow);
        group.MapPut("/tables/{name}/rows/{pkValue}", UpdateRow);
        group.MapDelete("/tables/{name}/rows/{pkValue}", DeleteRow);

        return group;
    }

    private static async Task<IResult> BrowseRows(
        string name, ICSharpDbClient db, int page = 1, int pageSize = 50)
    {
        if (page < 1) page = 1;
        if (pageSize < 1) pageSize = 50;
        if (pageSize > 1000) pageSize = 1000;

        var result = await db.BrowseTableAsync(name, page, pageSize);

        var columnNames = result.Schema.Columns.Select(c => c.Name).ToArray();
        var rows = JsonHelper.RowsToNamedDictionaries(columnNames, result.Rows);

        return Results.Ok(new BrowseResponse(
            columnNames, rows, result.TotalRows, result.Page, result.PageSize, result.TotalPages));
    }

    private static async Task<IResult> GetRowByPk(
        string name, string pkValue, ICSharpDbClient db, string pkColumn = "id")
    {
        object coerced = CoercePkValue(pkValue);
        var row = await db.GetRowByPkAsync(name, pkColumn, coerced);
        return row is null
            ? Results.NotFound(new { error = $"Row with {pkColumn}='{pkValue}' not found in '{name}'." })
            : Results.Ok(row);
    }

    private static async Task<IResult> InsertRow(string name, InsertRowRequest req, ICSharpDbClient db)
    {
        var values = JsonHelper.CoerceDictionary(req.Values);
        var affected = await db.InsertRowAsync(name, values);
        return Results.Created($"/api/tables/{name}/rows", new MutationResponse(affected));
    }

    private static async Task<IResult> UpdateRow(
        string name, string pkValue, UpdateRowRequest req, ICSharpDbClient db, string pkColumn = "id")
    {
        object coerced = CoercePkValue(pkValue);
        var values = JsonHelper.CoerceDictionary(req.Values);
        var affected = await db.UpdateRowAsync(name, pkColumn, coerced, values);
        return Results.Ok(new MutationResponse(affected));
    }

    private static async Task<IResult> DeleteRow(
        string name, string pkValue, ICSharpDbClient db, string pkColumn = "id")
    {
        object coerced = CoercePkValue(pkValue);
        var affected = await db.DeleteRowAsync(name, pkColumn, coerced);
        return affected == 0
            ? Results.NotFound(new { error = $"Row with {pkColumn}='{pkValue}' not found in '{name}'." })
            : Results.Ok(new MutationResponse(affected));
    }

    /// <summary>
    /// PK values from route segments are always strings — try to parse as long first.
    /// </summary>
    private static object CoercePkValue(string raw) =>
        long.TryParse(raw, out long l) ? l : raw;
}

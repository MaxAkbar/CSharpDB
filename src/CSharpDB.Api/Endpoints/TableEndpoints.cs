using CSharpDB.Api.Dtos;
using CSharpDB.Core;
using CSharpDB.Service;

namespace CSharpDB.Api.Endpoints;

public static class TableEndpoints
{
    public static RouteGroupBuilder MapTableEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/tables", GetTableNames);
        group.MapGet("/tables/{name}/schema", GetTableSchema);
        group.MapGet("/tables/{name}/count", GetRowCount);
        group.MapDelete("/tables/{name}", DropTable);
        group.MapPatch("/tables/{name}/rename", RenameTable);
        group.MapPost("/tables/{name}/columns", AddColumn);
        group.MapDelete("/tables/{name}/columns/{col}", DropColumn);
        group.MapPatch("/tables/{name}/columns/{col}/rename", RenameColumn);

        return group;
    }

    private static async Task<IResult> GetTableNames(CSharpDbService db)
    {
        var names = await db.GetTableNamesAsync();
        return Results.Ok(names);
    }

    private static async Task<IResult> GetTableSchema(string name, CSharpDbService db)
    {
        var schema = await db.GetTableSchemaAsync(name);
        if (schema is null)
            return Results.NotFound(new { error = $"Table '{name}' not found." });

        var response = new TableSchemaResponse(
            schema.TableName,
            schema.Columns.Select(c => new ColumnResponse(
                c.Name, c.Type.ToString(), c.Nullable, c.IsPrimaryKey)).ToList());

        return Results.Ok(response);
    }

    private static async Task<IResult> GetRowCount(string name, CSharpDbService db)
    {
        var count = await db.GetRowCountAsync(name);
        return Results.Ok(new RowCountResponse(name, count));
    }

    private static async Task<IResult> DropTable(string name, CSharpDbService db)
    {
        await db.DropTableAsync(name);
        return Results.NoContent();
    }

    private static async Task<IResult> RenameTable(string name, RenameTableRequest req, CSharpDbService db)
    {
        await db.RenameTableAsync(name, req.NewName);
        return Results.NoContent();
    }

    private static async Task<IResult> AddColumn(string name, AddColumnRequest req, CSharpDbService db)
    {
        if (!Enum.TryParse<DbType>(req.Type, ignoreCase: true, out var dbType))
            return Results.BadRequest(new { error = $"Invalid column type '{req.Type}'. Valid types: Integer, Real, Text, Blob." });

        await db.AddColumnAsync(name, req.ColumnName, dbType, req.NotNull);
        return Results.NoContent();
    }

    private static async Task<IResult> DropColumn(string name, string col, CSharpDbService db)
    {
        await db.DropColumnAsync(name, col);
        return Results.NoContent();
    }

    private static async Task<IResult> RenameColumn(string name, string col, RenameColumnRequest req, CSharpDbService db)
    {
        await db.RenameColumnAsync(name, col, req.NewName);
        return Results.NoContent();
    }
}

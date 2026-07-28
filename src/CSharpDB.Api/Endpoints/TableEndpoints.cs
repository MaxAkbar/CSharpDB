using CSharpDB.Api.Dtos;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Api.Endpoints;

public static class TableEndpoints
{
    public static RouteGroupBuilder MapTableEndpoints(this RouteGroupBuilder group)
    {
        group.MapGet("/tables", GetTableNames);
        group.MapGet("/tables/schema", GetTableSchema);
        group.MapGet("/table-operations/count", GetRowCount);
        group.MapDelete("/table-operations/table", DropTable);
        group.MapPatch("/table-operations/table/rename", RenameTable);
        group.MapPost("/table-operations/columns", AddColumn);
        group.MapDelete("/table-operations/columns", DropColumn);
        group.MapPatch("/table-operations/columns/rename", RenameColumn);

        // Retain the original path-based routes for existing callers. New
        // clients use the separate query-based namespace above so quoted
        // identifiers that contain path syntax such as '/' round-trip without
        // route rewriting or shadowing table names.
        group.MapGet("/tables/{name}/schema", GetTableSchema);
        group.MapGet("/tables/{name}/count", GetRowCount);
        group.MapDelete("/tables/{name}", DropTable);
        group.MapPatch("/tables/{name}/rename", RenameTable);
        group.MapPost("/tables/{name}/columns", AddColumn);
        group.MapDelete("/tables/{name}/columns/{col}", DropColumn);
        group.MapPatch("/tables/{name}/columns/{col}/rename", RenameColumn);

        return group;
    }

    private static async Task<IResult> GetTableNames(ICSharpDbClient db)
    {
        var names = await db.GetTableNamesAsync();
        return Results.Ok(names);
    }

    private static async Task<IResult> GetTableSchema(string name, ICSharpDbClient db)
    {
        var schema = await db.GetTableSchemaAsync(name);
        if (schema is null)
            return Results.NotFound(new { error = $"Table '{name}' not found." });

        var response = new TableSchemaResponse(
            schema.TableName,
            schema.Columns.Select(c => new ColumnResponse(
                c.Name, c.Type.ToString(), c.Nullable, c.IsPrimaryKey, c.IsIdentity, c.IsRowVersion, c.Collation, c.DefaultSql, c.SchemaId)).ToList(),
            schema.ForeignKeys.Select(fk => new ForeignKeyResponse(
                fk.ConstraintName,
                fk.ColumnName,
                fk.ReferencedTableName,
                fk.ReferencedColumnName,
                fk.OnDelete.ToString(),
                fk.SupportingIndexName,
                fk.ColumnNames.Count > 0 ? fk.ColumnNames : [fk.ColumnName],
                fk.ReferencedColumnNames.Count > 0 ? fk.ReferencedColumnNames : [fk.ReferencedColumnName],
                fk.SchemaId,
                fk.ColumnSchemaIds,
                fk.ReferencedTableSchemaId,
                fk.ReferencedColumnSchemaIds,
                fk.ReferencedKeySchemaId,
                fk.OnUpdate.ToString())).ToList(),
            schema.KeyConstraints.Select(key => new KeyConstraintResponse(
                key.ConstraintName,
                key.Kind.ToString(),
                key.Columns,
                key.BackingIndexName,
                key.SchemaId)).ToList(),
            schema.CheckConstraints.Select(check => new CheckConstraintResponse(
                check.ConstraintName,
                check.ExpressionSql,
                check.ColumnName,
                check.SchemaId)).ToList(),
            schema.NextRowId,
            schema.SchemaId);

        return Results.Ok(response);
    }

    private static async Task<IResult> GetRowCount(string name, ICSharpDbClient db)
    {
        var count = await db.GetRowCountAsync(name);
        return Results.Ok(new RowCountResponse(name, count));
    }

    private static async Task<IResult> DropTable(string name, ICSharpDbClient db)
    {
        await db.DropTableAsync(name);
        return Results.NoContent();
    }

    private static async Task<IResult> RenameTable(string name, RenameTableRequest req, ICSharpDbClient db)
    {
        await db.RenameTableAsync(name, req.NewName);
        return Results.NoContent();
    }

    private static async Task<IResult> AddColumn(string name, AddColumnRequest req, ICSharpDbClient db)
    {
        if (!Enum.TryParse<DbType>(req.Type, ignoreCase: true, out var dbType) ||
            !Enum.IsDefined(dbType))
            return Results.BadRequest(new { error = $"Invalid column type '{req.Type}'. Valid types: Integer, Real, Text, Blob." });

        await db.AddColumnAsync(name, req.ColumnName, dbType, req.NotNull, req.Collation);
        return Results.NoContent();
    }

    private static async Task<IResult> DropColumn(string name, string col, ICSharpDbClient db)
    {
        await db.DropColumnAsync(name, col);
        return Results.NoContent();
    }

    private static async Task<IResult> RenameColumn(string name, string col, RenameColumnRequest req, ICSharpDbClient db)
    {
        await db.RenameColumnAsync(name, col, req.NewName);
        return Results.NoContent();
    }
}

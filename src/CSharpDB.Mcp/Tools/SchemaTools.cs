using System.ComponentModel;
using CSharpDB.Client;
using CSharpDB.Mcp.Helpers;
using ModelContextProtocol.Server;

namespace CSharpDB.Mcp.Tools;

[McpServerToolType]
public static class SchemaTools
{
    [McpServerTool, Description("Get the database file path and basic info.")]
    public static string GetDatabaseInfo(ICSharpDbClient db)
    {
        return JsonHelper.Serialize(new { dataSource = db.DataSource });
    }

    [McpServerTool, Description("List all table names in the database.")]
    public static async Task<string> ListTables(ICSharpDbClient db)
    {
        var tables = await db.GetTableNamesAsync();
        return JsonHelper.Serialize(tables);
    }

    [McpServerTool, Description(
        "Get the schema (columns, types, constraints) of a table.")]
    public static async Task<string> DescribeTable(
        ICSharpDbClient db,
        [Description("Name of the table to describe.")] string tableName)
    {
        var schema = await db.GetTableSchemaAsync(tableName);
        if (schema is null)
            return JsonHelper.Serialize(new { error = $"Table '{tableName}' not found." });

        var columns = schema.Columns.Select(c => new
        {
            name = c.Name,
            type = c.Type.ToString(),
            nullable = c.Nullable,
            isPrimaryKey = c.IsPrimaryKey,
            isIdentity = c.IsIdentity,
        });

        return JsonHelper.Serialize(new { tableName = schema.TableName, columns });
    }

    [McpServerTool, Description("List all indexes in the database with their table, columns, and uniqueness.")]
    public static async Task<string> ListIndexes(ICSharpDbClient db)
    {
        var indexes = await db.GetIndexesAsync();
        var result = indexes.Select(i => new
        {
            indexName = i.IndexName,
            tableName = i.TableName,
            columns = i.Columns,
            isUnique = i.IsUnique,
        });
        return JsonHelper.Serialize(result);
    }

    [McpServerTool, Description("List all view names in the database.")]
    public static async Task<string> ListViews(ICSharpDbClient db)
    {
        var views = await db.GetViewsAsync();
        var result = views.Select(v => new
        {
            name = v.Name,
            sql = v.Sql,
        });
        return JsonHelper.Serialize(result);
    }

    [McpServerTool, Description("List all triggers in the database with their table, timing, event, and body.")]
    public static async Task<string> ListTriggers(ICSharpDbClient db)
    {
        var triggers = await db.GetTriggersAsync();
        var result = triggers.Select(t => new
        {
            triggerName = t.TriggerName,
            tableName = t.TableName,
            timing = t.Timing.ToString().ToLowerInvariant(),
            triggerEvent = t.Event.ToString().ToLowerInvariant(),
            bodySql = t.BodySql,
        });
        return JsonHelper.Serialize(result);
    }
}

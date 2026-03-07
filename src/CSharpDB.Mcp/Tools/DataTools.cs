using System.ComponentModel;
using CSharpDB.Mcp.Helpers;
using CSharpDB.Service;
using ModelContextProtocol.Server;

namespace CSharpDB.Mcp.Tools;

[McpServerToolType]
public static class DataTools
{
    [McpServerTool, Description(
        "Browse rows in a table with pagination. Returns rows, schema, and total count.")]
    public static async Task<string> BrowseTable(
        CSharpDbService db,
        [Description("Name of the table to browse.")] string tableName,
        [Description("Page number (1-based). Defaults to 1.")] int page = 1,
        [Description("Rows per page. Defaults to 50.")] int pageSize = 50)
    {
        var result = await db.BrowseTableAsync(tableName, page, pageSize);

        var columns = result.Schema.Columns.Select(c => new
        {
            name = c.Name,
            type = c.Type.ToString(),
            nullable = c.Nullable,
            isPrimaryKey = c.IsPrimaryKey,
            isIdentity = c.IsIdentity,
        });

        return JsonHelper.Serialize(new
        {
            tableName = result.TableName,
            columns,
            rows = FormatRows(result.Schema.Columns.Select(c => c.Name).ToArray(), result.Rows),
            totalRows = result.TotalRows,
            page = result.Page,
            pageSize = result.PageSize,
            totalPages = result.TotalPages,
        });
    }

    [McpServerTool, Description(
        "Browse rows in a view with pagination. Returns rows and total count.")]
    public static async Task<string> BrowseView(
        CSharpDbService db,
        [Description("Name of the view to browse.")] string viewName,
        [Description("Page number (1-based). Defaults to 1.")] int page = 1,
        [Description("Rows per page. Defaults to 50.")] int pageSize = 50)
    {
        var result = await db.BrowseViewAsync(viewName, page, pageSize);

        return JsonHelper.Serialize(new
        {
            viewName = result.ViewName,
            columns = result.ColumnNames,
            rows = FormatRows(result.ColumnNames, result.Rows),
            totalRows = result.TotalRows,
            page = result.Page,
            pageSize = result.PageSize,
            totalPages = result.TotalPages,
        });
    }

    [McpServerTool, Description("Get a single row by primary key value.")]
    public static async Task<string> GetRowByPk(
        CSharpDbService db,
        [Description("Name of the table.")] string tableName,
        [Description("Name of the primary key column.")] string pkColumn,
        [Description("Primary key value to look up.")] string pkValue)
    {
        // Try to parse as long first, then keep as string
        object coerced = long.TryParse(pkValue, out long l) ? l : pkValue;

        var row = await db.GetRowByPkAsync(tableName, pkColumn, coerced);
        if (row is null)
            return JsonHelper.Serialize(new { error = $"No row found with {pkColumn} = {pkValue}." });

        return JsonHelper.Serialize(row);
    }

    [McpServerTool, Description("Get the total number of rows in a table.")]
    public static async Task<string> GetRowCount(
        CSharpDbService db,
        [Description("Name of the table.")] string tableName)
    {
        int count = await db.GetRowCountAsync(tableName);
        return JsonHelper.Serialize(new { tableName, rowCount = count });
    }

    /// <summary>
    /// Convert a list of object?[] rows into a list of dictionaries keyed by column name,
    /// which is easier for AI models to interpret.
    /// </summary>
    private static List<Dictionary<string, object?>> FormatRows(
        string[] columnNames, IEnumerable<object?[]> rows)
    {
        var formatted = new List<Dictionary<string, object?>>();
        foreach (var row in rows)
        {
            var dict = new Dictionary<string, object?>();
            for (int i = 0; i < columnNames.Length && i < row.Length; i++)
                dict[columnNames[i]] = row[i];
            formatted.Add(dict);
        }
        return formatted;
    }
}

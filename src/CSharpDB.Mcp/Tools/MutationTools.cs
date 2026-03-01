using System.ComponentModel;
using CSharpDB.Mcp.Helpers;
using CSharpDB.Service;
using ModelContextProtocol.Server;

namespace CSharpDB.Mcp.Tools;

[McpServerToolType]
public static class MutationTools
{
    [McpServerTool, Description(
        "Insert a row into a table. Pass column values as a JSON object string.")]
    public static async Task<string> InsertRow(
        CSharpDbService db,
        [Description("Name of the table.")] string tableName,
        [Description("JSON object with column names as keys and values, e.g. {\"name\":\"Alice\",\"age\":30}.")] string valuesJson)
    {
        try
        {
            var values = JsonHelper.ParseAndCoerceValues(valuesJson);
            int affected = await db.InsertRowAsync(tableName, values);
            return JsonHelper.Serialize(new { success = true, rowsAffected = affected });
        }
        catch (Exception ex)
        {
            return JsonHelper.Serialize(new { success = false, error = ex.Message });
        }
    }

    [McpServerTool, Description(
        "Update a row in a table by primary key. Pass new column values as a JSON object string.")]
    public static async Task<string> UpdateRow(
        CSharpDbService db,
        [Description("Name of the table.")] string tableName,
        [Description("Name of the primary key column.")] string pkColumn,
        [Description("Primary key value of the row to update.")] string pkValue,
        [Description("JSON object with column names and new values, e.g. {\"name\":\"Bob\",\"age\":31}.")] string valuesJson)
    {
        try
        {
            object coercedPk = long.TryParse(pkValue, out long l) ? l : pkValue;
            var values = JsonHelper.ParseAndCoerceValues(valuesJson);
            int affected = await db.UpdateRowAsync(tableName, pkColumn, coercedPk, values);
            return JsonHelper.Serialize(new { success = true, rowsAffected = affected });
        }
        catch (Exception ex)
        {
            return JsonHelper.Serialize(new { success = false, error = ex.Message });
        }
    }

    [McpServerTool, Description("Delete a row from a table by primary key.")]
    public static async Task<string> DeleteRow(
        CSharpDbService db,
        [Description("Name of the table.")] string tableName,
        [Description("Name of the primary key column.")] string pkColumn,
        [Description("Primary key value of the row to delete.")] string pkValue)
    {
        try
        {
            object coercedPk = long.TryParse(pkValue, out long l) ? l : pkValue;
            int affected = await db.DeleteRowAsync(tableName, pkColumn, coercedPk);
            return JsonHelper.Serialize(new { success = true, rowsAffected = affected });
        }
        catch (Exception ex)
        {
            return JsonHelper.Serialize(new { success = false, error = ex.Message });
        }
    }
}

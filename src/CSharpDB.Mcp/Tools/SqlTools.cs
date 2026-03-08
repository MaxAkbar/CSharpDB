using System.ComponentModel;
using CSharpDB.Client;
using CSharpDB.Mcp.Helpers;
using ModelContextProtocol.Server;

namespace CSharpDB.Mcp.Tools;

[McpServerToolType]
public static class SqlTools
{
    [McpServerTool, Description(
        "Execute arbitrary SQL against the database. " +
        "Use for SELECT queries, CREATE TABLE, CREATE INDEX, CREATE VIEW, CREATE TRIGGER, " +
        "ALTER TABLE, DROP, INSERT, UPDATE, DELETE, and any other SQL statement. " +
        "Returns query results for SELECT or rows-affected count for DML/DDL.")]
    public static async Task<string> ExecuteSql(
        ICSharpDbClient db,
        [Description("The SQL statement to execute.")] string sql)
    {
        var result = await db.ExecuteSqlAsync(sql);

        if (result.Error is not null)
        {
            return JsonHelper.Serialize(new
            {
                success = false,
                error = result.Error,
                elapsedMs = result.Elapsed.TotalMilliseconds,
            });
        }

        if (result.IsQuery)
        {
            // Format rows as list of dictionaries for better AI readability
            var rows = new List<Dictionary<string, object?>>();
            if (result.ColumnNames is not null && result.Rows is not null)
            {
                foreach (var row in result.Rows)
                {
                    var dict = new Dictionary<string, object?>();
                    for (int i = 0; i < result.ColumnNames.Length && i < row.Length; i++)
                        dict[result.ColumnNames[i]] = row[i];
                    rows.Add(dict);
                }
            }

            return JsonHelper.Serialize(new
            {
                success = true,
                isQuery = true,
                columns = result.ColumnNames,
                rows,
                rowCount = result.RowsAffected,
                elapsedMs = result.Elapsed.TotalMilliseconds,
            });
        }

        return JsonHelper.Serialize(new
        {
            success = true,
            isQuery = false,
            rowsAffected = result.RowsAffected,
            elapsedMs = result.Elapsed.TotalMilliseconds,
        });
    }

    [McpServerTool, Description(
        "Get the CSharpDB SQL syntax reference. " +
        "Call this when you get a syntax error or need to know what SQL features CSharpDB supports. " +
        "Returns every supported statement, operator, aggregate, JOIN type, and a list of what is NOT supported.")]
    public static string GetSqlReference()
    {
        return SqlReference.Text;
    }
}

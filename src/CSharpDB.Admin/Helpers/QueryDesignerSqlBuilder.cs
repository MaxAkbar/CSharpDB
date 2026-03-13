using CSharpDB.Admin.Models;
using System.Text;

namespace CSharpDB.Admin.Helpers;

public static class QueryDesignerSqlBuilder
{
    public static string Build(QueryDesignerState state)
    {
        if (state.Tables.Count == 0)
            return "-- Add tables to begin";

        var sb = new StringBuilder();

        // SELECT
        var outputRows = state.GridRows.Where(r => r.Output).ToList();
        if (outputRows.Count == 0)
        {
            sb.Append("SELECT *");
        }
        else
        {
            sb.Append("SELECT ");
            for (int i = 0; i < outputRows.Count; i++)
            {
                if (i > 0) sb.Append(",\n       ");
                var row = outputRows[i];
                sb.Append($"{row.TableName}.{row.ColumnExpr}");
                if (!string.IsNullOrWhiteSpace(row.Alias))
                    sb.Append($" AS {row.Alias}");
            }
        }

        // FROM ... JOIN
        sb.Append($"\nFROM {state.Tables[0].TableName}");
        foreach (var join in state.Joins)
        {
            string joinKeyword = join.JoinType switch
            {
                DesignerJoinType.Left  => "LEFT JOIN",
                DesignerJoinType.Right => "RIGHT JOIN",
                DesignerJoinType.Full  => "FULL OUTER JOIN",
                _                      => "INNER JOIN"
            };
            sb.Append($"\n    {joinKeyword} {join.RightTable}" +
                      $" ON {join.LeftTable}.{join.LeftColumn} = {join.RightTable}.{join.RightColumn}");
        }

        // WHERE (filter clauses AND-combined)
        var filters = state.GridRows
            .Where(r => !string.IsNullOrWhiteSpace(r.Filter))
            .ToList();
        if (filters.Count > 0)
        {
            sb.Append("\nWHERE ");
            for (int i = 0; i < filters.Count; i++)
            {
                if (i > 0) sb.Append("\n  AND ");
                var row = filters[i];
                sb.Append($"{row.TableName}.{row.ColumnExpr} {row.Filter}");
            }
        }

        // ORDER BY
        var sortRows = state.GridRows
            .Where(r => r.SortOrder.HasValue)
            .OrderBy(r => r.SortOrder!.Value)
            .ToList();
        if (sortRows.Count > 0)
        {
            sb.Append("\nORDER BY ");
            for (int i = 0; i < sortRows.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                var row = sortRows[i];
                string dir = row.SortType == DesignerSortDirection.Descending ? " DESC" : "";
                sb.Append($"{row.TableName}.{row.ColumnExpr}{dir}");
            }
        }

        return sb.ToString();
    }
}

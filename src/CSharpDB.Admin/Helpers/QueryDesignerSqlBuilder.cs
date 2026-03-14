using CSharpDB.Admin.Models;
using System.Text;

namespace CSharpDB.Admin.Helpers;

public static class QueryDesignerSqlBuilder
{
    public static string Build(QueryDesignerState state)
    {
        var tableNames = state.Tables
            .Select(t => t.TableName)
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (tableNames.Count == 0)
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
        var deferredJoinConditions = AppendFromClause(sb, tableNames, state.Joins);

        // WHERE (filter clauses AND-combined)
        var filters = deferredJoinConditions
            .Concat(state.GridRows
            .Where(r => !string.IsNullOrWhiteSpace(r.Filter))
            .Select(r => $"{r.TableName}.{r.ColumnExpr} {r.Filter}"))
            .ToList();
        if (filters.Count > 0)
        {
            sb.Append("\nWHERE ");
            for (int i = 0; i < filters.Count; i++)
            {
                if (i > 0) sb.Append("\n  AND ");
                sb.Append(filters[i]);
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

    private static List<string> AppendFromClause(
        StringBuilder sb,
        IReadOnlyList<string> tableNames,
        IReadOnlyList<DesignerJoin> joins)
    {
        sb.Append($"\nFROM {tableNames[0]}");

        var tableSet = new HashSet<string>(tableNames, StringComparer.OrdinalIgnoreCase);
        var includedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { tableNames[0] };
        var pendingJoins = joins
            .Where(join =>
                !string.IsNullOrWhiteSpace(join.LeftTable) &&
                !string.IsNullOrWhiteSpace(join.RightTable) &&
                !string.IsNullOrWhiteSpace(join.LeftColumn) &&
                !string.IsNullOrWhiteSpace(join.RightColumn) &&
                tableSet.Contains(join.LeftTable) &&
                tableSet.Contains(join.RightTable) &&
                !string.Equals(join.LeftTable, join.RightTable, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var deferredJoinConditions = new List<string>();

        while (true)
        {
            bool progressed = false;
            for (int i = 0; i < pendingJoins.Count; i++)
            {
                var join = pendingJoins[i];
                bool hasLeft = includedTables.Contains(join.LeftTable);
                bool hasRight = includedTables.Contains(join.RightTable);

                if (hasLeft && hasRight)
                {
                    if (join.JoinType.NormalizeSupported() == DesignerJoinType.Inner)
                    {
                        deferredJoinConditions.Add(BuildJoinCondition(join));
                        pendingJoins.RemoveAt(i);
                        i--;
                        progressed = true;
                    }

                    continue;
                }

                if (hasLeft == hasRight)
                    continue;

                string nextTable;
                string joinKeyword;
                if (hasLeft)
                {
                    nextTable = join.RightTable;
                    joinKeyword = ToSqlJoinKeyword(join.JoinType.NormalizeSupported());
                }
                else
                {
                    nextTable = join.LeftTable;
                    joinKeyword = ToSqlJoinKeyword(join.JoinType.ReverseForConnectedChain());
                }

                sb.Append($"\n    {joinKeyword} {nextTable} ON {BuildJoinCondition(join)}");
                includedTables.Add(nextTable);
                pendingJoins.RemoveAt(i);
                progressed = true;
                break;
            }

            if (progressed)
                continue;

            string? nextDisconnectedTable = tableNames.FirstOrDefault(table => !includedTables.Contains(table));
            if (nextDisconnectedTable is null)
                break;

            sb.Append($"\n    CROSS JOIN {nextDisconnectedTable}");
            includedTables.Add(nextDisconnectedTable);
        }

        return deferredJoinConditions;
    }

    private static string BuildJoinCondition(DesignerJoin join) =>
        $"{join.LeftTable}.{join.LeftColumn} = {join.RightTable}.{join.RightColumn}";

    private static string ToSqlJoinKeyword(DesignerJoinType joinType) => joinType switch
    {
        DesignerJoinType.Left => "LEFT JOIN",
        DesignerJoinType.Right => "RIGHT JOIN",
        _ => "INNER JOIN",
    };
}

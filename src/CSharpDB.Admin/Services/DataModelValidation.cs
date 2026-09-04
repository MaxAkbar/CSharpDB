using System.Globalization;
using CSharpDB.Admin.Models;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Services;

public sealed partial class DataModelService
{
    public async Task<IReadOnlyList<DataModelDataCheckResult>> CheckDataAsync(DataModelState state, string tableName, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        // Read the entire route-local schema; canvas membership must not hide violations.
        var live = await BuildModelAsync(autoLayoutLimit: int.MaxValue, ct: ct);
        if (state.SchemaFingerprint is not null && state.SchemaFingerprint != live.SchemaFingerprint)
            throw new InvalidOperationException("The schema changed. Refresh and review before checking existing data.");
        var liveNode = FindStateNode(live, tableName) ?? throw new InvalidOperationException("Data checks require an existing physical table.");
        if (liveNode.Kind != DataModelNodeKind.Table) throw new InvalidOperationException("External tables are read-only; data checks require a physical table.");
        var preview = new DataModelSchemaProjection(live);
        foreach (var op in state.PendingOperations) { ValidateOperation(preview.State, op); preview.Apply(op); }
        var node = preview.FromCanvas(liveNode) ?? throw new InvalidOperationException("This table is staged for removal. Remove that pending operation before checking it.");
        var checks = new List<(string Description, string? Sql, string? Skipped)>();
        const string unavailable = "Skipped: a participating table/column is new or has a pending type conversion. The engine will validate it during Apply.";

        string? Expression(DataModelNode table, string name, string? collation = null, string? alias = null)
        {
            var column = table.Columns.FirstOrDefault(column => column.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            string? original = column is null ? null : preview.LiveColumn(column);
            if (original is null) return null;
            string expression = (alias is null ? "" : alias + ".") + FormatIdentifier(original);
            string? effectiveCollation = collation ?? column!.Collation;
            return string.IsNullOrWhiteSpace(effectiveCollation) ? expression : expression + " COLLATE " + FormatIdentifier(effectiveCollation);
        }

        string tableSql = FormatIdentifier(preview.LiveName(node)!);
        foreach (var column in node.Columns.Where(column => !column.Nullable))
        {
            string? expression = Expression(node, column.Name);
            checks.Add(($"NULL values in {node.Name}.{column.Name}", expression is null ? null : $"SELECT COUNT(*) FROM {tableSql} WHERE {expression} IS NULL;", expression is null ? unavailable : null));
        }

        var candidates = node.Keys.Select(key => (Name: key.ConstraintName ?? key.Kind.ToString(), Columns: key.Columns, Collations: (IReadOnlyList<string?>)[]))
            .Concat(node.Indexes.Where(index => index.IsUnique).Select(index => (Name: index.IndexName, Columns: index.Columns, Collations: index.ColumnCollations)));
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var candidate in candidates)
        {
            string?[] expressions = candidate.Columns.Select((column, i) => Expression(node, column, i < candidate.Collations.Count ? candidate.Collations[i] : null)).ToArray();
            string identity = string.Join("\0", expressions);
            if (expressions.All(expression => expression is not null) && !seen.Add(identity)) continue;
            if (expressions.Any(expression => expression is null)) { checks.Add(($"Duplicate tuples for {candidate.Name}", null, unavailable)); continue; }
            string columns = string.Join(", ", expressions);
            string nonNull = string.Join(" AND ", expressions.Select(expression => expression + " IS NOT NULL"));
            checks.Add(($"Duplicate tuples for {candidate.Name}", $"WITH duplicates AS (SELECT {columns} FROM {tableSql} WHERE {nonNull} GROUP BY {columns} HAVING COUNT(*) > 1) SELECT COUNT(*) FROM duplicates;", null));
        }

        foreach (var relationship in preview.State.Relationships.Where(r => r.Kind != DataModelRelationshipKind.ExternalArchiveForeignKey &&
                     (r.LeftTable.Equals(node.Name, StringComparison.OrdinalIgnoreCase) || r.RightTable.Equals(node.Name, StringComparison.OrdinalIgnoreCase))))
        {
            var child = preview.Find(relationship.LeftTable); var parent = preview.Find(relationship.RightTable);
            string description = $"Orphans for {relationship.LeftTable}.{relationship.ConstraintName ?? relationship.Id}";
            if (child is null || parent is null || preview.LiveName(child) is null || preview.LiveName(parent) is null)
            { checks.Add((description, null, unavailable)); continue; }
            var mappings = relationship.EffectiveColumnPairs.Select(pair => (Child: Expression(child, pair.ChildColumn, alias: "c"), Parent: Expression(parent, pair.ParentColumn, alias: "p"))).ToArray();
            if (mappings.Any(pair => pair.Child is null || pair.Parent is null)) { checks.Add((description, null, unavailable)); continue; }
            string nonNull = string.Join(" AND ", mappings.Select(pair => pair.Child + " IS NOT NULL"));
            string equality = string.Join(" AND ", mappings.Select(pair => pair.Child + " = " + pair.Parent));
            checks.Add((description, $"SELECT COUNT(*) FROM {FormatIdentifier(preview.LiveName(child)!)} AS c WHERE {nonNull} AND NOT EXISTS (SELECT 1 FROM {FormatIdentifier(preview.LiveName(parent)!)} AS p WHERE {equality});", null));
        }
        var results = new List<DataModelDataCheckResult>();
        foreach (var check in checks)
        {
            ct.ThrowIfCancellationRequested();
            if (check.Sql is null) { results.Add(new(check.Description, 0, check.Skipped)); continue; }
            var result = await client.ExecuteSqlAsync(check.Sql, ct); ThrowIfSqlError(result);
            results.Add(new(check.Description, Convert.ToInt64(result.Rows?[0][0] ?? 0, CultureInfo.InvariantCulture)));
        }
        return results;
    }
}

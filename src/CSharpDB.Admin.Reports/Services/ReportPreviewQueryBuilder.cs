using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Services;

public static class ReportPreviewQueryBuilder
{
    public static string Build(ReportDefinition report, ReportSourceDefinition source, int rowLimit = 10001)
    {
        string baseSql = ReportSql.NormalizeSqlText(source.BaseSql);
        if (source.Kind == ReportSourceKind.SavedQuery)
            return baseSql;

        var orderParts = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sortableFields = source.Fields
            .Where(field =>
            {
                try
                {
                    ReportSql.RequireIdentifier(field.Name, nameof(field.Name));
                    return true;
                }
                catch
                {
                    return false;
                }
            })
            .ToDictionary(field => field.Name, field => ReportSql.RequireIdentifier(field.Name, nameof(field.Name)), StringComparer.OrdinalIgnoreCase);

        foreach (ReportGroupDefinition group in report.Groups)
        {
            if (sortableFields.TryGetValue(group.FieldName, out var fieldName) && fieldName is not null && seen.Add(group.FieldName))
                orderParts.Add($"{fieldName}{(group.Descending ? " DESC" : string.Empty)}");
        }

        foreach (ReportSortDefinition sort in report.Sorts)
        {
            if (sortableFields.TryGetValue(sort.FieldName, out var fieldName) && fieldName is not null && seen.Add(sort.FieldName))
                orderParts.Add($"{fieldName}{(sort.Descending ? " DESC" : string.Empty)}");
        }

        string orderSql = orderParts.Count == 0 ? string.Empty : $"\nORDER BY {string.Join(", ", orderParts)}";
        return $"""
            {baseSql}{orderSql}
            LIMIT {rowLimit};
            """;
    }
}

using System.Text.RegularExpressions;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Services;

public sealed class DbReportSourceProvider(ICSharpDbClient dbClient) : IReportSourceProvider
{
    private const string DesignerLayoutPrefix = "__designer_layout:";

    public async Task<IReadOnlyList<ReportSourceReferenceItem>> ListSourceReferencesAsync()
    {
        IReadOnlyList<string> tables = (await dbClient.GetTableNamesAsync())
            .Where(IsUserTableName)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        IReadOnlyList<string> views = (await dbClient.GetViewNamesAsync())
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        IReadOnlyList<SavedQueryDefinition> savedQueries = (await dbClient.GetSavedQueriesAsync())
            .Where(IsSupportedSavedQuery)
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return tables.Select(name => new ReportSourceReferenceItem(ReportSourceKind.Table, name, $"Table: {name}"))
            .Concat(views.Select(name => new ReportSourceReferenceItem(ReportSourceKind.View, name, $"View: {name}")))
            .Concat(savedQueries.Select(item => new ReportSourceReferenceItem(ReportSourceKind.SavedQuery, item.Name, $"Saved Query: {item.Name}")))
            .ToArray();
    }

    public async Task<ReportSourceDefinition?> GetSourceDefinitionAsync(ReportSourceReference source)
    {
        return source.Kind switch
        {
            ReportSourceKind.Table => await GetTableSourceAsync(source.Name),
            ReportSourceKind.View => await GetViewSourceAsync(source.Name),
            ReportSourceKind.SavedQuery => await GetSavedQuerySourceAsync(source.Name),
            _ => null,
        };
    }

    internal static bool IsSupportedSavedQuery(SavedQueryDefinition savedQuery)
    {
        if (savedQuery.Name.StartsWith(DesignerLayoutPrefix, StringComparison.Ordinal))
            return false;

        string sql = ReportSql.NormalizeSqlText(savedQuery.SqlText);
        if (!(sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase) || sql.StartsWith("WITH", StringComparison.OrdinalIgnoreCase)))
            return false;

        return ReportSql.IsSavedQueryParameterless(sql);
    }

    private async Task<ReportSourceDefinition?> GetTableSourceAsync(string tableName)
    {
        TableSchema? schema = await dbClient.GetTableSchemaAsync(tableName);
        if (schema is null)
            return null;

        string sourceName = ReportSql.RequireIdentifier(schema.TableName, nameof(tableName));
        return new ReportSourceDefinition(
            ReportSourceKind.Table,
            schema.TableName,
            schema.TableName,
            $"SELECT * FROM {sourceName}",
            ComputeTableSignature(schema),
            schema.Columns.Select(MapField).ToArray());
    }

    private async Task<ReportSourceDefinition?> GetViewSourceAsync(string viewName)
    {
        ViewDefinition? view = await dbClient.GetViewAsync(viewName);
        if (view is null)
            return null;

        string identifier = ReportSql.RequireIdentifier(view.Name, nameof(viewName));
        SqlExecutionResult preview = await dbClient.ExecuteSqlAsync($"""
            SELECT *
            FROM {identifier}
            LIMIT 1;
            """);
        ReportSql.ThrowIfError(preview);

        IReadOnlyList<ReportFieldDefinition> fields = MapPreviewFields(preview);
        return new ReportSourceDefinition(
            ReportSourceKind.View,
            view.Name,
            view.Name,
            $"SELECT * FROM {identifier}",
            ComputeViewSignature(view, fields),
            fields);
    }

    private async Task<ReportSourceDefinition?> GetSavedQuerySourceAsync(string queryName)
    {
        SavedQueryDefinition? saved = await dbClient.GetSavedQueryAsync(queryName);
        if (saved is null || !IsSupportedSavedQuery(saved))
            return null;

        string normalizedSql = ReportSql.NormalizeSqlText(saved.SqlText);
        SqlExecutionResult preview = await dbClient.ExecuteSqlAsync(ReportSql.AppendLimit(normalizedSql, 1));
        ReportSql.ThrowIfError(preview);

        IReadOnlyList<ReportFieldDefinition> fields = MapPreviewFields(preview);
        return new ReportSourceDefinition(
            ReportSourceKind.SavedQuery,
            saved.Name,
            saved.Name,
            normalizedSql,
            ComputeSavedQuerySignature(saved, fields),
            fields);
    }

    private static bool IsUserTableName(string name)
        => !name.StartsWith("_", StringComparison.Ordinal);

    private static ReportFieldDefinition MapField(ColumnDefinition column)
        => new(
            column.Name,
            column.Type,
            column.Nullable,
            column.IsIdentity,
            ToDisplayName(column.Name),
            new Dictionary<string, object?>
            {
                ["isPrimaryKey"] = column.IsPrimaryKey,
                ["isIdentity"] = column.IsIdentity,
                ["collation"] = column.Collation,
            });

    private static IReadOnlyList<ReportFieldDefinition> MapPreviewFields(SqlExecutionResult preview)
    {
        string[] columnNames = preview.ColumnNames ?? [];
        IReadOnlyList<Dictionary<string, object?>> rows = ReportSql.ReadRows(preview);
        return columnNames
            .Select(name => MapPreviewField(name, rows))
            .ToArray();
    }

    private static ReportFieldDefinition MapPreviewField(string columnName, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        object? sampleValue = null;
        foreach (Dictionary<string, object?> row in rows)
        {
            if (row.TryGetValue(columnName, out object? candidate) && candidate is not null)
            {
                sampleValue = candidate;
                break;
            }

            string? actualKey = row.Keys.FirstOrDefault(key => string.Equals(key, columnName, StringComparison.OrdinalIgnoreCase));
            if (actualKey is not null && row.TryGetValue(actualKey, out candidate) && candidate is not null)
            {
                sampleValue = candidate;
                break;
            }
        }

        return new ReportFieldDefinition(
            columnName,
            InferFieldType(sampleValue),
            true,
            true,
            ToDisplayName(columnName));
    }

    private static DbType InferFieldType(object? value)
    {
        object? normalized = ReportSql.NormalizeValue(value);
        return normalized switch
        {
            byte[] => DbType.Blob,
            long or int or short or byte or sbyte or uint or ulong or ushort => DbType.Integer,
            double or float or decimal => DbType.Real,
            _ => DbType.Text,
        };
    }

    private static string ComputeTableSignature(TableSchema schema)
        => ReportSql.ComputeSignature(new
        {
            Kind = "table",
            schema.TableName,
            Columns = schema.Columns.Select(column => new
            {
                column.Name,
                Type = column.Type.ToString(),
                column.Nullable,
                column.IsPrimaryKey,
                column.IsIdentity,
                column.Collation,
            }),
        });

    private static string ComputeViewSignature(ViewDefinition view, IReadOnlyList<ReportFieldDefinition> fields)
        => ReportSql.ComputeSignature(new
        {
            Kind = "view",
            view.Name,
            view.Sql,
            Fields = fields.Select(field => new
            {
                field.Name,
                Type = field.DataType.ToString(),
            }),
        });

    private static string ComputeSavedQuerySignature(SavedQueryDefinition savedQuery, IReadOnlyList<ReportFieldDefinition> fields)
        => ReportSql.ComputeSignature(new
        {
            Kind = "savedQuery",
            savedQuery.Name,
            Sql = ReportSql.NormalizeSqlText(savedQuery.SqlText),
            Fields = fields.Select(field => new
            {
                field.Name,
                Type = field.DataType.ToString(),
            }),
        });

    private static string ToDisplayName(string value)
    {
        string spaced = Regex.Replace(value.Replace('_', ' '), "([a-z0-9])([A-Z])", "$1 $2");
        return string.Join(" ", spaced.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(segment => char.ToUpperInvariant(segment[0]) + segment[1..]));
    }
}

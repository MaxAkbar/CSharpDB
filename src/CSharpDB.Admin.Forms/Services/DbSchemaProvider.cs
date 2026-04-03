using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DbSchemaProvider(ICSharpDbClient dbClient) : ISchemaProvider
{
    public async Task<FormTableDefinition?> GetTableDefinitionAsync(string tableName)
    {
        if (string.Equals(tableName, DbFormRepository.MetadataTableName, StringComparison.OrdinalIgnoreCase))
            return null;

        TableSchema? schema = await dbClient.GetTableSchemaAsync(tableName);
        if (schema is not null)
            return Map(schema);

        ViewDefinition? view = await dbClient.GetViewAsync(tableName);
        if (view is null)
            return null;

        return await MapViewAsync(view);
    }

    public async Task<IReadOnlyList<string>> ListSourceNamesAsync()
    {
        IReadOnlyList<string> tables = await ListTableNamesAsync();
        IReadOnlyList<string> views = (await dbClient.GetViewNamesAsync())
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return tables
            .Concat(views)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<IReadOnlyList<string>> ListTableNamesAsync()
    {
        return (await dbClient.GetTableNamesAsync())
            .Where(IsUserTableName)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    internal static FormTableDefinition Map(TableSchema schema)
    {
        return new FormTableDefinition(
            schema.TableName,
            ComputeSourceSchemaSignature(schema),
            schema.Columns.Select(MapField).ToArray(),
            schema.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name).ToArray(),
            schema.ForeignKeys.Select(MapForeignKey).ToArray(),
            FormSourceKind.Table);
    }

    internal static string ComputeSourceSchemaSignature(TableSchema schema)
    {
        var payload = JsonSerializer.Serialize(new
        {
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
            ForeignKeys = schema.ForeignKeys.Select(foreignKey => new
            {
                foreignKey.ConstraintName,
                foreignKey.ColumnName,
                foreignKey.ReferencedTableName,
                foreignKey.ReferencedColumnName,
                OnDelete = foreignKey.OnDelete.ToString(),
                foreignKey.SupportingIndexName,
            }),
        });

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(payload)));
    }

    internal static FormTableDefinition Map(ViewDefinition view, SqlExecutionResult preview)
    {
        string[] columnNames = preview.ColumnNames ?? [];
        IReadOnlyList<Dictionary<string, object?>> rows = FormSql.ReadRows(preview);

        return new FormTableDefinition(
            view.Name,
            ComputeSourceSchemaSignature(view, columnNames),
            columnNames.Select(columnName => MapViewField(columnName, rows)).ToArray(),
            [],
            [],
            FormSourceKind.View,
            new Dictionary<string, object?>
            {
                ["viewSql"] = view.Sql,
            });
    }

    internal static string ComputeSourceSchemaSignature(ViewDefinition view, IReadOnlyList<string> columnNames)
    {
        var payload = JsonSerializer.Serialize(new
        {
            view.Name,
            view.Sql,
            Columns = columnNames,
        });

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(payload)));
    }

    private static FormFieldDefinition MapField(ColumnDefinition column)
    {
        return new FormFieldDefinition(
            column.Name,
            MapFieldType(column.Type),
            column.Nullable,
            column.IsIdentity,
            ToDisplayName(column.Name),
            Metadata: new Dictionary<string, object?>
            {
                ["dbType"] = column.Type.ToString(),
                ["isPrimaryKey"] = column.IsPrimaryKey,
                ["isIdentity"] = column.IsIdentity,
                ["collation"] = column.Collation,
            });
    }

    private static FormForeignKeyDefinition MapForeignKey(CSharpDB.Client.Models.ForeignKeyDefinition foreignKey)
    {
        return new FormForeignKeyDefinition(
            foreignKey.ConstraintName,
            [foreignKey.ColumnName],
            foreignKey.ReferencedTableName,
            [foreignKey.ReferencedColumnName]);
    }

    private async Task<FormTableDefinition> MapViewAsync(ViewDefinition view)
    {
        string viewName = FormSql.RequireIdentifier(view.Name, nameof(view.Name));
        SqlExecutionResult preview = await dbClient.ExecuteSqlAsync($"""
            SELECT *
            FROM {viewName}
            LIMIT 1;
            """);
        FormSql.ThrowIfError(preview);
        return Map(view, preview);
    }

    private static FormFieldDefinition MapViewField(string columnName, IReadOnlyList<Dictionary<string, object?>> sampleRows)
    {
        object? sampleValue = null;
        foreach (Dictionary<string, object?> row in sampleRows)
        {
            if (TryGetCaseInsensitive(row, columnName, out object? candidate) && candidate is not null)
            {
                sampleValue = FormSql.NormalizeValue(candidate);
                break;
            }
        }

        return new FormFieldDefinition(
            columnName,
            InferViewFieldType(sampleValue),
            IsNullable: true,
            IsReadOnly: true,
            DisplayName: ToDisplayName(columnName),
            Metadata: new Dictionary<string, object?>
            {
                ["sourceKind"] = "view",
            });
    }

    private static FieldDataType MapFieldType(CSharpDB.Client.Models.DbType type) => type switch
    {
        CSharpDB.Client.Models.DbType.Integer => FieldDataType.Int64,
        CSharpDB.Client.Models.DbType.Real => FieldDataType.Double,
        CSharpDB.Client.Models.DbType.Text => FieldDataType.String,
        CSharpDB.Client.Models.DbType.Blob => FieldDataType.Blob,
        _ => FieldDataType.String,
    };

    private static FieldDataType InferViewFieldType(object? value) => value switch
    {
        byte[] => FieldDataType.Blob,
        long or int or short or sbyte or uint or ushort or ulong or byte => FieldDataType.Int64,
        double or float or decimal => FieldDataType.Double,
        _ => FieldDataType.String,
    };

    private static string ToDisplayName(string value)
    {
        string spaced = Regex.Replace(value.Replace('_', ' '), "([a-z0-9])([A-Z])", "$1 $2");
        return string.Join(" ", spaced.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(segment => char.ToUpperInvariant(segment[0]) + segment[1..]));
    }

    private static bool TryGetCaseInsensitive(IReadOnlyDictionary<string, object?> values, string key, out object? value)
    {
        if (values.TryGetValue(key, out value))
            return true;

        string? actualKey = values.Keys.FirstOrDefault(candidate => string.Equals(candidate, key, StringComparison.OrdinalIgnoreCase));
        if (actualKey is not null)
        {
            value = values[actualKey];
            return true;
        }

        value = null;
        return false;
    }

    private static bool IsUserTableName(string name)
        => !string.Equals(name, DbFormRepository.MetadataTableName, StringComparison.OrdinalIgnoreCase)
           && !name.StartsWith("_", StringComparison.Ordinal);
}

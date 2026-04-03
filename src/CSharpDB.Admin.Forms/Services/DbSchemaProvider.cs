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
        return schema is null ? null : Map(schema);
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
            schema.ForeignKeys.Select(MapForeignKey).ToArray());
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

    private static FieldDataType MapFieldType(CSharpDB.Client.Models.DbType type) => type switch
    {
        CSharpDB.Client.Models.DbType.Integer => FieldDataType.Int64,
        CSharpDB.Client.Models.DbType.Real => FieldDataType.Double,
        CSharpDB.Client.Models.DbType.Text => FieldDataType.String,
        CSharpDB.Client.Models.DbType.Blob => FieldDataType.Blob,
        _ => FieldDataType.String,
    };

    private static string ToDisplayName(string value)
    {
        string spaced = Regex.Replace(value.Replace('_', ' '), "([a-z0-9])([A-Z])", "$1 $2");
        return string.Join(" ", spaced.Split(' ', StringSplitOptions.RemoveEmptyEntries)
            .Select(segment => char.ToUpperInvariant(segment[0]) + segment[1..]));
    }

    private static bool IsUserTableName(string name)
        => !string.Equals(name, DbFormRepository.MetadataTableName, StringComparison.OrdinalIgnoreCase)
           && !name.StartsWith("_", StringComparison.Ordinal);
}

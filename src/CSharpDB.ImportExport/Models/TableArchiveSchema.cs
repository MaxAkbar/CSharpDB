using CSharpDB.Primitives;
using System.Text.Json.Serialization;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveSchema
{
    public Guid SchemaId { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<TableArchiveColumn> Columns { get; init; }
    public IReadOnlyList<TableArchiveForeignKey> ForeignKeys { get; init; } = Array.Empty<TableArchiveForeignKey>();
    public IReadOnlyList<TableArchiveCheckConstraint> CheckConstraints { get; init; } = Array.Empty<TableArchiveCheckConstraint>();
    public IReadOnlyList<TableArchiveKeyConstraint> KeyConstraints { get; init; } = Array.Empty<TableArchiveKeyConstraint>();
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<TableArchiveSecondaryIndex>? SecondaryIndexes { get; init; }
    public long NextRowId { get; init; }

    public static TableArchiveSchema FromTableSchema(
        TableSchema schema,
        IReadOnlyList<IndexSchema>? secondaryIndexes = null) => new()
    {
        SchemaId = schema.SchemaId,
        TableName = schema.TableName,
        Columns = schema.Columns.Select(TableArchiveColumn.FromColumn).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(TableArchiveForeignKey.FromForeignKey).ToArray(),
        CheckConstraints = schema.CheckConstraints.Select(TableArchiveCheckConstraint.FromCheckConstraint).ToArray(),
        KeyConstraints = schema.KeyConstraints.Select(TableArchiveKeyConstraint.FromKeyConstraint).ToArray(),
        SecondaryIndexes = NullIfEmpty((secondaryIndexes ?? Array.Empty<IndexSchema>())
            .Where(index =>
                index.Kind == IndexKind.Sql &&
                index.State == IndexState.Ready &&
                string.Equals(index.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase))
            .Select(TableArchiveSecondaryIndex.FromIndex)
            .ToArray()),
        NextRowId = schema.NextRowId,
    };

    public TableSchema ToTableSchema(string? tableNameOverride = null)
    {
        string targetTableName = string.IsNullOrWhiteSpace(tableNameOverride) ? TableName : tableNameOverride;
        return new TableSchema
        {
            SchemaId = SchemaId,
            TableName = targetTableName,
            Columns = Columns.Select(static column => column.ToColumn()).ToArray(),
            ForeignKeys = ForeignKeys.Select(foreignKey =>
                foreignKey.ToForeignKey(
                    string.Equals(
                        foreignKey.ReferencedTableName,
                        TableName,
                        StringComparison.OrdinalIgnoreCase)
                        ? targetTableName
                        : null)).ToArray(),
            CheckConstraints = CheckConstraints.Select(static check => check.ToCheckConstraint()).ToArray(),
            KeyConstraints = KeyConstraints.Select(static key => key.ToKeyConstraint()).ToArray(),
            NextRowId = NextRowId,
        };
    }

    public IReadOnlyList<IndexSchema> ToSecondaryIndexes(string? tableNameOverride = null)
    {
        string targetTableName = string.IsNullOrWhiteSpace(tableNameOverride) ? TableName : tableNameOverride;
        return (SecondaryIndexes ?? Array.Empty<TableArchiveSecondaryIndex>())
            .Select(index => index.ToIndex(targetTableName))
            .ToArray();
    }

    private static IReadOnlyList<T>? NullIfEmpty<T>(IReadOnlyList<T> values) =>
        values.Count == 0 ? null : values;
}

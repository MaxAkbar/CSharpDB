using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveForeignKey
{
    public required string ConstraintName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public IReadOnlyList<string> ColumnNames { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> ReferencedColumnNames { get; init; } = Array.Empty<string>();
    public ForeignKeyOnDeleteAction OnDelete { get; init; }
    public required string SupportingIndexName { get; init; }

    public static TableArchiveForeignKey FromForeignKey(ForeignKeyDefinition foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        ColumnNames = foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames.ToArray() : [foreignKey.ColumnName],
        ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0 ? foreignKey.ReferencedColumnNames.ToArray() : [foreignKey.ReferencedColumnName],
        OnDelete = foreignKey.OnDelete,
        SupportingIndexName = foreignKey.SupportingIndexName,
    };

    public ForeignKeyDefinition ToForeignKey() => new()
    {
        ConstraintName = ConstraintName,
        ColumnName = ColumnName,
        ReferencedTableName = ReferencedTableName,
        ReferencedColumnName = ReferencedColumnName,
        ColumnNames = ColumnNames.Count > 0 ? ColumnNames.ToArray() : [ColumnName],
        ReferencedColumnNames = ReferencedColumnNames.Count > 0 ? ReferencedColumnNames.ToArray() : [ReferencedColumnName],
        OnDelete = OnDelete,
        SupportingIndexName = SupportingIndexName,
    };
}

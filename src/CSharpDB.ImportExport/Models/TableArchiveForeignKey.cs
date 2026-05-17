using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveForeignKey
{
    public required string ConstraintName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public ForeignKeyOnDeleteAction OnDelete { get; init; }
    public required string SupportingIndexName { get; init; }

    public static TableArchiveForeignKey FromForeignKey(ForeignKeyDefinition foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        OnDelete = foreignKey.OnDelete,
        SupportingIndexName = foreignKey.SupportingIndexName,
    };

    public ForeignKeyDefinition ToForeignKey() => new()
    {
        ConstraintName = ConstraintName,
        ColumnName = ColumnName,
        ReferencedTableName = ReferencedTableName,
        ReferencedColumnName = ReferencedColumnName,
        OnDelete = OnDelete,
        SupportingIndexName = SupportingIndexName,
    };
}

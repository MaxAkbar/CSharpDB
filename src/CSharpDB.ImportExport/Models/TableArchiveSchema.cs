using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveSchema
{
    public required string TableName { get; init; }
    public required IReadOnlyList<TableArchiveColumn> Columns { get; init; }
    public IReadOnlyList<TableArchiveForeignKey> ForeignKeys { get; init; } = Array.Empty<TableArchiveForeignKey>();
    public IReadOnlyList<TableArchiveCheckConstraint> CheckConstraints { get; init; } = Array.Empty<TableArchiveCheckConstraint>();
    public IReadOnlyList<TableArchiveKeyConstraint> KeyConstraints { get; init; } = Array.Empty<TableArchiveKeyConstraint>();
    public long NextRowId { get; init; }

    public static TableArchiveSchema FromTableSchema(TableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(TableArchiveColumn.FromColumn).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(TableArchiveForeignKey.FromForeignKey).ToArray(),
        CheckConstraints = schema.CheckConstraints.Select(TableArchiveCheckConstraint.FromCheckConstraint).ToArray(),
        KeyConstraints = schema.KeyConstraints.Select(TableArchiveKeyConstraint.FromKeyConstraint).ToArray(),
        NextRowId = schema.NextRowId,
    };

    public TableSchema ToTableSchema(string? tableNameOverride = null) => new()
    {
        TableName = string.IsNullOrWhiteSpace(tableNameOverride) ? TableName : tableNameOverride,
        Columns = Columns.Select(static column => column.ToColumn()).ToArray(),
        ForeignKeys = ForeignKeys.Select(static foreignKey => foreignKey.ToForeignKey()).ToArray(),
        CheckConstraints = CheckConstraints.Select(static check => check.ToCheckConstraint()).ToArray(),
        KeyConstraints = KeyConstraints.Select(static key => key.ToKeyConstraint()).ToArray(),
        NextRowId = NextRowId,
    };
}

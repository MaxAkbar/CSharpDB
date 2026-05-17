using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveSchema
{
    public required string TableName { get; init; }
    public required IReadOnlyList<TableArchiveColumn> Columns { get; init; }
    public IReadOnlyList<TableArchiveForeignKey> ForeignKeys { get; init; } = Array.Empty<TableArchiveForeignKey>();
    public long NextRowId { get; init; }

    public static TableArchiveSchema FromTableSchema(TableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(TableArchiveColumn.FromColumn).ToArray(),
        ForeignKeys = schema.ForeignKeys.Select(TableArchiveForeignKey.FromForeignKey).ToArray(),
        NextRowId = schema.NextRowId,
    };

    public TableSchema ToTableSchema(string? tableNameOverride = null) => new()
    {
        TableName = string.IsNullOrWhiteSpace(tableNameOverride) ? TableName : tableNameOverride,
        Columns = Columns.Select(static column => column.ToColumn()).ToArray(),
        ForeignKeys = ForeignKeys.Select(static foreignKey => foreignKey.ToForeignKey()).ToArray(),
        NextRowId = NextRowId,
    };
}

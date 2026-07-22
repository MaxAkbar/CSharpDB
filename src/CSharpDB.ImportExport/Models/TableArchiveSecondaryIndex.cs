using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

/// <summary>
/// A user-created SQL index belonging to the single table represented by an archive.
/// This is deliberately separate from <see cref="TableArchiveIndexManifest"/>, whose
/// entries describe physical lookup structures inside the archive file itself.
/// </summary>
public sealed class TableArchiveSecondaryIndex
{
    public required string Name { get; init; }

    public required IReadOnlyList<string> Columns { get; init; }

    public IReadOnlyList<string?> ColumnCollations { get; init; } = Array.Empty<string?>();

    public bool IsUnique { get; init; }

    public static TableArchiveSecondaryIndex FromIndex(IndexSchema index)
    {
        ArgumentNullException.ThrowIfNull(index);
        if (index.Kind != IndexKind.Sql)
            throw new ArgumentException("Only user-created SQL indexes can be archived as secondary indexes.", nameof(index));
        if (index.State != IndexState.Ready)
            throw new ArgumentException("Only ready SQL indexes can be archived.", nameof(index));

        return new TableArchiveSecondaryIndex
        {
            Name = index.IndexName,
            Columns = index.Columns.ToArray(),
            ColumnCollations = index.ColumnCollations.ToArray(),
            IsUnique = index.IsUnique,
        };
    }

    public IndexSchema ToIndex(string tableName) => new()
    {
        IndexName = Name,
        TableName = tableName,
        Columns = Columns.ToArray(),
        ColumnCollations = ColumnCollations.ToArray(),
        IsUnique = IsUnique,
        Kind = IndexKind.Sql,
        State = IndexState.Ready,
    };
}

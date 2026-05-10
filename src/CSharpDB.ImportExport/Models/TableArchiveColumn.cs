using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveColumn
{
    public required string Name { get; init; }
    public required DbType Type { get; init; }
    public bool Nullable { get; init; }
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
    public string? Collation { get; init; }

    public static TableArchiveColumn FromColumn(ColumnDefinition column) => new()
    {
        Name = column.Name,
        Type = column.Type,
        Nullable = column.Nullable,
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        Collation = column.Collation,
    };

    public ColumnDefinition ToColumn() => new()
    {
        Name = Name,
        Type = Type,
        Nullable = Nullable,
        IsPrimaryKey = IsPrimaryKey,
        IsIdentity = IsIdentity,
        Collation = Collation,
    };
}

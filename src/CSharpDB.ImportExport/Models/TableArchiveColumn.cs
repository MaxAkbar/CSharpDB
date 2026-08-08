using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveColumn
{
    public Guid SchemaId { get; init; }
    public required string Name { get; init; }
    public required DbType Type { get; init; }
    /// <summary>
    /// The declared logical SQL type and facets. This is absent only for
    /// archives produced from legacy schemas which predate logical types.
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public SqlTypeDescriptor? DeclaredType { get; init; }
    public bool Nullable { get; init; }
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
    public bool IsRowVersion { get; init; }
    public string? Collation { get; init; }
    public string? DefaultSql { get; init; }

    public static TableArchiveColumn FromColumn(ColumnDefinition column) => new()
    {
        SchemaId = column.SchemaId,
        Name = column.Name,
        Type = column.Type,
        DeclaredType = column.DeclaredType,
        Nullable = column.Nullable,
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Collation = column.Collation,
        DefaultSql = column.DefaultSql,
    };

    public ColumnDefinition ToColumn() => new()
    {
        SchemaId = SchemaId,
        Name = Name,
        Type = Type,
        DeclaredType = DeclaredType,
        Nullable = Nullable,
        IsPrimaryKey = IsPrimaryKey,
        IsIdentity = IsIdentity,
        IsRowVersion = IsRowVersion,
        Collation = Collation,
        DefaultSql = DefaultSql,
    };

    internal TableArchiveColumn WithDeclaredType(SqlTypeDescriptor declaredType) => new()
    {
        SchemaId = SchemaId,
        Name = Name,
        Type = Type,
        DeclaredType = declaredType,
        Nullable = Nullable,
        IsPrimaryKey = IsPrimaryKey,
        IsIdentity = IsIdentity,
        IsRowVersion = IsRowVersion,
        Collation = Collation,
        DefaultSql = DefaultSql,
    };
}

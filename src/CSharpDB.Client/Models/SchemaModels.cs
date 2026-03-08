namespace CSharpDB.Client.Models;

public enum DbType
{
    Integer,
    Real,
    Text,
    Blob,
}

public sealed class ColumnDefinition
{
    public required string Name { get; init; }
    public required DbType Type { get; init; }
    public bool Nullable { get; init; } = true;
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
}

public sealed class TableSchema
{
    public required string TableName { get; init; }
    public required IReadOnlyList<ColumnDefinition> Columns { get; init; }
}

public sealed class IndexSchema
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public bool IsUnique { get; init; }
}

public sealed class ViewDefinition
{
    public required string Name { get; init; }
    public required string Sql { get; init; }
}

public enum TriggerTiming
{
    Before,
    After,
}

public enum TriggerEvent
{
    Insert,
    Update,
    Delete,
}

public sealed class TriggerSchema
{
    public required string TriggerName { get; init; }
    public required string TableName { get; init; }
    public required TriggerTiming Timing { get; init; }
    public required TriggerEvent Event { get; init; }
    public required string BodySql { get; init; }
}

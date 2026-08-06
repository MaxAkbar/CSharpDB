namespace CSharpDB.Client.Models;

public enum DbType
{
    Integer,
    Real,
    Text,
    Blob,
    Decimal,
}

/// <summary>
/// Logical SQL type declared for a column. Multiple logical types can share
/// the same compact physical <see cref="DbType"/> representation.
/// </summary>
public enum SqlTypeKind : byte
{
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Decimal,
    Char,
    VarChar,
    Text,
    Binary,
    VarBinary,
    Blob,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampWithTimeZone,
    IntervalYearToMonth,
    IntervalDayToSecond,
    Json,
    Xml,
    Bit,
    VarBit,
}

/// <summary>
/// Transport-safe description of a declared SQL type and its optional facets.
/// </summary>
public sealed class SqlTypeDescriptor
{
    public required SqlTypeKind Kind { get; init; }
    public int? Length { get; init; }
    public int? Precision { get; init; }
    public int? Scale { get; init; }
    public int? FractionalSecondsPrecision { get; init; }

    public DbType StorageType => Kind switch
    {
        SqlTypeKind.Boolean or
        SqlTypeKind.TinyInt or
        SqlTypeKind.SmallInt or
        SqlTypeKind.Integer or
        SqlTypeKind.BigInt => DbType.Integer,
        SqlTypeKind.Real or SqlTypeKind.Double => DbType.Real,
        SqlTypeKind.Decimal => DbType.Decimal,
        SqlTypeKind.Binary or
        SqlTypeKind.VarBinary or
        SqlTypeKind.Blob or
        SqlTypeKind.Uuid or
        SqlTypeKind.Bit or
        SqlTypeKind.VarBit => DbType.Blob,
        _ => DbType.Text,
    };

    public static SqlTypeDescriptor FromLegacy(DbType type) => new()
    {
        Kind = type switch
        {
            DbType.Integer => SqlTypeKind.BigInt,
            DbType.Real => SqlTypeKind.Double,
            DbType.Text => SqlTypeKind.Text,
            DbType.Blob => SqlTypeKind.Blob,
            DbType.Decimal => SqlTypeKind.Decimal,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unsupported database type."),
        },
    };

    /// <summary>Returns the stable canonical SQL spelling of this logical type.</summary>
    public string ToSql() => Kind switch
    {
        SqlTypeKind.Boolean => "BOOLEAN",
        SqlTypeKind.TinyInt => "TINYINT",
        SqlTypeKind.SmallInt => "SMALLINT",
        SqlTypeKind.Integer => "INTEGER",
        SqlTypeKind.BigInt => "BIGINT",
        SqlTypeKind.Real => "REAL",
        SqlTypeKind.Double => "DOUBLE PRECISION",
        SqlTypeKind.Decimal => FormatPrecisionAndScale("DECIMAL"),
        SqlTypeKind.Char => FormatLength("CHAR"),
        SqlTypeKind.VarChar => FormatLength("VARCHAR"),
        SqlTypeKind.Text => "TEXT",
        SqlTypeKind.Binary => FormatLength("BINARY"),
        SqlTypeKind.VarBinary => FormatLength("VARBINARY"),
        SqlTypeKind.Blob => "BLOB",
        SqlTypeKind.Uuid => "UUID",
        SqlTypeKind.Date => "DATE",
        SqlTypeKind.Time => FormatFractionalSeconds("TIME"),
        SqlTypeKind.Timestamp => FormatFractionalSeconds("DATETIME2"),
        SqlTypeKind.TimestampWithTimeZone => FormatFractionalSeconds("DATETIMEOFFSET"),
        SqlTypeKind.IntervalYearToMonth => "INTERVAL YEAR TO MONTH",
        SqlTypeKind.IntervalDayToSecond =>
            FractionalSecondsPrecision is int fractionalSecondsPrecision
                ? $"INTERVAL DAY TO SECOND({fractionalSecondsPrecision})"
                : "INTERVAL DAY TO SECOND",
        SqlTypeKind.Json => "JSON",
        SqlTypeKind.Xml => "XML",
        SqlTypeKind.Bit => FormatLength("BIT"),
        SqlTypeKind.VarBit => FormatLength("BIT VARYING"),
        _ => throw new InvalidOperationException($"Unsupported SQL type kind '{Kind}'."),
    };

    public override string ToString() => ToSql();

    private string FormatLength(string name) =>
        Length is int length ? $"{name}({length})" : name;

    private string FormatPrecisionAndScale(string name)
    {
        if (Precision is not int precision)
            return name;
        return Scale is int scale
            ? $"{name}({precision},{scale})"
            : $"{name}({precision})";
    }

    private string FormatFractionalSeconds(string name) =>
        FractionalSecondsPrecision is int precision
            ? $"{name}({precision})"
            : name;
}

public sealed class ColumnDefinition
{
    public Guid SchemaId { get; init; }
    public required string Name { get; init; }
    public required DbType Type { get; init; }
    public SqlTypeDescriptor? DeclaredType { get; init; }
    public SqlTypeDescriptor EffectiveType => DeclaredType ?? SqlTypeDescriptor.FromLegacy(Type);
    public bool Nullable { get; init; } = true;
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
    public bool IsRowVersion { get; init; }
    public string? Collation { get; init; }
    public string? DefaultSql { get; init; }
}

public enum ForeignKeyOnDeleteAction
{
    Restrict = 0,
    Cascade = 1,
    NoAction = 2,
    SetNull = 3,
    SetDefault = 4,
}

public sealed class ForeignKeyDefinition
{
    public Guid SchemaId { get; init; }
    public IReadOnlyList<Guid> ColumnSchemaIds { get; init; } = Array.Empty<Guid>();
    public Guid ReferencedTableSchemaId { get; init; }
    public IReadOnlyList<Guid> ReferencedColumnSchemaIds { get; init; } = Array.Empty<Guid>();
    public Guid ReferencedKeySchemaId { get; init; }
    public required string ConstraintName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    /// <summary>Ordered child columns; legacy payloads expose the scalar column as the only entry.</summary>
    public IReadOnlyList<string> ColumnNames { get; init; } = Array.Empty<string>();
    /// <summary>Ordered referenced columns; legacy payloads expose the scalar column as the only entry.</summary>
    public IReadOnlyList<string> ReferencedColumnNames { get; init; } = Array.Empty<string>();
    public ForeignKeyOnDeleteAction OnDelete { get; init; } = ForeignKeyOnDeleteAction.Restrict;
    public ForeignKeyOnDeleteAction OnUpdate { get; init; } = ForeignKeyOnDeleteAction.Restrict;
    public required string SupportingIndexName { get; init; }
}

public enum KeyConstraintKind
{
    PrimaryKey = 0,
    Unique = 1,
}

public sealed class KeyConstraintDefinition
{
    public Guid SchemaId { get; init; }
    public string? ConstraintName { get; init; }
    public KeyConstraintKind Kind { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public string? BackingIndexName { get; init; }
}

public sealed class CheckConstraintDefinition
{
    public Guid SchemaId { get; init; }
    public string? ConstraintName { get; init; }
    public required string ExpressionSql { get; init; }
    public string? ColumnName { get; init; }
}

public sealed class TableSchema
{
    public Guid SchemaId { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<ColumnDefinition> Columns { get; init; }
    public IReadOnlyList<ForeignKeyDefinition> ForeignKeys { get; init; } = Array.Empty<ForeignKeyDefinition>();
    public IReadOnlyList<CheckConstraintDefinition> CheckConstraints { get; init; } = Array.Empty<CheckConstraintDefinition>();
    public IReadOnlyList<KeyConstraintDefinition> KeyConstraints { get; init; } = Array.Empty<KeyConstraintDefinition>();
    public long NextRowId { get; init; }
}

public sealed class IndexSchema
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public IReadOnlyList<string?> ColumnCollations { get; init; } = Array.Empty<string?>();
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

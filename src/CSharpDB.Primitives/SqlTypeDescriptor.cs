using System.ComponentModel;

namespace CSharpDB.Primitives;

/// <summary>
/// Immutable description of a declared SQL type and its optional facets.
/// </summary>
public sealed record SqlTypeDescriptor
{
    public const int MaximumDecimalPrecision = 18;
    // The temporal execution and public ADO surfaces use CLR tick precision
    // (100 nanoseconds), so all fractional-second SQL types share this limit.
    public const int MaximumFractionalSecondsPrecision = 7;

    public SqlTypeDescriptor(
        SqlTypeKind kind,
        int? length = null,
        int? precision = null,
        int? scale = null,
        int? fractionalSecondsPrecision = null)
    {
        if (!Enum.IsDefined(kind))
            throw new InvalidEnumArgumentException(nameof(kind), (int)kind, typeof(SqlTypeKind));

        ValidateFacets(kind, length, precision, scale, fractionalSecondsPrecision);

        Kind = kind;
        Length = length;
        Precision = precision;
        Scale = scale;
        FractionalSecondsPrecision = fractionalSecondsPrecision;
    }

    public SqlTypeKind Kind { get; }
    public int? Length { get; }
    public int? Precision { get; }
    public int? Scale { get; }
    public int? FractionalSecondsPrecision { get; }

    /// <summary>The physical record representation used for values of this type.</summary>
    public DbType StorageType => Kind switch
    {
        SqlTypeKind.Boolean or
        SqlTypeKind.TinyInt or
        SqlTypeKind.SmallInt or
        SqlTypeKind.Integer or
        SqlTypeKind.BigInt => DbType.Integer,

        SqlTypeKind.Real or
        SqlTypeKind.Double => DbType.Real,

        SqlTypeKind.Decimal => DbType.Decimal,

        SqlTypeKind.Char or
        SqlTypeKind.VarChar or
        SqlTypeKind.Text or
        SqlTypeKind.Date or
        SqlTypeKind.Time or
        SqlTypeKind.Timestamp or
        SqlTypeKind.TimestampWithTimeZone or
        SqlTypeKind.IntervalYearToMonth or
        SqlTypeKind.IntervalDayToSecond or
        SqlTypeKind.Json or
        SqlTypeKind.Xml => DbType.Text,

        SqlTypeKind.Binary or
        SqlTypeKind.VarBinary or
        SqlTypeKind.Blob or
        SqlTypeKind.Uuid or
        SqlTypeKind.Bit or
        SqlTypeKind.VarBit => DbType.Blob,

        _ => throw new InvalidOperationException($"Unsupported SQL type kind '{Kind}'."),
    };

    public static SqlTypeDescriptor Create(
        SqlTypeKind kind,
        int? length = null,
        int? precision = null,
        int? scale = null,
        int? fractionalSecondsPrecision = null) =>
        new(kind, length, precision, scale, fractionalSecondsPrecision);

    /// <summary>
    /// Creates the logical compatibility view for metadata written before
    /// declared SQL types were persisted.
    /// </summary>
    public static SqlTypeDescriptor FromLegacy(DbType type) => type switch
    {
        DbType.Integer => new(SqlTypeKind.BigInt),
        DbType.Real => new(SqlTypeKind.Double),
        DbType.Text => new(SqlTypeKind.Text),
        DbType.Blob => new(SqlTypeKind.Blob),
        DbType.Decimal => new(SqlTypeKind.Decimal),
        _ => throw new ArgumentOutOfRangeException(
            nameof(type),
            type,
            "NULL and unknown physical types do not have a declared SQL column type."),
    };

    /// <summary>Returns a stable canonical SQL spelling for this descriptor.</summary>
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
        // Timestamp remains the persisted enum name for compatibility. Its
        // canonical SQL spelling is DATETIME2 now that bare TIMESTAMP denotes
        // a generated rowversion column.
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

    private static void ValidateFacets(
        SqlTypeKind kind,
        int? length,
        int? precision,
        int? scale,
        int? fractionalSecondsPrecision)
    {
        bool supportsLength = kind is
            SqlTypeKind.Char or
            SqlTypeKind.VarChar or
            SqlTypeKind.Binary or
            SqlTypeKind.VarBinary or
            SqlTypeKind.Bit or
            SqlTypeKind.VarBit;
        if (length.HasValue && !supportsLength)
            throw new ArgumentException($"SQL type {kind} does not accept a length facet.", nameof(length));
        if (length is <= 0)
            throw new ArgumentOutOfRangeException(nameof(length), length, "Length must be positive.");

        if (kind != SqlTypeKind.Decimal && (precision.HasValue || scale.HasValue))
        {
            throw new ArgumentException(
                $"SQL type {kind} does not accept precision or scale facets.",
                precision.HasValue ? nameof(precision) : nameof(scale));
        }

        if (kind == SqlTypeKind.Decimal)
        {
            if (precision is < 1 or > MaximumDecimalPrecision)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(precision),
                    precision,
                    $"Decimal precision must be between 1 and {MaximumDecimalPrecision}.");
            }
            if (scale.HasValue && !precision.HasValue)
                throw new ArgumentException("Decimal scale requires a precision.", nameof(scale));
            if (scale is < 0)
                throw new ArgumentOutOfRangeException(nameof(scale), scale, "Decimal scale cannot be negative.");
            if (scale.HasValue && scale > precision)
                throw new ArgumentOutOfRangeException(nameof(scale), scale, "Decimal scale cannot exceed precision.");
        }

        bool supportsFractionalSeconds = kind is
            SqlTypeKind.Time or
            SqlTypeKind.Timestamp or
            SqlTypeKind.TimestampWithTimeZone or
            SqlTypeKind.IntervalDayToSecond;
        if (fractionalSecondsPrecision.HasValue && !supportsFractionalSeconds)
        {
            throw new ArgumentException(
                $"SQL type {kind} does not accept fractional-seconds precision.",
                nameof(fractionalSecondsPrecision));
        }
        if (fractionalSecondsPrecision is < 0 or > MaximumFractionalSecondsPrecision)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fractionalSecondsPrecision),
                fractionalSecondsPrecision,
                $"Fractional-seconds precision must be between 0 and {MaximumFractionalSecondsPrecision}.");
        }
    }
}

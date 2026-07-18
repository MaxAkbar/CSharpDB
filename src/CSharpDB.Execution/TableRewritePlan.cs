using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Describes a row-preserving table rewrite. The first slice supports copying
/// source columns and supplying constant values for newly materialized columns;
/// later type/collation changes can extend the mapping with conversions.
/// </summary>
internal sealed class TableRewritePlan
{
    private readonly TableRewriteColumnMapping[] _columns;
    private readonly int? _targetRowIdSourceOrdinal;

    public TableRewritePlan(
        TableSchema sourceSchema,
        TableSchema targetSchema,
        IReadOnlyList<TableRewriteColumnMapping> columns,
        int? targetRowIdSourceOrdinal = null)
    {
        ArgumentNullException.ThrowIfNull(sourceSchema);
        ArgumentNullException.ThrowIfNull(targetSchema);
        ArgumentNullException.ThrowIfNull(columns);

        if (!string.Equals(sourceSchema.TableName, targetSchema.TableName, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("A table rewrite cannot rename the table.", nameof(targetSchema));
        if (columns.Count != targetSchema.Columns.Count)
            throw new ArgumentException("Every target column requires a rewrite mapping.", nameof(columns));

        for (int i = 0; i < columns.Count; i++)
        {
            TableRewriteColumnMapping mapping = columns[i];
            int sourceOrdinal = mapping.SourceOrdinal;
            if (sourceOrdinal < TableRewriteColumnMapping.ConstantOrdinal ||
                sourceOrdinal >= sourceSchema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(columns),
                    $"Target column '{targetSchema.Columns[i].Name}' has invalid source ordinal {sourceOrdinal}.");
            }

            if (mapping.Conversion == TableRewriteValueConversion.IntegerToReal &&
                (sourceSchema.Columns[sourceOrdinal].Type != DbType.Integer ||
                 targetSchema.Columns[i].Type != DbType.Real))
            {
                throw new ArgumentException(
                    $"Target column '{targetSchema.Columns[i].Name}' has an invalid INTEGER-to-REAL rewrite mapping.",
                    nameof(columns));
            }

            if (mapping.Conversion == TableRewriteValueConversion.RealToInteger &&
                (sourceSchema.Columns[sourceOrdinal].Type != DbType.Real ||
                 targetSchema.Columns[i].Type != DbType.Integer))
            {
                throw new ArgumentException(
                    $"Target column '{targetSchema.Columns[i].Name}' has an invalid REAL-to-INTEGER rewrite mapping.",
                    nameof(columns));
            }
        }

        if (targetRowIdSourceOrdinal is int rowIdOrdinal &&
            (rowIdOrdinal < 0 || rowIdOrdinal >= sourceSchema.Columns.Count))
        {
            throw new ArgumentOutOfRangeException(
                nameof(targetRowIdSourceOrdinal));
        }

        SourceSchema = sourceSchema;
        TargetSchema = targetSchema;
        _columns = columns.ToArray();
        _targetRowIdSourceOrdinal = targetRowIdSourceOrdinal;
    }

    public TableSchema SourceSchema { get; }

    public TableSchema TargetSchema { get; }

    public long RewriteRowId(long sourceRowId, ReadOnlySpan<DbValue> sourceRow)
    {
        if (_targetRowIdSourceOrdinal is not int sourceOrdinal)
            return sourceRowId;

        if (sourceOrdinal >= sourceRow.Length)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Rekey source row has {sourceRow.Length} values, but ordinal {sourceOrdinal} is required.");
        }

        DbValue value = sourceRow[sourceOrdinal];
        if (value.Type != DbType.Integer)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                "A physical INTEGER primary-key rewrite encountered a NULL or non-INTEGER key value.");
        }

        return value.AsInteger;
    }

    public DbValue[] RewriteRow(ReadOnlySpan<DbValue> sourceRow)
    {
        var targetRow = new DbValue[_columns.Length];
        for (int i = 0; i < _columns.Length; i++)
        {
            TableRewriteColumnMapping mapping = _columns[i];
            DbValue sourceValue = mapping.SourceOrdinal == TableRewriteColumnMapping.ConstantOrdinal
                ? mapping.ConstantValue
                : mapping.SourceOrdinal < sourceRow.Length
                    ? sourceRow[mapping.SourceOrdinal]
                    : DbValue.Null;
            targetRow[i] = ConvertValue(sourceValue, mapping.Conversion, TargetSchema.Columns[i]);
        }

        return targetRow;
    }

    private DbValue ConvertValue(
        DbValue value,
        TableRewriteValueConversion conversion,
        ColumnDefinition targetColumn)
    {
        if (value.IsNull || conversion == TableRewriteValueConversion.None)
            return value;

        const long largestConsecutivelyRepresentableInteger = 9_007_199_254_740_992L;
        const double minimumInt64Inclusive = -9_223_372_036_854_775_808d;
        const double maximumInt64Exclusive = 9_223_372_036_854_775_808d;
        string qualifiedColumn = $"{TargetSchema.TableName}.{targetColumn.Name}";

        switch (conversion)
        {
            case TableRewriteValueConversion.IntegerToReal:
                if (value.Type == DbType.Real)
                    return value;
                if (value.Type != DbType.Integer)
                {
                    throw new CSharpDbException(
                        ErrorCode.TypeMismatch,
                        $"Cannot convert stored {value.Type} value in column '{qualifiedColumn}' to REAL.");
                }

                long integer = value.AsInteger;
                if (integer is < -largestConsecutivelyRepresentableInteger or > largestConsecutivelyRepresentableInteger)
                {
                    throw new CSharpDbException(
                        ErrorCode.TypeMismatch,
                        $"Cannot convert value {integer} in column '{qualifiedColumn}' to REAL without losing integer precision.");
                }

                return DbValue.FromReal(integer);

            case TableRewriteValueConversion.RealToInteger:
                if (value.Type == DbType.Integer)
                    return value;
                if (value.Type != DbType.Real)
                {
                    throw new CSharpDbException(
                        ErrorCode.TypeMismatch,
                        $"Cannot convert stored {value.Type} value in column '{qualifiedColumn}' to INTEGER.");
                }

                double real = value.AsReal;
                if (!double.IsFinite(real) ||
                    Math.Truncate(real) != real ||
                    real < minimumInt64Inclusive ||
                    real >= maximumInt64Exclusive)
                {
                    throw new CSharpDbException(
                        ErrorCode.TypeMismatch,
                        $"Cannot convert value {real:R} in column '{qualifiedColumn}' to an exact 64-bit INTEGER.");
                }

                return DbValue.FromInteger((long)real);

            default:
                throw new ArgumentOutOfRangeException(nameof(conversion), conversion, null);
        }
    }
}

internal enum TableRewriteValueConversion
{
    None = 0,
    IntegerToReal = 1,
    RealToInteger = 2,
}

internal readonly record struct TableRewriteColumnMapping(
    int SourceOrdinal,
    DbValue ConstantValue,
    TableRewriteValueConversion Conversion)
{
    public const int ConstantOrdinal = -1;

    public static TableRewriteColumnMapping FromSource(int sourceOrdinal) =>
        new(sourceOrdinal, DbValue.Null, TableRewriteValueConversion.None);

    public static TableRewriteColumnMapping ConvertFromSource(
        int sourceOrdinal,
        TableRewriteValueConversion conversion) =>
        new(sourceOrdinal, DbValue.Null, conversion);

    public static TableRewriteColumnMapping Constant(DbValue value) =>
        new(ConstantOrdinal, value, TableRewriteValueConversion.None);
}

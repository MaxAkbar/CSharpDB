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

    public TableRewritePlan(
        TableSchema sourceSchema,
        TableSchema targetSchema,
        IReadOnlyList<TableRewriteColumnMapping> columns)
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
            int sourceOrdinal = columns[i].SourceOrdinal;
            if (sourceOrdinal < TableRewriteColumnMapping.ConstantOrdinal ||
                sourceOrdinal >= sourceSchema.Columns.Count)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(columns),
                    $"Target column '{targetSchema.Columns[i].Name}' has invalid source ordinal {sourceOrdinal}.");
            }
        }

        SourceSchema = sourceSchema;
        TargetSchema = targetSchema;
        _columns = columns.ToArray();
    }

    public TableSchema SourceSchema { get; }

    public TableSchema TargetSchema { get; }

    public DbValue[] RewriteRow(ReadOnlySpan<DbValue> sourceRow)
    {
        var targetRow = new DbValue[_columns.Length];
        for (int i = 0; i < _columns.Length; i++)
        {
            TableRewriteColumnMapping mapping = _columns[i];
            targetRow[i] = mapping.SourceOrdinal == TableRewriteColumnMapping.ConstantOrdinal
                ? mapping.ConstantValue
                : mapping.SourceOrdinal < sourceRow.Length
                    ? sourceRow[mapping.SourceOrdinal]
                    : DbValue.Null;
        }

        return targetRow;
    }
}

internal readonly record struct TableRewriteColumnMapping(int SourceOrdinal, DbValue ConstantValue)
{
    public const int ConstantOrdinal = -1;

    public static TableRewriteColumnMapping FromSource(int sourceOrdinal) =>
        new(sourceOrdinal, DbValue.Null);

    public static TableRewriteColumnMapping Constant(DbValue value) =>
        new(ConstantOrdinal, value);
}

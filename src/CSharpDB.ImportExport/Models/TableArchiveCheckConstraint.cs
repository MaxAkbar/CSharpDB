using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveCheckConstraint
{
    public string? ConstraintName { get; init; }
    public required string ExpressionSql { get; init; }
    public string? ColumnName { get; init; }

    public static TableArchiveCheckConstraint FromCheckConstraint(CheckConstraintDefinition check) => new()
    {
        ConstraintName = check.ConstraintName,
        ExpressionSql = check.ExpressionSql,
        ColumnName = check.ColumnName,
    };

    public CheckConstraintDefinition ToCheckConstraint() => new()
    {
        ConstraintName = ConstraintName,
        ExpressionSql = ExpressionSql,
        ColumnName = ColumnName,
    };
}

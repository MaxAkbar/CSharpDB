using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveCheckConstraint
{
    public Guid SchemaId { get; init; }
    public string? ConstraintName { get; init; }
    public required string ExpressionSql { get; init; }
    public string? ColumnName { get; init; }

    public static TableArchiveCheckConstraint FromCheckConstraint(CheckConstraintDefinition check) => new()
    {
        SchemaId = check.SchemaId,
        ConstraintName = check.ConstraintName,
        ExpressionSql = check.ExpressionSql,
        ColumnName = check.ColumnName,
    };

    public CheckConstraintDefinition ToCheckConstraint() => new()
    {
        SchemaId = SchemaId,
        ConstraintName = ConstraintName,
        ExpressionSql = ExpressionSql,
        ColumnName = ColumnName,
    };
}

using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

public sealed partial class QueryPlanner
{
    /// <summary>Preview the same CHECK-reference rewrite used by ALTER COLUMN RENAME, without executing DDL.</summary>
    public static CheckConstraintDefinition PreviewCheckColumnRename(CheckConstraintDefinition check, string oldName, string newName) =>
        RenameCheckConstraintForColumn(check, oldName, newName);

    /// <summary>Inspect CHECK dependencies using the engine's expression rules, without executing SQL.</summary>
    public static bool CheckReferencesColumn(CheckConstraintDefinition check, string columnName) =>
        CheckConstraintReferencesColumn(check, columnName);

    /// <summary>Validate a column collation with the same type and collation registry used by DDL.</summary>
    public static string? ValidateColumnCollation(string columnName, SqlTypeDescriptor type, string? collation) =>
        ValidateAndNormalizeColumnCollation(columnName, type, collation);
}

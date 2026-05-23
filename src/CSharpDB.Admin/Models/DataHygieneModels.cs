namespace CSharpDB.Admin.Models;

public enum DataHygieneMode
{
    Duplicates,
    Validation,
    Orphans,
    History
}

public enum DataHygieneKeepMode
{
    First,
    Last
}

public sealed record DataHygieneSeed(
    DataHygieneMode Mode = DataHygieneMode.Duplicates,
    string? TableName = null,
    string? ChildTableName = null,
    string? ChildColumnName = null,
    string? ParentTableName = null,
    string? ParentColumnName = null);

public sealed record DataHygieneResultSet<T>(
    string Sql,
    TimeSpan Elapsed,
    IReadOnlyList<T> Rows,
    int RowsAffected = 0);

public sealed record DataHygieneDuplicateGroup(
    string? KeyValues,
    long GroupSize,
    string? WinnerRowId,
    string? WinnerPrimaryKey,
    string? DuplicateRowIds,
    string? DuplicatePrimaryKeys);

public sealed record DataHygieneMutationSummary(
    string Operation,
    string TableName,
    long DuplicateGroupCount,
    long RowsDeleted,
    long RowsKept,
    long RowsUpdated,
    long MergeConflictCount,
    string? MergeConflicts);

public sealed record DataHygieneValidationRuleRow(
    string RuleName,
    string TableName,
    string? ColumnName,
    string ExpressionSql,
    string Message,
    string? CreatedUtc,
    bool IsEnabled);

public sealed record DataHygieneValidationViolation(
    string RuleName,
    string TableName,
    string? ColumnName,
    string? RowId,
    string? PrimaryKey,
    string Message);

public sealed record DataHygieneOrphanRow(
    string? ConstraintName,
    string ChildTable,
    string ChildColumn,
    string? ChildRowId,
    string? ChildValue,
    string ParentTable,
    string ParentColumn);

public sealed record DataHygieneForeignKeyOption(
    string ConstraintName,
    string ChildTable,
    string ChildColumn,
    string ParentTable,
    string ParentColumn);

public sealed record DataHygieneHistoryEntry(
    DateTime Timestamp,
    string Operation,
    string Sql,
    string Summary,
    TimeSpan? Elapsed,
    bool IsError = false);

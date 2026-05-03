namespace CSharpDB.Admin.Models;

public sealed record DataGridFilterSummary(
    int ColumnIndex,
    string ColumnName,
    DataGridFilterMatchMode MatchMode,
    string Value);

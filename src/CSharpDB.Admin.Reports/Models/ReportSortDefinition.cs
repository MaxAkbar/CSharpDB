namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportSortDefinition(
    string FieldName,
    bool Descending = false);

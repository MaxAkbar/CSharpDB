namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportGroupDefinition(
    string GroupId,
    string FieldName,
    bool Descending = false,
    bool ShowHeader = true,
    bool ShowFooter = true);

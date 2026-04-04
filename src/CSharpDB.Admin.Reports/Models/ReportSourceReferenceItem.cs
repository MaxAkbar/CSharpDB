namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportSourceReferenceItem(
    ReportSourceKind Kind,
    string Name,
    string DisplayLabel);

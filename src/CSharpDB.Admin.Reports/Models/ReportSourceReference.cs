namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportSourceReference(
    ReportSourceKind Kind,
    string Name);

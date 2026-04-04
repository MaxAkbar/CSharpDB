namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportPreviewResult(
    ReportDefinition Report,
    ReportSourceDefinition Source,
    IReadOnlyList<ReportPreviewPage> Pages,
    int TotalRows,
    bool IsTruncated,
    bool HasSchemaDrift,
    string? WarningMessage,
    DateTime GeneratedUtc);

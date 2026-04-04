namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportPreviewPage(
    int PageNumber,
    IReadOnlyList<ReportRenderedBand> Bands);

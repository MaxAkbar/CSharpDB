namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportBandDefinition(
    string BandId,
    ReportBandKind BandKind,
    double Height,
    string? GroupId,
    IReadOnlyList<ReportControlDefinition> Controls);

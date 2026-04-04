namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportRenderedBand(
    string BandId,
    ReportBandKind BandKind,
    string? GroupId,
    double Height,
    IReadOnlyList<ReportRenderedControl> Controls);

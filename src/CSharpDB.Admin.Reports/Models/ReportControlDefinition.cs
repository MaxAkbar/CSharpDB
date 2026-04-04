namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportControlDefinition(
    string ControlId,
    ReportControlType ControlType,
    string BandId,
    Rect Rect,
    string? BoundFieldName,
    string? Expression,
    string? FormatString,
    PropertyBag Props);

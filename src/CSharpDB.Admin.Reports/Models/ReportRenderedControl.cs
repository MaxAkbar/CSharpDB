namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportRenderedControl(
    string ControlId,
    ReportControlType ControlType,
    Rect Rect,
    string? Text,
    PropertyBag Props);

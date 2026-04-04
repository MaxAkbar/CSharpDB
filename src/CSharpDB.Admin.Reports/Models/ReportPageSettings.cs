namespace CSharpDB.Admin.Reports.Models;

public sealed record ReportPageSettings(
    string PaperSize,
    string Orientation,
    double MarginLeftInches,
    double MarginTopInches,
    double MarginRightInches,
    double MarginBottomInches)
{
    public static ReportPageSettings DefaultLetterPortrait { get; } =
        new("Letter", "Portrait", 0.5, 0.5, 0.5, 0.5);
}

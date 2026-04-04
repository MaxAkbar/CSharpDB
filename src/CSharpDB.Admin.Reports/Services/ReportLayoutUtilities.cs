using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Services;

public static class ReportLayoutUtilities
{
    public const double PixelsPerInch = 96.0;
    private const double SidePadding = 20.0;
    private const double MinimumUsableWidth = 320.0;

    public static double GetUsablePageWidth(ReportPageSettings settings)
    {
        double widthInches = settings.PaperSize.Equals("A4", StringComparison.OrdinalIgnoreCase) ? 8.27 : 8.5;
        double heightInches = settings.PaperSize.Equals("A4", StringComparison.OrdinalIgnoreCase) ? 11.69 : 11.0;
        if (settings.Orientation.Equals("Landscape", StringComparison.OrdinalIgnoreCase))
            (widthInches, heightInches) = (heightInches, widthInches);

        double usableInches = widthInches - settings.MarginLeftInches - settings.MarginRightInches;
        return Math.Max(MinimumUsableWidth, usableInches * PixelsPerInch);
    }

    public static ReportColumnLayout CreateColumnLayout(ReportPageSettings settings, int columnCount)
    {
        double usableWidth = GetUsablePageWidth(settings);
        int safeColumnCount = Math.Max(1, columnCount);
        double gap = safeColumnCount == 1 ? 0 : Math.Clamp(usableWidth / (safeColumnCount * 24.0), 2.0, 8.0);
        double contentWidth = Math.Max(80, usableWidth - (SidePadding * 2) - (gap * (safeColumnCount - 1)));
        double columnWidth = Math.Max(48, contentWidth / safeColumnCount);
        double footerLeftWidth = Math.Min(260, Math.Max(180, usableWidth * 0.35));
        double footerRightWidth = Math.Min(180, Math.Max(120, usableWidth * 0.22));
        double footerRightX = Math.Max(SidePadding, usableWidth - SidePadding - footerRightWidth);
        double reportHeaderWidth = Math.Max(220, usableWidth - (SidePadding * 2));
        return new ReportColumnLayout(usableWidth, SidePadding, gap, columnWidth, reportHeaderWidth, footerLeftWidth, footerRightWidth, footerRightX);
    }

    public static ReportDefinition AutoFitColumns(ReportDefinition report)
    {
        ReportBandDefinition? detailBand = FindBand(report, ReportBandKind.Detail);
        if (detailBand is null)
            return report;

        ReportControlDefinition[] detailColumns = detailBand.Controls
            .Where(control => control.ControlType == ReportControlType.BoundText && !string.IsNullOrWhiteSpace(control.BoundFieldName))
            .OrderBy(control => control.Rect.X)
            .ThenBy(control => control.Rect.Y)
            .ToArray();

        if (detailColumns.Length == 0)
            return RepositionFrameBands(report, CreateColumnLayout(report.PageSettings, 1));

        ReportColumnLayout layout = CreateColumnLayout(report.PageSettings, detailColumns.Length);
        var detailRects = detailColumns
            .Select((control, index) => new KeyValuePair<string, Rect>(control.ControlId, control.Rect with
            {
                X = layout.SidePadding + (index * (layout.ColumnWidth + layout.ColumnGap)),
                Width = layout.ColumnWidth,
            }))
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

        ReportDefinition updated = UpdateBand(report, detailBand.BandId, band => band with
        {
            Controls = band.Controls.Select(control => detailRects.TryGetValue(control.ControlId, out Rect? rect) && rect is not null ? control with { Rect = rect } : control).ToArray(),
        });

        ReportBandDefinition? pageHeaderBand = FindBand(updated, ReportBandKind.PageHeader);
        if (pageHeaderBand is not null)
        {
            ReportControlDefinition[] pageHeaderColumns = pageHeaderBand.Controls
                .Where(control => control.ControlType is ReportControlType.Label or ReportControlType.BoundText or ReportControlType.CalculatedText)
                .OrderBy(control => control.Rect.X)
                .ThenBy(control => control.Rect.Y)
                .Take(detailColumns.Length)
                .ToArray();

            if (pageHeaderColumns.Length > 0)
            {
                var headerRects = pageHeaderColumns
                    .Select((control, index) => new KeyValuePair<string, Rect>(control.ControlId, control.Rect with
                    {
                        X = layout.SidePadding + (index * (layout.ColumnWidth + layout.ColumnGap)),
                        Width = layout.ColumnWidth,
                    }))
                    .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

                updated = UpdateBand(updated, pageHeaderBand.BandId, band => band with
                {
                    Controls = band.Controls.Select(control => headerRects.TryGetValue(control.ControlId, out Rect? rect) && rect is not null ? control with { Rect = rect } : control).ToArray(),
                });
            }
        }

        return RepositionFrameBands(updated, layout);
    }

    private static ReportDefinition RepositionFrameBands(ReportDefinition report, ReportColumnLayout layout)
    {
        ReportDefinition updated = report;

        ReportBandDefinition? reportHeaderBand = FindBand(updated, ReportBandKind.ReportHeader);
        if (reportHeaderBand is not null)
        {
            ReportControlDefinition? primaryHeader = reportHeaderBand.Controls
                .Where(control => control.ControlType is ReportControlType.Label or ReportControlType.BoundText or ReportControlType.CalculatedText)
                .OrderBy(control => control.Rect.Y)
                .ThenBy(control => control.Rect.X)
                .FirstOrDefault();

            if (primaryHeader is not null)
            {
                updated = UpdateBand(updated, reportHeaderBand.BandId, band => band with
                {
                    Controls = band.Controls.Select(control => control.ControlId == primaryHeader.ControlId
                        ? control with { Rect = control.Rect with { X = layout.SidePadding, Width = layout.ReportHeaderWidth } }
                        : control).ToArray(),
                });
            }
        }

        ReportBandDefinition? pageFooterBand = FindBand(updated, ReportBandKind.PageFooter);
        if (pageFooterBand is not null)
        {
            updated = UpdateBand(updated, pageFooterBand.BandId, band => band with
            {
                Controls = band.Controls.Select(control => control.Expression switch
                {
                    "=PrintDate" => control with { Rect = control.Rect with { X = layout.SidePadding, Width = layout.FooterLeftWidth } },
                    "=PageNumber" => control with { Rect = control.Rect with { X = layout.FooterRightX, Width = layout.FooterRightWidth } },
                    _ => control,
                }).ToArray(),
            });
        }

        return updated;
    }

    private static ReportBandDefinition? FindBand(ReportDefinition report, ReportBandKind kind)
        => report.Bands.FirstOrDefault(band => band.BandKind == kind);

    private static ReportDefinition UpdateBand(ReportDefinition report, string bandId, Func<ReportBandDefinition, ReportBandDefinition> update)
        => report with
        {
            Bands = report.Bands.Select(band => string.Equals(band.BandId, bandId, StringComparison.Ordinal) ? update(band) : band).ToArray(),
        };
}

public sealed record ReportColumnLayout(
    double UsableWidth,
    double SidePadding,
    double ColumnGap,
    double ColumnWidth,
    double ReportHeaderWidth,
    double FooterLeftWidth,
    double FooterRightWidth,
    double FooterRightX);

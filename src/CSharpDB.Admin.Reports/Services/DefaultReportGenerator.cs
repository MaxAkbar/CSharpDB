using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Services;

public sealed class DefaultReportGenerator : IReportGenerator
{
    public ReportDefinition GenerateDefault(ReportSourceDefinition source)
    {
        ReportPageSettings pageSettings = ReportPageSettings.DefaultLetterPortrait;
        int fieldCount = Math.Max(1, source.Fields.Count);
        ReportColumnLayout layout = ReportLayoutUtilities.CreateColumnLayout(pageSettings, fieldCount);
        double headerTop = 6;
        const double rowHeight = 24;

        var reportHeaderControls = new List<ReportControlDefinition>
        {
            new(
                Guid.NewGuid().ToString("N"),
                ReportControlType.Label,
                "report-header",
                new Rect(layout.SidePadding, 6, layout.ReportHeaderWidth, 28),
                BoundFieldName: null,
                Expression: null,
                FormatString: null,
                Props: new PropertyBag(new Dictionary<string, object?> { ["text"] = $"{source.DisplayName} Report", ["fontSize"] = 22L, ["fontWeight"] = "600" }))
        };

        var pageHeaderControls = new List<ReportControlDefinition>();
        var detailControls = new List<ReportControlDefinition>();

        for (int i = 0; i < source.Fields.Count; i++)
        {
            ReportFieldDefinition field = source.Fields[i];
            double x = layout.SidePadding + (i * (layout.ColumnWidth + layout.ColumnGap));

            pageHeaderControls.Add(new ReportControlDefinition(
                Guid.NewGuid().ToString("N"),
                ReportControlType.Label,
                "page-header",
                new Rect(x, headerTop, layout.ColumnWidth, rowHeight),
                BoundFieldName: null,
                Expression: null,
                FormatString: null,
                Props: new PropertyBag(new Dictionary<string, object?> { ["text"] = field.DisplayName ?? field.Name, ["fontWeight"] = "600" })));

            detailControls.Add(new ReportControlDefinition(
                Guid.NewGuid().ToString("N"),
                ReportControlType.BoundText,
                "detail",
                new Rect(x, 2, layout.ColumnWidth, rowHeight),
                BoundFieldName: field.Name,
                Expression: null,
                FormatString: null,
                Props: PropertyBag.Empty));
        }

        var pageFooterControls = new List<ReportControlDefinition>
        {
            new(
                Guid.NewGuid().ToString("N"),
                ReportControlType.CalculatedText,
                "page-footer",
                new Rect(layout.FooterRightX, 4, layout.FooterRightWidth, 20),
                BoundFieldName: null,
                Expression: "=PageNumber",
                FormatString: null,
                Props: new PropertyBag(new Dictionary<string, object?> { ["textAlign"] = "right", ["prefix"] = "Page " })),
            new(
                Guid.NewGuid().ToString("N"),
                ReportControlType.CalculatedText,
                "page-footer",
                new Rect(layout.SidePadding, 4, layout.FooterLeftWidth, 20),
                BoundFieldName: null,
                Expression: "=PrintDate",
                FormatString: "g",
                Props: PropertyBag.Empty),
        };

        return new ReportDefinition(
            ReportId: string.Empty,
            Name: $"{source.DisplayName} Report",
            Source: new ReportSourceReference(source.Kind, source.Name),
            DefinitionVersion: 1,
            SourceSchemaSignature: source.SourceSchemaSignature,
            PageSettings: pageSettings,
            Groups: [],
            Sorts: [],
            Bands:
            [
                new ReportBandDefinition("report-header", ReportBandKind.ReportHeader, 40, GroupId: null, reportHeaderControls),
                new ReportBandDefinition("page-header", ReportBandKind.PageHeader, 30, GroupId: null, pageHeaderControls),
                new ReportBandDefinition("detail", ReportBandKind.Detail, 28, GroupId: null, detailControls),
                new ReportBandDefinition("page-footer", ReportBandKind.PageFooter, 26, GroupId: null, pageFooterControls),
                new ReportBandDefinition("report-footer", ReportBandKind.ReportFooter, 24, GroupId: null, []),
            ]);
    }
}

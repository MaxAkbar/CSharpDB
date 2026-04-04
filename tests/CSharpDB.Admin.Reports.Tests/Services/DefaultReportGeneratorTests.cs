using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Tests.Services;

public class DefaultReportGeneratorTests
{
    private readonly DefaultReportGenerator _generator = new();

    [Fact]
    public void GenerateDefault_FitsHeaderAndDetailColumnsWithinPrintableWidth()
    {
        ReportSourceDefinition source = CreateSource(
            "Customers",
            "sig:customers:v1",
            "Id",
            "TenantId",
            "FirstName",
            "LastName",
            "Email",
            "Phone");

        ReportDefinition report = _generator.GenerateDefault(source);
        double usableWidth = 7.5 * 96.0; // Letter portrait with 0.5in margins on both sides

        ReportBandDefinition pageHeader = Assert.Single(report.Bands, band => band.BandKind == ReportBandKind.PageHeader);
        ReportBandDefinition detail = Assert.Single(report.Bands, band => band.BandKind == ReportBandKind.Detail);

        Assert.All(pageHeader.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
        Assert.All(detail.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
    }

    [Fact]
    public void GenerateDefault_RepositionsFooterControlsInsidePrintableWidth()
    {
        ReportSourceDefinition source = CreateSource("Customers", "sig:customers:v1", "Id", "Name");

        ReportDefinition report = _generator.GenerateDefault(source);
        double usableWidth = 7.5 * 96.0;

        ReportBandDefinition pageFooter = Assert.Single(report.Bands, band => band.BandKind == ReportBandKind.PageFooter);
        Assert.All(pageFooter.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
    }

    [Fact]
    public void AutoFitColumns_RepositionsWideExistingLayoutInsidePrintableWidth()
    {
        ReportDefinition report = new(
            "report-1",
            "Customers Report",
            new ReportSourceReference(ReportSourceKind.Table, "Customers"),
            1,
            "sig:customers:v1",
            ReportPageSettings.DefaultLetterPortrait,
            [],
            [],
            [
                new ReportBandDefinition(
                    "page-header",
                    ReportBandKind.PageHeader,
                    30,
                    null,
                    [
                        new ReportControlDefinition("h1", ReportControlType.Label, "page-header", new Rect(20, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "Id" })),
                        new ReportControlDefinition("h2", ReportControlType.Label, "page-header", new Rect(160, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "TenantId" })),
                        new ReportControlDefinition("h3", ReportControlType.Label, "page-header", new Rect(300, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "FirstName" })),
                        new ReportControlDefinition("h4", ReportControlType.Label, "page-header", new Rect(440, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "LastName" })),
                        new ReportControlDefinition("h5", ReportControlType.Label, "page-header", new Rect(580, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "Email" })),
                        new ReportControlDefinition("h6", ReportControlType.Label, "page-header", new Rect(720, 6, 132, 24), null, null, null, new PropertyBag(new Dictionary<string, object?> { ["text"] = "Phone" })),
                    ]),
                new ReportBandDefinition(
                    "detail",
                    ReportBandKind.Detail,
                    28,
                    null,
                    [
                        new ReportControlDefinition("d1", ReportControlType.BoundText, "detail", new Rect(20, 2, 132, 24), "Id", null, null, PropertyBag.Empty),
                        new ReportControlDefinition("d2", ReportControlType.BoundText, "detail", new Rect(160, 2, 132, 24), "TenantId", null, null, PropertyBag.Empty),
                        new ReportControlDefinition("d3", ReportControlType.BoundText, "detail", new Rect(300, 2, 132, 24), "FirstName", null, null, PropertyBag.Empty),
                        new ReportControlDefinition("d4", ReportControlType.BoundText, "detail", new Rect(440, 2, 132, 24), "LastName", null, null, PropertyBag.Empty),
                        new ReportControlDefinition("d5", ReportControlType.BoundText, "detail", new Rect(580, 2, 132, 24), "Email", null, null, PropertyBag.Empty),
                        new ReportControlDefinition("d6", ReportControlType.BoundText, "detail", new Rect(720, 2, 132, 24), "Phone", null, null, PropertyBag.Empty),
                    ]),
                new ReportBandDefinition(
                    "page-footer",
                    ReportBandKind.PageFooter,
                    26,
                    null,
                    [
                        new ReportControlDefinition("f1", ReportControlType.CalculatedText, "page-footer", new Rect(20, 4, 260, 20), null, "=PrintDate", "g", PropertyBag.Empty),
                        new ReportControlDefinition("f2", ReportControlType.CalculatedText, "page-footer", new Rect(560, 4, 160, 20), null, "=PageNumber", null, new PropertyBag(new Dictionary<string, object?> { ["textAlign"] = "right", ["prefix"] = "Page " })),
                    ]),
            ]);

        ReportDefinition updated = ReportLayoutUtilities.AutoFitColumns(report);
        double usableWidth = 7.5 * 96.0;

        ReportBandDefinition pageHeader = Assert.Single(updated.Bands, band => band.BandKind == ReportBandKind.PageHeader);
        ReportBandDefinition detail = Assert.Single(updated.Bands, band => band.BandKind == ReportBandKind.Detail);
        ReportBandDefinition pageFooter = Assert.Single(updated.Bands, band => band.BandKind == ReportBandKind.PageFooter);

        Assert.All(pageHeader.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
        Assert.All(detail.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
        Assert.All(pageFooter.Controls, control => Assert.InRange(control.Rect.X + control.Rect.Width, 0, usableWidth + 0.001));
    }

    private static ReportSourceDefinition CreateSource(string name, string signature, params string[] fieldNames)
        => new(
            ReportSourceKind.Table,
            name,
            name,
            $"SELECT * FROM {name};",
            signature,
            fieldNames.Select((fieldName, index) => new ReportFieldDefinition(fieldName, DbType.Text, IsNullable: true, IsReadOnly: index == 0, DisplayName: fieldName)).ToArray());
}

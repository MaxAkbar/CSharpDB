using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Tests.Services;

public class ReportPreviewQueryBuilderTests
{
    [Fact]
    public void Build_WrapsSourceSqlAndUsesFieldOrdinalsForGroupingAndSorting()
    {
        var report = new ReportDefinition(
            "r1",
            "Sales Report",
            new ReportSourceReference(ReportSourceKind.Table, "Sales"),
            1,
            "sig:sales:v1",
            ReportPageSettings.DefaultLetterPortrait,
            [new ReportGroupDefinition("g1", "Region")],
            [new ReportSortDefinition("Id", Descending: true), new ReportSortDefinition("Region", Descending: true)],
            []);

        var source = new ReportSourceDefinition(
            ReportSourceKind.Table,
            "Sales",
            "Sales",
            "SELECT Id, Region, Total FROM Sales",
            "sig:sales:v1",
            [
                new ReportFieldDefinition("Id", DbType.Integer, false, false),
                new ReportFieldDefinition("Region", DbType.Text, false, false),
                new ReportFieldDefinition("Total", DbType.Real, false, false),
            ]);

        string sql = ReportPreviewQueryBuilder.Build(report, source);

        Assert.Contains("SELECT Id, Region, Total FROM Sales", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY Region, Id DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 10001", sql, StringComparison.Ordinal);
    }
}

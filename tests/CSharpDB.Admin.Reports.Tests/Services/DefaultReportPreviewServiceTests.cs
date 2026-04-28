using System.Text;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Tests.Services;

public class DefaultReportPreviewServiceTests
{
    [Fact]
    public async Task BuildPreviewAsync_RendersGroupBandsAndRepeatsPageBandsAcrossPages()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSalesSchemaAsync(db);
        var provider = new DbReportSourceProvider(db.Client);
        var generator = new DefaultReportGenerator();
        var source = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Sales")))!;
        var previewService = new DefaultReportPreviewService(db.Client, provider);

        ReportDefinition report = CreateGroupedReport(generator.GenerateDefault(source));
        ReportPreviewResult result = await previewService.BuildPreviewAsync(report, TestContext.Current.CancellationToken);

        Assert.True(result.Pages.Count > 1);
        Assert.False(result.IsTruncated);
        Assert.False(result.HasSchemaDrift);
        Assert.All(result.Pages, page => Assert.Contains(page.Bands, band => band.BandKind == ReportBandKind.PageHeader));
        Assert.All(result.Pages, page => Assert.Contains(page.Bands, band => band.BandKind == ReportBandKind.PageFooter));

        string[] groupHeaderTexts = result.Pages
            .SelectMany(page => page.Bands)
            .Where(band => band.BandKind == ReportBandKind.GroupHeader)
            .SelectMany(band => band.Controls)
            .Select(control => control.Text)
            .OfType<string>()
            .ToArray();
        Assert.Equal(["East", "West"], groupHeaderTexts);

        string[] groupFooterTexts = result.Pages
            .SelectMany(page => page.Bands)
            .Where(band => band.BandKind == ReportBandKind.GroupFooter)
            .SelectMany(band => band.Controls)
            .Select(control => control.Text)
            .OfType<string>()
            .ToArray();
        Assert.Equal(2, groupFooterTexts.Count(text => text == "Count: 2"));
    }

    [Fact]
    public async Task BuildPreviewAsync_PageCapSetsTruncationWarning()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSalesSchemaAsync(db);
        await InsertSalesRowsAsync(db, count: 600, startingId: 100);
        var provider = new DbReportSourceProvider(db.Client);
        var generator = new DefaultReportGenerator();
        var source = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Sales")))!;
        var previewService = new DefaultReportPreviewService(db.Client, provider);

        ReportDefinition report = generator.GenerateDefault(source) with
        {
            Bands = generator.GenerateDefault(source).Bands
                .Select(band => band.BandKind == ReportBandKind.Detail ? band with { Height = 920 } : band)
                .ToArray(),
        };

        ReportPreviewResult result = await previewService.BuildPreviewAsync(report, TestContext.Current.CancellationToken);

        Assert.True(result.IsTruncated, $"Pages={result.Pages.Count}; Rows={result.TotalRows}; Warning={result.WarningMessage ?? "(none)"}");
        Assert.Equal(250, result.Pages.Count);
        Assert.Equal(604, result.TotalRows);
        Assert.Contains("250 pages", result.WarningMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task BuildPreviewAsync_DispatchesReportLifecycleEvents()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSalesSchemaAsync(db);
        var provider = new DbReportSourceProvider(db.Client);
        var generator = new DefaultReportGenerator();
        var source = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Sales")))!;
        var captured = new List<DbCommandContext>();
        var commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("RecordReportEvent", context =>
            {
                captured.Add(context);
                return DbCommandResult.Success();
            });
        });
        var previewService = new DefaultReportPreviewService(
            db.Client,
            provider,
            reportEvents: new DefaultReportEventDispatcher(commands));

        ReportDefinition report = generator.GenerateDefault(source) with
        {
            EventBindings =
            [
                new ReportEventBinding(ReportEventKind.OnOpen, "RecordReportEvent"),
                new ReportEventBinding(ReportEventKind.BeforeRender, "RecordReportEvent", new Dictionary<string, object?> { ["configured"] = "yes" }),
                new ReportEventBinding(ReportEventKind.AfterRender, "RecordReportEvent"),
            ],
        };

        ReportPreviewResult result = await previewService.BuildPreviewAsync(report, TestContext.Current.CancellationToken);

        Assert.Equal(4, result.TotalRows);
        Assert.Equal(["OnOpen", "BeforeRender", "AfterRender"], captured.Select(context => context.Metadata["event"]).ToArray());
        Assert.All(captured, context => Assert.Equal("AdminReports", context.Metadata["surface"]));
        Assert.Equal(4, captured[1].Arguments["rowCount"].AsInteger);
        Assert.Equal("yes", captured[1].Arguments["configured"].AsText);
        Assert.Equal(result.Pages.Count, captured[2].Arguments["pageCount"].AsInteger);
        Assert.Equal(0, captured[2].Arguments["hasSchemaDrift"].AsInteger);
    }

    [Fact]
    public async Task BuildPreviewAsync_FailsWhenReportEventCommandFails()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSalesSchemaAsync(db);
        var provider = new DbReportSourceProvider(db.Client);
        var generator = new DefaultReportGenerator();
        var source = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Sales")))!;
        var commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("RejectReport", _ => DbCommandResult.Failure("Rejected by host command.")));
        var previewService = new DefaultReportPreviewService(
            db.Client,
            provider,
            reportEvents: new DefaultReportEventDispatcher(commands));

        ReportDefinition report = generator.GenerateDefault(source) with
        {
            EventBindings = [new ReportEventBinding(ReportEventKind.OnOpen, "RejectReport")],
        };

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => previewService.BuildPreviewAsync(report, TestContext.Current.CancellationToken));
        Assert.Contains("Rejected by host command.", ex.Message);
    }

    private static async Task CreateSalesSchemaAsync(TestDatabaseScope db)
    {
        await db.ExecuteAsync(
            """
            CREATE TABLE Sales (
                Id INTEGER PRIMARY KEY,
                Region TEXT NOT NULL,
                Name TEXT NOT NULL,
                Total REAL NOT NULL
            );
            INSERT INTO Sales (Id, Region, Name, Total) VALUES
                (1, 'West', 'Delta', 30.0),
                (2, 'East', 'Alpha', 10.0),
                (3, 'West', 'Gamma', 25.0),
                (4, 'East', 'Beta', 20.0);
            """);
    }

    private static Task InsertSalesRowsAsync(TestDatabaseScope db, int count, int startingId)
    {
        var sql = new StringBuilder();
        for (int i = 0; i < count; i++)
        {
            int id = startingId + i;
            string region = i % 2 == 0 ? "East" : "West";
            sql.Append("INSERT INTO Sales (Id, Region, Name, Total) VALUES (")
                .Append(id)
                .Append(", '")
                .Append(region)
                .Append("', 'Row ")
                .Append(id)
                .Append("', ")
                .Append(100 + i)
                .AppendLine(");");
        }

        return db.ExecuteAsync(sql.ToString());
    }

    private static ReportDefinition CreateGroupedReport(ReportDefinition report)
    {
        const string groupId = "group:region";

        return report with
        {
            Groups = [new ReportGroupDefinition(groupId, "Region")],
            Sorts = [new ReportSortDefinition("Name")],
            Bands = report.Bands
                .Select(band => band.BandKind == ReportBandKind.Detail ? band with { Height = 420 } : band)
                .Concat(
                [
                    new ReportBandDefinition(
                        "group-header:region",
                        ReportBandKind.GroupHeader,
                        28,
                        groupId,
                        [
                            new ReportControlDefinition(
                                "group-header-control",
                                ReportControlType.BoundText,
                                "group-header:region",
                                new Rect(20, 2, 220, 24),
                                "Region",
                                null,
                                null,
                                PropertyBag.Empty),
                        ]),
                    new ReportBandDefinition(
                        "group-footer:region",
                        ReportBandKind.GroupFooter,
                        28,
                        groupId,
                        [
                            new ReportControlDefinition(
                                "group-footer-control",
                                ReportControlType.CalculatedText,
                                "group-footer:region",
                                new Rect(20, 2, 220, 24),
                                null,
                                "=COUNT(Id)",
                                null,
                                new PropertyBag(new Dictionary<string, object?> { ["prefix"] = "Count: " })),
                        ]),
                ])
                .ToArray(),
        };
    }
}

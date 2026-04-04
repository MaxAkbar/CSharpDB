using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Admin.Reports.Serialization;
using System.Text.Json;

namespace CSharpDB.Admin.Reports.Tests.Services;

public class DbReportRepositoryTests
{
    [Fact]
    public async Task CreateAsync_InitializesMetadataTableAndPersistsReport()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);

        ReportDefinition created = await repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, "Customers", "Customer Report", "sig:customers:v1"));

        Assert.Equal("r1", created.ReportId);
        Assert.Equal(1, created.DefinitionVersion);

        IReadOnlyList<Dictionary<string, object?>> rows = await db.QueryRowsAsync(
            "SELECT id, name, source_kind, source_name, definition_version, source_schema_signature FROM __reports;");
        Dictionary<string, object?> row = Assert.Single(rows);
        Assert.Equal("r1", row["id"]);
        Assert.Equal("Customer Report", row["name"]);
        Assert.Equal("Table", row["source_kind"]);
        Assert.Equal("Customers", row["source_name"]);
        Assert.Equal(1L, row["definition_version"]);
        Assert.Equal("sig:customers:v1", row["source_schema_signature"]);

        var indexes = await db.Client.GetIndexesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(indexes, index => index.IndexName == "idx___reports_source" && index.TableName == "__reports");
    }

    [Fact]
    public async Task CreateAsync_GeneratesReportIdAndNormalizesName()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);

        ReportDefinition created = await repository.CreateAsync(CreateReport(string.Empty, ReportSourceKind.Table, "Customers", "   ", "sig:customers:v1"));

        Assert.False(string.IsNullOrWhiteSpace(created.ReportId));
        Assert.Equal("Customers Report", created.Name);
        Assert.Equal(1, created.DefinitionVersion);
    }

    [Fact]
    public async Task TryUpdateAsync_CorrectVersion_UpdatesAndIncrementsVersion()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);
        await repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, "Customers", "Customer Report", "sig:customers:v1"));

        ReportUpdateResult result = await repository.TryUpdateAsync(
            "r1",
            1,
            CreateReport("r1", ReportSourceKind.Table, "Customers", "Updated Report", "sig:customers:v1"));

        var ok = Assert.IsType<ReportUpdateResult.Ok>(result);
        Assert.Equal(2, ok.Doc.DefinitionVersion);
        Assert.Equal("Updated Report", ok.Doc.Name);
    }

    [Fact]
    public async Task TryUpdateAsync_WrongVersion_ReturnsConflict()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);
        await repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, "Customers", "Customer Report", "sig:customers:v1"));

        ReportUpdateResult result = await repository.TryUpdateAsync(
            "r1",
            99,
            CreateReport("r1", ReportSourceKind.Table, "Customers", "Updated Report", "sig:customers:v1"));

        Assert.IsType<ReportUpdateResult.Conflict>(result);
    }

    [Fact]
    public async Task TryUpdateAsync_NonExistent_ReturnsNotFound()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);

        ReportUpdateResult result = await repository.TryUpdateAsync(
            "missing",
            1,
            CreateReport("missing", ReportSourceKind.Table, "Customers", "Missing", "sig:customers:v1"));

        Assert.IsType<ReportUpdateResult.NotFound>(result);
    }

    [Fact]
    public async Task ListAsync_FiltersBySourceAndDeleteAsync_RemovesReport()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);
        await repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, "Customers", "Customer Report", "sig:customers:v1"));
        await repository.CreateAsync(CreateReport("r2", ReportSourceKind.View, "CustomerView", "Customer View Report", "sig:view:v1"));

        IReadOnlyList<ReportDefinition> customerReports = await repository.ListAsync(ReportSourceKind.Table, "Customers");
        IReadOnlyList<ReportDefinition> all = await repository.ListAsync();
        bool deleted = await repository.DeleteAsync("r1");

        ReportDefinition filtered = Assert.Single(customerReports);
        Assert.Equal("Customers", filtered.Source.Name);
        Assert.Equal(2, all.Count);
        Assert.True(deleted);
        Assert.Null(await repository.GetAsync("r1"));
    }

    [Fact]
    public async Task CreateAsync_LargeReportDefinition_StoresChunksAndCanBeReadBack()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);
        ReportDefinition largeReport = CreateLargeReport("big-report", "Wide Customer Report");

        ReportDefinition created = await repository.CreateAsync(largeReport);
        ReportDefinition? loaded = await repository.GetAsync(created.ReportId);

        Assert.NotNull(loaded);
        Assert.Equal(created.ReportId, loaded!.ReportId);
        Assert.Equal(created.Name, loaded.Name);
        Assert.Equal(largeReport.Bands.Sum(static band => band.Controls.Count), loaded.Bands.Sum(static band => band.Controls.Count));

        IReadOnlyList<Dictionary<string, object?>> chunks = await db.QueryRowsAsync(
            "SELECT chunk_id, storage_id, chunk_ordinal FROM __report_definition_chunks WHERE report_id = 'big-report' ORDER BY chunk_ordinal;");
        Assert.True(chunks.Count > 1, "Expected the large report definition to be stored across multiple chunks.");
    }

    [Fact]
    public async Task GetAsync_LegacyInlineDefinition_RemainsReadable()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);
        await repository.ListAsync();

        ReportDefinition legacy = CreateReport("legacy-report", ReportSourceKind.Table, "Customers", "Legacy Report", "sig:customers:v1") with
        {
            Bands =
            [
                new ReportBandDefinition(
                    "detail",
                    ReportBandKind.Detail,
                    28,
                    null,
                    [
                        new ReportControlDefinition(
                            "legacy-control",
                            ReportControlType.Label,
                            "detail",
                            new Rect(12, 6, 160, 24),
                            null,
                            null,
                            null,
                            new PropertyBag(new Dictionary<string, object?> { ["text"] = "Legacy inline report" }))
                    ]),
            ],
        };

        string json = JsonSerializer.Serialize(legacy, JsonDefaults.Options);
        await db.ExecuteAsync($"""
            INSERT INTO __reports (
                id,
                name,
                source_kind,
                source_name,
                definition_json,
                definition_version,
                source_schema_signature,
                created_utc,
                updated_utc
            )
            VALUES (
                'legacy-report',
                'Legacy Report',
                'Table',
                'Customers',
                '{EscapeSqlLiteral(json)}',
                1,
                'sig:customers:v1',
                '2026-04-03T00:00:00.0000000Z',
                '2026-04-03T00:00:00.0000000Z'
            );
            """);

        ReportDefinition? loaded = await repository.GetAsync("legacy-report");

        Assert.NotNull(loaded);
        Assert.Equal("Legacy Report", loaded!.Name);
        Assert.Single(loaded.Bands);
        Assert.Single(loaded.Bands[0].Controls);
    }

    [Fact]
    public async Task CreateAsync_RejectsBlankSourceName()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, string.Empty, "Broken", "sig:missing")));

        Assert.Contains("bound to a source", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateAsync_RejectsBlankSourceSchemaSignature()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbReportRepository(db.Client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.CreateAsync(CreateReport("r1", ReportSourceKind.Table, "Customers", "Broken", string.Empty)));

        Assert.Contains("source schema signature", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static ReportDefinition CreateReport(string reportId, ReportSourceKind sourceKind, string sourceName, string name, string sourceSchemaSignature)
        => new(
            reportId,
            name,
            new ReportSourceReference(sourceKind, sourceName),
            1,
            sourceSchemaSignature,
            ReportPageSettings.DefaultLetterPortrait,
            [],
            [],
            [
                new ReportBandDefinition("detail", ReportBandKind.Detail, 28, null, []),
            ]);

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static ReportDefinition CreateLargeReport(string reportId, string name)
    {
        var headerControls = new List<ReportControlDefinition>();
        var detailControls = new List<ReportControlDefinition>();
        string repeatedText = new('W', 220);

        for (int i = 0; i < 60; i++)
        {
            headerControls.Add(new ReportControlDefinition(
                $"header-{i:D3}",
                ReportControlType.Label,
                "page-header",
                new Rect(12 + ((i % 6) * 118), 4 + ((i / 6) * 18), 112, 16),
                null,
                null,
                null,
                new PropertyBag(new Dictionary<string, object?>
                {
                    ["text"] = $"Header {i} {repeatedText}",
                    ["fontWeight"] = "600",
                })));

            detailControls.Add(new ReportControlDefinition(
                $"detail-{i:D3}",
                ReportControlType.BoundText,
                "detail",
                new Rect(12 + ((i % 6) * 118), 2 + ((i / 6) * 18), 112, 16),
                $"Field{i:D3}",
                null,
                null,
                new PropertyBag(new Dictionary<string, object?>
                {
                    ["textAlign"] = i % 2 == 0 ? "left" : "right",
                    ["prefix"] = repeatedText,
                })));
        }

        return new ReportDefinition(
            reportId,
            name,
            new ReportSourceReference(ReportSourceKind.Table, "Customers"),
            1,
            "sig:customers:v1",
            ReportPageSettings.DefaultLetterPortrait,
            [],
            [],
            [
                new ReportBandDefinition("report-header", ReportBandKind.ReportHeader, 48, null, []),
                new ReportBandDefinition("page-header", ReportBandKind.PageHeader, 220, null, headerControls),
                new ReportBandDefinition("detail", ReportBandKind.Detail, 220, null, detailControls),
                new ReportBandDefinition("page-footer", ReportBandKind.PageFooter, 24, null, []),
            ]);
    }
}

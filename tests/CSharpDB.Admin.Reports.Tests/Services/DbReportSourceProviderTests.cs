using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Reports.Tests.Services;

public class DbReportSourceProviderTests
{
    [Fact]
    public async Task GetSourceDefinitionAsync_Table_MapsFieldsAndSignatureChangesWhenSchemaChanges()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        var provider = new DbReportSourceProvider(db.Client);

        ReportSourceDefinition first = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Customers")))!;
        await db.Client.AddColumnAsync("Customers", "Email", DbType.Text, notNull: false, ct: TestContext.Current.CancellationToken);
        ReportSourceDefinition second = (await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.Table, "Customers")))!;

        Assert.Equal("Customers", first.Name);
        Assert.Equal("SELECT * FROM Customers", first.BaseSql);
        Assert.Contains(first.Fields, field => field.Name == "Id" && field.DataType == DbType.Integer);
        Assert.Contains(first.Fields, field => field.Name == "Total" && field.DataType == DbType.Real);
        Assert.NotEqual(first.SourceSchemaSignature, second.SourceSchemaSignature);
    }

    [Fact]
    public async Task GetSourceDefinitionAsync_View_ResolvesPreviewFields()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.Client.CreateViewAsync("CustomerTotals", "SELECT Name, Total FROM Customers", TestContext.Current.CancellationToken);
        var provider = new DbReportSourceProvider(db.Client);

        ReportSourceDefinition? view = await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.View, "CustomerTotals"));

        Assert.NotNull(view);
        Assert.Equal(ReportSourceKind.View, view!.Kind);
        Assert.Equal("CustomerTotals", view.Name);
        Assert.Equal(["Name", "Total"], view.Fields.Select(field => field.Name).ToArray());
        Assert.Equal(DbType.Real, Assert.Single(view.Fields, field => field.Name == "Total").DataType);
    }

    [Fact]
    public async Task GetSourceDefinitionAsync_SavedQuery_ResolvesRowSource()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.Client.UpsertSavedQueryAsync("west_customers", "SELECT Name, Total FROM Customers WHERE Region = 'West' ORDER BY Name;", TestContext.Current.CancellationToken);
        var provider = new DbReportSourceProvider(db.Client);

        ReportSourceDefinition? saved = await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.SavedQuery, "west_customers"));

        Assert.NotNull(saved);
        Assert.Equal(ReportSourceKind.SavedQuery, saved!.Kind);
        Assert.StartsWith("SELECT Name, Total FROM Customers", saved.BaseSql, StringComparison.Ordinal);
        Assert.Equal(["Name", "Total"], saved.Fields.Select(field => field.Name).ToArray());
    }

    [Fact]
    public async Task ListSourceReferencesAsync_FiltersUnsupportedSavedQueriesAndSystemTables()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.ExecuteAsync("CREATE TABLE _internal_metrics (Id INTEGER PRIMARY KEY);");
        await db.Client.CreateViewAsync("CustomerTotals", "SELECT Name, Total FROM Customers", TestContext.Current.CancellationToken);
        await db.Client.UpsertSavedQueryAsync("west_customers", "SELECT Name, Total FROM Customers WHERE Region = 'West';", TestContext.Current.CancellationToken);
        await db.Client.UpsertSavedQueryAsync("cleanup_customers", "DELETE FROM Customers WHERE Total < 0;", TestContext.Current.CancellationToken);
        await db.Client.UpsertSavedQueryAsync("customer_by_id", "SELECT * FROM Customers WHERE Id = @id;", TestContext.Current.CancellationToken);
        await db.Client.UpsertSavedQueryAsync("__designer_layout:customers", "SELECT * FROM Customers;", TestContext.Current.CancellationToken);

        var provider = new DbReportSourceProvider(db.Client);
        IReadOnlyList<ReportSourceReferenceItem> sources = await provider.ListSourceReferencesAsync();

        Assert.Contains(sources, item => item.Kind == ReportSourceKind.Table && item.Name == "Customers");
        Assert.Contains(sources, item => item.Kind == ReportSourceKind.View && item.Name == "CustomerTotals");
        Assert.Contains(sources, item => item.Kind == ReportSourceKind.SavedQuery && item.Name == "west_customers");
        Assert.DoesNotContain(sources, item => item.Name == "_internal_metrics");
        Assert.DoesNotContain(sources, item => item.Name == "cleanup_customers");
        Assert.DoesNotContain(sources, item => item.Name == "customer_by_id");
        Assert.DoesNotContain(sources, item => item.Name == "__designer_layout:customers");

        Assert.Null(await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.SavedQuery, "cleanup_customers")));
        Assert.Null(await provider.GetSourceDefinitionAsync(new ReportSourceReference(ReportSourceKind.SavedQuery, "customer_by_id")));
    }

    private static Task CreateSchemaAsync(TestDatabaseScope db)
        => db.ExecuteAsync(
            """
            CREATE TABLE Customers (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Region TEXT NOT NULL,
                Total REAL NOT NULL
            );
            INSERT INTO Customers (Id, Name, Region, Total) VALUES
                (1, 'Ada', 'West', 10.5),
                (2, 'Ben', 'East', 21.0),
                (3, 'Cara', 'West', 30.0);
            """);
}

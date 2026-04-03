using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Evaluation;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;

namespace CSharpDB.Admin.Forms.Tests.Services;

public class DbFormRecordServiceTests
{
    [Fact]
    public async Task ListRecordsAsync_OrdersByPrimaryKeyAndSupportsFiltering()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateCustomerSchemaAsync(db);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition customers = (await provider.GetTableDefinitionAsync("Customers"))!;

        List<Dictionary<string, object?>> all = await service.ListRecordsAsync(customers, TestContext.Current.CancellationToken);
        List<Dictionary<string, object?>> active = await service.ListFilteredRecordsAsync(customers, "IsActive", 1L, TestContext.Current.CancellationToken);

        Assert.Equal([1L, 2L], all.Select(row => row["Id"]).ToArray());
        Assert.Single(active);
        Assert.Equal("Ada", active[0]["Name"]);
    }

    [Fact]
    public async Task ListRecordPageAsync_ReturnsRequestedSliceAndClampsToLastPage()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'One');
            INSERT INTO Events VALUES (2, 'Two');
            INSERT INTO Events VALUES (3, 'Three');
            INSERT INTO Events VALUES (4, 'Four');
            INSERT INTO Events VALUES (5, 'Five');
            """);

        var provider = new DbSchemaProvider(db.Client);
        IFormRecordService service = new DbFormRecordService(db.Client);
        FormTableDefinition eventsTable = (await provider.GetTableDefinitionAsync("Events"))!;

        FormRecordPage firstPage = await service.ListRecordPageAsync(eventsTable, 1, 2, TestContext.Current.CancellationToken);
        FormRecordPage clampedPage = await service.ListRecordPageAsync(eventsTable, 99, 2, TestContext.Current.CancellationToken);

        Assert.Equal(1, firstPage.PageNumber);
        Assert.Equal(2, firstPage.PageSize);
        Assert.Equal(5, firstPage.TotalCount);
        Assert.Equal([1L, 2L], firstPage.Records.Select(row => row["Id"]).ToArray());

        Assert.Equal(3, clampedPage.PageNumber);
        Assert.Equal([5L], clampedPage.Records.Select(row => row["Id"]).ToArray());
    }

    [Fact]
    public async Task GetRecordOrdinalAsync_ReturnsSortedZeroBasedPosition()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateCustomerSchemaAsync(db);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition customers = (await provider.GetTableDefinitionAsync("Customers"))!;

        int? firstOrdinal = await service.GetRecordOrdinalAsync(customers, 1L, TestContext.Current.CancellationToken);
        int? secondOrdinal = await service.GetRecordOrdinalAsync(customers, 2L, TestContext.Current.CancellationToken);
        int? missingOrdinal = await service.GetRecordOrdinalAsync(customers, 999L, TestContext.Current.CancellationToken);

        Assert.Equal(0, firstOrdinal);
        Assert.Equal(1, secondOrdinal);
        Assert.Null(missingOrdinal);
    }

    [Fact]
    public async Task SearchRecordPageAsync_ReturnsPagedMatchingRows()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'Alpha');
            INSERT INTO Events VALUES (2, 'Beta');
            INSERT INTO Events VALUES (3, 'Alpha Prime');
            INSERT INTO Events VALUES (4, 'Gamma');
            """);

        var provider = new DbSchemaProvider(db.Client);
        IFormRecordService service = new DbFormRecordService(db.Client);
        FormTableDefinition eventsTable = (await provider.GetTableDefinitionAsync("Events"))!;

        FormRecordPage firstPage = await service.SearchRecordPageAsync(eventsTable, "Name", "Alpha", 1, 1, TestContext.Current.CancellationToken);
        FormRecordPage secondPage = await service.SearchRecordPageAsync(eventsTable, "Name", "Alpha", 2, 1, TestContext.Current.CancellationToken);

        Assert.Equal(2, firstPage.TotalCount);
        Assert.Equal([1L], firstPage.Records.Select(row => row["Id"]).ToArray());
        Assert.Equal([3L], secondPage.Records.Select(row => row["Id"]).ToArray());
    }

    [Fact]
    public async Task GetRecordWindowAsync_ReturnsFocusedWindowAroundRecord()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (45, 'Forty Five');
            INSERT INTO Events VALUES (46, 'Forty Six');
            INSERT INTO Events VALUES (47, 'Forty Seven');
            INSERT INTO Events VALUES (48, 'Forty Eight');
            INSERT INTO Events VALUES (49, 'Forty Nine');
            INSERT INTO Events VALUES (50, 'Fifty');
            INSERT INTO Events VALUES (51, 'Fifty One');
            INSERT INTO Events VALUES (52, 'Fifty Two');
            INSERT INTO Events VALUES (53, 'Fifty Three');
            INSERT INTO Events VALUES (54, 'Fifty Four');
            INSERT INTO Events VALUES (55, 'Fifty Five');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition eventsTable = (await provider.GetTableDefinitionAsync("Events"))!;

        FormRecordWindow? window = await service.GetRecordWindowAsync(eventsTable, 50L, 5, TestContext.Current.CancellationToken);

        Assert.NotNull(window);
        Assert.Equal([48L, 49L, 50L, 51L, 52L], window!.Records.Select(row => row["Id"]).ToArray());
        Assert.Equal(2, window.SelectedIndex);
        Assert.True(window.HasPreviousRecords);
        Assert.True(window.HasNextRecords);
    }

    [Fact]
    public async Task GetAdjacentRecordAsync_ReturnsPreviousAndNextRows()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (10, 'Ten');
            INSERT INTO Events VALUES (11, 'Eleven');
            INSERT INTO Events VALUES (12, 'Twelve');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition eventsTable = (await provider.GetTableDefinitionAsync("Events"))!;

        Dictionary<string, object?>? previous = await service.GetAdjacentRecordAsync(eventsTable, 11L, previous: true, TestContext.Current.CancellationToken);
        Dictionary<string, object?>? next = await service.GetAdjacentRecordAsync(eventsTable, 11L, previous: false, TestContext.Current.CancellationToken);
        Dictionary<string, object?>? missing = await service.GetAdjacentRecordAsync(eventsTable, 10L, previous: true, TestContext.Current.CancellationToken);

        Assert.NotNull(previous);
        Assert.Equal(10L, previous!["Id"]);
        Assert.NotNull(next);
        Assert.Equal(12L, next!["Id"]);
        Assert.Null(missing);
    }

    [Fact]
    public async Task GetRecordOrdinalAsync_WithSearchFilter_ReturnsOrdinalWithinMatches()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'Alpha');
            INSERT INTO Events VALUES (2, 'Beta');
            INSERT INTO Events VALUES (3, 'Alpha Prime');
            INSERT INTO Events VALUES (4, 'Gamma');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition eventsTable = (await provider.GetTableDefinitionAsync("Events"))!;

        int? firstOrdinal = await service.GetRecordOrdinalAsync(eventsTable, 1L, "Name", "Alpha", TestContext.Current.CancellationToken);
        int? secondOrdinal = await service.GetRecordOrdinalAsync(eventsTable, 3L, "Name", "Alpha", TestContext.Current.CancellationToken);
        int? filteredOutOrdinal = await service.GetRecordOrdinalAsync(eventsTable, 4L, "Name", "Alpha", TestContext.Current.CancellationToken);

        Assert.Equal(0, firstOrdinal);
        Assert.Equal(1, secondOrdinal);
        Assert.Null(filteredOutOrdinal);
    }

    [Fact]
    public async Task GetRecordWindowAsync_ForUnsupportedPrimaryKeyType_FallsBackToPagedWindow()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Measurements (
                Reading REAL PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Measurements VALUES (1.5, 'One');
            INSERT INTO Measurements VALUES (2.5, 'Two');
            INSERT INTO Measurements VALUES (3.5, 'Three');
            INSERT INTO Measurements VALUES (4.5, 'Four');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition measurements = (await provider.GetTableDefinitionAsync("Measurements"))!;

        FormRecordWindow? window = await service.GetRecordWindowAsync(measurements, 3.5, 2, TestContext.Current.CancellationToken);

        Assert.NotNull(window);
        Assert.Equal([3.5, 4.5], window!.Records.Select(row => Convert.ToDouble(row["Reading"])).ToArray());
        Assert.Equal(0, window.SelectedIndex);
        Assert.True(window.HasPreviousRecords);
        Assert.False(window.HasNextRecords);
    }

    [Fact]
    public async Task GetAdjacentRecordAsync_ForUnsupportedPrimaryKeyType_UsesPagedFallback()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Measurements (
                Reading REAL PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Measurements VALUES (1.5, 'One');
            INSERT INTO Measurements VALUES (2.5, 'Two');
            INSERT INTO Measurements VALUES (3.5, 'Three');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition measurements = (await provider.GetTableDefinitionAsync("Measurements"))!;

        Dictionary<string, object?>? next = await service.GetAdjacentRecordAsync(measurements, 2.5, previous: false, TestContext.Current.CancellationToken);

        Assert.NotNull(next);
        Assert.Equal(3.5, Convert.ToDouble(next!["Reading"]));
    }

    [Fact]
    public async Task CreateRecordAsync_WithValues_ReturnsInsertedRow()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync("CREATE TABLE Products (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Price REAL);");

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition products = (await provider.GetTableDefinitionAsync("Products"))!;

        Dictionary<string, object?> created = await service.CreateRecordAsync(
            products,
            new Dictionary<string, object?> { ["id"] = 10L, ["name"] = "Widget", ["price"] = 12.5 },
            TestContext.Current.CancellationToken);

        Assert.Equal(10L, created["Id"]);
        Assert.Equal("Widget", created["Name"]);
        Assert.Equal(12.5, created["Price"]);
    }

    [Fact]
    public async Task CreateRecordAsync_WithoutValues_UsesDefaultValuesAndReloads()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Templates (
                Id INTEGER PRIMARY KEY
            );
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition templates = (await provider.GetTableDefinitionAsync("Templates"))!;

        Dictionary<string, object?> created = await service.CreateRecordAsync(
            templates,
            new Dictionary<string, object?>(),
            TestContext.Current.CancellationToken);

        Assert.True(Convert.ToInt64(created["Id"]) > 0);
        Assert.Single(created);
    }

    [Fact]
    public async Task UpdateRecordAsync_UpdatesRowAndDeleteRecordAsync_RemovesIt()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Price REAL
            );
            INSERT INTO Products VALUES (1, 'Widget', 10.0);
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition products = (await provider.GetTableDefinitionAsync("Products"))!;

        Dictionary<string, object?> updated = await service.UpdateRecordAsync(
            products,
            1L,
            new Dictionary<string, object?> { ["Name"] = "Widget Pro", ["Price"] = 15.5 },
            TestContext.Current.CancellationToken);
        await service.DeleteRecordAsync(products, 1L, TestContext.Current.CancellationToken);
        List<Dictionary<string, object?>> remaining = await service.ListRecordsAsync(products, TestContext.Current.CancellationToken);

        Assert.Equal("Widget Pro", updated["Name"]);
        Assert.Equal(15.5, updated["Price"]);
        Assert.Empty(remaining);
    }

    [Fact]
    public void GetPrimaryKeyColumn_RejectsCompositeKeys()
    {
        var service = new DbFormRecordService(dbClient: null!);
        var table = new FormTableDefinition("Composite", "sig:composite", [], ["A", "B"], []);

        var ex = Assert.Throws<InvalidOperationException>(() => service.GetPrimaryKeyColumn(table));

        Assert.Contains("exactly one primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ListFilteredRecordsAsync_SupportsChildAggregateCalculations()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Orders (
                Id INTEGER PRIMARY KEY,
                CustomerName TEXT NOT NULL
            );
            CREATE TABLE OrderItems (
                Id INTEGER PRIMARY KEY,
                OrderId INTEGER REFERENCES Orders(Id),
                LineTotal REAL NOT NULL
            );
            INSERT INTO Orders VALUES (1, 'Ada');
            INSERT INTO Orders VALUES (2, 'Grace');
            INSERT INTO OrderItems VALUES (10, 1, 12.5);
            INSERT INTO OrderItems VALUES (11, 1, 13.0);
            INSERT INTO OrderItems VALUES (12, 2, 99.0);
            """);

        var provider = new DbSchemaProvider(db.Client);
        var service = new DbFormRecordService(db.Client);
        FormTableDefinition items = (await provider.GetTableDefinitionAsync("OrderItems"))!;

        List<Dictionary<string, object?>> childRows = await service.ListFilteredRecordsAsync(items, "OrderId", 1L, TestContext.Current.CancellationToken);
        double? total = FormulaEvaluator.EvaluateAggregate(
            "SUM",
            childRows.Select(row => (double?)Convert.ToDouble(row["LineTotal"])));

        Assert.Equal(2, childRows.Count);
        Assert.Equal(25.5, total);
    }

    private static Task CreateCustomerSchemaAsync(TestDatabaseScope db)
        => db.ExecuteAsync(
            """
            CREATE TABLE Customers (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                IsActive INTEGER NOT NULL
            );
            INSERT INTO Customers VALUES (2, 'Grace', 0);
            INSERT INTO Customers VALUES (1, 'Ada', 1);
            """);
}

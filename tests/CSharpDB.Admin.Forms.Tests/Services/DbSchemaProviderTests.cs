using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Forms.Tests.Services;

public class DbSchemaProviderTests
{
    private const string MetadataTableName = "__forms";

    [Fact]
    public async Task GetTableDefinitionAsync_MapsFieldsPrimaryKeyAndForeignKeys()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        var provider = new DbSchemaProvider(db.Client);

        FormTableDefinition? orders = await provider.GetTableDefinitionAsync("Orders");

        Assert.NotNull(orders);
        Assert.Equal("Orders", orders!.TableName);
        Assert.Equal(["Id"], orders.PrimaryKey);

        FormFieldDefinition id = Assert.Single(orders.Fields, field => field.Name == "Id");
        FormFieldDefinition customerId = Assert.Single(orders.Fields, field => field.Name == "CustomerId");
        FormFieldDefinition total = Assert.Single(orders.Fields, field => field.Name == "Total");

        Assert.Equal(FieldDataType.Int64, id.DataType);
        Assert.Equal(FieldDataType.Int64, customerId.DataType);
        Assert.Equal(FieldDataType.Double, total.DataType);
        Assert.Equal("Customer Id", customerId.DisplayName);

        FormForeignKeyDefinition fk = Assert.Single(orders.ForeignKeys);
        Assert.Equal("Customers", fk.ReferencedTable);
        Assert.Equal("CustomerId", fk.LocalFields[0]);
        Assert.Equal("Id", fk.ReferencedFields[0]);
    }

    [Fact]
    public async Task GetTableDefinitionAsync_FormatsDisplayNamesForUnderscoresAndCamelCase()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        var provider = new DbSchemaProvider(db.Client);

        FormTableDefinition? customers = await provider.GetTableDefinitionAsync("Customers");

        Assert.NotNull(customers);
        Assert.Equal("Full Name", Assert.Single(customers!.Fields, field => field.Name == "full_name").DisplayName);
        Assert.Equal("Created At", Assert.Single(customers.Fields, field => field.Name == "CreatedAt").DisplayName);
    }

    [Fact]
    public async Task GetTableDefinitionAsync_View_MapsReadOnlyFieldsWithoutPrimaryKey()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.ExecuteAsync(
            """
            CREATE VIEW CustomerSummaries AS
            SELECT full_name, CreatedAt
            FROM Customers;
            """);

        var provider = new DbSchemaProvider(db.Client);
        FormTableDefinition? view = await provider.GetTableDefinitionAsync("CustomerSummaries");

        Assert.NotNull(view);
        Assert.Equal(FormSourceKind.View, view!.SourceKind);
        Assert.Empty(view.PrimaryKey);
        Assert.False(view.SupportsWriteOperations);
        Assert.All(view.Fields, field => Assert.True(field.IsReadOnly));
        Assert.Equal(["full_name", "CreatedAt"], view.Fields.Select(field => field.Name).ToArray());
    }

    [Fact]
    public async Task ListTableNamesAsync_FiltersMetadataTable()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.ExecuteAsync("CREATE TABLE _internal_metrics (Id INTEGER PRIMARY KEY);");

        var repository = new DbFormRepository(db.Client);
        await repository.ListAsync();

        var provider = new DbSchemaProvider(db.Client);
        IReadOnlyList<string> tableNames = await provider.ListTableNamesAsync();

        Assert.Contains("Customers", tableNames);
        Assert.Contains("Orders", tableNames);
        Assert.DoesNotContain(MetadataTableName, tableNames);
        Assert.DoesNotContain("_internal_metrics", tableNames);
    }

    [Fact]
    public async Task ListSourceNamesAsync_IncludesViews()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        await db.ExecuteAsync(
            """
            CREATE VIEW CustomerSummaries AS
            SELECT full_name
            FROM Customers;
            """);

        var provider = new DbSchemaProvider(db.Client);
        IReadOnlyList<string> sourceNames = await provider.ListSourceNamesAsync();

        Assert.Contains("Customers", sourceNames);
        Assert.Contains("Orders", sourceNames);
        Assert.Contains("CustomerSummaries", sourceNames);
    }

    [Fact]
    public async Task GetTableDefinitionAsync_MetadataTable_ReturnsNull()
    {
        await using var db = await TestDatabaseScope.CreateAsync();

        var repository = new DbFormRepository(db.Client);
        await repository.ListAsync();

        var provider = new DbSchemaProvider(db.Client);
        FormTableDefinition? table = await provider.GetTableDefinitionAsync(MetadataTableName);

        Assert.Null(table);
    }

    [Fact]
    public async Task GetTableDefinitionAsync_MissingTable_ReturnsNull()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var provider = new DbSchemaProvider(db.Client);

        FormTableDefinition? table = await provider.GetTableDefinitionAsync("DoesNotExist");

        Assert.Null(table);
    }

    [Fact]
    public async Task SourceSchemaSignature_ChangesWhenSchemaChanges()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSchemaAsync(db);
        var provider = new DbSchemaProvider(db.Client);

        FormTableDefinition first = (await provider.GetTableDefinitionAsync("Customers"))!;
        await db.Client.AddColumnAsync("Customers", "Email", DbType.Text, notNull: false, ct: TestContext.Current.CancellationToken);
        FormTableDefinition second = (await provider.GetTableDefinitionAsync("Customers"))!;

        Assert.NotEqual(first.SourceSchemaSignature, second.SourceSchemaSignature);
    }

    private static Task CreateSchemaAsync(TestDatabaseScope db)
        => db.ExecuteAsync(
            """
            CREATE TABLE Customers (
                Id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                CreatedAt TEXT
            );
            CREATE TABLE Orders (
                Id INTEGER PRIMARY KEY,
                CustomerId INTEGER REFERENCES Customers(Id) ON DELETE CASCADE,
                Total REAL NOT NULL
            );
            """);
}

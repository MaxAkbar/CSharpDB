using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Client.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public class DbFormRepositoryTests
{
    [Fact]
    public async Task CreateAsync_InitializesMetadataTableAndPersistsForm()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSourceTablesAsync(db);
        var repository = new DbFormRepository(db.Client);

        FormDefinition created = await repository.CreateAsync(CreateForm("f1", "Customers", "Customer Form", "sig:customers:v1"));

        Assert.Equal("f1", created.FormId);
        Assert.Equal(1, created.DefinitionVersion);

        IReadOnlyList<Dictionary<string, object?>> rows = await db.QueryRowsAsync(
            "SELECT id, name, table_name, definition_version, source_schema_signature FROM __forms;");
        Dictionary<string, object?> row = Assert.Single(rows);
        Assert.Equal("f1", row["id"]);
        Assert.Equal("Customer Form", row["name"]);
        Assert.Equal("Customers", row["table_name"]);
        Assert.Equal(1L, row["definition_version"]);
        Assert.Equal("sig:customers:v1", row["source_schema_signature"]);

        var indexes = await db.Client.GetIndexesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(indexes, index => index.IndexName == "idx___forms_table_name" && index.TableName == "__forms");
    }

    [Fact]
    public async Task CreateAsync_GeneratesFormIdAndNormalizesName()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);

        FormDefinition created = await repository.CreateAsync(CreateForm("", "Customers", "   ", "sig:customers:v1"));

        Assert.False(string.IsNullOrWhiteSpace(created.FormId));
        Assert.Equal("Customers Form", created.Name);
        Assert.Equal(1, created.DefinitionVersion);
    }

    [Fact]
    public async Task CreateAsync_StoresGeneratedAutomationMetadata()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);

        FormDefinition created = await repository.CreateAsync(CreateForm("f-auto", "Orders", "Order Form", "sig:orders:v1") with
        {
            EventBindings =
            [
                new FormEventBinding(
                    FormEventKind.OnLoad,
                    "LoadOrder",
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditOrderLoad"),
                    ],
                    Name: "LoadActions")),
            ],
        });
        FormDefinition loaded = (await repository.GetAsync(created.FormId))!;

        Assert.NotNull(created.Automation);
        Assert.NotNull(loaded.Automation);
        Assert.Contains(loaded.Automation!.Commands!, command => command.Name == "LoadOrder");
        Assert.Contains(loaded.Automation.Commands!, command => command.Name == "AuditOrderLoad");

        IReadOnlyList<Dictionary<string, object?>> rows = await db.QueryRowsAsync(
            "SELECT definition_json FROM __forms WHERE id = 'f-auto';");
        string json = Assert.Single(rows)["definition_json"]!.ToString()!;
        Assert.Contains("\"automation\"", json);
    }

    [Fact]
    public async Task TryUpdateAsync_CorrectVersion_UpdatesAndIncrementsVersion()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);
        await repository.CreateAsync(CreateForm("f1", "Customers", "Customer Form", "sig:customers:v1"));

        UpdateResult result = await repository.TryUpdateAsync(
            "f1",
            1,
            CreateForm("f1", "Customers", "Updated Form", "sig:customers:v1"));

        var ok = Assert.IsType<UpdateResult.Ok>(result);
        Assert.Equal(2, ok.Doc.DefinitionVersion);
        Assert.Equal("Updated Form", ok.Doc.Name);
    }

    [Fact]
    public async Task TryUpdateAsync_WrongVersion_ReturnsConflict()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);
        await repository.CreateAsync(CreateForm("f1", "Customers", "Customer Form", "sig:customers:v1"));

        UpdateResult result = await repository.TryUpdateAsync(
            "f1",
            99,
            CreateForm("f1", "Customers", "Updated Form", "sig:customers:v1"));

        Assert.IsType<UpdateResult.Conflict>(result);
    }

    [Fact]
    public async Task TryUpdateAsync_NonExistent_ReturnsNotFound()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);

        UpdateResult result = await repository.TryUpdateAsync(
            "missing",
            1,
            CreateForm("missing", "Customers", "Missing", "sig:customers:v1"));

        Assert.IsType<UpdateResult.NotFound>(result);
    }

    [Fact]
    public async Task ListAsync_FiltersByTableNameAndDeleteAsync_RemovesForm()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);
        await repository.CreateAsync(CreateForm("f1", "Customers", "Customer Form", "sig:customers:v1"));
        await repository.CreateAsync(CreateForm("f2", "Orders", "Order Form", "sig:orders:v1"));

        IReadOnlyList<FormDefinition> customers = await repository.ListAsync("Customers");
        IReadOnlyList<FormDefinition> all = await repository.ListAsync();
        bool deleted = await repository.DeleteAsync("f1");

        Assert.Single(customers);
        Assert.Equal("Customers", customers[0].TableName);
        Assert.Equal(2, all.Count);
        Assert.True(deleted);
        Assert.Null(await repository.GetAsync("f1"));
    }

    [Fact]
    public async Task CreateAsync_RejectsBlankSourceObjectName()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.CreateAsync(CreateForm("f1", "", "Broken", "sig:missing")));

        Assert.Contains("source object", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateAsync_RejectsBlankSourceSchemaSignature()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        var repository = new DbFormRepository(db.Client);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            repository.CreateAsync(CreateForm("f1", "Customers", "Broken", "")));

        Assert.Contains("source schema signature", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PersistedFormSignature_CanBeComparedAgainstCurrentSchema()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await CreateSourceTablesAsync(db);

        var provider = new DbSchemaProvider(db.Client);
        var repository = new DbFormRepository(db.Client);

        FormTableDefinition originalTable = (await provider.GetTableDefinitionAsync("Customers"))!;
        await repository.CreateAsync(CreateForm("f1", "Customers", "Customer Form", originalTable.SourceSchemaSignature));

        await db.Client.AddColumnAsync("Customers", "Email", CSharpDB.Client.Models.DbType.Text, notNull: false, ct: TestContext.Current.CancellationToken);

        FormDefinition stored = (await repository.GetAsync("f1"))!;
        FormTableDefinition currentTable = (await provider.GetTableDefinitionAsync("Customers"))!;

        Assert.NotEqual(stored.SourceSchemaSignature, currentTable.SourceSchemaSignature);
    }

    private static Task CreateSourceTablesAsync(TestDatabaseScope db)
        => db.ExecuteAsync(
            """
            CREATE TABLE Customers (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE Orders (
                Id INTEGER PRIMARY KEY,
                CustomerId INTEGER REFERENCES Customers(Id),
                Total REAL NOT NULL
            );
            """);

    private static FormDefinition CreateForm(string formId, string tableName, string name, string sourceSchemaSignature)
        => new(
            formId,
            name,
            tableName,
            1,
            sourceSchemaSignature,
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            []);
}

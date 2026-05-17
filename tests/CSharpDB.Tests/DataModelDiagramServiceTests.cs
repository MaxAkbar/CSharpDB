using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class DataModelDiagramServiceTests : IAsyncLifetime
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_diagram_{Guid.NewGuid():N}.db");
    private ICSharpDbClient _client = null!;
    private DataModelService _service = null!;

    public async ValueTask InitializeAsync()
    {
        _client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _dbPath });
        _service = new DataModelService(_client);
        Assert.Null((await _client.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);")).Error);
        Assert.Null((await _client.ExecuteSqlAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id));")).Error);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task SaveLoadDiagram_RoundTripsTableMembershipAndPlacement()
    {
        DataModelState state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        state.Nodes.RemoveAll(node => node.Name == "orders");
        state.Relationships.RemoveAll(relationship => relationship.LeftTable == "orders" || relationship.RightTable == "orders");
        DataModelNode customers = Assert.Single(state.Nodes, node => node.Name == "customers");
        customers.X = 321;
        customers.Y = 123;
        customers.IsCollapsed = true;

        await _service.SaveDiagramAsync("Customer Diagram", state, TestContext.Current.CancellationToken);

        IReadOnlyList<DataModelDiagramSummary> diagrams = await _service.GetDiagramsAsync(TestContext.Current.CancellationToken);
        DataModelDiagramSummary summary = Assert.Single(diagrams);
        Assert.Equal("Customer Diagram", summary.Name);
        Assert.Equal(1, summary.SourceCount);

        DataModelState? loaded = await _service.LoadDiagramAsync("Customer Diagram", TestContext.Current.CancellationToken);

        Assert.NotNull(loaded);
        DataModelNode loadedCustomers = Assert.Single(loaded.Nodes, node => node.Name == "customers");
        Assert.Equal(321, loadedCustomers.X);
        Assert.Equal(123, loadedCustomers.Y);
        Assert.True(loadedCustomers.IsCollapsed);
        Assert.DoesNotContain(loaded.Nodes, node => node.Name == "orders");
    }

    [Fact]
    public async Task SaveDiagram_CreatesHiddenInternalTableAndSysDiagramsMetadata()
    {
        DataModelState state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        state.Nodes.RemoveAll(node => node.Name == "orders");
        state.Relationships.RemoveAll(relationship => relationship.LeftTable == "orders" || relationship.RightTable == "orders");
        await _service.SaveDiagramAsync("Metadata", state, TestContext.Current.CancellationToken);

        SqlExecutionResult sys = await _client.ExecuteSqlAsync(
            "SELECT name, source_count FROM sys.diagrams WHERE name = 'Metadata';",
            TestContext.Current.CancellationToken);
        Assert.Null(sys.Error);
        object?[] row = Assert.Single(sys.Rows!);
        Assert.Equal("Metadata", row[0]);
        Assert.Equal(1L, Convert.ToInt64(row[1]));

        IReadOnlyList<string> tables = await _client.GetTableNamesAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain("__data_model_diagrams", tables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ApplyPendingOperations_CreatesStagedTableAndClearsPendingOperations()
    {
        var state = new DataModelState
        {
            DiagramName = "Pending",
            Nodes =
            [
                new DataModelNode
                {
                    Name = "diagram_created",
                    IsDraft = true,
                    Columns =
                    [
                        new DataModelColumn
                        {
                            Name = "id",
                            TypeLabel = "INTEGER",
                            IsPrimaryKey = true,
                            IsIdentity = true,
                            Nullable = false,
                        },
                    ],
                },
            ],
            PendingOperations =
            [
                new DataModelPendingOperation
                {
                    Kind = DataModelPendingOperationKind.CreateTable,
                    TableName = "diagram_created",
                    Description = "Create table diagram_created",
                    Columns =
                    [
                        new DataModelColumn
                        {
                            Name = "id",
                            TypeLabel = "INTEGER",
                            IsPrimaryKey = true,
                            IsIdentity = true,
                            Nullable = false,
                        },
                    ],
                },
            ],
        };
        await _service.SaveDiagramAsync("Pending", state, TestContext.Current.CancellationToken);

        DataModelApplyResult result = await _service.ApplyPendingOperationsAsync(state, TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Empty(state.PendingOperations);
        Assert.NotNull(await _client.GetTableSchemaAsync("diagram_created", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ApplyPendingOperations_RemovesDroppedTableFromSavedDiagram()
    {
        Assert.Null((await _client.ExecuteSqlAsync("CREATE TABLE diagram_drop (id INTEGER PRIMARY KEY);", TestContext.Current.CancellationToken)).Error);
        DataModelState state = await _service.BuildModelAsync("diagram_drop", ct: TestContext.Current.CancellationToken);
        state.DiagramName = "Drop Diagram Table";
        state.Relationships.Clear();
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.DropTable,
            TableName = "diagram_drop",
            Description = "Drop table diagram_drop",
        });
        await _service.SaveDiagramAsync("Drop Diagram Table", state, TestContext.Current.CancellationToken);

        await _service.ApplyPendingOperationsAsync(state, TestContext.Current.CancellationToken);
        DataModelState? loaded = await _service.LoadDiagramAsync("Drop Diagram Table", TestContext.Current.CancellationToken);

        Assert.Empty(state.PendingOperations);
        Assert.DoesNotContain(state.Nodes, node => node.Name == "diagram_drop");
        Assert.NotNull(loaded);
        Assert.DoesNotContain(loaded!.Nodes, node => node.Name == "diagram_drop");
        Assert.Null(await _client.GetTableSchemaAsync("diagram_drop", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ApplyPendingOperations_UpdatesSavedDiagramForRenameAndColumnChanges()
    {
        DataModelState state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        state.DiagramName = "Schema Edits";
        state.Nodes.RemoveAll(node => node.Name == "orders");
        state.Relationships.Clear();
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.RenameTable,
            TableName = "customers",
            NewTableName = "clients",
            Description = "Rename table customers to clients",
        });
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.AddColumn,
            TableName = "clients",
            ColumnName = "email",
            ColumnType = "TEXT",
            Description = "Add column clients.email",
        });
        await _service.SaveDiagramAsync("Schema Edits", state, TestContext.Current.CancellationToken);

        await _service.ApplyPendingOperationsAsync(state, TestContext.Current.CancellationToken);
        DataModelState? loaded = await _service.LoadDiagramAsync("Schema Edits", TestContext.Current.CancellationToken);
        TableSchema? schema = await _client.GetTableSchemaAsync("clients", TestContext.Current.CancellationToken);

        Assert.NotNull(schema);
        Assert.Contains(schema!.Columns, column => column.Name == "email");
        Assert.DoesNotContain(state.Nodes, node => node.Name == "customers");
        Assert.Contains(state.Nodes, node => node.Name == "clients");
        Assert.NotNull(loaded);
        Assert.Contains(loaded!.Nodes, node => node.Name == "clients");
    }
}

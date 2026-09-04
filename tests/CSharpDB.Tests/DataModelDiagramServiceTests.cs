using CSharpDB.Admin.Models;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class DataModelDiagramServiceTests : IAsyncLifetime
{
    [Theory]
    [InlineData(AdminHostOpenMode.Direct, false)]
    [InlineData(AdminHostOpenMode.Direct, true)]
    [InlineData(AdminHostOpenMode.HybridIncrementalDurable, false)]
    [InlineData(AdminHostOpenMode.HybridIncrementalDurable, true)]
    public async Task DeleteDiagram_AfterLoadAllAndCopy_DeletesOnlySelectedRecordIncludingLast(AdminHostOpenMode mode, bool reopen)
    {
        var ct = TestContext.Current.CancellationToken;
        await _client.DisposeAsync();
        var options = AdminClientOptionsBuilder.BuildDirectDataSource(_dbPath, new AdminHostDatabaseOptions { OpenMode = mode });
        _client = CSharpDbClient.Create(options);
        _service = new DataModelService(_client);
        var all = await _service.BuildModelAsync(autoLayoutLimit: int.MaxValue, ct: ct);
        all.Nodes[0].X = 123;
        await _service.SaveDiagramAsync("Default Diagram", all, ct);
        await _service.SaveDiagramAsync("Fulfillment", all, ct);
        await _service.SaveDiagramAsync("Empty workspace", await _service.BuildSelectionAsync([], ct: ct), ct);
        if (reopen)
        {
            await _client.DisposeAsync();
            _client = CSharpDbClient.Create(options);
            _service = new DataModelService(_client);
        }

        Assert.Equal(3, (await _service.GetDiagramsAsync(ct)).Count);
        Assert.NotNull(await _service.LoadDiagramAsync("Fulfillment", ct));
        await _service.LoadDiagramAsync("Default Diagram", ct);
        await _service.DeleteDiagramAsync("Default Diagram", ct);
        Assert.Null(await _service.LoadDiagramAsync("Default Diagram", ct));
        Assert.Equal(new[] { "Empty workspace", "Fulfillment" }, (await _service.GetDiagramsAsync(ct)).Select(diagram => diagram.Name).ToArray());
        var copy = await _service.LoadDiagramAsync("Fulfillment", ct);
        Assert.NotNull(copy);
        Assert.Equal(123, copy.Nodes[0].X);
        await _service.DeleteDiagramAsync("Empty workspace", ct);
        await _service.DeleteDiagramAsync("Fulfillment", ct);
        Assert.Empty(await _service.GetDiagramsAsync(ct));
        // An already-deleted name is a no-op, not another count decrement.
        await _service.DeleteDiagramAsync("Fulfillment", ct);
        Assert.NotNull(await _client.GetTableSchemaAsync("customers", ct));
        Assert.NotNull(await _client.GetTableSchemaAsync("orders", ct));
        var stats = await _client.ExecuteSqlAsync("SELECT COUNT(*) FROM __data_model_diagrams", ct);
        Assert.Null(stats.Error);
        Assert.Equal(0L, Convert.ToInt64(Assert.Single(stats.Rows!)[0]));
        await _client.DisposeAsync();
        _client = CSharpDbClient.Create(options);
        _service = new DataModelService(_client);
        Assert.Empty(await _service.GetDiagramsAsync(ct));
    }

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
    public async Task SaveLoadDiagram_RestoresConnectorLayoutWithoutChangingSchemaOrPendingOperations()
    {
        DataModelState state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        string schemaPreview = _service.BuildPreviewSql(state);
        string pendingPreview = _service.BuildPendingOperationsPreview(state);
        Assert.Single(state.Relationships).ConnectorLayout = CustomLayout();
        state.ViewportX = 147;
        state.ViewportY = 235;
        state.Scale = 1.25;

        await _service.SaveDiagramAsync("Custom connectors", state, TestContext.Current.CancellationToken);
        DataModelState loaded = Assert.IsType<DataModelState>(
            await _service.LoadDiagramAsync("Custom connectors", TestContext.Current.CancellationToken));

        Assert.Equal(5, loaded.Version);
        AssertCustomLayout(Assert.Single(loaded.Relationships).ConnectorLayout);
        Assert.Equal((147d, 235d, 1.25d), (loaded.ViewportX, loaded.ViewportY, loaded.Scale));
        Assert.Equal(schemaPreview, _service.BuildPreviewSql(loaded));
        Assert.Equal(pendingPreview, _service.BuildPendingOperationsPreview(loaded));
        Assert.Empty(loaded.PendingOperations);
    }

    [Theory]
    [InlineData(300)]
    [InlineData(380.5)]
    [InlineData(480)]
    public async Task SlidSegment_RestoresOneStraightMiddleRunAfterDatabaseReopen(double x)
    {
        var ct = TestContext.Current.CancellationToken;
        var state = await _service.BuildModelAsync("customers", ct: ct);
        var parent = state.Nodes.Single(node => node.Name == "customers");
        var child = state.Nodes.Single(node => node.Name == "orders");
        parent.X = 24; parent.Y = 40; child.X = 520; child.Y = 160;
        var relationship = Assert.Single(state.Relationships);
        double parentY = DataModelCanvasMetrics.ColumnCenterY(state, parent, relationship.RightColumn);
        double childY = DataModelCanvasMetrics.ColumnCenterY(state, child, relationship.LeftColumn);
        relationship.ConnectorLayout = new DataModelConnectorLayout
        {
            ParentSide = DataModelConnectorSide.Right, ChildSide = DataModelConnectorSide.Left,
            FollowEndpointRows = true,
            Waypoints = [new() { Id = "upper-corner", X = x, Y = parentY }, new() { Id = "lower-corner", X = x, Y = childY }]
        };
        // Reproduce slide -> move both tables -> save. Do not rewrite the saved corner Ys.
        parent.Y += 50; child.Y += 80;
        parentY += 50; childY += 80;
        string schema = _service.BuildPreviewSql(state);
        await _service.SaveDiagramAsync("Sliding segment", state, ct);
        await _client.DisposeAsync();
        _client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _dbPath });
        _service = new DataModelService(_client);
        var loaded = Assert.IsType<DataModelState>(await _service.LoadDiagramAsync("Sliding segment", ct));
        var layout = Assert.IsType<DataModelConnectorLayout>(Assert.Single(loaded.Relationships).ConnectorLayout);
        Assert.True(layout.FollowEndpointRows);
        Assert.Equal(parentY - 50, layout.Waypoints[0].Y);
        Assert.Equal(childY - 80, layout.Waypoints[1].Y);
        Assert.Equal(new[] { "upper-corner", "lower-corner" }, layout.Waypoints.Select(point => point.Id));
        Assert.All(layout.Waypoints, point => Assert.Equal(x, point.X));
        var route = DataModelConnectorRouter.Route(new(244, parentY, DataModelConnectorSide.Right),
            new(520, childY, DataModelConnectorSide.Left),
            loaded.Nodes.Select(node => new DataModelConnectorObstacle(node.X, node.Y, DataModelCanvasMetrics.NodeWidth,
                DataModelCanvasMetrics.NodeHeight(loaded, node))).ToArray(), layout);
        Assert.False(route.UsedAutomaticFallback);
        Assert.Equal(new DataModelConnectorPoint[] { new(244, parentY), new(x, parentY), new(x, childY), new(520, childY) }, route.Points);
        Assert.Equal(state.Nodes.Select(node => (node.Name, node.X, node.Y)), loaded.Nodes.Select(node => (node.Name, node.X, node.Y)));
        Assert.Equal(schema, _service.BuildPreviewSql(loaded));
        Assert.Empty(loaded.PendingOperations);
    }

    [Fact]
    public async Task ApplyPendingOperations_TransfersDraftConnectorRouteToAppliedPhysicalForeignKey()
    {
        Assert.Null((await _client.ExecuteSqlAsync(
            "CREATE TABLE shipments (id INTEGER PRIMARY KEY, customer_id INTEGER);",
            TestContext.Current.CancellationToken)).Error);
        DataModelState state = await _service.BuildSelectionAsync(["customers", "shipments"], ct: TestContext.Current.CancellationToken);
        state.DiagramName = "Draft routes";
        var operation = new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.AddForeignKey,
            TableName = "shipments",
            ColumnName = "customer_id",
            ReferencedTableName = "customers",
            ReferencedColumnName = "id",
        };
        state.PendingOperations.Add(operation);
        state.Relationships.Add(new DataModelRelationship
        {
            Id = operation.Id,
            Kind = DataModelRelationshipKind.Draft,
            LeftTable = "shipments",
            LeftColumn = "customer_id",
            RightTable = "customers",
            RightColumn = "id",
            ConnectorLayout = CustomLayout(),
        });

        DataModelApplyResult result = await _service.ApplyPendingOperationsAsync(state, TestContext.Current.CancellationToken);
        DataModelRelationship applied = Assert.Single(state.Relationships);
        Assert.True(result.Succeeded);
        Assert.Equal(DataModelRelationshipKind.PhysicalForeignKey, applied.Kind);
        Assert.NotEqual(operation.Id, applied.Id);
        Assert.False(string.IsNullOrWhiteSpace(applied.ConstraintName));
        AssertCustomLayout(applied.ConnectorLayout);
        Assert.Empty(state.PendingOperations);

        DataModelState loaded = Assert.IsType<DataModelState>(
            await _service.LoadDiagramAsync("Draft routes", TestContext.Current.CancellationToken));
        DataModelRelationship restored = Assert.Single(loaded.Relationships);
        Assert.Equal(applied.ConstraintName, restored.ConstraintName);
        Assert.Equal(DataModelRelationshipKind.PhysicalForeignKey, restored.Kind);
        AssertCustomLayout(restored.ConnectorLayout);
    }

    [Fact]
    public async Task ApplyPendingOperations_RenamesPreserveCustomConnectorLayoutAfterReload()
    {
        DataModelState state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        state.DiagramName = "Renamed routes";
        Assert.Single(state.Relationships).ConnectorLayout = CustomLayout();
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.RenameTable,
            TableName = "orders",
            NewTableName = "purchases",
        });
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.RenameColumn,
            TableName = "purchases",
            ColumnName = "customer_id",
            NewColumnName = "buyer_id",
        });

        await _service.ApplyPendingOperationsAsync(state, TestContext.Current.CancellationToken);
        DataModelState loaded = Assert.IsType<DataModelState>(
            await _service.LoadDiagramAsync("Renamed routes", TestContext.Current.CancellationToken));

        DataModelRelationship restored = Assert.Single(loaded.Relationships);
        Assert.Equal("purchases", restored.LeftTable);
        Assert.Equal("buyer_id", restored.LeftColumn);
        AssertCustomLayout(restored.ConnectorLayout);
        Assert.Empty(loaded.Warnings);
    }

    [Fact]
    public async Task GroupsSaveLoadAndAppliedRenameKeepMembershipAndDiagramOnlyMetadata()
    {
        var state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        string ddl = _service.BuildPreviewSql(state);
        var group = DataModelGroups.Create(state, ["customers", "orders"])!;
        group.Name = "Customer Orders"; group.Color = DataModelGroupColor.Green;
        state.DiagramName = "Grouped";
        Assert.Single(state.Relationships).ConnectorLayout = CustomLayout();
        DataModelGroups.Move(state, group.Id, 75, 50);
        var positions = state.Nodes.ToDictionary(node => node.Name, node => (node.X, node.Y));
        await _service.SaveDiagramAsync("Grouped", state, TestContext.Current.CancellationToken);
        var loaded = (await _service.LoadDiagramAsync("Grouped", TestContext.Current.CancellationToken))!;
        Assert.Equal(group.Name, Assert.Single(loaded.Groups).Name);
        Assert.Equal(group.Color, loaded.Groups[0].Color);
        Assert.All(loaded.Nodes, node => { Assert.Equal(group.Id, node.GroupId); Assert.Equal(positions[node.Name], (node.X, node.Y)); });
        Assert.Equal(ddl, _service.BuildPreviewSql(loaded));
        Assert.Empty(loaded.PendingOperations);
        loaded.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.RenameTable, TableName = "orders", NewTableName = "purchases" });
        await _service.ApplyPendingOperationsAsync(loaded, TestContext.Current.CancellationToken);
        var renamed = (await _service.LoadDiagramAsync("Grouped", TestContext.Current.CancellationToken))!;
        Assert.Equal(group.Id, renamed.Nodes.Single(node => node.Name == "purchases").GroupId);
        Assert.Equal(positions["orders"], (renamed.Nodes.Single(node => node.Name == "purchases").X, renamed.Nodes.Single(node => node.Name == "purchases").Y));
        Assert.Empty(renamed.PendingOperations);
    }

    [Fact]
    public async Task MissingSourcesPruneGroupMembershipButKeepRemainingSingletonAndWarnings()
    {
        var state = await _service.BuildModelAsync("customers", ct: TestContext.Current.CancellationToken);
        var group = DataModelGroups.Create(state, ["customers", "orders"])!;
        await _service.SaveDiagramAsync("Missing group member", state, TestContext.Current.CancellationToken);
        Assert.Null((await _client.ExecuteSqlAsync("DROP TABLE orders", TestContext.Current.CancellationToken)).Error);
        var loaded = (await _service.LoadDiagramAsync("Missing group member", TestContext.Current.CancellationToken))!;
        Assert.Single(loaded.Groups);
        Assert.Equal(group.Id, Assert.Single(loaded.Nodes).GroupId);
        Assert.Contains(loaded.Warnings, warning => warning.Contains("orders"));
    }

    [Fact]
    public async Task GroupedDiagramsWithSameNameStayIsolatedBetweenRouteClients()
    {
        string otherPath = Path.Combine(Path.GetTempPath(), $"csharpdb_group_route_{Guid.NewGuid():N}.db");
        try
        {
            await using var otherClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = otherPath });
            Assert.Null((await otherClient.ExecuteSqlAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY); CREATE TABLE orders (id INTEGER PRIMARY KEY);", TestContext.Current.CancellationToken)).Error);
            var otherService = new DataModelService(otherClient);
            var first = await _service.BuildSelectionAsync(["customers", "orders"], ct: TestContext.Current.CancellationToken);
            var second = await otherService.BuildSelectionAsync(["customers", "orders"], ct: TestContext.Current.CancellationToken);
            DataModelGroups.Create(first, ["customers", "orders"])!.Name = "Route A";
            DataModelGroups.Create(second, ["customers", "orders"])!.Name = "Route B";
            await _service.SaveDiagramAsync("Same diagram name", first, TestContext.Current.CancellationToken);
            await otherService.SaveDiagramAsync("Same diagram name", second, TestContext.Current.CancellationToken);
            Assert.Equal("Route A", Assert.Single((await _service.LoadDiagramAsync("Same diagram name", TestContext.Current.CancellationToken))!.Groups).Name);
            Assert.Equal("Route B", Assert.Single((await otherService.LoadDiagramAsync("Same diagram name", TestContext.Current.CancellationToken))!.Groups).Name);
        }
        finally
        {
            if (File.Exists(otherPath)) File.Delete(otherPath);
            if (File.Exists(otherPath + ".wal")) File.Delete(otherPath + ".wal");
        }
    }

    private static DataModelConnectorLayout CustomLayout() => new()
    {
        ParentSide = DataModelConnectorSide.Right,
        ChildSide = DataModelConnectorSide.Left,
        Waypoints = [new DataModelConnectorWaypoint { Id = "saved-bend", X = 700.25, Y = 345.5 }],
    };

    private static void AssertCustomLayout(DataModelConnectorLayout? layout)
    {
        Assert.NotNull(layout);
        Assert.Equal(DataModelConnectorSide.Right, layout.ParentSide);
        Assert.Equal(DataModelConnectorSide.Left, layout.ChildSide);
        DataModelConnectorWaypoint waypoint = Assert.Single(layout.Waypoints);
        Assert.Equal(("saved-bend", 700.25, 345.5), (waypoint.Id, waypoint.X, waypoint.Y));
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
            SchemaFingerprint = (await _service.BuildSelectionAsync([], ct: TestContext.Current.CancellationToken)).SchemaFingerprint,
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
    public async Task ApplyPendingOperations_AddColumnPreservesLogicalTypeFacets()
    {
        DataModelState state = await _service.BuildModelAsync(
            "orders",
            ct: TestContext.Current.CancellationToken);
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Kind = DataModelPendingOperationKind.AddColumn,
            TableName = "orders",
            ColumnName = "amount",
            ColumnType = "decimal(18,4)",
        });

        DataModelApplyResult result = await _service.ApplyPendingOperationsAsync(
            state,
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        TableSchema schema = Assert.IsType<TableSchema>(
            await _client.GetTableSchemaAsync(
                "orders",
                TestContext.Current.CancellationToken));
        Assert.Equal(
            "DECIMAL(18,4)",
            Assert.Single(schema.Columns, column => column.Name == "amount")
                .EffectiveType
                .ToSql());
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

using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Tests;

public sealed class DataModelGraphBuilderTests
{
    [Fact]
    public void RelationshipRules_SetNullRequiresNullableChildColumn()
    {
        var nullable = new DataModelColumn { Name = "ParentId", Nullable = true };
        var required = new DataModelColumn { Name = "ParentId", Nullable = false };
        var primaryKey = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = true,
            IsPrimaryKey = true,
        };

        Assert.True(
            DataModelRelationshipRules.IsDeleteActionCompatible(
                "SET NULL",
                nullable));
        Assert.False(
            DataModelRelationshipRules.IsDeleteActionCompatible(
                "SET NULL",
                required));
        Assert.False(
            DataModelRelationshipRules.IsDeleteActionCompatible(
                "SET NULL",
                primaryKey));
        Assert.False(
            DataModelRelationshipRules.IsDeleteActionCompatible(
                "SET NULL",
                childColumn: null));
        Assert.True(
            DataModelRelationshipRules.IsActionCompatible(
                "CASCADE",
                required));
    }

    [Fact]
    public void RelationshipRules_SetDefaultRequiresNullableOrNonNullDefault()
    {
        var nullable = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = true,
        };
        var withDefault = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = false,
            DefaultSql = "2",
        };
        var withoutDefault = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = false,
        };
        var nullDefault = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = false,
            DefaultSql = "(NULL)",
        };
        var nullablePrimaryKey = new DataModelColumn
        {
            Name = "ParentId",
            Nullable = true,
            IsPrimaryKey = true,
        };

        Assert.True(
            DataModelRelationshipRules.IsActionCompatible(
                "SET DEFAULT",
                nullable));
        Assert.True(
            DataModelRelationshipRules.IsActionCompatible(
                "set-default",
                withDefault));
        Assert.False(
            DataModelRelationshipRules.IsActionCompatible(
                "SET DEFAULT",
                withoutDefault));
        Assert.False(
            DataModelRelationshipRules.IsActionCompatible(
                "SET DEFAULT",
                nullDefault));
        Assert.False(
            DataModelRelationshipRules.IsActionCompatible(
                "SET DEFAULT",
                nullablePrimaryKey));
        Assert.False(
            DataModelRelationshipRules.IsActionCompatible(
                "SET DEFAULT",
                childColumn: null));
    }

    [Fact]
    public void Build_MapsColumnsIndexesAndPhysicalForeignKeys()
    {
        DataModelState state = DataModelGraphBuilder.Build(
        [
            Customers(),
            Orders(),
        ]);

        DataModelNode orders = Assert.Single(state.Nodes, node => node.Name == "Orders");
        DataModelColumn customerId = Assert.Single(orders.Columns, column => column.Name == "CustomerId");
        Assert.True(customerId.IsForeignKey);
        Assert.True(customerId.IsIndexed);
        Assert.Equal(
            "'new'",
            Assert.Single(orders.Columns, column => column.Name == "Status").DefaultSql);

        DataModelRelationship relationship = Assert.Single(state.Relationships);
        Assert.Equal(DataModelRelationshipKind.PhysicalForeignKey, relationship.Kind);
        Assert.Equal("Orders", relationship.LeftTable);
        Assert.Equal("Customers", relationship.RightTable);
        Assert.True(relationship.IsResolved);
        Assert.Equal(DataModelCardinality.One, relationship.ReferencedEndCardinality);
        Assert.Equal(DataModelCardinality.ZeroOrMany, relationship.ReferencingEndCardinality);
        Assert.False(relationship.ChildColumnIsUnique);
    }

    [Fact]
    public void BuildSelection_ExactAndOneHopHaveCuratedMembership()
    {
        DataModelSourceMetadata orderLines = new()
        {
            TableName = "OrderLines",
            Columns =
            [
                new DataModelColumnMetadata { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true, Nullable = false },
                new DataModelColumnMetadata { Name = "OrderId", TypeLabel = "INTEGER", Nullable = false },
            ],
            ForeignKeys =
            [
                new DataModelForeignKeyMetadata
                {
                    ConstraintName = "fk_lines_orders",
                    ColumnName = "OrderId",
                    ReferencedTableName = "Orders",
                    ReferencedColumnName = "Id",
                },
            ],
        };
        DataModelSourceMetadata unrelated = new()
        {
            TableName = "AuditLog",
            Columns = [new DataModelColumnMetadata { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true }],
        };
        DataModelSourceMetadata[] sources = [unrelated, orderLines, Customers(), Orders()];

        DataModelState exact = DataModelGraphBuilder.BuildSelection(
            sources,
            ["Orders"],
            DataModelSelectionMode.Exact);
        DataModelState related = DataModelGraphBuilder.BuildSelection(
            sources,
            ["Orders"],
            DataModelSelectionMode.IncludeDirectlyRelated);

        Assert.Equal(["Orders"], exact.Nodes.Select(node => node.Name));
        Assert.Equal(
            ["Customers", "OrderLines", "Orders"],
            related.Nodes.Select(node => node.Name).OrderBy(name => name, StringComparer.OrdinalIgnoreCase));
        Assert.DoesNotContain(related.Nodes, node => node.Name == "AuditLog");
    }

    [Fact]
    public void Build_UniqueNullableChildInfersOptionalOneAtBothEnds()
    {
        DataModelSourceMetadata profile = new()
        {
            TableName = "CustomerProfiles",
            Columns =
            [
                new DataModelColumnMetadata { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true, Nullable = false },
                new DataModelColumnMetadata { Name = "CustomerId", TypeLabel = "INTEGER", Nullable = true },
            ],
            ForeignKeys =
            [
                new DataModelForeignKeyMetadata
                {
                    ConstraintName = "fk_profiles_customers",
                    ColumnName = "CustomerId",
                    ReferencedTableName = "Customers",
                    ReferencedColumnName = "Id",
                },
            ],
            Indexes =
            [
                new DataModelIndexMetadata
                {
                    IndexName = "uq_profiles_customer",
                    Columns = ["CustomerId"],
                    IsUnique = true,
                },
            ],
        };

        DataModelState state = DataModelGraphBuilder.Build([Customers(), profile]);

        DataModelRelationship relationship = Assert.Single(state.Relationships);
        Assert.Equal(DataModelCardinality.ZeroOrOne, relationship.ReferencedEndCardinality);
        Assert.Equal(DataModelCardinality.ZeroOrOne, relationship.ReferencingEndCardinality);
        Assert.True(relationship.ChildColumnIsUnique);
        Assert.True(Assert.Single(state.Nodes, node => node.Name == "CustomerProfiles").Columns.Single(column => column.Name == "CustomerId").IsUnique);
    }

    [Fact]
    public async Task DataModelService_MapsDefaultsAndPhase3ReferentialActions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_data_model_phase3_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = CSharpDB.Client.CSharpDbClient.Create(
                new CSharpDB.Client.CSharpDbClientOptions
                {
                    DataSource = databasePath,
                });
            CSharpDB.Client.Models.SqlExecutionResult create =
                await client.ExecuteSqlAsync(
                    """
                    CREATE TABLE model_parents (id INTEGER PRIMARY KEY);
                    CREATE TABLE model_children (
                        id INTEGER PRIMARY KEY,
                        parent_id INTEGER NOT NULL DEFAULT 1
                            REFERENCES model_parents(id)
                            ON DELETE SET DEFAULT
                            ON UPDATE CASCADE
                    );
                    """,
                    ct);
            Assert.Null(create.Error);

            var service = new DataModelService(client);
            DataModelState state = await service.BuildModelAsync(ct: ct);
            DataModelNode child = Assert.Single(
                state.Nodes,
                node => node.Name == "model_children");
            Assert.Equal(
                "1",
                Assert.Single(
                    child.Columns,
                    column => column.Name == "parent_id").DefaultSql);
            DataModelRelationship relationship = Assert.Single(
                state.Relationships,
                item => item.LeftTable == "model_children");
            Assert.Equal("SET DEFAULT", relationship.OnDelete);
            Assert.Equal("CASCADE", relationship.OnUpdate);
        }
        finally
        {
            if (File.Exists(databasePath))
                File.Delete(databasePath);
            if (File.Exists(databasePath + ".wal"))
                File.Delete(databasePath + ".wal");
        }
    }

    [Fact]
    public void Build_ExternalArchiveForeignKey_UsesRegisteredExternalNode()
    {
        DataModelState state = DataModelGraphBuilder.Build(
        [
            Customers(),
            ArchivedOrders(),
        ]);

        DataModelRelationship relationship = Assert.Single(state.Relationships);
        Assert.Equal(DataModelRelationshipKind.ExternalArchiveForeignKey, relationship.Kind);
        Assert.Equal("archived_orders", relationship.LeftTable);
        Assert.Equal("Customers", relationship.RightTable);
        Assert.True(relationship.IsResolved);
    }

    [Fact]
    public void Build_UnresolvedRelationship_AddsWarning()
    {
        DataModelSourceMetadata orphaned = new()
        {
            TableName = "Orders",
            Columns = Orders().Columns,
            ForeignKeys =
            [
                new DataModelForeignKeyMetadata
                {
                    ConstraintName = "fk_orders_missing",
                    ColumnName = "CustomerId",
                    ReferencedTableName = "MissingCustomers",
                    ReferencedColumnName = "Id",
                    OnDelete = "RESTRICT",
                },
            ],
        };

        DataModelState state = DataModelGraphBuilder.Build([orphaned]);

        DataModelRelationship relationship = Assert.Single(state.Relationships);
        Assert.False(relationship.IsResolved);
        Assert.Contains("MissingCustomers", Assert.Single(state.Warnings));
    }

    [Fact]
    public void Build_GlobalLargeModel_StartsEmptyWithWarning()
    {
        DataModelSourceMetadata[] sources = Enumerable.Range(1, 3)
            .Select(i => new DataModelSourceMetadata
            {
                TableName = $"Customers{i}",
                Columns = Customers().Columns,
            })
            .ToArray();

        DataModelState state = DataModelGraphBuilder.Build(sources, autoLayoutLimit: 2);

        Assert.Empty(state.Nodes);
        Assert.Contains("Add individual tables", Assert.Single(state.Warnings));
    }

    [Fact]
    public void ToQueryDesignerState_MapsCurrentCanvas()
    {
        DataModelState state = DataModelGraphBuilder.Build(
        [
            Customers(),
            Orders(),
        ]);

        QueryDesignerState designer = DataModelGraphBuilder.ToQueryDesignerState(state);

        Assert.Equal(2, designer.Tables.Count);
        Assert.Equal(5, designer.GridRows.Count);
        DesignerJoin join = Assert.Single(designer.Joins);
        Assert.Equal("Customers", join.LeftTable);
        Assert.Equal("Orders", join.RightTable);
        Assert.Equal(DesignerJoinType.Inner, join.JoinType);
    }

    [Fact]
    public void SerializeState_PreservesCanvasScale()
    {
        DataModelState state = DataModelGraphBuilder.Build([Customers()]);
        state.Scale = 1.4;

        DataModelState? roundTripped = DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state));

        Assert.NotNull(roundTripped);
        Assert.Equal(1.4, roundTripped.Scale);
    }

    [Fact]
    public void DeserializeState_MigratesVersionOneDetailWithoutLosingState()
    {
        const string json =
            """
            {
              "Version": 1,
              "DiagramName": "Legacy",
              "SavedLayoutName": "Legacy",
              "ViewportX": 41,
              "ViewportY": 73,
              "Scale": 1.35,
              "Warnings": ["legacy warning"],
              "Nodes": [
                { "Name": "Collapsed", "X": 12, "Y": 34, "IsCollapsed": true, "Columns": [] },
                { "Name": "Expanded", "X": 56, "Y": 78, "IsCollapsed": false, "Columns": [] }
              ],
              "Relationships": [],
              "PendingOperations": [
                { "Id": "pending", "Kind": 1, "TableName": "Expanded", "Description": "Drop table Expanded" }
              ]
            }
            """;

        DataModelState state = Assert.IsType<DataModelState>(DataModelGraphBuilder.DeserializeState(json));

        Assert.Equal(4, state.Version);
        Assert.Equal(DataModelNodeDetailLevel.Collapsed, Assert.Single(state.Nodes, node => node.Name == "Collapsed").DetailLevel);
        Assert.Equal(DataModelNodeDetailLevel.All, Assert.Single(state.Nodes, node => node.Name == "Expanded").DetailLevel);
        Assert.Equal((41d, 73d, 1.35d), (state.ViewportX, state.ViewportY, state.Scale));
        Assert.Equal((12d, 34d), (state.Nodes[0].X, state.Nodes[0].Y));
        Assert.Single(state.PendingOperations);
        Assert.Equal("legacy warning", Assert.Single(state.Warnings));
    }

    [Fact]
    public void SerializeState_RoundTripsVersionThreeDetailCardinalityViewportAndConnectorLayout()
    {
        DataModelState state = DataModelGraphBuilder.Build([Customers(), Orders()]);
        state.Nodes[0].DetailLevel = DataModelNodeDetailLevel.Collapsed;
        state.Nodes[1].DetailLevel = DataModelNodeDetailLevel.All;
        state.ViewportX = 115;
        state.ViewportY = 225;
        state.Scale = 0.8;
        Assert.Single(state.Relationships).ConnectorLayout = CustomLayout();

        DataModelState restored = Assert.IsType<DataModelState>(
            DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state)));

        Assert.Equal(4, restored.Version);
        Assert.Equal(DataModelNodeDetailLevel.Collapsed, restored.Nodes[0].DetailLevel);
        Assert.Equal(DataModelNodeDetailLevel.All, restored.Nodes[1].DetailLevel);
        Assert.Equal((115d, 225d, 0.8d), (restored.ViewportX, restored.ViewportY, restored.Scale));
        DataModelRelationship relationship = Assert.Single(restored.Relationships);
        Assert.Equal(DataModelCardinality.One, relationship.ReferencedEndCardinality);
        Assert.Equal(DataModelCardinality.ZeroOrMany, relationship.ReferencingEndCardinality);
        AssertCustomLayout(relationship.ConnectorLayout);
    }

    [Fact]
    public void DeserializeState_VersionTwoMigratesWithAutomaticRoutes()
    {
        const string json = """
            { "Version": 2, "Nodes": [{ "Name": "Orders", "X": 70, "Y": 90, "DetailLevel": 0 }],
              "Relationships": [{ "Id": "old-fk", "LeftTable": "Orders", "RightTable": "Customers" }],
              "ViewportX": 23, "ViewportY": 45, "Scale": 0.65 }
            """;

        DataModelState restored = Assert.IsType<DataModelState>(DataModelGraphBuilder.DeserializeState(json));

        Assert.Equal(4, restored.Version);
        Assert.Null(Assert.Single(restored.Relationships).ConnectorLayout);
        Assert.Equal((70d, 90d, DataModelNodeDetailLevel.Keys),
            (restored.Nodes[0].X, restored.Nodes[0].Y, restored.Nodes[0].DetailLevel));
        Assert.Equal((23d, 45d, 0.65d), (restored.ViewportX, restored.ViewportY, restored.Scale));
    }

    [Theory]
    [InlineData(DataModelRelationshipKind.PhysicalForeignKey)]
    [InlineData(DataModelRelationshipKind.ExternalArchiveForeignKey)]
    [InlineData(DataModelRelationshipKind.Draft)]
    public void BuildFromDiagramState_RestoresCustomLayoutsOntoRebuiltRelationships(DataModelRelationshipKind kind)
    {
        DataModelSourceMetadata child = kind == DataModelRelationshipKind.ExternalArchiveForeignKey ? ArchivedOrders() : Orders();
        DataModelState saved = DataModelGraphBuilder.Build([Customers(), child]);
        DataModelRelationship original = Assert.Single(saved.Relationships);
        original.ConnectorLayout = CustomLayout();
        if (kind == DataModelRelationshipKind.Draft)
        {
            original.Kind = kind;
            original.Id = "draft-relationship";
            child = new DataModelSourceMetadata { TableName = "Orders", Columns = Orders().Columns };
        }

        DataModelState restored = DataModelGraphBuilder.BuildFromDiagramState([Customers(), child], saved);

        DataModelRelationship relationship = Assert.Single(restored.Relationships);
        Assert.Equal(kind, relationship.Kind);
        AssertCustomLayout(relationship.ConnectorLayout);
        Assert.NotSame(original.ConnectorLayout, relationship.ConnectorLayout);
        Assert.NotSame(original.ConnectorLayout.Waypoints[0], relationship.ConnectorLayout!.Waypoints[0]);
    }

    [Theory]
    [InlineData("id")]
    [InlineData("constraint")]
    [InlineData("endpoints")]
    public void PreserveConnectorLayouts_UsesStableIdentityFallbacks(string identity)
    {
        DataModelRelationship original = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        original.ConnectorLayout = CustomLayout();
        DataModelRelationship target = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        if (identity != "id")
            target.Id = "rebuilt-id";
        if (identity == "constraint")
            target.LeftColumn = "RenamedCustomerId";
        if (identity == "endpoints")
            target.ConstraintName = "generated_fk_name";

        IReadOnlyList<string> warnings = DataModelGraphBuilder.PreserveConnectorLayouts([original], [target]);

        Assert.Empty(warnings);
        AssertCustomLayout(target.ConnectorLayout);
        target.ConnectorLayout!.Waypoints[0].X = 999;
        Assert.Equal(400.25, original.ConnectorLayout.Waypoints[0].X);
    }

    [Fact]
    public void PreserveConnectorLayouts_AmbiguousMatchRevertsToAutomaticWithWarning()
    {
        DataModelRelationship original = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        original.Id = "old-id";
        original.ConnectorLayout = CustomLayout();
        DataModelRelationship first = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        DataModelRelationship second = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        second.Id = "parallel-fk";

        IReadOnlyList<string> warnings = DataModelGraphBuilder.PreserveConnectorLayouts([original], [first, second]);

        Assert.Null(first.ConnectorLayout);
        Assert.Null(second.ConnectorLayout);
        Assert.Contains("ambiguous", Assert.Single(warnings));
    }

    [Fact]
    public void PreserveConnectorLayouts_MultipleSavedRoutesCannotOverwriteOneRelationship()
    {
        DataModelRelationship first = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        DataModelRelationship second = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        first.ConnectorLayout = CustomLayout();
        second.ConnectorLayout = CustomLayout();
        second.Id = "another-saved-id";
        DataModelRelationship target = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);

        IReadOnlyList<string> warnings = DataModelGraphBuilder.PreserveConnectorLayouts([first, second], [target]);

        Assert.Null(target.ConnectorLayout);
        Assert.Contains(warnings, warning => warning.Contains("ambiguous", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PreserveConnectorLayouts_AmbiguityDoesNotDependOnInputOrdering(bool reverse)
    {
        DataModelRelationship exact = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        DataModelRelationship ambiguous = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        exact.ConnectorLayout = CustomLayout();
        ambiguous.ConnectorLayout = CustomLayout();
        ambiguous.Id = "old-id";
        DataModelRelationship first = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        DataModelRelationship second = Assert.Single(DataModelGraphBuilder.Build([Customers(), Orders()]).Relationships);
        second.Id = "duplicate-constraint";

        IReadOnlyList<string> warnings = DataModelGraphBuilder.PreserveConnectorLayouts(
            reverse ? [ambiguous, exact] : [exact, ambiguous], [first, second]);

        Assert.Null(first.ConnectorLayout);
        Assert.Null(second.ConnectorLayout);
        Assert.Contains(warnings, warning => warning.Contains("ambiguous", StringComparison.Ordinal));
    }

    [Fact]
    public void IsValidConnectorLayout_RejectsNonFiniteCoordinatesInvalidSidesAndExcessiveWaypoints()
    {
        DataModelConnectorLayout layout = CustomLayout();
        layout.Waypoints[0].X = double.NaN;
        Assert.False(DataModelGraphBuilder.IsValidConnectorLayout(layout));
        Assert.Null(DataModelGraphBuilder.CloneConnectorLayout(layout));
        layout.Waypoints[0].X = 100;
        layout.ParentSide = (DataModelConnectorSide)42;
        Assert.False(DataModelGraphBuilder.IsValidConnectorLayout(layout));
        layout.ParentSide = DataModelConnectorSide.Left;
        layout.Waypoints = Enumerable.Range(0, 129)
            .Select(index => new DataModelConnectorWaypoint { Id = index.ToString(), X = 100, Y = 100 })
            .ToList();
        Assert.False(DataModelGraphBuilder.IsValidConnectorLayout(layout));
    }

    [Fact]
    public void BuildFromDiagramState_StaleCustomRouteWarnsWithoutBreakingLoad()
    {
        DataModelState saved = DataModelGraphBuilder.Build([Customers(), Orders()]);
        Assert.Single(saved.Relationships).ConnectorLayout = CustomLayout();
        var childWithoutForeignKey = new DataModelSourceMetadata { TableName = "Orders", Columns = Orders().Columns };

        DataModelState restored = DataModelGraphBuilder.BuildFromDiagramState([Customers(), childWithoutForeignKey], saved);

        Assert.Equal(2, restored.Nodes.Count);
        Assert.Empty(restored.Relationships);
        Assert.Contains("relationship no longer exists", Assert.Single(restored.Warnings));
    }

    [Theory]
    [InlineData("null")]
    [InlineData("[null]")]
    [InlineData("[{\"Id\":\"same\",\"X\":10,\"Y\":10},{\"Id\":\"same\",\"X\":20,\"Y\":20}]")]
    [InlineData("[{\"Id\":\"bad\",\"X\":-4,\"Y\":10}]")]
    public void DeserializeState_InvalidCustomRouteUsesAutomaticRouting(string waypoints)
    {
        string json = "{\"Version\":3,\"Relationships\":[{\"Id\":\"fk\",\"ConnectorLayout\":{\"Waypoints\":" + waypoints + "}}]}";

        DataModelState restored = Assert.IsType<DataModelState>(DataModelGraphBuilder.DeserializeState(json));

        Assert.Null(Assert.Single(restored.Relationships).ConnectorLayout);
        Assert.Contains("invalid layout data", Assert.Single(restored.Warnings));
    }

    private static DataModelConnectorLayout CustomLayout() => new()
    {
        ParentSide = DataModelConnectorSide.Left,
        ChildSide = DataModelConnectorSide.Right,
        Waypoints =
        [
            new DataModelConnectorWaypoint { Id = "bend-one", X = 400.25, Y = 85.5 },
            new DataModelConnectorWaypoint { Id = "bend-two", X = 400.25, Y = 300.125 },
        ],
    };

    private static void AssertCustomLayout(DataModelConnectorLayout? layout)
    {
        Assert.NotNull(layout);
        Assert.Equal(DataModelConnectorSide.Left, layout.ParentSide);
        Assert.Equal(DataModelConnectorSide.Right, layout.ChildSide);
        Assert.Equal(2, layout.Waypoints.Count);
        Assert.Equal(("bend-one", 400.25, 85.5), (layout.Waypoints[0].Id, layout.Waypoints[0].X, layout.Waypoints[0].Y));
        Assert.Equal(("bend-two", 400.25, 300.125), (layout.Waypoints[1].Id, layout.Waypoints[1].X, layout.Waypoints[1].Y));
    }

    [Fact]
    public void BuildFromDiagramState_PreservesMembershipPlacementAndPendingOperations()
    {
        var saved = new DataModelState
        {
            DiagramName = "Fulfillment",
            Scale = 1.25,
            Nodes =
            [
                new DataModelNode { Name = "Customers", X = 42, Y = 84, IsCollapsed = true },
            ],
            PendingOperations =
            [
                new DataModelPendingOperation
                {
                    Kind = DataModelPendingOperationKind.DropTable,
                    TableName = "Orders",
                    Description = "Drop table Orders",
                },
            ],
        };

        DataModelState restored = DataModelGraphBuilder.BuildFromDiagramState(
        [
            Customers(),
            Orders(),
        ],
            saved);

        DataModelNode node = Assert.Single(restored.Nodes);
        Assert.Equal("Customers", node.Name);
        Assert.Equal(42, node.X);
        Assert.Equal(84, node.Y);
        Assert.True(node.IsCollapsed);
        Assert.Equal(1.25, restored.Scale);
        Assert.Single(restored.PendingOperations);
        Assert.Empty(restored.Relationships);
    }

    [Fact]
    public void BuildFromDiagramState_MissingSourceWarnsWithoutFailing()
    {
        var saved = new DataModelState
        {
            Nodes =
            [
                new DataModelNode { Name = "MissingCustomers" },
            ],
        };

        DataModelState restored = DataModelGraphBuilder.BuildFromDiagramState([Customers()], saved);

        Assert.Empty(restored.Nodes);
        Assert.Contains("MissingCustomers", Assert.Single(restored.Warnings));
    }

    private static DataModelSourceMetadata Customers() => new()
    {
        TableName = "Customers",
        Columns =
        [
            new DataModelColumnMetadata { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true, IsIdentity = true, Nullable = false },
            new DataModelColumnMetadata { Name = "Name", TypeLabel = "TEXT" },
        ],
        Indexes =
        [
            new DataModelIndexMetadata { IndexName = "idx_customers_name", Columns = ["Name"] },
        ],
    };

    private static DataModelSourceMetadata Orders() => new()
    {
        TableName = "Orders",
        Columns =
        [
            new DataModelColumnMetadata { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true, IsIdentity = true, Nullable = false },
            new DataModelColumnMetadata { Name = "CustomerId", TypeLabel = "INTEGER", Nullable = false },
            new DataModelColumnMetadata { Name = "Status", TypeLabel = "TEXT", DefaultSql = "'new'" },
        ],
        ForeignKeys =
        [
            new DataModelForeignKeyMetadata
            {
                ConstraintName = "fk_orders_customers",
                ColumnName = "CustomerId",
                ReferencedTableName = "Customers",
                ReferencedColumnName = "Id",
                OnDelete = "CASCADE",
            },
        ],
        Indexes =
        [
            new DataModelIndexMetadata { IndexName = "idx_orders_customer", Columns = ["CustomerId"] },
        ],
        TriggerCount = 1,
    };

    private static DataModelSourceMetadata ArchivedOrders() => new()
    {
        TableName = "archived_orders",
        Kind = DataModelNodeKind.ExternalTable,
        SourceTableName = "Orders",
        ArchivePath = "exports/orders.csdbtable",
        Columns = Orders().Columns,
        ForeignKeys = Orders().ForeignKeys,
    };
}

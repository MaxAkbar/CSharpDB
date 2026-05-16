using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Tests;

public sealed class DataModelGraphBuilderTests
{
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

        DataModelRelationship relationship = Assert.Single(state.Relationships);
        Assert.Equal(DataModelRelationshipKind.PhysicalForeignKey, relationship.Kind);
        Assert.Equal("Orders", relationship.LeftTable);
        Assert.Equal("Customers", relationship.RightTable);
        Assert.True(relationship.IsResolved);
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
            new DataModelColumnMetadata { Name = "Status", TypeLabel = "TEXT" },
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

using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Tests;

public sealed class DataModelLayoutEngineTests
{
    [Fact]
    public void ArrangeAll_PlacesParentsLeftOfAcyclicChildren()
    {
        DataModelState state = Model(
            [Node("LineItems"), Node("Customers"), Node("Orders")],
            [Relationship("Orders", "Customers"), Relationship("LineItems", "Orders")]);

        DataModelLayoutEngine.ArrangeAll(state);

        Assert.True(Find(state, "Customers").X < Find(state, "Orders").X);
        Assert.True(Find(state, "Orders").X < Find(state, "LineItems").X);
    }

    [Fact]
    public void ArrangeAll_CyclesAreDeterministicAndDoNotOverlap()
    {
        DataModelState first = Model(
            [Node("C"), Node("A"), Node("B")],
            [Relationship("B", "A"), Relationship("C", "B"), Relationship("A", "C")]);
        DataModelState second = Model(
            [Node("B"), Node("C"), Node("A")],
            [Relationship("A", "C"), Relationship("B", "A"), Relationship("C", "B")]);

        DataModelLayoutEngine.ArrangeAll(first);
        DataModelLayoutEngine.ArrangeAll(second);

        foreach (DataModelNode node in first.Nodes)
        {
            DataModelNode matching = Find(second, node.Name);
            Assert.Equal(node.X, matching.X);
            Assert.Equal(node.Y, matching.Y);
        }
        AssertNoOverlap(first);
    }

    [Fact]
    public void ArrangeAll_SeparatesConnectedComponentsAndIsolatedLane()
    {
        DataModelState state = Model(
            [Node("ParentA"), Node("ChildA"), Node("ParentB"), Node("ChildB"), Node("Unrelated")],
            [Relationship("ChildA", "ParentA"), Relationship("ChildB", "ParentB")]);

        DataModelLayoutEngine.ArrangeAll(state);

        AssertNoOverlap(state);
        double connectedBottom = state.Nodes
            .Where(node => node.Name != "Unrelated")
            .Max(node => node.Y + DataModelCanvasMetrics.NodeHeight(state, node));
        Assert.True(Find(state, "Unrelated").Y > connectedBottom);
    }

    [Fact]
    public void ArrangeAll_UsesVisibleNodeHeight()
    {
        DataModelNode tall = Node("Tall", DataModelNodeDetailLevel.All, columnCount: 12);
        DataModelNode shortNode = Node("Short", DataModelNodeDetailLevel.Keys);
        DataModelState state = Model([tall, shortNode], []);

        DataModelLayoutEngine.ArrangeAll(state);

        AssertNoOverlap(state);
    }

    [Fact]
    public void CountOverlaps_UsesCurrentDetailHeightsAndArrangementClearsThem()
    {
        DataModelNode first = Node("First", DataModelNodeDetailLevel.All, columnCount: 8);
        DataModelNode second = Node("Second", DataModelNodeDetailLevel.All, columnCount: 8);
        first.X = second.X = 40;
        first.Y = 40;
        second.Y = 120;
        DataModelState state = Model([first, second], []);

        Assert.Equal(1, DataModelLayoutEngine.CountOverlaps(state));

        DataModelLayoutEngine.ArrangeAll(state);

        Assert.Equal(0, DataModelLayoutEngine.CountOverlaps(state));
    }

    [Fact]
    public void PlaceNewNodes_PreservesAllExistingCoordinates()
    {
        DataModelNode parent = Node("Parent");
        parent.X = 117;
        parent.Y = 83;
        DataModelNode sibling = Node("Sibling");
        sibling.X = 560;
        sibling.Y = 320;
        DataModelNode child = Node("Child");
        DataModelState state = Model(
            [child, sibling, parent],
            [Relationship("Child", "Parent")]);

        DataModelLayoutEngine.PlaceNewNodes(
            state,
            new HashSet<string>(["Parent", "Sibling"], StringComparer.OrdinalIgnoreCase));

        Assert.Equal((117d, 83d), (parent.X, parent.Y));
        Assert.Equal((560d, 320d), (sibling.X, sibling.Y));
        Assert.True(child.X > parent.X);
        AssertNoOverlap(state);
    }

    [Fact]
    public void ArrangeAll_IsStableAcrossRelationshipKinds()
    {
        DataModelState state = Model(
            [Node("Parent"), Node("Physical"), Node("Draft"), Node("Archive")],
            [
                Relationship("Physical", "Parent", DataModelRelationshipKind.PhysicalForeignKey),
                Relationship("Draft", "Parent", DataModelRelationshipKind.Draft),
                Relationship("Archive", "Parent", DataModelRelationshipKind.ExternalArchiveForeignKey),
            ]);

        DataModelLayoutEngine.ArrangeAll(state);
        var first = state.Nodes.ToDictionary(node => node.Name, node => (node.X, node.Y));
        DataModelLayoutEngine.ArrangeAll(state);

        foreach (DataModelNode node in state.Nodes)
            Assert.Equal(first[node.Name], (node.X, node.Y));
    }

    [Fact]
    public void CanvasMetrics_KeysIncludesEveryVisibleRelationshipEndpoint()
    {
        DataModelNode parent = Node("Parent", columnCount: 3);
        parent.Columns[2].Name = "NaturalKey";
        DataModelNode child = Node("Child", columnCount: 4);
        child.Columns[3].Name = "ParentNaturalKey";
        DataModelState state = Model(
            [parent, child],
            [new DataModelRelationship
            {
                Id = "Child.ParentNaturalKey->Parent.NaturalKey",
                LeftTable = "Child",
                LeftColumn = "ParentNaturalKey",
                RightTable = "Parent",
                RightColumn = "NaturalKey",
            }]);

        Assert.Equal(["Id", "NaturalKey"], DataModelCanvasMetrics.GetVisibleColumns(state, parent).Select(column => column.Name));
        Assert.Equal(["Id", "ParentNaturalKey"], DataModelCanvasMetrics.GetVisibleColumns(state, child).Select(column => column.Name));
        Assert.True(DataModelCanvasMetrics.ColumnCenterY(state, parent, "NaturalKey") > parent.Y + DataModelCanvasMetrics.NodeHeaderHeight);

        child.DetailLevel = DataModelNodeDetailLevel.Collapsed;
        Assert.Empty(DataModelCanvasMetrics.GetVisibleColumns(state, child));
        Assert.Equal(child.Y + DataModelCanvasMetrics.NodeHeaderHeight / 2, DataModelCanvasMetrics.ColumnCenterY(state, child, "ParentNaturalKey"));
    }

    [Fact]
    public void ConnectorRouter_RoutesAroundAnInterveningTable()
    {
        var blocker = new DataModelConnectorObstacle(360, 100, 220, 240);
        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new DataModelConnectorEndpoint(220, 200, DataModelConnectorSide.Right),
            new DataModelConnectorEndpoint(720, 220, DataModelConnectorSide.Left),
            [
                new DataModelConnectorObstacle(0, 100, 220, 240),
                blocker,
                new DataModelConnectorObstacle(720, 100, 220, 240),
            ]);

        Assert.False(DataModelConnectorRouter.IntersectsObstacleInterior(route, blocker));
        Assert.True(route.Points.Count >= 6);
        Assert.All(route.Points.Zip(route.Points.Skip(1)), pair =>
            Assert.True(pair.First.X == pair.Second.X || pair.First.Y == pair.Second.Y));
    }

    [Fact]
    public void ConnectorRouter_IsDeterministicRegardlessOfObstacleOrder()
    {
        DataModelConnectorObstacle[] obstacles =
        [
            new(0, 100, 220, 180),
            new(360, 40, 220, 180),
            new(360, 300, 220, 180),
            new(720, 120, 220, 180),
        ];
        var start = new DataModelConnectorEndpoint(220, 180, DataModelConnectorSide.Right);
        var end = new DataModelConnectorEndpoint(720, 200, DataModelConnectorSide.Left);

        DataModelConnectorRoute first = DataModelConnectorRouter.Route(start, end, obstacles);
        DataModelConnectorRoute second = DataModelConnectorRouter.Route(start, end, obstacles.Reverse().ToArray());

        Assert.Equal(first.Points, second.Points);
    }

    [Fact]
    public void ConnectorRouter_UsesHorizontalEndpointStubsAndClearance()
    {
        var parent = new DataModelConnectorObstacle(24, 40, 220, 160);
        var child = new DataModelConnectorObstacle(520, 260, 220, 160);
        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new DataModelConnectorEndpoint(244, 120, DataModelConnectorSide.Right),
            new DataModelConnectorEndpoint(520, 340, DataModelConnectorSide.Left),
            [parent, child]);

        Assert.Equal(new DataModelConnectorPoint(244, 120), route.Points[0]);
        Assert.Equal(120, route.Points[1].Y);
        Assert.True(route.Points[1].X >= 268);
        Assert.Equal(340, route.Points[^2].Y);
        Assert.True(route.Points[^2].X <= 496);
        Assert.Equal(new DataModelConnectorPoint(520, 340), route.Points[^1]);
    }

    private static DataModelState Model(List<DataModelNode> nodes, List<DataModelRelationship> relationships) => new()
    {
        Nodes = nodes,
        Relationships = relationships,
    };

    private static DataModelNode Node(
        string name,
        DataModelNodeDetailLevel detailLevel = DataModelNodeDetailLevel.Keys,
        int columnCount = 2) => new()
    {
        Name = name,
        DetailLevel = detailLevel,
        Columns = Enumerable.Range(0, columnCount)
            .Select(index => new DataModelColumn
            {
                Name = index == 0 ? "Id" : $"Column{index}",
                TypeLabel = "INTEGER",
                IsPrimaryKey = index == 0,
            })
            .ToList(),
    };

    private static DataModelRelationship Relationship(
        string child,
        string parent,
        DataModelRelationshipKind kind = DataModelRelationshipKind.PhysicalForeignKey) => new()
    {
        Id = $"{child}->{parent}",
        LeftTable = child,
        LeftColumn = "Id",
        RightTable = parent,
        RightColumn = "Id",
        Kind = kind,
    };

    private static DataModelNode Find(DataModelState state, string name) =>
        Assert.Single(state.Nodes, node => node.Name == name);

    private static void AssertNoOverlap(DataModelState state)
    {
        for (int i = 0; i < state.Nodes.Count; i++)
        {
            DataModelNode left = state.Nodes[i];
            for (int j = i + 1; j < state.Nodes.Count; j++)
            {
                DataModelNode right = state.Nodes[j];
                bool overlaps = left.X < right.X + DataModelCanvasMetrics.NodeWidth &&
                                left.X + DataModelCanvasMetrics.NodeWidth > right.X &&
                                left.Y < right.Y + DataModelCanvasMetrics.NodeHeight(state, right) &&
                                left.Y + DataModelCanvasMetrics.NodeHeight(state, left) > right.Y;
                Assert.False(overlaps, $"{left.Name} overlaps {right.Name}.");
            }
        }
    }
}

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

    [Fact]
    public void ConnectorRouter_VisitsOrderedManualWaypointsAndAvoidsTables()
    {
        DataModelConnectorObstacle[] obstacles =
        [
            new(24, 100, 220, 160),
            new(400, 100, 220, 300),
            new(800, 100, 220, 160),
        ];
        DataModelConnectorLayout layout = CustomRoute((300, 60), (680, 60), (680, 320));

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(244, 174, DataModelConnectorSide.Right),
            new(800, 202, DataModelConnectorSide.Left), obstacles, layout);

        Assert.False(route.UsedAutomaticFallback);
        AssertOrthogonalAndClear(route, obstacles);
        int previous = -1;
        foreach (DataModelConnectorWaypoint waypoint in layout.Waypoints)
        {
            int index = route.Points.ToList().FindIndex(previous + 1, point => point == new DataModelConnectorPoint(waypoint.X, waypoint.Y));
            Assert.True(index > previous, "Manual waypoints must occur in their saved order.");
            previous = index;
        }
    }

    [Fact]
    public void ConnectorRouter_PreservesCollinearGuideAndDeliberateReversal()
    {
        DataModelConnectorLayout layout = CustomRoute((200, 40), (320, 40), (260, 40), (260, 160));
        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(40, 40, DataModelConnectorSide.Right),
            new(500, 160, DataModelConnectorSide.Left), [], layout);

        Assert.False(route.UsedAutomaticFallback);
        int last = -1;
        foreach (DataModelConnectorWaypoint guide in layout.Waypoints)
        {
            int index = route.Points.ToList().FindIndex(last + 1, point => point == new DataModelConnectorPoint(guide.X, guide.Y));
            Assert.True(index > last);
            last = index;
        }
    }

    [Fact]
    public void ConnectorRouter_ManualRoutesAreDeterministicRegardlessOfObstacleOrder()
    {
        DataModelConnectorObstacle[] obstacles =
        [new(24, 100, 220, 160), new(400, 100, 220, 300), new(800, 100, 220, 160)];
        DataModelConnectorLayout layout = CustomRoute((300, 60), (680, 60));
        var start = new DataModelConnectorEndpoint(244, 174, DataModelConnectorSide.Right);
        var end = new DataModelConnectorEndpoint(800, 202, DataModelConnectorSide.Left);

        DataModelConnectorRoute first = DataModelConnectorRouter.Route(start, end, obstacles, layout);
        DataModelConnectorRoute second = DataModelConnectorRouter.Route(start, end, obstacles.Reverse().ToArray(), layout);

        Assert.Equal(first.Points, second.Points);
        Assert.Equal((first.LabelX, first.LabelY), (second.LabelX, second.LabelY));
    }

    [Theory]
    [InlineData(400, 200)]
    [InlineData(390, 90)] // Inside the clearance gutter, although outside the card itself.
    [InlineData(0, 40)]
    [InlineData(40, -1)]
    [InlineData(double.NaN, 40)]
    [InlineData(40, double.PositiveInfinity)]
    public void ConnectorRouter_InvalidSavedWaypointFallsBackWithoutChangingTheLayout(double x, double y)
    {
        DataModelConnectorObstacle[] obstacles =
        [new(24, 100, 220, 160), new(400, 100, 220, 300), new(800, 100, 220, 160)];
        DataModelConnectorLayout layout = CustomRoute((x, y));
        var start = new DataModelConnectorEndpoint(244, 174, DataModelConnectorSide.Right);
        var end = new DataModelConnectorEndpoint(800, 202, DataModelConnectorSide.Left);

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(start, end, obstacles, layout);
        DataModelConnectorRoute automatic = DataModelConnectorRouter.Route(start, end, obstacles);

        Assert.True(route.UsedAutomaticFallback);
        Assert.Equal(automatic.Points, route.Points);
        Assert.Equal(x, layout.Waypoints[0].X);
        Assert.Equal(y, layout.Waypoints[0].Y);
        AssertOrthogonalAndClear(route, obstacles);
    }

    [Fact]
    public void ConnectorRouter_UnreachableWaypointUsesSafeAutomaticFallback()
    {
        DataModelConnectorObstacle[] obstacles =
        [
            new(24, 100, 220, 160), new(800, 100, 220, 160),
            // An enclosed cavity whose guide is valid but cannot be reached.
            new(350, 350, 300, 40), new(350, 610, 300, 40),
            new(350, 350, 40, 300), new(610, 350, 40, 300),
        ];
        DataModelConnectorLayout layout = CustomRoute((500, 500));

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(244, 174, DataModelConnectorSide.Right),
            new(800, 202, DataModelConnectorSide.Left), obstacles, layout);

        Assert.True(DataModelConnectorRouter.IsValidWaypoint(new(500, 500), obstacles));
        Assert.True(route.UsedAutomaticFallback);
        AssertOrthogonalAndClear(route, obstacles);
    }

    [Fact]
    public void ConnectorRouter_TableMovementKeepsTheManualCorridorFixed()
    {
        DataModelConnectorLayout layout = CustomRoute((300, 60), (680, 60));
        DataModelConnectorObstacle[] before = [new(24, 100, 220, 160), new(800, 100, 220, 160)];
        DataModelConnectorObstacle[] after = [new(24, 300, 220, 160), new(800, 400, 220, 160)];

        DataModelConnectorRoute first = DataModelConnectorRouter.Route(
            new(244, 174, DataModelConnectorSide.Right), new(800, 202, DataModelConnectorSide.Left), before, layout);
        DataModelConnectorRoute second = DataModelConnectorRouter.Route(
            new(244, 374, DataModelConnectorSide.Right), new(800, 502, DataModelConnectorSide.Left), after, layout);

        Assert.False(first.UsedAutomaticFallback);
        Assert.False(second.UsedAutomaticFallback);
        foreach (DataModelConnectorWaypoint guide in layout.Waypoints)
        {
            Assert.Contains(new(guide.X, guide.Y), first.Points);
            Assert.Contains(new(guide.X, guide.Y), second.Points);
        }
        AssertOrthogonalAndClear(second, after);
    }

    [Theory]
    [InlineData(DataModelConnectorSide.Left)]
    [InlineData(DataModelConnectorSide.Right)]
    public void ConnectorRouter_CustomSelfRelationshipStaysOutsideItsTable(DataModelConnectorSide side)
    {
        DataModelConnectorObstacle[] obstacles = [new(300, 100, 220, 300)];
        double edgeX = side == DataModelConnectorSide.Left ? 300 : 520;
        double guideX = side == DataModelConnectorSide.Left ? 240 : 580;
        DataModelConnectorLayout layout = CustomRoute((guideX, 174), (guideX, 230));

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(edgeX, 174, side), new(edgeX, 230, side), obstacles, layout);

        Assert.False(route.UsedAutomaticFallback);
        AssertOrthogonalAndClear(route, obstacles);
        Assert.Equal(174, route.Points[1].Y);
        Assert.Equal(230, route.Points[^2].Y);
    }

    [Fact]
    public void ConnectorRouter_ReversedTablesKeepSavedAttachmentSides()
    {
        DataModelConnectorObstacle[] obstacles = [new(700, 100, 220, 160), new(200, 100, 220, 160)];
        DataModelConnectorLayout layout = CustomRoute((980, 50), (140, 50));

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(920, 174, DataModelConnectorSide.Right), new(200, 202, DataModelConnectorSide.Left), obstacles, layout);

        Assert.False(route.UsedAutomaticFallback);
        Assert.True(route.Points[1].X >= 944);
        Assert.True(route.Points[^2].X <= 176);
        AssertOrthogonalAndClear(route, obstacles);
    }

    [Fact]
    public void ConnectorRouter_TightManualTableGapDoesNotRouteThroughCards()
    {
        DataModelConnectorObstacle[] obstacles = [new(24, 100, 220, 200), new(254, 100, 220, 200)];

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(244, 174, DataModelConnectorSide.Right), new(254, 230, DataModelConnectorSide.Left), obstacles);

        Assert.False(route.UsedAutomaticFallback);
        AssertOrthogonalAndClear(route, obstacles);
    }

    [Fact]
    public void ConnectorRouter_ParallelRelationshipsCanUseDistinctManualCorridors()
    {
        DataModelConnectorObstacle[] obstacles = [new(24, 100, 220, 160), new(800, 100, 220, 160)];
        var start = new DataModelConnectorEndpoint(244, 174, DataModelConnectorSide.Right);
        var end = new DataModelConnectorEndpoint(800, 202, DataModelConnectorSide.Left);

        DataModelConnectorRoute upper = DataModelConnectorRouter.Route(start, end, obstacles, CustomRoute((300, 60), (680, 60)));
        DataModelConnectorRoute lower = DataModelConnectorRouter.Route(start, end, obstacles, CustomRoute((300, 320), (680, 320)));

        Assert.NotEqual(upper.Points, lower.Points);
        AssertOrthogonalAndClear(upper, obstacles);
        AssertOrthogonalAndClear(lower, obstacles);
    }

    [Fact]
    public void ConnectorRouter_ExcessiveWaypointCountUsesAutomaticFallback()
    {
        DataModelConnectorLayout layout = CustomRoute(Enumerable.Range(0, DataModelConnectorRouter.MaximumWaypoints + 1)
            .Select(index => (200d + index, 60d)).ToArray());

        DataModelConnectorRoute route = DataModelConnectorRouter.Route(
            new(40, 40, DataModelConnectorSide.Right), new(500, 160, DataModelConnectorSide.Left), [], layout);

        Assert.True(route.UsedAutomaticFallback);
        Assert.Equal(DataModelConnectorRouter.MaximumWaypoints + 1, layout.Waypoints.Count);
    }

    private static DataModelConnectorLayout CustomRoute(params (double X, double Y)[] points) => new()
    {
        ParentSide = DataModelConnectorSide.Right,
        ChildSide = DataModelConnectorSide.Left,
        Waypoints = points.Select((point, index) => new DataModelConnectorWaypoint
        {
            Id = $"guide-{index}", X = point.X, Y = point.Y,
        }).ToList(),
    };

    private static void AssertOrthogonalAndClear(DataModelConnectorRoute route, IReadOnlyList<DataModelConnectorObstacle> obstacles)
    {
        Assert.All(route.Points.Zip(route.Points.Skip(1)), pair =>
            Assert.True(pair.First.X == pair.Second.X || pair.First.Y == pair.Second.Y));
        Assert.All(obstacles, obstacle => Assert.False(DataModelConnectorRouter.IntersectsObstacleInterior(route, obstacle)));
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

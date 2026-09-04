using System.Text.Json;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Tests;

public sealed class DataModelGroupTests
{
    private static DataModelState Model() => new()
    {
        Nodes = [Node("Parent", 100, 100), Node("Child", 500, 200), Node("Outside", 900, 500)],
        Relationships = [Edge("internal", "Child", "Parent"), Edge("crossing", "Outside", "Child")],
        PendingOperations = [new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "Parent", ColumnName = "Note" }],
    };
    private static DataModelNode Node(string name, double x, double y) => new()
    {
        Name = name, X = x, Y = y,
        Columns = [new() { Name = "id", IsPrimaryKey = true }, new() { Name = "name" }],
    };
    private static DataModelRelationship Edge(string id, string child, string parent) => new()
    {
        Id = id, LeftTable = child, RightTable = parent, LeftColumn = "id", RightColumn = "id",
        ConnectorLayout = new() { Waypoints = [new() { Id = id + "-guide", X = 400, Y = 450 }] },
    };

    [Fact]
    public void MembershipIsExplicitSingleOwnerAndNeverChangesPositionsOrSchema()
    {
        var state = Model();
        string operations = JsonSerializer.Serialize(state.PendingOperations);
        var positions = state.Nodes.Select(node => (node.X, node.Y)).ToArray();
        Assert.Null(DataModelGroups.Create(state, ["Parent"]));
        var first = DataModelGroups.Create(state, ["parent", "Child", "missing"])!;
        var second = DataModelGroups.Create(state, ["Child", "Outside"])!;
        Assert.Equal("Group 1", first.Name);
        Assert.Equal("Group 2", second.Name);
        Assert.Single(DataModelGroups.Members(state, first.Id));
        Assert.Equal(second.Id, state.Nodes[1].GroupId);
        DataModelGroups.Assign(state, ["Parent"], second.Id);
        Assert.Single(state.Groups);
        DataModelGroups.Ungroup(state, second.Id);
        Assert.Empty(state.Groups);
        Assert.All(state.Nodes, node => Assert.Null(node.GroupId));
        Assert.Equal(positions, state.Nodes.Select(node => (node.X, node.Y)));
        Assert.Equal(operations, JsonSerializer.Serialize(state.PendingOperations));
    }

    [Theory]
    [InlineData(DataModelNodeDetailLevel.Keys)]
    [InlineData(DataModelNodeDetailLevel.All)]
    [InlineData(DataModelNodeDetailLevel.Collapsed)]
    public void BoundsFollowVisibleCardsAndIndividualMoves(DataModelNodeDetailLevel level)
    {
        var state = Model();
        foreach (var node in state.Nodes) node.DetailLevel = level;
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        var before = DataModelGroups.Bounds(state, group.Id)!.Value;
        Assert.Equal(84, before.X);
        Assert.Equal(56, before.Y);
        Assert.Equal(736, before.Right);
        Assert.Equal(216 + DataModelCanvasMetrics.NodeHeight(state, state.Nodes[1]), before.Bottom);
        state.Nodes[1].X += 75;
        Assert.Equal(before.Width + 75, DataModelGroups.Bounds(state, group.Id)!.Value.Width);
        Assert.Equal(group.Id, state.Nodes[1].GroupId);
    }

    [Fact]
    public void MoveTranslatesMembersAndOnlyInternalGuidesAtomically()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        string operations = JsonSerializer.Serialize(state.PendingOperations);
        Assert.True(DataModelGroups.Move(state, group.Id, 25, 35));
        Assert.Equal((125d, 135d), (state.Nodes[0].X, state.Nodes[0].Y));
        Assert.Equal((525d, 235d), (state.Nodes[1].X, state.Nodes[1].Y));
        Assert.Equal((900d, 500d), (state.Nodes[2].X, state.Nodes[2].Y));
        Assert.Equal(425, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
        Assert.Equal(400, state.Relationships[1].ConnectorLayout!.Waypoints[0].X);
        Assert.Equal(operations, JsonSerializer.Serialize(state.PendingOperations));
        string before = JsonSerializer.Serialize(state);
        Assert.False(DataModelGroups.Move(state, group.Id, double.NaN, 20));
        Assert.False(DataModelGroups.Move(state, "missing", 20, 20));
        Assert.Equal(before, JsonSerializer.Serialize(state));
    }

    [Fact]
    public void MoveClampsWholeDeltaAtNodesAndGuideBounds()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        state.Relationships[0].ConnectorLayout!.Waypoints[0].X = 20;
        Assert.True(DataModelGroups.Move(state, group.Id, -1000, -1000));
        Assert.Equal(84, state.Nodes[0].X);
        Assert.Equal(0, state.Nodes[0].Y);
        Assert.Equal(4, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
        Assert.Equal(400, state.Nodes[1].X - state.Nodes[0].X);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void OlderDiagramsMigrateWithoutGroupsOrPositionChanges(int version)
    {
        var state = Model();
        DataModelGroups.Create(state, ["Parent", "Child"]);
        state.Version = version;
        state.ViewportX = 23; state.ViewportY = 47; state.Scale = 1.25;
        var loaded = DataModelGraphBuilder.DeserializeState(JsonSerializer.Serialize(state))!;
        Assert.Equal(5, loaded.Version);
        Assert.Empty(loaded.Groups);
        Assert.All(loaded.Nodes, node => Assert.Null(node.GroupId));
        Assert.Equal(100, loaded.Nodes[0].X);
        Assert.Equal((23d, 47d, 1.25d), (loaded.ViewportX, loaded.ViewportY, loaded.Scale));
        Assert.Single(loaded.PendingOperations);
        if (version == 3) Assert.NotNull(loaded.Relationships[0].ConnectorLayout);
    }

    [Fact]
    public void VersionFourRoundTripsAndPrunesInvalidOrMissingMembership()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        group.Name = "Fulfillment"; group.Color = DataModelGroupColor.Violet;
        var loaded = DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state))!;
        Assert.Equal(group.Id, loaded.Nodes[0].GroupId);
        Assert.Equal(group.Name, loaded.Groups[0].Name);
        Assert.Equal(group.Color, loaded.Groups[0].Color);
        Assert.NotSame(group, loaded.Groups[0]);
        var refreshed = new DataModelState { Nodes = [Node("PARENT", 100, 100)] };
        DataModelGroups.Preserve(loaded, refreshed);
        Assert.Single(refreshed.Groups);
        Assert.Equal(group.Id, refreshed.Nodes[0].GroupId);
        refreshed.Nodes.Clear();
        DataModelGroups.Normalize(refreshed);
        Assert.Empty(refreshed.Groups);
        loaded.Groups.Clear();
        DataModelGroups.Normalize(loaded);
        Assert.All(loaded.Nodes, node => Assert.Null(node.GroupId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ArrangementKeepsGroupsIntactDeterministicAndNonOverlapping(bool cyclic)
    {
        var state = Model();
        state.Nodes[1].IsDraft = true;
        state.Nodes[2].Kind = DataModelNodeKind.ExternalTable;
        state.Nodes.Add(Node("Isolated", 20, 20));
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        if (cyclic) state.Relationships.Add(Edge("cycle", "Parent", "Outside"));
        var copy = DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state))!;
        copy.Nodes.Reverse(); copy.Relationships.Reverse(); copy.Groups.Reverse();
        double offset = state.Relationships[0].ConnectorLayout!.Waypoints[0].X - state.Nodes[0].X;
        DataModelLayoutEngine.ArrangeAll(state); DataModelLayoutEngine.ArrangeAll(copy);
        Assert.Equal(0, DataModelGroups.CountOverlaps(state));
        Assert.Equal(0, DataModelLayoutEngine.CountOverlaps(state));
        Assert.Equal((400d, 100d), (state.Nodes[1].X - state.Nodes[0].X, state.Nodes[1].Y - state.Nodes[0].Y));
        Assert.Equal(offset, state.Relationships[0].ConnectorLayout!.Waypoints[0].X - state.Nodes[0].X);
        foreach (var node in state.Nodes)
        {
            var other = copy.Nodes.Single(other => other.Name == node.Name);
            Assert.Equal((node.X, node.Y), (other.X, other.Y));
        }
        var arranged = state.Nodes.Select(node => (node.X, node.Y)).ToArray();
        DataModelLayoutEngine.ArrangeAll(state);
        Assert.Equal(arranged, state.Nodes.Select(node => (node.X, node.Y)));
        if (!cyclic) Assert.True(DataModelGroups.Bounds(state, group.Id)!.Value.Right < state.Nodes[2].X);
    }

    [Fact]
    public void NewSourcesStayOutsideFramesAndExistingPositionsNeverMove()
    {
        var state = Model();
        DataModelGroups.Create(state, ["Parent", "Child"]);
        var before = state.Nodes.Select(node => (node.X, node.Y)).ToArray();
        var existing = state.Nodes.Select(node => node.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        state.Nodes.Add(Node("New", 0, 0));
        state.Relationships.Add(Edge("new", "New", "Parent"));
        DataModelLayoutEngine.PlaceNewNodes(state, existing);
        Assert.Equal(before, state.Nodes.Take(3).Select(node => (node.X, node.Y)));
        Assert.Null(state.Nodes[^1].GroupId);
        Assert.Equal(0, DataModelGroups.CountOverlaps(state));
    }

    [Fact]
    public void OverlapWarningIgnoresMembersButCountsNonmembersAndOtherGroups()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        Assert.Equal(0, DataModelGroups.CountOverlaps(state));
        state.Nodes[2].X = 300; state.Nodes[2].Y = 150;
        Assert.Equal(1, DataModelGroups.CountOverlaps(state));
        Assert.Null(state.Nodes[2].GroupId);
        DataModelGroups.Assign(state, ["Outside"], group.Id);
        Assert.Equal(0, DataModelGroups.CountOverlaps(state));
    }
}

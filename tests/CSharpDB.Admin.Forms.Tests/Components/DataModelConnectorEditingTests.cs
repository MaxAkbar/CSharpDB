using System.Reflection;
using System.Text.Json;
using CSharpDB.Admin.Components.Shared;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class DataModelConnectorEditingTests
{
    [Fact]
    public async Task Canvas_FinalConnectorEventDeepClonesLayoutWithoutSchemaOperations()
    {
        DataModelState state = Model();
        DataModelRelationship relationship = Assert.Single(state.Relationships);
        state.PendingOperations.Add(new DataModelPendingOperation
        {
            Id = "existing-operation",
            Kind = DataModelPendingOperationKind.AddColumn,
            TableName = "Child",
            ColumnName = "Note",
        });
        string operations = JsonSerializer.Serialize(state.PendingOperations);
        var received = new List<DataModelConnectorLayoutChange>();
        var canvas = new SchemaCanvas();
        SetProperty(canvas, nameof(SchemaCanvas.State), state);
        SetProperty(canvas, nameof(SchemaCanvas.OnConnectorRouteChanged),
            EventCallback.Factory.Create<DataModelConnectorLayoutChange>(new object(), received.Add));
        DataModelConnectorLayout submitted = Layout();

        bool accepted = await canvas.OnConnectorLayoutChanged(relationship.Id, submitted);

        Assert.True(accepted);
        Assert.Single(received);
        Assert.NotSame(submitted, relationship.ConnectorLayout);
        Assert.NotSame(submitted.Waypoints[0], relationship.ConnectorLayout!.Waypoints[0]);
        Assert.Equal(relationship.Id, received[0].RelationshipId);
        submitted.Waypoints[0].X = 999;
        Assert.Equal(430, relationship.ConnectorLayout.Waypoints[0].X);
        Assert.Equal(operations, JsonSerializer.Serialize(state.PendingOperations));
        Assert.Equal(DataModelRelationshipKind.PhysicalForeignKey, relationship.Kind);
        Assert.Equal(("Child", "ParentId", "Parent", "Id"),
            (relationship.LeftTable, relationship.LeftColumn, relationship.RightTable, relationship.RightColumn));

        Assert.True(await canvas.OnConnectorLayoutChanged(relationship.Id, new DataModelConnectorLayout()));
        Assert.Null(relationship.ConnectorLayout);
        Assert.Equal(2, received.Count);
        Assert.Equal(operations, JsonSerializer.Serialize(state.PendingOperations));
    }

    [Theory]
    [InlineData("inside-table")]
    [InlineData("invalid-side")]
    [InlineData("null-waypoints")]
    [InlineData("duplicate-ids")]
    [InlineData("non-finite")]
    public async Task Canvas_InvalidFinalConnectorEventKeepsPreviousRouteAndAnnouncesRejection(string invalidKind)
    {
        DataModelState state = Model();
        DataModelRelationship relationship = Assert.Single(state.Relationships);
        relationship.ConnectorLayout = Layout();
        string before = JsonSerializer.Serialize(relationship.ConnectorLayout);
        DataModelConnectorLayout invalid = Layout();
        switch (invalidKind)
        {
            case "inside-table": invalid.Waypoints[0].X = 90; invalid.Waypoints[0].Y = 120; break;
            case "invalid-side": invalid.ParentSide = (DataModelConnectorSide)99; break;
            case "null-waypoints": invalid.Waypoints = null!; break;
            case "duplicate-ids": invalid.Waypoints.Add(new DataModelConnectorWaypoint { Id = "bend", X = 450, Y = 360 }); break;
            case "non-finite": invalid.Waypoints[0].X = double.NaN; break;
        }
        int callbackCount = 0;
        await WithRenderedCanvasAsync(state, async (renderer, canvas, html) =>
        {
            SetProperty(canvas, nameof(SchemaCanvas.OnConnectorRouteChanged),
                EventCallback.Factory.Create<DataModelConnectorLayoutChange>(new object(), _ => callbackCount++));

            bool accepted = await canvas.OnConnectorLayoutChanged(relationship.Id, invalid);

            Assert.False(accepted);
            Assert.Equal(0, callbackCount);
            Assert.Equal(before, JsonSerializer.Serialize(relationship.ConnectorLayout));
            Assert.Contains("Invalid connector bend. The previous route was kept.", html());
        });
    }

    [Fact]
    public async Task Canvas_UnknownAndUnresolvedRelationshipsCannotBeEdited()
    {
        DataModelState state = Model();
        var canvas = new SchemaCanvas();
        SetProperty(canvas, nameof(SchemaCanvas.State), state);

        Assert.False(await canvas.OnConnectorLayoutChanged("missing", Layout()));
        state.Relationships[0].IsResolved = false;
        Assert.False(await canvas.OnConnectorLayoutChanged(state.Relationships[0].Id, Layout()));
        Assert.Null(state.Relationships[0].ConnectorLayout);
    }

    [Fact]
    public async Task Canvas_RenderingIncludesSavedRouteSelectedHandlesAndEndpointColumns()
    {
        DataModelState state = Model();
        state.Relationships[0].ConnectorLayout = Layout();

        await WithRenderedCanvasAsync(state, (renderer, canvas, html) =>
        {
            string markup = System.Net.WebUtility.HtmlDecode(html());
            Assert.Contains("data-connector-layout=\"{\"ParentSide\":1,\"ChildSide\":0,\"Waypoints\":[{\"Id\":\"bend\",\"X\":430,\"Y\":350}]}", markup);
            Assert.Contains("schema-relationship selected", markup);
            Assert.Contains("data-model-route-handles", markup);
            Assert.Contains("data-parent-column=\"Id\"", markup);
            Assert.Contains("data-child-column=\"ParentId\"", markup);
            Assert.Contains("relationship-endpoint", markup);
            Assert.Contains("aria-live=\"polite\"", markup);
            Assert.Equal(4, state.Version);
            return Task.CompletedTask;
        });
    }

    [Theory]
    [InlineData(DataModelNodeDetailLevel.Keys)]
    [InlineData(DataModelNodeDetailLevel.All)]
    public void Canvas_ManualRouteAnchorsAtTheParticipatingVisibleRows(DataModelNodeDetailLevel level)
    {
        DataModelState state = Model();
        state.Relationships[0].ConnectorLayout = Layout();
        foreach (DataModelNode node in state.Nodes) node.DetailLevel = level;
        var canvas = new SchemaCanvas();
        SetProperty(canvas, nameof(SchemaCanvas.State), state);

        IReadOnlyList<DataModelConnectorPoint> points = GeometryPoints(canvas, state.Relationships[0]);

        Assert.Equal(DataModelCanvasMetrics.ColumnCenterY(state, state.Nodes[0], "Id"), points[0].Y);
        Assert.Equal(state.Nodes[0].X + DataModelCanvasMetrics.NodeWidth, points[0].X);
        Assert.Equal(DataModelCanvasMetrics.ColumnCenterY(state, state.Nodes[1], "ParentId"), points[^1].Y);
        Assert.Equal(state.Nodes[1].X, points[^1].X);
    }

    [Fact]
    public void Canvas_CollapsedNodesKeepDistinctHeaderLanesForMultipleRelationships()
    {
        DataModelState state = Model();
        foreach (DataModelNode node in state.Nodes) node.DetailLevel = DataModelNodeDetailLevel.Collapsed;
        state.Relationships.Add(new DataModelRelationship
        {
            Id = "second-fk", LeftTable = "Child", LeftColumn = "OtherParentId", RightTable = "Parent", RightColumn = "Id",
        });
        state.Relationships[0].ConnectorLayout = Layout();
        var canvas = new SchemaCanvas();
        SetProperty(canvas, nameof(SchemaCanvas.State), state);

        IReadOnlyList<DataModelConnectorPoint> first = GeometryPoints(canvas, state.Relationships[0]);
        IReadOnlyList<DataModelConnectorPoint> second = GeometryPoints(canvas, state.Relationships[1]);

        Assert.NotEqual(first[0].Y, second[0].Y);
        Assert.NotEqual(first[^1].Y, second[^1].Y);
        foreach (double y in new[] { first[0].Y, second[0].Y })
            Assert.InRange(y, state.Nodes[0].Y, state.Nodes[0].Y + DataModelCanvasMetrics.NodeHeaderHeight);
    }

    [Fact]
    public async Task Canvas_ObstructedSavedBendDisplaysFallbackWithoutDiscardingSavedCoordinates()
    {
        DataModelState state = Model();
        DataModelConnectorLayout layout = Layout();
        layout.Waypoints[0].X = 90;
        layout.Waypoints[0].Y = 120;
        state.Relationships[0].ConnectorLayout = layout;

        await WithRenderedCanvasAsync(state, (renderer, canvas, html) =>
        {
            Assert.Contains("data-route-fallback=\"true\"", html());
            Assert.Contains("A saved bend is obstructed", html());
            Assert.Same(layout, state.Relationships[0].ConnectorLayout);
            Assert.Equal((90d, 120d), (layout.Waypoints[0].X, layout.Waypoints[0].Y));
            return Task.CompletedTask;
        });
    }

    [Fact]
    public void Canvas_CustomRouteExtentsIncreaseCanvasBounds()
    {
        DataModelState state = Model();
        state.Relationships[0].ConnectorLayout = Layout();
        state.Relationships[0].ConnectorLayout!.Waypoints[0].X = 2400;
        state.Relationships[0].ConnectorLayout!.Waypoints[0].Y = 1800;
        var canvas = new SchemaCanvas();
        SetProperty(canvas, nameof(SchemaCanvas.State), state);

        Assert.True(GetProperty<double>(canvas, "SvgWidth") >= 2480);
        Assert.True(GetProperty<double>(canvas, "SvgHeight") >= 1880);
    }

    [Fact]
    public async Task Canvas_RepeatedIdenticalAnnouncementsAdvanceTheLiveRegionKey()
    {
        await WithRenderedCanvasAsync(Model(), async (renderer, canvas, html) =>
        {
            long initialSequence = GetField<long>(canvas, "_connectorAnnouncementSequence");

            await canvas.OnConnectorAnnouncement("Connector bend moved.");
            Assert.Equal(initialSequence + 1, GetField<long>(canvas, "_connectorAnnouncementSequence"));
            Assert.Contains("<span>Connector bend moved.</span>", html());

            await canvas.OnConnectorAnnouncement("Connector bend moved.");
            Assert.Equal(initialSequence + 2, GetField<long>(canvas, "_connectorAnnouncementSequence"));
            Assert.Contains("<span>Connector bend moved.</span>", html());
            Assert.Contains("aria-live=\"polite\" aria-atomic=\"true\"", html());
        });
    }

    [Theory]
    [InlineData(0.5)]
    [InlineData(1.0)]
    [InlineData(2.0)]
    public async Task Canvas_InsetKeepsPinnedOutwardConnectorVisibleAtZeroTableCoordinate(double scale)
    {
        DataModelState state = Model();
        state.Scale = scale;
        state.Nodes[0].X = 0;
        state.Relationships[0].ConnectorLayout = Layout();
        state.Relationships[0].ConnectorLayout!.ParentSide = DataModelConnectorSide.Left;

        await WithRenderedCanvasAsync(state, (renderer, canvas, html) =>
        {
            string markup = html();
            Assert.Contains("data-canvas-inset=\"64\"", markup);
            string scaleText = scale.ToString("0.###", System.Globalization.CultureInfo.InvariantCulture);
            Assert.Contains($"transform:scale({scaleText}) translate(64px,64px)", markup);
            Assert.Equal((GetProperty<double>(canvas, "SvgWidth") + 128) * scale, GetProperty<double>(canvas, "ScaledSvgWidth"));
            Assert.Equal((GetProperty<double>(canvas, "SvgHeight") + 128) * scale, GetProperty<double>(canvas, "ScaledSvgHeight"));
            IReadOnlyList<DataModelConnectorPoint> points = GeometryPoints(canvas, state.Relationships[0]);
            Assert.Contains(points, point => point.X < 0);
            Assert.All(points, point => Assert.True((point.X + 64) * scale >= 0));
            Assert.Equal(0, state.Nodes[0].X);
            Assert.Equal(430, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
            return Task.CompletedTask;
        });
    }

    [Fact]
    public void Workspace_MergeRetainsCustomRouteAndExistingNodePlacement()
    {
        DataModelState state = Model();
        state.Relationships[0].ConnectorLayout = Layout();
        DataModelConnectorLayout original = state.Relationships[0].ConnectorLayout!;
        DataModelState addition = Model();
        addition.Nodes[0].X = 900;
        DataModelTab tab = CreateTab(state);

        Invoke(tab, "Merge", addition);

        Assert.Equal(50, state.Nodes[0].X);
        Assert.Same(addition.Relationships[0], state.Relationships[0]);
        Assert.NotSame(original, state.Relationships[0].ConnectorLayout);
        Assert.Equal("bend", Assert.Single(state.Relationships[0].ConnectorLayout!.Waypoints).Id);
        Assert.Empty(state.PendingOperations);
    }

    [Fact]
    public async Task Workspace_UnsavedRefreshRetainsRoutesPositionsAndPendingChanges()
    {
        DataModelState state = Model();
        state.Relationships[0].ConnectorLayout = Layout();
        state.ViewportX = 42;
        state.ViewportY = 54;
        state.Scale = 0.75;
        state.Nodes[0].X = 123;
        state.Nodes[0].DetailLevel = DataModelNodeDetailLevel.All;
        state.PendingOperations.Add(new DataModelPendingOperation { Id = "pending", TableName = "Child" });
        DataModelTab tab = CreateTab(state);
        SetProperty(tab, "DataModels", new ModelServiceFake(Model()));

        await InvokeAsync(tab, "RefreshAsync");

        DataModelState refreshed = GetField<DataModelState>(tab, "_state");
        Assert.NotSame(state, refreshed);
        Assert.Null(GetField<string?>(tab, "_error"));
        Assert.Equal(123, refreshed.Nodes[0].X);
        Assert.Equal(DataModelNodeDetailLevel.All, refreshed.Nodes[0].DetailLevel);
        Assert.Equal((42d, 54d, 0.75d), (refreshed.ViewportX, refreshed.ViewportY, refreshed.Scale));
        Assert.Equal(430, Assert.Single(refreshed.Relationships[0].ConnectorLayout!.Waypoints).X);
        Assert.Equal("pending", Assert.Single(refreshed.PendingOperations).Id);
        Assert.NotNull(GetProperty<TabDescriptor>(tab, nameof(DataModelTab.Tab)).DataModelStateJson);
    }

    [Fact]
    public async Task Workspace_AutoArrangePreservesRoutesAndPersistsFinalLayout()
    {
        DataModelState state = Model();
        state.DiagramName = "Routes";
        state.Relationships[0].ConnectorLayout = Layout();
        var diagrams = new DiagramServiceFake();
        DataModelTab tab = CreateTab(state, diagrams);

        await InvokeAsync(tab, "AutoArrangeAsync");

        DataModelState saved = Assert.Single(diagrams.Saved);
        Assert.Equal(430, Assert.Single(saved.Relationships[0].ConnectorLayout!.Waypoints).X);
        Assert.Equal((state.Nodes[0].X, state.Nodes[0].Y), (saved.Nodes[0].X, saved.Nodes[0].Y));
        Assert.True(GetField<bool>(tab, "_fitAfterRender"));
        Assert.Empty(saved.PendingOperations);
    }

    [Fact]
    public async Task Workspace_TidyRequiresConfirmationBeforeClearingAndSavingCustomRoutes()
    {
        DataModelState state = Model();
        state.DiagramName = "Routes";
        state.Relationships[0].ConnectorLayout = Layout();
        state.Nodes[0].DetailLevel = DataModelNodeDetailLevel.All;
        var diagrams = new DiagramServiceFake();
        DataModelTab tab = CreateTab(state, diagrams);

        await InvokeAsync(tab, "TidyModelAsync");

        Assert.True(GetField<bool>(tab, "_confirmTidyRoutes"));
        Assert.NotNull(state.Relationships[0].ConnectorLayout);
        Assert.Equal(50, state.Nodes[0].X);
        Assert.Empty(diagrams.Saved);

        await InvokeAsync(tab, "ApplyTidyModelAsync");

        Assert.False(GetField<bool>(tab, "_confirmTidyRoutes"));
        Assert.Null(state.Relationships[0].ConnectorLayout);
        Assert.All(state.Nodes, node => Assert.Equal(DataModelNodeDetailLevel.Keys, node.DetailLevel));
        Assert.Null(Assert.Single(diagrams.Saved).Relationships[0].ConnectorLayout);
        Assert.Empty(state.PendingOperations);
    }

    [Fact]
    public async Task Workspace_FinalConnectorChangeSavesVersionThreeAndResetClearsOnlyItsRoute()
    {
        DataModelState state = Model();
        state.DiagramName = "Routes";
        var diagrams = new DiagramServiceFake();
        DataModelTab tab = CreateTab(state, diagrams);
        SetField(tab, "_selectedRelationshipId", state.Relationships[0].Id);

        await InvokeAsync(tab, "OnConnectorRouteChangedAsync", new DataModelConnectorLayoutChange(state.Relationships[0].Id, Layout()));

        Assert.NotNull(Assert.Single(diagrams.Saved).Relationships[0].ConnectorLayout);
        string json = GetProperty<TabDescriptor>(tab, nameof(DataModelTab.Tab)).DataModelStateJson!;
        DataModelState restored = Assert.IsType<DataModelState>(DataModelGraphBuilder.DeserializeState(json));
        Assert.Equal(4, restored.Version);
        Assert.Equal(430, restored.Relationships[0].ConnectorLayout!.Waypoints[0].X);

        await InvokeAsync(tab, "ResetConnectorRouteAsync");

        Assert.Equal(2, diagrams.Saved.Count);
        Assert.Null(diagrams.Saved[1].Relationships[0].ConnectorLayout);
        Assert.Single(state.Relationships);
        Assert.Empty(state.PendingOperations);
    }

    [Theory]
    [InlineData("load")]
    [InlineData("saved-refresh")]
    [InlineData("unsaved-refresh")]
    [InlineData("clear")]
    [InlineData("route-reset")]
    public async Task Workspace_ReplacingDiagramClearsStaleTidyConfirmationAndWaypointSelection(string replacement)
    {
        DataModelState previous = Model();
        previous.DiagramName = replacement == "unsaved-refresh" ? null : "Previous";
        previous.Relationships[0].ConnectorLayout = Layout();
        DataModelState loaded = Model();
        loaded.DiagramName = "Loaded";
        loaded.Relationships[0].ConnectorLayout = Layout();
        loaded.Relationships[0].ConnectorLayout!.Waypoints[0].Id = "new-diagram-bend";
        var diagrams = new DiagramServiceFake { Loaded = loaded };
        DataModelTab tab = CreateTab(previous, diagrams);
        SetProperty(tab, "DataModels", new ModelServiceFake(Model()));
        SetField(tab, "_confirmTidyRoutes", true);
        SetField(tab, "_selectedWaypointId", "bend");
        SetField(tab, "_selectedRelationshipId", previous.Relationships[0].Id);

        switch (replacement)
        {
            case "load": await InvokeAsync(tab, "LoadDiagramAsync", "Loaded"); break;
            case "saved-refresh":
            case "unsaved-refresh": await InvokeAsync(tab, "RefreshAsync"); break;
            case "clear": await InvokeAsync(tab, "ClearCanvas"); break;
            case "route-reset": Invoke(tab, "ResetRouteScopedState"); break;
        }

        Assert.False(GetField<bool>(tab, "_confirmTidyRoutes"));
        Assert.Null(GetField<string?>(tab, "_selectedWaypointId"));
        Assert.Null(GetField<string?>(tab, "_error"));
        DataModelState current = GetField<DataModelState>(tab, "_state");
        Assert.NotSame(previous, current);
        if (replacement is "load" or "saved-refresh")
        {
            Assert.Same(loaded, current);
            Assert.Equal("new-diagram-bend", current.Relationships[0].ConnectorLayout!.Waypoints[0].Id);
        }
        else if (replacement is "clear" or "route-reset")
        {
            Assert.Empty(current.Nodes);
            Assert.Empty(current.Relationships);
        }
        else
        {
            Assert.Equal("bend", current.Relationships[0].ConnectorLayout!.Waypoints[0].Id);
        }
        Assert.NotNull(previous.Relationships[0].ConnectorLayout);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Workspace_SaveFailureRetainsLocalRouteAndTabJsonUntilSuccessfulRetry(bool explicitSave)
    {
        DataModelState state = Model();
        state.DiagramName = "Routes";
        var diagrams = new DiagramServiceFake { SaveFailure = new InvalidOperationException("Database is read-only.") };
        DataModelTab tab = CreateTab(state, diagrams);
        SetField(tab, "_diagramName", "Routes");
        if (explicitSave)
        {
            state.Relationships[0].ConnectorLayout = Layout();
            Invoke(tab, "SaveState");
            await InvokeAsync(tab, "SaveDiagramAsync");
        }
        else
        {
            await InvokeAsync(tab, "OnConnectorRouteChangedAsync", new DataModelConnectorLayoutChange(state.Relationships[0].Id, Layout()));
        }

        Assert.Equal(1, diagrams.SaveAttempts);
        Assert.Empty(diagrams.Saved);
        string error = Assert.IsType<string>(GetField<string?>(tab, "_diagramSaveError"));
        Assert.Contains("Diagram changes were not saved", error);
        Assert.Contains("Database is read-only", error);
        Assert.Equal(430, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
        TabDescriptor descriptor = GetProperty<TabDescriptor>(tab, nameof(DataModelTab.Tab));
        DataModelState locallySaved = Assert.IsType<DataModelState>(DataModelGraphBuilder.DeserializeState(descriptor.DataModelStateJson!));
        Assert.Equal(430, locallySaved.Relationships[0].ConnectorLayout!.Waypoints[0].X);
        Assert.Equal(4, locallySaved.Version);
        Assert.Empty(state.PendingOperations);

        // Selecting another table must not erase the durable save-failure message.
        Invoke(tab, "SelectNode", "Parent");
        Assert.Equal(error, GetField<string?>(tab, "_diagramSaveError"));
        diagrams.SaveFailure = null;

        await InvokeAsync(tab, "SaveDiagramAsync");

        Assert.Equal(2, diagrams.SaveAttempts);
        Assert.Null(GetField<string?>(tab, "_diagramSaveError"));
        Assert.Equal(430, Assert.Single(diagrams.Saved).Relationships[0].ConnectorLayout!.Waypoints[0].X);
        Assert.Equal(430, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
        Assert.NotNull(descriptor.DataModelStateJson);
    }

    [Fact]
    public async Task Workspace_GroupCommandsPreservePositionsAndSaveEachExplicitMutation()
    {
        var state = Model();
        state.DiagramName = "Groups";
        var diagrams = new DiagramServiceFake();
        var tab = CreateTab(state, diagrams);
        Invoke(tab, "SelectNode", "Parent");
        Invoke(tab, "SelectTables", new DataModelNodeSelection("Child", true));
        Assert.Equal(2, GetProperty<HashSet<string>>(tab, "SelectedTableNames").Count);
        Assert.Null(GetField<string?>(tab, "_selectedNodeName"));
        await InvokeAsync(tab, "CreateGroupAsync");
        var group = Assert.Single(state.Groups);
        Assert.All(state.Nodes, node => Assert.Equal(group.Id, node.GroupId));
        Assert.Equal(1, diagrams.SaveAttempts);
        Assert.Equal(50, state.Nodes[0].X);
        SetField(tab, "_groupName", "Order Domain");
        await InvokeAsync(tab, "RenameGroupAsync");
        await InvokeAsync(tab, "ChangeGroupColorAsync", new ChangeEventArgs { Value = "Amber" });
        Assert.Equal("Order Domain", group.Name);
        Assert.Equal(DataModelGroupColor.Amber, group.Color);
        await InvokeAsync(tab, "RemoveGroupMemberAsync", "Child");
        Assert.Single(state.Groups);
        Assert.Null(state.Nodes[1].GroupId);
        await InvokeAsync(tab, "UngroupAsync");
        Assert.Empty(state.Groups);
        Assert.All(state.Nodes, node => Assert.Null(node.GroupId));
        Assert.Empty(state.PendingOperations);
        Assert.Equal(5, diagrams.SaveAttempts);
    }

    [Fact]
    public async Task Canvas_GroupMarkupAndMoveCallbackApplyOneAtomicMutation()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        state.Relationships[0].ConnectorLayout = Layout();
        int calls = 0;
        await WithRenderedCanvasAsync(state, async (renderer, canvas, html) =>
        {
            Assert.Contains("schema-table-group group-blue", html());
            Assert.Contains("aria-pressed=\"true\"", html());
            Assert.Contains($"data-group-title=\"{group.Id}\"", html());
            SetProperty(canvas, nameof(SchemaCanvas.OnGroupMoved), EventCallback.Factory.Create<DataModelGroupMove>(new object(), _ => calls++));
            Assert.False(await canvas.OnTableGroupMoved("missing", 20, 30));
            Assert.True(await canvas.OnTableGroupMoved(group.Id, 20, 30));
            Assert.Equal(1, calls);
            Assert.Equal(70, state.Nodes[0].X);
            Assert.Equal(620, state.Nodes[1].X);
            Assert.Equal(450, state.Relationships[0].ConnectorLayout!.Waypoints[0].X);
            Assert.Empty(state.PendingOperations);
        });
    }

    [Fact]
    public async Task Workspace_RefreshArrangeAndTidyPreserveGroupMembershipAndRelativePositions()
    {
        var state = Model();
        var group = DataModelGroups.Create(state, ["Parent", "Child"])!;
        var diagrams = new DiagramServiceFake();
        var tab = CreateTab(state, diagrams);
        SetProperty(tab, "DataModels", new ModelServiceFake(Model()));
        await InvokeAsync(tab, "RefreshAsync");
        var refreshed = GetField<DataModelState>(tab, "_state");
        Assert.Equal(group.Id, Assert.Single(refreshed.Groups).Id);
        await InvokeAsync(tab, "AutoArrangeAsync");
        Assert.Equal((550d, 80d), (refreshed.Nodes[1].X - refreshed.Nodes[0].X, refreshed.Nodes[1].Y - refreshed.Nodes[0].Y));
        await InvokeAsync(tab, "ApplyTidyModelAsync");
        Assert.Single(refreshed.Groups);
        Assert.Equal((550d, 80d), (refreshed.Nodes[1].X - refreshed.Nodes[0].X, refreshed.Nodes[1].Y - refreshed.Nodes[0].Y));
        Invoke(tab, "ClearSelection");
        Assert.Empty(GetProperty<HashSet<string>>(tab, "SelectedTableNames"));
        Assert.Null(GetField<string?>(tab, "_selectedGroupId"));
    }

    private static DataModelState Model() => new()
    {
        Nodes =
        [
            new DataModelNode
            {
                Name = "Parent", X = 50, Y = 100,
                Columns = [new DataModelColumn { Name = "Id", IsPrimaryKey = true }, new DataModelColumn { Name = "Name" }],
            },
            new DataModelNode
            {
                Name = "Child", X = 600, Y = 180,
                Columns = [new DataModelColumn { Name = "Id", IsPrimaryKey = true }, new DataModelColumn { Name = "Name" }, new DataModelColumn { Name = "ParentId", IsForeignKey = true }],
            },
        ],
        Relationships = [new DataModelRelationship { Id = "fk", LeftTable = "Child", LeftColumn = "ParentId", RightTable = "Parent", RightColumn = "Id", ConstraintName = "fk_child_parent" }],
    };

    private static DataModelConnectorLayout Layout() => new()
    {
        ParentSide = DataModelConnectorSide.Right,
        ChildSide = DataModelConnectorSide.Left,
        Waypoints = [new DataModelConnectorWaypoint { Id = "bend", X = 430, Y = 350 }],
    };

    private static DataModelTab CreateTab(DataModelState state, DiagramServiceFake? diagrams = null)
    {
        var component = new DataModelTab();
        SetProperty(component, nameof(DataModelTab.Tab), new TabDescriptor("model", "Model", "diagram", TabKind.DataModel) { InitialDataModelSourceName = "Child" });
        SetProperty(component, "Diagrams", diagrams ?? new DiagramServiceFake());
        SetProperty(component, "Toast", new ToastService());
        SetField(component, "_state", state);
        return component;
    }

    private static IReadOnlyList<DataModelConnectorPoint> GeometryPoints(SchemaCanvas canvas, DataModelRelationship relationship) =>
        GetProperty<IReadOnlyList<DataModelConnectorPoint>>(Invoke(canvas, "RelationshipPoints", relationship)!, "Points");

    private static async Task WithRenderedCanvasAsync(DataModelState state, Func<HtmlRenderer, SchemaCanvas, Func<string>, Task> test)
    {
        using ServiceProvider services = new ServiceCollection().AddSingleton<IJSRuntime, NoopJsRuntime>().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            SchemaCanvas? canvas = null;
            var root = await renderer.RenderComponentAsync<CanvasHost>(ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                [nameof(CanvasHost.State)] = state,
                [nameof(CanvasHost.Capture)] = (Action<SchemaCanvas>)(component => canvas = component),
            }));
            Assert.NotNull(canvas);
            await test(renderer, canvas, root.ToHtmlString);
        });
    }

    public sealed class CanvasHost : ComponentBase
    {
        [Parameter] public DataModelState State { get; set; } = new();
        [Parameter] public Action<SchemaCanvas>? Capture { get; set; }
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenComponent<SchemaCanvas>(0);
            builder.AddAttribute(1, nameof(SchemaCanvas.State), State);
            builder.AddAttribute(2, nameof(SchemaCanvas.CanvasId), "connector-test");
            builder.AddAttribute(3, nameof(SchemaCanvas.SelectedRelationshipId), State.Relationships.FirstOrDefault()?.Id);
            builder.AddAttribute(4, nameof(SchemaCanvas.SelectedGroupId), State.Groups.FirstOrDefault()?.Id);
            builder.AddComponentReferenceCapture(5, component => Capture?.Invoke((SchemaCanvas)component));
            builder.CloseComponent();
        }
    }

    private sealed class NoopJsRuntime : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => ValueTask.FromResult(default(TValue)!);
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) => InvokeAsync<TValue>(identifier, args);
    }

    private sealed class DiagramServiceFake : IDataModelDiagramService
    {
        public List<DataModelState> Saved { get; } = [];
        public DataModelState? Loaded { get; set; }
        public Exception? SaveFailure { get; set; }
        public int SaveAttempts { get; private set; }
        public Task<IReadOnlyList<DataModelDiagramSummary>> GetDiagramsAsync(CancellationToken ct = default) => Task.FromResult<IReadOnlyList<DataModelDiagramSummary>>([]);
        public Task<DataModelState?> LoadDiagramAsync(string name, CancellationToken ct = default) => Task.FromResult(Loaded);
        public Task SaveDiagramAsync(string name, DataModelState state, CancellationToken ct = default)
        {
            SaveAttempts++;
            if (SaveFailure is not null)
                return Task.FromException(SaveFailure);
            Saved.Add(DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state))!);
            return Task.CompletedTask;
        }
        public Task RenameDiagramAsync(string existingName, string newName, DataModelState state, CancellationToken ct = default) => throw new NotSupportedException();
        public Task DeleteDiagramAsync(string name, CancellationToken ct = default) => throw new NotSupportedException();
        public Task<DataModelApplyResult> ApplyPendingOperationsAsync(DataModelState state, CancellationToken ct = default) => throw new NotSupportedException();
    }

    private sealed class ModelServiceFake(DataModelState refreshed) : IDataModelService
    {
        public Task<DataModelState> BuildModelAsync(string? seedSourceName = null, int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit, CancellationToken ct = default) => Task.FromResult(refreshed);
        public Task<DataModelState> BuildSelectionAsync(IReadOnlyCollection<string> sourceNames, DataModelSelectionMode selectionMode = DataModelSelectionMode.Exact, CancellationToken ct = default) => Task.FromResult(refreshed);
        public Task<IReadOnlyList<DataModelSourceOption>> GetSourceOptionsAsync(CancellationToken ct = default) => Task.FromResult<IReadOnlyList<DataModelSourceOption>>([]);
        public string BuildPreviewSql(DataModelState state) => "";
        public string BuildPendingOperationsPreview(DataModelState state) => "";
        public QueryDesignerState ToQueryDesignerState(DataModelState state) => DataModelGraphBuilder.ToQueryDesignerState(state);
    }

    private static object? Invoke(object component, string name, params object?[] arguments) =>
        component.GetType().GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(component, arguments);
    private static Task InvokeAsync(object component, string name, params object?[] arguments) => (Task)Invoke(component, name, arguments)!;
    private static T GetField<T>(object component, string name) => (T)component.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(component)!;
    private static void SetField(object component, string name, object? value) => component.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(component, value);
    private static T GetProperty<T>(object component, string name) => (T)component.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!.GetValue(component)!;
    private static void SetProperty(object component, string name, object? value) => component.GetType().GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!.SetValue(component, value);
}

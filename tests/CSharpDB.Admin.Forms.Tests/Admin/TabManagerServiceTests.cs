using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.DevOps;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public class TabManagerServiceTests
{
    [Fact]
    public void Constructor_OpensWelcomeTab()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = Assert.Single(manager.Tabs);
        Assert.Equal(TabKind.Welcome, tab.Kind);
        Assert.Equal(tab, manager.ActiveTab);
        Assert.False(tab.Closable);
    }

    [Fact]
    public void OpenFormDesignerTab_NewForm_CreatesTabWithInitialTable()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenFormDesignerTab(initialTableName: "Customers", title: "Customers Form");

        Assert.Equal(TabKind.FormDesigner, tab.Kind);
        Assert.Equal("Customers", tab.InitialTableName);
        Assert.Equal("Customers Form", tab.Title);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenFormDesignerTab_ExistingForm_DeduplicatesByFormId()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenFormDesignerTab(formId: "form-1", title: "Customer Form");
        TabDescriptor second = manager.OpenFormDesignerTab(formId: "form-1", title: "Other Title");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal("form-1", second.FormId);
    }

    [Fact]
    public void OpenFormEntryTab_DeduplicatesByFormId()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenFormEntryTab("form-1", "Customer Form");
        TabDescriptor second = manager.OpenFormEntryTab("form-1", "Different Title");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(TabKind.FormEntry, second.Kind);
    }

    [Fact]
    public void OpenCollectionTab_CreatesCollectionDataTab()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenCollectionTab("profiles");

        Assert.Equal("collection:profiles", tab.Id);
        Assert.Equal("profiles", tab.Title);
        Assert.Equal("profiles", tab.ObjectName);
        Assert.Equal(TabKind.CollectionData, tab.Kind);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenCollectionTab_DeduplicatesByCollectionName()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenCollectionTab("profiles");
        TabDescriptor second = manager.OpenCollectionTab("profiles");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenImportExportTab_CreatesSeededTabFromTable()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenImportExportTab("customers");

        Assert.Equal("import-export:customers", tab.Id);
        Assert.Equal(TabKind.ImportExport, tab.Kind);
        Assert.Equal("customers", tab.InitialTableName);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenImportExportTab_DeduplicatesBySeededTable()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenImportExportTab("customers");
        TabDescriptor second = manager.OpenImportExportTab("customers");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataModelTab_CreatesGlobalTab()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenDataModelTab();

        Assert.Equal("data-model", tab.Id);
        Assert.Equal(TabKind.DataModel, tab.Kind);
        Assert.Null(tab.InitialDataModelSourceName);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataModelTab_CreatesSeededTab()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenDataModelTab("customers");

        Assert.Equal("data-model:customers", tab.Id);
        Assert.Equal(TabKind.DataModel, tab.Kind);
        Assert.Equal("customers", tab.InitialDataModelSourceName);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataModelTab_DeduplicatesBySeed()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenDataModelTab("customers");
        TabDescriptor second = manager.OpenDataModelTab("customers");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataHygieneTab_CreatesGlobalTabWithSeed()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenDataHygieneTab(new DataHygieneSeed(DataHygieneMode.Validation, TableName: "customers"));

        Assert.Equal("data-hygiene", tab.Id);
        Assert.Equal(TabKind.DataHygiene, tab.Kind);
        Assert.Equal(DataHygieneMode.Validation, tab.InitialDataHygieneSeed!.Mode);
        Assert.Equal("customers", tab.InitialDataHygieneSeed.TableName);
        Assert.Equal(1, tab.DataHygieneSeedVersion);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataHygieneTab_DeduplicatesAndUpdatesSeed()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenDataHygieneTab(new DataHygieneSeed(DataHygieneMode.Duplicates, TableName: "customers"));
        TabDescriptor second = manager.OpenDataHygieneTab(new DataHygieneSeed(
            DataHygieneMode.Orphans,
            TableName: "orders",
            ChildTableName: "orders",
            ChildColumnName: "customer_id",
            ParentTableName: "customers",
            ParentColumnName: "id"));

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(DataHygieneMode.Orphans, second.InitialDataHygieneSeed!.Mode);
        Assert.Equal("orders", second.InitialDataHygieneSeed.TableName);
        Assert.Equal("customer_id", second.InitialDataHygieneSeed.ChildColumnName);
        Assert.Equal(2, second.DataHygieneSeedVersion);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenDataHygieneTab_InheritsActiveRouteContext()
    {
        var manager = new TabManagerService();
        TabDescriptor parent = manager.OpenTableTab("orders");
        SetRoute(parent, "tenant", "customer-42", "shard-a");

        TabDescriptor tab = manager.OpenDataHygieneTab(new DataHygieneSeed(DataHygieneMode.Orphans, TableName: "orders"));

        AssertRoute(tab, "tenant", "customer-42", "shard-a");
    }

    [Fact]
    public void OpenCompareDeployTab_DeduplicatesAndUpdatesSeed()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenCompareDeployTab(new CompareDeploySeed(CompareDeployMode.Schema, TableName: "customers"));
        TabDescriptor second = manager.OpenCompareDeployTab(new CompareDeploySeed(CompareDeployMode.Data, TableName: "orders", SourcePath: "baseline.db"));

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal("compare-deploy", second.Id);
        Assert.Equal(TabKind.CompareDeploy, second.Kind);
        Assert.Equal(CompareDeployMode.Data, second.InitialCompareDeploySeed!.Mode);
        Assert.Equal("orders", second.InitialCompareDeploySeed.TableName);
        Assert.Equal("baseline.db", second.InitialCompareDeploySeed.SourcePath);
        Assert.Equal(2, second.CompareDeploySeedVersion);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenTableScriptTab_SeedsCompareDeployForSelectedTable()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenTableScriptTab("customers");

        Assert.Equal("compare-deploy", tab.Id);
        Assert.Equal(TabKind.CompareDeploy, tab.Kind);
        Assert.Equal(CompareDeployMode.Schema, tab.InitialCompareDeploySeed!.Mode);
        Assert.Equal("customers", tab.InitialCompareDeploySeed.TableName);
        Assert.Equal(CompareDeployEndpointKind.CurrentDatabase, tab.InitialCompareDeploySeed.SourceKind);
        Assert.Equal(CompareDeployEndpointKind.CurrentDatabase, tab.InitialCompareDeploySeed.TargetKind);
        Assert.Equal(CompareDeployScriptEndpoint.Target, tab.InitialCompareDeploySeed.ScriptEndpoint);
        Assert.Equal(SchemaObjectKind.Table, tab.InitialCompareDeploySeed.ScriptObjectKind);
        Assert.Equal("customers", tab.InitialCompareDeploySeed.ScriptObjectName);
        Assert.True(tab.InitialCompareDeploySeed.ScriptIncludeIndexes.GetValueOrDefault());
        Assert.True(tab.InitialCompareDeploySeed.ScriptIncludeTriggers.GetValueOrDefault());
        Assert.True(tab.InitialCompareDeploySeed.ScriptOnOpen);
        Assert.Equal(1, tab.CompareDeploySeedVersion);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenQueryDesignerTab_SeedsDesignerMode()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenQueryDesignerTab(new QueryDesignerState
        {
            Tables =
            [
                new DesignerTableNode { TableName = "customers" },
            ],
        });

        Assert.Equal(TabKind.Query, tab.Kind);
        Assert.Equal("Designer", tab.State["QueryMode"]);
        Assert.Contains("customers", tab.DesignerStateJson);
    }

    [Fact]
    public void OpenCallbacksTab_CreatesHostCallbacksTab()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenCallbacksTab("normalize_name", "ScalarFunction", 1);

        Assert.Equal("callbacks:host", tab.Id);
        Assert.Equal("Callbacks", tab.Title);
        Assert.Equal(TabKind.HostCallbacks, tab.Kind);
        Assert.Equal("normalize_name", tab.State["SelectedCallbackName"]);
        Assert.Equal("ScalarFunction", tab.State["SelectedCallbackKind"]);
        Assert.Equal(1, tab.State["SelectedCallbackArity"]);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenCallbacksTab_DeduplicatesAndUpdatesSelection()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenCallbacksTab("normalize_name", "ScalarFunction", 1);
        TabDescriptor second = manager.OpenCallbacksTab("refresh_cache", "Command");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal("refresh_cache", second.State["SelectedCallbackName"]);
        Assert.Equal("Command", second.State["SelectedCallbackKind"]);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void OpenShardingTab_Deduplicates()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenShardingTab();
        TabDescriptor second = manager.OpenShardingTab();

        Assert.Same(first, second);
        Assert.Equal("sharding:admin", second.Id);
        Assert.Equal("Sharding", second.Title);
        Assert.Equal(TabKind.Sharding, second.Kind);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(second, manager.ActiveTab);
    }

    [Fact]
    public void CloseTabsForObject_ClosesCollectionTab()
    {
        var manager = new TabManagerService();
        manager.OpenCollectionTab("profiles");
        manager.OpenTableTab("customers");

        manager.CloseTabsForObject("profiles");

        Assert.Equal(["welcome", "table:customers"], manager.Tabs.Select(tab => tab.Id).ToArray());
        Assert.Equal("table:customers", manager.ActiveTab!.Id);
    }

    [Fact]
    public void OpenFormEntryTab_UpdatesInitialStateWhenExistingTabIsReopened()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenFormEntryTab("form-1", "Customer Form", initialRecordId: 10L);
        TabDescriptor second = manager.OpenFormEntryTab(
            "form-1",
            "Customer Form",
            initialRecordId: 42L,
            initialMode: "view",
            initialFilterExpression: "[Status] = @status",
            initialFilterParameters: new Dictionary<string, object?> { ["status"] = "Open" });

        Assert.Same(first, second);
        Assert.Equal(42L, second.InitialRecordId);
        Assert.Equal("view", second.InitialFormEntryMode);
        Assert.Equal("[Status] = @status", second.InitialFilterExpression);
        Assert.Equal("Open", second.InitialFilterParameters!["status"]);
    }

    [Fact]
    public void OpenFormTabs_InheritActiveRouteContext()
    {
        var manager = new TabManagerService();
        TabDescriptor parent = manager.OpenTableTab("customers");
        SetRoute(parent, "tenant", "customer-42", "shard-a");

        TabDescriptor designer = manager.OpenFormDesignerTab(initialTableName: "customers");
        TabDescriptor entry = manager.OpenFormEntryTab("form-1", "Customer Form");

        AssertRoute(designer, "tenant", "customer-42", "shard-a");
        AssertRoute(entry, "tenant", "customer-42", "shard-a");
    }

    [Fact]
    public void OpenFormEntryTab_ReopenedExistingTabUpdatesRouteContext()
    {
        var manager = new TabManagerService();
        TabDescriptor firstParent = manager.OpenTableTab("customers");
        SetRoute(firstParent, "tenant", "customer-1", "shard-a");
        TabDescriptor entry = manager.OpenFormEntryTab("form-1", "Customer Form");

        TabDescriptor secondParent = manager.OpenCollectionTab("profiles");
        SetRoute(secondParent, "tenant", "customer-2", "shard-b");
        TabDescriptor reopened = manager.OpenFormEntryTab("form-1", "Customer Form");

        Assert.Same(entry, reopened);
        AssertRoute(reopened, "tenant", "customer-2", "shard-b");
    }

    [Fact]
    public void CloseTabsForForm_ClosesDesignerAndEntryTabs()
    {
        var manager = new TabManagerService();
        manager.OpenFormDesignerTab(formId: "form-1", title: "Customer Form");
        manager.OpenFormEntryTab("form-1", "Customer Form");
        manager.OpenFormDesignerTab(formId: "form-2", title: "Order Form");

        manager.CloseTabsForForm("form-1");

        Assert.Equal(["welcome", "form-designer:form-2"], manager.Tabs.Select(tab => tab.Id).ToArray());
        Assert.Equal("form-designer:form-2", manager.ActiveTab!.Id);
    }

    private static void SetRoute(TabDescriptor tab, string keyspace, string key, string shardId)
    {
        tab.RouteKeyspace = keyspace;
        tab.RouteKey = key;
        tab.RouteShardId = shardId;
        tab.RouteBucket = 7;
        tab.RouteMapVersion = 3;
        tab.RouteToken = 123UL;
    }

    private static void AssertRoute(TabDescriptor tab, string keyspace, string key, string shardId)
    {
        Assert.Equal(keyspace, tab.RouteKeyspace);
        Assert.Equal(key, tab.RouteKey);
        Assert.Equal(shardId, tab.RouteShardId);
        Assert.Equal(7, tab.RouteBucket);
        Assert.Equal(3, tab.RouteMapVersion);
        Assert.Equal(123UL, tab.RouteToken);
    }
}

using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

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
}

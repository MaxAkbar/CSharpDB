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

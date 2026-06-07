using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;

namespace CSharpDB.Admin.Reports.Tests.Admin;

public class TabManagerServiceTests
{
    [Fact]
    public void OpenReportDesignerTab_NewReport_CreatesTabWithInitialSource()
    {
        var manager = new TabManagerService();

        TabDescriptor tab = manager.OpenReportDesignerTab(sourceKind: "Table", sourceName: "Customers", title: "Customers Report");

        Assert.Equal(TabKind.ReportDesigner, tab.Kind);
        Assert.Equal("Table", tab.InitialReportSourceKind);
        Assert.Equal("Customers", tab.InitialReportSourceName);
        Assert.Equal("Customers Report", tab.Title);
        Assert.Equal(tab, manager.ActiveTab);
    }

    [Fact]
    public void OpenReportDesignerTab_ExistingReport_DeduplicatesByReportId()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenReportDesignerTab(reportId: "report-1", title: "Sales Report");
        TabDescriptor second = manager.OpenReportDesignerTab(reportId: "report-1", title: "Other Title");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal("report-1", second.ReportId);
    }

    [Fact]
    public void OpenReportPreviewTab_DeduplicatesByReportId()
    {
        var manager = new TabManagerService();

        TabDescriptor first = manager.OpenReportPreviewTab("report-1", "Sales Report");
        TabDescriptor second = manager.OpenReportPreviewTab("report-1", "Different Title");

        Assert.Same(first, second);
        Assert.Equal(2, manager.Tabs.Count);
        Assert.Equal(TabKind.ReportPreview, second.Kind);
    }

    [Fact]
    public void OpenReportTabs_InheritActiveRouteContext()
    {
        var manager = new TabManagerService();
        TabDescriptor parent = manager.OpenTableTab("customers");
        SetRoute(parent, "tenant", "customer-42", "shard-a");

        TabDescriptor designer = manager.OpenReportDesignerTab(sourceKind: "Table", sourceName: "customers");
        TabDescriptor preview = manager.OpenReportPreviewTab("report-1", "Customer Report");

        AssertRoute(designer, "tenant", "customer-42", "shard-a");
        AssertRoute(preview, "tenant", "customer-42", "shard-a");
    }

    [Fact]
    public void OpenReportPreviewTab_ReopenedExistingTabUpdatesRouteContext()
    {
        var manager = new TabManagerService();
        TabDescriptor firstParent = manager.OpenTableTab("customers");
        SetRoute(firstParent, "tenant", "customer-1", "shard-a");
        TabDescriptor preview = manager.OpenReportPreviewTab("report-1", "Customer Report");

        TabDescriptor secondParent = manager.OpenCollectionTab("profiles");
        SetRoute(secondParent, "tenant", "customer-2", "shard-b");
        TabDescriptor reopened = manager.OpenReportPreviewTab("report-1", "Customer Report");

        Assert.Same(preview, reopened);
        AssertRoute(reopened, "tenant", "customer-2", "shard-b");
    }

    [Fact]
    public void CloseTabsForReport_ClosesDesignerAndPreviewTabs()
    {
        var manager = new TabManagerService();
        manager.OpenReportDesignerTab(reportId: "report-1", title: "Sales Report");
        manager.OpenReportPreviewTab("report-1", "Sales Report");
        manager.OpenReportDesignerTab(reportId: "report-2", title: "Inventory Report");

        manager.CloseTabsForReport("report-1");

        Assert.Equal(["welcome", "report-designer:report-2"], manager.Tabs.Select(tab => tab.Id).ToArray());
        Assert.Equal("report-designer:report-2", manager.ActiveTab!.Id);
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

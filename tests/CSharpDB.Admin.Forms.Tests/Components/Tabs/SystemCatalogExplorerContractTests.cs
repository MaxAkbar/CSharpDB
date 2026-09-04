using CSharpDB.Admin.Components.Layout;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class SystemCatalogExplorerContractTests
{
    [Fact]
    public void SystemCatalogExplorer_UsesTheCompleteSharedRegistry()
    {
        string[] expectedNames =
        [
            "sys.tables",
            "sys.columns",
            "sys.indexes",
            "sys.foreign_keys",
            "sys.key_constraints",
            "sys.check_constraints",
            "sys.functions",
            "sys.views",
            "sys.triggers",
            "sys.objects",
            "sys.saved_queries",
            "sys.external_tables",
            "sys.diagrams",
            "sys.validation_rules",
            "sys.temp_tables",
            "sys.temp_columns",
            "sys.table_stats",
            "sys.column_stats",
            "sys.planner_histograms",
            "sys.planner_heavy_hitters",
            "sys.planner_index_prefix_stats",
            "sys.internal_tables",
        ];

        Assert.Equal(expectedNames, DbSystemCatalogRegistry.Catalogs.Select(static catalog => catalog.Name));
        Assert.All(DbSystemCatalogRegistry.Catalogs, static catalog =>
        {
            Assert.False(string.IsNullOrWhiteSpace(catalog.Description));
            Assert.StartsWith("SELECT * FROM ", catalog.DefaultSql, StringComparison.Ordinal);
            Assert.Contains(catalog.Name, catalog.DefaultSql, StringComparison.OrdinalIgnoreCase);
        });

        string navigation = ReadAdminSource("Components", "Layout", "NavMenu.razor");
        string queryTab = ReadAdminSource("Components", "Tabs", "QueryTab.razor");
        Assert.Contains("DbSystemCatalogRegistry.Catalogs", navigation, StringComparison.Ordinal);
        Assert.Contains("DbSystemCatalogRegistry.Catalogs", queryTab, StringComparison.Ordinal);
        Assert.Contains("DbSystemCatalogRegistry.UnderscoredAliases.Keys", queryTab, StringComparison.Ordinal);
        Assert.DoesNotContain("s_systemCatalogItems", navigation, StringComparison.Ordinal);
        Assert.DoesNotContain("s_systemCatalogSourceNames", queryTab, StringComparison.Ordinal);
    }

    [Fact]
    public void InternalStorageExplorer_RendersLogicalReplacementAndRecoverableFailureState()
    {
        string navigation = ReadAdminSource("Components", "Layout", "NavMenu.razor");

        Assert.Contains("<span>Internal Storage</span>", navigation, StringComparison.Ordinal);
        Assert.Contains("FROM sys.internal_tables", navigation, StringComparison.Ordinal);
        Assert.Contains("@FormatInternalStorageHint(currentItem)", navigation, StringComparison.Ordinal);
        Assert.Contains("$\"→ {replacement}\"", navigation, StringComparison.Ordinal);
        Assert.Contains("_internalStorageLoadError = result.Error;", navigation, StringComparison.Ordinal);
        Assert.Contains("Internal storage is unavailable.", navigation, StringComparison.Ordinal);
        Assert.Contains("role=\"alert\"", navigation, StringComparison.Ordinal);
        Assert.Contains("RetryInternalStorageAsync", navigation, StringComparison.Ordinal);
        Assert.Contains("@onclick:stopPropagation=\"true\"", navigation, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("__data_model_diagrams", "SELECT * FROM sys.internal_tables WHERE table_name = '__data_model_diagrams';")]
    [InlineData("backing'table", "SELECT * FROM sys.internal_tables WHERE table_name = 'backing''table';")]
    public void BuildInternalStorageQuery_ExposesEveryCatalogFieldAndQuotesTheFilter(
        string tableName,
        string expectedSql)
    {
        Assert.Equal(expectedSql, NavMenu.BuildInternalStorageQuery(tableName));
    }

    private static string ReadAdminSource(params string[] segments)
    {
        string[] path = [FindRepositoryRoot(), "src", "CSharpDB.Admin", .. segments];
        return File.ReadAllText(Path.Combine(path));
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }
}

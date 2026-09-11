using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.DevOps;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class DefinitionExplorerIntegrationTests
{
    [Fact]
    public void Navigation_PreservesColumnAndSeparatesRoutes()
    {
        var manager = new TabManagerService(); var first = manager.OpenTableTab("Orders");
        first.RouteKeyspace = "tenants"; first.RouteKey = "a";
        var search = manager.OpenDefinitionExplorerTab("Orders", "CustomerId", ColumnChangeKind.Drop);
        Assert.Equal("a", search.RouteKey); Assert.Equal(TabKind.DefinitionExplorer, search.Kind);
        Assert.Equal(new DefinitionExplorerSeed("Orders", "CustomerId", ColumnChangeKind.Drop), search.State["DefinitionSeed"]);
        var query = manager.OpenQueryTab(); query.RouteKey = "b";
        var other = manager.OpenDefinitionExplorerTab(); Assert.NotEqual(search.Id, other.Id); Assert.Equal("b", other.RouteKey);
        manager.ActivateTab(first.Id); Assert.Same(search, manager.OpenDefinitionExplorerTab());
        search.RouteKey = "c";
        manager.ActivateTab(first.Id);
        var returned = manager.OpenDefinitionExplorerTab("Orders", "Amount");
        Assert.NotSame(search, returned); Assert.Equal("a", returned.RouteKey); Assert.Equal("c", search.RouteKey);
        Assert.Equal(new DefinitionExplorerSeed("Orders", "Amount", ColumnChangeKind.Rename), returned.State["DefinitionSeed"]);
    }

    [Theory]
    [InlineData("UPDATE \"__forms\" SET name = 'Changed'", true)]
    [InlineData("INSERT INTO _etl_pipeline_versions (name) VALUES ('p')", true)]
    [InlineData("DELETE FROM __code_modules WHERE module_id = 'm'", true)]
    [InlineData("UPDATE __data_model_diagrams SET diagram_json = '{}'", true)]
    [InlineData("DELETE FROM __data_model_diagrams WHERE name = 'Old model'", true)]
    [InlineData("UPDATE __external_tables SET archive_path = 'changed.cdbtable'", true)]
    [InlineData("SELECT 'UPDATE __forms'", false)]
    [InlineData("UPDATE Orders SET Amount = 2", false)]
    public void RelevantChanges_RefreshDefinitions(string sql, bool expected)
    {
        var changes = new DatabaseChangeService(); bool notified = false;
        changes.Changed += () => notified = true; changes.NotifyFromSql(sql); Assert.Equal(expected, notified);
    }

    [Fact]
    public void ModelSearchNavigation_PreservesRoutesAndDoesNotReplaceModelDrafts()
    {
        var manager = new TabManagerService();
        var source = manager.OpenTableTab("Orders"); source.RouteKeyspace = "tenants"; source.RouteKey = "one";
        var references = manager.OpenDefinitionReferencesTab("Orders", "CustomerId");
        Assert.Equal("one", references.RouteKey);
        Assert.Equal(new DefinitionObjectSeed(null, "Used by", "CustomerId", "Orders"), references.State["DefinitionSeed"]);
        var model = manager.OpenSavedDataModelTab("Order model");
        Assert.Equal("one", model.RouteKey);
        model.DataModelStateJson = "existing draft";
        manager.OpenDefinitionObjectTab("DataModel:1");
        Assert.Same(model, manager.OpenSavedDataModelTab("Order model"));
        Assert.Equal("existing draft", model.DataModelStateJson);
        var query = manager.OpenQueryTab(); query.RouteKey = "two";
        var other = manager.OpenSavedDataModelTab("Order model");
        Assert.NotSame(model, other);
        Assert.Equal("two", other.RouteKey);
        Assert.Null(other.DataModelStateJson);
        var search = manager.OpenDefinitionObjectTab("DataModel:1");
        Assert.NotSame(references, search);
        Assert.Equal("two", search.RouteKey);
    }

    [Fact]
    public async Task Catalog_HolderRouteLeaseAndTokensAreIsolated()
    {
        string directory = Path.Combine(Path.GetTempPath(), "definition-routes-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        var ct = TestContext.Current.CancellationToken;
        var options = new CSharpDbClientOptions { DataSource = Path.Combine(directory, "master.db") };
        try
        {
            await using var holder = new DatabaseClientHolder(CSharpDbClient.Create(options), null, options,
                new AdminHostDatabaseOptions { OpenMode = AdminHostOpenMode.Direct }, DbFunctionRegistry.Create(_ => { }));
            await holder.CreateShardCatalogAndReloadAsync(new CSharpDbShardingOptions
            {
                Keyspace = "tenants", MapVersion = 1, VirtualBucketCount = 2,
                Shards = [new() { ShardId = "a", DataSource = Path.Combine(directory, "a.db") }, new() { ShardId = "b", DataSource = Path.Combine(directory, "b.db") }],
                BucketRanges = [new() { StartBucketInclusive = 0, EndBucketExclusive = 1, ShardId = "a" }, new() { StartBucketInclusive = 1, EndBucketExclusive = 2, ShardId = "b" }],
                ExactKeyPins = new Dictionary<string, string> { ["one"] = "a", ["two"] = "b" },
            }, ct);
            await using var a = holder.CreateRouteBoundClient(new CSharpDbRouteContext { Keyspace = "tenants", Key = "one" });
            await using var b = holder.CreateRouteBoundClient(new CSharpDbRouteContext { Keyspace = "tenants", Key = "two" });
            Assert.Null((await a.ExecuteSqlAsync("CREATE TABLE OnlyA (Id INTEGER, Value TEXT)", ct)).Error);
            Assert.Null((await b.ExecuteSqlAsync("CREATE TABLE OnlyB (Id INTEGER, Value TEXT)", ct)).Error);
            Assert.Null((await a.ExecuteSqlAsync("""
                CREATE TABLE __data_model_diagrams (id TEXT, name TEXT, diagram_json TEXT);
                INSERT INTO __data_model_diagrams VALUES ('1', 'Shared model name', '{"nodes":[{"name":"OnlyA","columns":[{"name":"Id"}]}]}');
                """, ct)).Error);
            Assert.Null((await b.ExecuteSqlAsync("""
                CREATE TABLE __data_model_diagrams (id TEXT, name TEXT, diagram_json TEXT);
                INSERT INTO __data_model_diagrams VALUES ('1', 'Shared model name', '{"nodes":[{"name":"OnlyB","columns":[{"name":"Id"}]}]}');
                """, ct)).Error);
            var first = await ((ICSharpDbDefinitionCatalogReader)a).ReadDefinitionCatalogAsync(pageSize: 1, ct: ct);
            await Assert.ThrowsAsync<InvalidOperationException>(() => ((ICSharpDbDefinitionCatalogReader)b).ReadDefinitionCatalogAsync(first.ContinuationToken, 1, ct));
            var tab = new TabDescriptor("test", "test", "", TabKind.DefinitionExplorer) { RouteKeyspace = "tenants", RouteKey = "one" };
            await using var lease = new AdminRouteClientLease(holder); await lease.UpdateAsync(tab);
            var firstCatalog = await DefinitionCatalogService.ReadAsync(lease.GetActiveClient(holder), ct: ct);
            Assert.Contains(firstCatalog.Definitions, d => d.Name == "OnlyA");
            tab.RouteKey = "two"; await lease.UpdateAsync(tab);
            var catalog = await DefinitionCatalogService.ReadAsync(lease.GetActiveClient(holder), ct: ct);
            Assert.Contains(catalog.Definitions, d => d.Name == "OnlyB"); Assert.DoesNotContain(catalog.Definitions, d => d.Name == "OnlyA");
            var analyzer = new DefinitionDependencyService();
            var firstAnalysis = analyzer.Analyze(firstCatalog, ct);
            var secondAnalysis = analyzer.Analyze(catalog, ct);
            string firstTable = firstCatalog.Definitions.Single(d => d.Kind == "Table").Id;
            string secondTable = catalog.Definitions.Single(d => d.Kind == "Table").Id;
            Assert.Contains(firstAnalysis.Dependencies, d => d.SourceId == "DataModel:1" && d.TargetId == firstTable);
            Assert.Contains(secondAnalysis.Dependencies, d => d.SourceId == "DataModel:1" && d.TargetId == secondTable);
            Assert.DoesNotContain(secondAnalysis.Dependencies, d => d.TargetId == firstTable);
        }
        finally { foreach (string file in Directory.EnumerateFiles(directory)) File.Delete(file); Directory.Delete(directory); }
    }
}

using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Components.Shared;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.DataPrivacy;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.JSInterop;

namespace CSharpDB.DataPrivacy.Tests;

public sealed class PrivacyComponentTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SaveWithExternalCatalogCompletesOnRendererAndLeavesDispatcherResponsive(bool hybrid)
    {
        string directory = Path.Combine(Path.GetTempPath(), "csharpdb-privacy-dispatcher", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var ct = TestContext.Current.CancellationToken;
        try
        {
            string path = Path.Combine(directory, "ui.db");
            // A persisted external catalog forces the cold metadata read seen when
            // policy save opens its private session in the desktop host.
            await using (var db = await CSharpDB.Engine.Database.OpenAsync(path, ct))
            {
                await using (var create = await db.ExecuteAsync("CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Email TEXT);", ct)) { }
                await using (var insert = await db.ExecuteAsync("INSERT INTO Customers VALUES (1, 'fixture@example.test');", ct)) { }
                await using (var rows = await db.ExecuteAsync("SELECT * FROM Customers;", ct))
                    await CSharpDB.ImportExport.TableArchives.TableArchiveWriter.WriteAsync(
                        Path.Combine(directory, "customers.csdbtable"), db.GetTableSchema("Customers")!,
                        CSharpDB.ImportExport.TableArchives.TableArchiveWriter.ToAsyncRows(await rows.ToListAsync(ct), ct), ct);
                await using (var external = await db.ExecuteAsync("CREATE EXTERNAL TABLE ArchivedCustomers FROM 'customers.csdbtable';", ct)) { }
            }
            var options = new CSharpDbClientOptions
            {
                DataSource = path,
                DirectDatabaseOptions = new CSharpDB.Engine.DatabaseOptions { Functions = AdminHostCallbacks.CreateFunctionRegistry() },
                HybridDatabaseOptions = hybrid ? new CSharpDB.Engine.HybridDatabaseOptions() : null
            };
            await using var holder = new DatabaseClientHolder(CSharpDbClient.Create(options), null, options,
                new AdminHostDatabaseOptions(), AdminHostCallbacks.CreateFunctionRegistry());
            var service = new PrivacyAdminService(holder, new DatabaseChangeService(), new PrivacyLimits());
            await using var services = new ServiceCollection().BuildServiceProvider();
            await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
            var policy = new PrivacyPolicy
            {
                Name = "Customer Email", RootTable = "Customers",
                Eligibility = new() { Kind = PrivacyConditionKind.IsNotNull, Column = "Email" },
                Targets = [new() { Table = "Customers", Columns = [new() { Column = "Email", Kind = PrivacyMaskKind.Partial, KeepPrefix = 2, KeepSuffix = 4 }] }]
            };
            var saved = await Task.Run(() => renderer.Dispatcher.InvokeAsync(() => service.SaveAsync(policy, ct)), ct)
                .WaitAsync(TimeSpan.FromSeconds(20), ct);
            Assert.Equal(1, saved.Revision);
            var loaded = await renderer.Dispatcher.InvokeAsync(() => service.LoadAsync(ct)).WaitAsync(TimeSpan.FromSeconds(20), ct);
            Assert.Equal(saved.Id, Assert.Single(loaded.Policies).Id);
            Assert.Null(await renderer.Dispatcher.InvokeAsync(() => service.ReconcileAsync(Guid.NewGuid(), ct)).WaitAsync(TimeSpan.FromSeconds(20), ct));
            await renderer.Dispatcher.InvokeAsync(() => Assert.NotNull(SynchronizationContext.Current)).WaitAsync(TimeSpan.FromSeconds(5), ct);
            var unchanged = await holder.ExecuteSqlAsync("SELECT Email FROM Customers WHERE Id = 1;", ct);
            Assert.Equal("fixture@example.test", Assert.Single(unchanged.Rows!)[0]);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task SavedRulesTableBoundsRenderingForThousandsOfPolicies()
    {
        var policies = Enumerable.Range(1, 2000).Select(i => new PrivacyPolicy
        {
            Name = $"Email cleanup {i:D4}", Revision = 1, RootTable = "Customers",
            Eligibility = new() { Kind = PrivacyConditionKind.IsNotNull, Column = "Email" },
            Targets = [new() { Table = "Customers", Columns = [new() { Column = "Email", Kind = PrivacyMaskKind.Anonymous }] }]
        }).ToArray();
        await using var services = new ServiceCollection().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<PrivacyPolicyBrowser>(ParameterView.FromDictionary(new Dictionary<string, object?>
        { [nameof(PrivacyPolicyBrowser.Policies)] = policies, [nameof(PrivacyPolicyBrowser.SelectedId)] = policies[0].Id.ToString() })));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Search rules", html); Assert.Contains("Filter by table", html);
        Assert.Contains("Email cleanup 0001", html); Assert.Contains("Email cleanup 0020", html); Assert.DoesNotContain("Email cleanup 0021", html);
        Assert.Contains("Customers.Email: Anonymous placeholder", html); Assert.Contains("Email IS NOT NULL", html);
        Assert.Contains("Open in editor", html);
        Assert.Equal(20, System.Text.RegularExpressions.Regex.Matches(html, "Open / edit").Count);
    }

    [Fact]
    public async Task WorkspaceLoadsWithStudioCallbacksAndDoesNotMutateBusinessData()
    {
        string directory = Path.Combine(Path.GetTempPath(), "csharpdb-privacy-components", Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            var options = new CSharpDbClientOptions { DataSource = Path.Combine(directory, "ui.db"), DirectDatabaseOptions = new CSharpDB.Engine.DatabaseOptions { Functions = AdminHostCallbacks.CreateFunctionRegistry() } };
            await using var client = CSharpDbClient.Create(options);
            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name TEXT, LastSeen DATE);", TestContext.Current.CancellationToken)).Error);
            await using var holder = new DatabaseClientHolder(client, null, options, new AdminHostDatabaseOptions(), AdminHostCallbacks.CreateFunctionRegistry());
            await using var services = new ServiceCollection().AddSingleton(holder).AddSingleton<DatabaseChangeService>()
                .AddSingleton<PrivacyLimits>().AddSingleton<PrivacyAdminService>().AddSingleton<ModalService>().AddSingleton<IJSRuntime, NoJs>().BuildServiceProvider();
            await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
            var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<PrivacyRetentionPanel>(ParameterView.FromDictionary(new Dictionary<string, object?> { [nameof(PrivacyRetentionPanel.SeedTable)] = "Customers" })));
            string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
            Assert.Contains("Privacy &amp; Retention", html); Assert.Contains("Customers", html); Assert.Contains("Older than N days", html);
            Assert.Contains("Save the policy before previewing", html); Assert.DoesNotContain("role=\"alert\"", html);
            Assert.Empty(await new PrivacyPolicyStore().ListAsync(client, TestContext.Current.CancellationToken));
        }
        finally { Directory.Delete(directory, true); }
    }
    private sealed class NoJs : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => ValueTask.FromResult(default(TValue)!);
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) => ValueTask.FromResult(default(TValue)!);
    }
}

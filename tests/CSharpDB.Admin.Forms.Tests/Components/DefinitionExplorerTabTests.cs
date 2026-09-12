using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class DefinitionExplorerTabTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SynchronousCatalogRead_KeepsLoadingUiAndDispatcherResponsive(bool closeWhileLoading)
    {
        using var release = new ManualResetEventSlim();
        var readStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var readCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = CreateClient(ct =>
        {
            readStarted.TrySetResult();
            try
            {
                // Reproduce an embedded reader doing synchronous work before it
                // returns its Task. The safety timeout prevents a failed test hanging.
                if (!release.Wait(TimeSpan.FromSeconds(15), ct))
                    throw new TimeoutException("The test did not release the catalog read.");
            }
            catch (OperationCanceledException)
            {
                readCancelled.TrySetResult();
                throw;
            }

            return Task.FromResult(CatalogPage());
        });
        await using var holder = CreateHolder(client);
        await using var services = CreateServices(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);

        // Start outside the test's execution context so an incorrectly blocked
        // renderer cannot also prevent the timeout and cleanup from running.
        var beginRender = Task.Run(() => renderer.Dispatcher.InvokeAsync(() =>
            renderer.BeginRenderingComponent<DefinitionExplorerTab>(Parameters())));
        try
        {
            await readStarted.Task.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
            var root = await beginRender.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
            string loadingHtml = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString).WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
            Assert.Contains("Reading and analyzing saved definitions", loadingHtml);
            Assert.Contains(">Cancel</button>", loadingHtml);
            Assert.False(root.QuiescenceTask.IsCompleted);

            // Navigation uses this same dispatcher. It must remain usable while
            // the reader is still blocked, not only after the catalog returns.
            var tabs = new TabManagerService();
            await renderer.Dispatcher.InvokeAsync(() => tabs.OpenQueryTab()).WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
            Assert.Equal(TabKind.Query, tabs.ActiveTab!.Kind);
            Assert.False(release.IsSet);

            if (closeWhileLoading)
            {
                await renderer.DisposeAsync().AsTask().WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
                await readCancelled.Task.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
                Assert.False(release.IsSet);
            }
            else
            {
                release.Set();
                await root.QuiescenceTask.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
                string loadedHtml = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString).WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
                Assert.Contains("test_definition", loadedHtml);
                Assert.Contains("1 definitions", loadedHtml);
                Assert.DoesNotContain("Reading and analyzing saved definitions", loadedHtml);
                Assert.DoesNotContain("role=\"alert\"", loadedHtml);
            }
        }
        finally
        {
            release.Set();
            var root = await beginRender.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
            await root.QuiescenceTask.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task CatalogReadFailure_RendersRecoverableErrorAndLeavesDispatcherUsable()
    {
        var client = CreateClient(_ => throw new InvalidOperationException("Catalog temporarily unavailable."));
        await using var holder = CreateHolder(client);
        await using var services = CreateServices(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);

        var root = await renderer.Dispatcher.InvokeAsync(() =>
            renderer.RenderComponentAsync<DefinitionExplorerTab>(Parameters())).WaitAsync(TestTimeout, TestContext.Current.CancellationToken);
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString).WaitAsync(TestTimeout, TestContext.Current.CancellationToken);

        Assert.Contains("role=\"alert\"", html);
        Assert.Contains("Catalog temporarily unavailable.", html);
        Assert.Contains(">Retry</button>", html);
        Assert.DoesNotContain("Reading and analyzing saved definitions", html);
        Assert.DoesNotContain(">Cancel</button>", html);
    }

    [Theory]
    [InlineData("Table:Customers", "Used by", "Model membership", "Proposed changes")]
    [InlineData("DataModel:1", "Definition", "Open data model", "Saved diagram")]
    [InlineData("ModelChange:1:rename", "Definition", "Open data model", "has not been applied")]
    public async Task ModelResults_KeepMembershipAndProposalsDistinct(string objectId, string view, string expected, string detail)
    {
        var client = CreateClient(_ => Task.FromResult(new DefinitionCatalogPage
        {
            CatalogVersion = "models", CapturedUtc = DateTimeOffset.UtcNow,
            Records =
            [
                new() { Id = "Table:Customers", Kind = "Table", Name = "Customers", Format = "table", Source = """{"tableName":"Customers","columns":[{"name":"Id","type":0}]}""" },
                new() { Id = "DataModel:1", Kind = "Data model", Name = "Customer model", Format = "dataModel", Source = """{"nodes":[{"name":"Customers","kind":0,"columns":[{"name":"Id"}]}],"relationships":[],"pendingOperations":[{"id":"rename","kind":5,"tableName":"Customers","columnName":"Id","newColumnName":"CustomerId"}]}""" },
                new() { Id = "ModelChange:1:rename", Kind = "Proposed change", Name = "Rename Id", OwnerName = "Customer model", Format = "modelChange", Source = """{"id":"rename","kind":5,"tableName":"Customers","columnName":"Id","newColumnName":"CustomerId"}""", MetadataJson = """{"modelId":"DataModel:1","modelName":"Customer model"}""" },
            ]
        }));
        await using var holder = CreateHolder(client);
        await using var services = CreateServices(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var tab = new TabDescriptor("definitions", "SQL Search & Dependencies", "bi-search", TabKind.DefinitionExplorer);
        tab.State["DefinitionSeed"] = new DefinitionObjectSeed(objectId, view, view == "Used by" ? "Id" : null);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<DefinitionExplorerTab>(ParameterView.FromDictionary(new Dictionary<string, object?> { [nameof(DefinitionExplorerTab.Tab)] = tab })));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains(expected, html);
        Assert.Contains(detail, html);
        if (view == "Used by")
        {
            Assert.Contains("class=\"relationship-heading membership\"", html);
            Assert.Contains("class=\"relationship-heading proposed\"", html);
        }
        Assert.DoesNotContain("role=\"alert\"", html);
    }

    private static ParameterView Parameters() => ParameterView.FromDictionary(new Dictionary<string, object?>
    {
        [nameof(DefinitionExplorerTab.Tab)] = new TabDescriptor(
            "definitions", "SQL Search & Dependencies", "bi-search", TabKind.DefinitionExplorer)
    });

    private static DefinitionCatalogPage CatalogPage() => new()
    {
        CatalogVersion = "test-catalog",
        CapturedUtc = DateTimeOffset.UtcNow,
        Records = [new DefinitionCatalogRecord
        {
            Id = "view:test_definition", Kind = "View", Name = "test_definition", Source = "SELECT 1 AS value"
        }]
    };

    private static DatabaseClientHolder CreateHolder(ICSharpDbClient client) => new(
        client, null, null, new AdminHostDatabaseOptions(), DbFunctionRegistry.Create(_ => { }));

    private static ServiceProvider CreateServices(DatabaseClientHolder holder) => new ServiceCollection()
        .AddSingleton(holder)
        .AddSingleton<ICSharpDbShardAdminClient>(holder)
        .AddSingleton<DatabaseChangeService>()
        .AddSingleton<TabManagerService>()
        .AddSingleton<ToastService>()
        .BuildServiceProvider();

    private static ICatalogClient CreateClient(Func<CancellationToken, Task<DefinitionCatalogPage>> read)
    {
        var client = DispatchProxy.Create<ICatalogClient, CatalogClientProxy>();
        ((CatalogClientProxy)(object)client).Read = read;
        return client;
    }

    public interface ICatalogClient : ICSharpDbClient, ICSharpDbDefinitionCatalogReader;

    public class CatalogClientProxy : DispatchProxy
    {
        public Func<CancellationToken, Task<DefinitionCatalogPage>> Read { get; set; } = default!;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
        {
            nameof(ICSharpDbDefinitionCatalogReader.ReadDefinitionCatalogAsync) => Read((CancellationToken)args![2]!),
            "get_DataSource" => "definition-explorer-test.db",
            nameof(IAsyncDisposable.DisposeAsync) => ValueTask.CompletedTask,
            _ => throw new NotSupportedException($"Unexpected client call: {targetMethod?.Name}")
        };
    }
}

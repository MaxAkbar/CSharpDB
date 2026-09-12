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
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class DatabaseDocumenterTabTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task OlderServer_RendersDictionaryWithDisabledEditingAndCoverageNotice()
    {
        var client = Client(_ => Task.FromResult(Page()));
        await using var holder = Holder(client);
        await using var services = Services(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<DatabaseDocumenterTab>(Parameters()));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Database Documenter", html);
        Assert.Contains("visible_items", html);
        Assert.Contains("Incomplete documentation", html);
        Assert.Contains("Upgrade the server", html);
        Assert.Contains("Export HTML", html);
        Assert.DoesNotContain("role=\"alert\"", html);
    }

    [Fact]
    public async Task SlowCatalog_RemainsResponsiveAndCancelsOnClose()
    {
        using var release = new ManualResetEventSlim();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = Client(ct =>
        {
            started.TrySetResult();
            try { release.Wait(TimeSpan.FromSeconds(20), ct); }
            catch (OperationCanceledException) { cancelled.TrySetResult(); throw; }
            return Task.FromResult(Page());
        });
        await using var holder = Holder(client);
        await using var services = Services(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var begin = Task.Run(() => renderer.Dispatcher.InvokeAsync(() => renderer.BeginRenderingComponent<DatabaseDocumenterTab>(Parameters())), Ct);
        try
        {
            await started.Task.WaitAsync(Timeout, Ct);
            var root = await begin.WaitAsync(Timeout, Ct);
            string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString).WaitAsync(Timeout, Ct);
            Assert.Contains("Reading saved database definitions", html);
            Assert.Contains(">Cancel</button>", html);
            Assert.False(root.QuiescenceTask.IsCompleted);
            await renderer.DisposeAsync();
            await cancelled.Task.WaitAsync(Timeout, Ct);
            await root.QuiescenceTask.WaitAsync(Timeout, Ct);
        }
        finally { release.Set(); }
    }

    [Fact]
    public async Task CaptureClient_PinsOriginalForWholeOperationUntilReleased()
    {
        var oldClient = Client(_ => Task.FromResult(Page()));
        var newClient = Client(_ => Task.FromResult(Page()));
        var oldProxy = (CatalogProxy)(object)oldClient;
        await using var holder = Holder(oldClient);
        var lease = holder.CaptureClient();
        var switchTask = holder.ReplaceClientAsync(newClient, null, null);
        Assert.False(switchTask.IsCompleted);
        Assert.False(lease.IsCurrent);
        Assert.Equal(0, oldProxy.DisposeCount);
        Assert.Same(oldClient, lease.Client);
        await using (var next = holder.CaptureClient())
        {
            Assert.Same(newClient, next.Client);
            Assert.True(next.IsCurrent);
        }
        await lease.DisposeAsync();
        await switchTask.WaitAsync(Timeout, Ct);
        Assert.Equal(1, oldProxy.DisposeCount);
        await lease.DisposeAsync();
        Assert.Equal(1, oldProxy.DisposeCount);
    }

    [Fact]
    public async Task FailedRead_ShowsRecoverableError()
    {
        await using var holder = Holder(Client(_ => throw new InvalidOperationException("Catalog unavailable")));
        await using var services = Services(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<DatabaseDocumenterTab>(Parameters()));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("role=\"alert\"", html);
        Assert.Contains("Catalog unavailable", html);
        Assert.DoesNotContain("Reading saved database definitions", html);
    }

    [Fact]
    public async Task SearchingBeforeCatalogLoads_CanBeRepeated()
    {
        var component = new DatabaseDocumenterTab();
        var search = typeof(DatabaseDocumenterTab).GetMethod("SearchAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)search.Invoke(component, null)!;
        await (Task)search.Invoke(component, null)!;
    }

    [Fact]
    public void DocumenterTabs_InheritAndSeparateRouteContexts()
    {
        var manager = new TabManagerService();
        var first = manager.OpenQueryTab();
        first.RouteKeyspace = "Customers"; first.RouteKey = "east";
        var east = manager.OpenDatabaseDocumenterTab("Orders");
        Assert.Equal("east", east.RouteKey);
        Assert.Equal("Orders", east.State["DocumenterTable"]);
        Assert.Same(east, manager.OpenDatabaseDocumenterTab());
        var request = east.State["DocumenterRequest"];
        Assert.Same(east, manager.OpenDatabaseDocumenterTab("Orders"));
        Assert.NotSame(request, east.State["DocumenterRequest"]);
        var other = manager.OpenQueryTab();
        other.RouteKeyspace = "Customers"; other.RouteKey = "west";
        var west = manager.OpenDatabaseDocumenterTab();
        Assert.NotSame(east, west);
        Assert.Equal("west", west.RouteKey);
        Assert.Equal("east", east.RouteKey);
    }

    private static ParameterView Parameters() => ParameterView.FromDictionary(new Dictionary<string, object?>
    { [nameof(DatabaseDocumenterTab.Tab)] = new TabDescriptor("docs", "Database Documenter", "bi-book", TabKind.DatabaseDocumenter) });
    private static DefinitionCatalogPage Page() => new()
    {
        CatalogVersion = "test", CapturedUtc = DateTimeOffset.UnixEpoch,
        Records = [new() { Id = "View:visible_items", Kind = "View", Name = "visible_items", Source = "SELECT 1" }],
    };
    private static DatabaseClientHolder Holder(ICSharpDbClient client) => new(client, null, null, new AdminHostDatabaseOptions(), DbFunctionRegistry.Create(_ => { }));
    private static ServiceProvider Services(DatabaseClientHolder holder) => new ServiceCollection()
        .AddSingleton(holder).AddSingleton<DatabaseChangeService>()
        .AddSingleton<IJSRuntime, NoJs>().BuildServiceProvider();
    private static ICatalogClient Client(Func<CancellationToken, Task<DefinitionCatalogPage>> read)
    {
        var client = DispatchProxy.Create<ICatalogClient, CatalogProxy>();
        ((CatalogProxy)(object)client).Read = read;
        return client;
    }
    public interface ICatalogClient : ICSharpDbClient, ICSharpDbDefinitionCatalogReader;
    public class CatalogProxy : DispatchProxy
    {
        public Func<CancellationToken, Task<DefinitionCatalogPage>> Read = null!;
        public int DisposeCount;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            switch (method?.Name)
            {
                case "ReadDefinitionCatalogAsync": return Read((CancellationToken)args![2]!);
                case "get_DataSource": return "dictionary-test.db";
                case "DisposeAsync": Interlocked.Increment(ref DisposeCount); return ValueTask.CompletedTask;
                default: throw new NotSupportedException("Unexpected client call: " + method?.Name);
            }
        }
    }
    private sealed class NoJs : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => ValueTask.FromResult(default(TValue)!);
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) => ValueTask.FromResult(default(TValue)!);
    }
}

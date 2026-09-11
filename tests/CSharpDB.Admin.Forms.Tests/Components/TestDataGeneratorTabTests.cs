using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.DataGeneration;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class TestDataGeneratorTabTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task UnsupportedConnectionShowsScopeAndDoesNotReadTheCatalog()
    {
        var client = DispatchProxy.Create<IGeneratorClient, ClientProxy>();
        await using var holder = new DatabaseClientHolder(client, null, null, new AdminHostDatabaseOptions(), DbFunctionRegistry.Empty);
        await using var services = Services(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<TestDataGeneratorTab>(Parameters()));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Connect directly to one database", html);
        Assert.DoesNotContain("Preview data", html);
        Assert.Equal(0, ((ClientProxy)(object)client).Reads);
    }

    [Fact]
    public async Task ClosingTabCancelsPendingCatalogReadAndAwaitsCleanup()
    {
        var client = DispatchProxy.Create<IGeneratorClient, ClientProxy>();
        var proxy = (ClientProxy)(object)client; proxy.Supported = true;
        await using var holder = new DatabaseClientHolder(client, null, null, new AdminHostDatabaseOptions(), DbFunctionRegistry.Empty);
        await using var services = Services(holder);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.BeginRenderingComponent<TestDataGeneratorTab>(Parameters()));
        await proxy.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Reading schema", html);
        Assert.Contains(">Cancel</button>", html);
        Assert.False(root.QuiescenceTask.IsCompleted);
        await renderer.DisposeAsync();
        await proxy.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await root.QuiescenceTask.WaitAsync(TimeSpan.FromSeconds(10), Ct);
    }

    [Fact]
    public void TableEntryReusesItsTabAndKeepsRoutesSeparate()
    {
        var manager = new TabManagerService();
        var query = manager.OpenQueryTab(); query.RouteKeyspace = "Tenants"; query.RouteKey = "east";
        var east = manager.OpenTestDataGeneratorTab("Orders");
        Assert.Equal("east", east.RouteKey);
        Assert.Equal("Orders", east.State["GeneratorTable"]);
        Assert.Same(east, manager.OpenTestDataGeneratorTab());
        object request = east.State["GeneratorRequest"]!;
        Assert.Same(east, manager.OpenTestDataGeneratorTab("Customers"));
        Assert.NotSame(request, east.State["GeneratorRequest"]);
        query = manager.OpenQueryTab(); query.RouteKeyspace = "Tenants"; query.RouteKey = "west";
        var west = manager.OpenTestDataGeneratorTab();
        Assert.NotSame(east, west); Assert.Equal("west", west.RouteKey); Assert.Equal("east", east.RouteKey);
    }

    private static ParameterView Parameters() => ParameterView.FromDictionary(new Dictionary<string, object?>
    { [nameof(TestDataGeneratorTab.Tab)] = new TabDescriptor("generator", "Test Data Generator", "bi-dice-5", TabKind.TestDataGenerator) });

    private static ServiceProvider Services(DatabaseClientHolder holder) => new ServiceCollection()
        .AddSingleton(holder).AddSingleton<DatabaseChangeService>().AddSingleton<GenerationLimits>()
        .AddSingleton<TestDataGenerationAdminService>().AddSingleton<IJSRuntime, NoJs>().BuildServiceProvider();

    public interface IGeneratorClient : ICSharpDbClient, ICSharpDbTransactionalSnapshotReader;
    public class ClientProxy : DispatchProxy
    {
        public bool Supported;
        public int Reads;
        public TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method?.Name switch
        {
            "get_SupportsTransactionalSnapshotReads" => Supported,
            "get_DataSource" => "generator-test.db",
            "GetIndexesAsync" => Task.FromResult<IReadOnlyList<CSharpDB.Client.Models.IndexSchema>>([]),
            "GetTableNamesAsync" => ReadAsync((CancellationToken)args![0]!),
            "DisposeAsync" => ValueTask.CompletedTask,
            _ => throw new NotSupportedException("Unexpected client call: " + method?.Name),
        };
        private async Task<IReadOnlyList<string>> ReadAsync(CancellationToken ct)
        {
            Reads++; Started.TrySetResult();
            try { await Task.Delay(Timeout.Infinite, ct); return []; }
            catch (OperationCanceledException) { Cancelled.TrySetResult(); throw; }
        }
    }
    private sealed class NoJs : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args) => ValueTask.FromResult(default(TValue)!);
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args) => ValueTask.FromResult(default(TValue)!);
    }
}

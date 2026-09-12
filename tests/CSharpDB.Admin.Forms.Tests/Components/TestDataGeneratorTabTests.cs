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
    public async Task DatabaseSwitchClearsUnknownOutcomeButSchemaRefreshKeepsItLocked()
    {
        var original = DispatchProxy.Create<IGeneratorClient, ClientProxy>();
        var replacement = DispatchProxy.Create<IGeneratorClient, ClientProxy>();
        foreach (var client in new[] { original, replacement })
        {
            var proxy = (ClientProxy)(object)client;
            proxy.Supported = true; proxy.BlockReads = false;
        }
        await using var holder = new DatabaseClientHolder(original, null, null, new AdminHostDatabaseOptions(), DbFunctionRegistry.Empty);
        var activator = new GeneratorActivator();
        await using var services = Services(holder, activator);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<TestDataGeneratorTab>(Parameters()));
        var component = activator.Component;
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            SetField(component, "_unknownOutcome", true);
            SetField(component, "_receipt", new GenerationReceipt("Unknown", 42, "profile", StableRandom.Version,
                DateTime.UtcNow, "original.db", new Dictionary<string, int>(), TimeSpan.Zero, "Lost acknowledgement for original database"));
            SetField(component, "_progress", new GenerationProgress("OldTable", 1, 1, "Committing"));
            SetField(component, "_profile", new GenerationProfile { Tables = [new() { TableName = "OldTable" }] });
            await ((IComponent)component).SetParametersAsync(Parameters());
            services.GetRequiredService<DatabaseChangeService>().NotifyChanged();
        });
        string before = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Matches("<button[^>]*disabled[^>]*>Preview data</button>", before);
        Assert.Contains("Lost acknowledgement for original database", before);
        Assert.True((bool)GetField(component, "_unknownOutcome")!);

        await renderer.Dispatcher.InvokeAsync(() => holder.ReplaceClientAsync(replacement, null, null));
        string after = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.False((bool)GetField(component, "_unknownOutcome")!);
        Assert.Null(GetField(component, "_receipt"));
        Assert.Null(GetField(component, "_progress"));
        Assert.DoesNotContain("Lost acknowledgement for original database", after);
        Assert.DoesNotContain("OldTable", after);
        Assert.Equal(1, ((ClientProxy)(object)replacement).Reads);
        await renderer.Dispatcher.InvokeAsync(async () =>
        {
            SetField(component, "_profile", new GenerationProfile { Tables = [new() { TableName = "NewTable" }] });
            await ((IComponent)component).SetParametersAsync(Parameters());
        });
        Assert.Matches("<button(?![^>]*disabled)[^>]*>Preview data</button>", await renderer.Dispatcher.InvokeAsync(root.ToHtmlString));
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

    private static ServiceProvider Services(DatabaseClientHolder holder, GeneratorActivator? activator = null) => new ServiceCollection()
        .AddSingleton<IComponentActivator>(activator ?? new GeneratorActivator())
        .AddSingleton(holder).AddSingleton<DatabaseChangeService>().AddSingleton<GenerationLimits>()
        .AddSingleton<TestDataGenerationAdminService>().AddSingleton<IJSRuntime, NoJs>().BuildServiceProvider();

    private static object? GetField(TestDataGeneratorTab component, string name)
        => typeof(TestDataGeneratorTab).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(component);
    private static void SetField(TestDataGeneratorTab component, string name, object? value)
        => typeof(TestDataGeneratorTab).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(component, value);
    private sealed class GeneratorActivator : IComponentActivator
    {
        public TestDataGeneratorTab Component { get; private set; } = null!;
        public IComponent CreateInstance(Type componentType)
            => componentType == typeof(TestDataGeneratorTab) ? Component = new() : (IComponent)Activator.CreateInstance(componentType)!;
    }

    public interface IGeneratorClient : ICSharpDbClient, ICSharpDbTransactionalSnapshotReader;
    public class ClientProxy : DispatchProxy
    {
        public bool Supported;
        public bool BlockReads = true;
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
            if (!BlockReads) return [];
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

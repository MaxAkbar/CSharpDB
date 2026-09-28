using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Storage.Diagnostics;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Components;

public sealed class StorageTabTests
{
    [Fact]
    public async Task OpeningAndRefreshingStorage_RequestOnlySummary_AndShowUncheckedState()
    {
        var client = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)client;
        var activator = new StorageActivator();
        await using var services = Services(client, activator);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<StorageTab>());
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Equal([DatabaseInspectionMode.Summary], proxy.Modes);
        Assert.Contains("Analyze storage", html);
        Assert.Contains("Not analyzed", html);
        Assert.Contains("Integrity checks have not run", html);
        Assert.DoesNotContain("No warnings or errors detected", html);
        Assert.Equal(0, proxy.WalChecks);
        await renderer.Dispatcher.InvokeAsync(() => InvokeAsync(activator.Component, "RefreshAllAsync"));
        Assert.Equal([DatabaseInspectionMode.Summary, DatabaseInspectionMode.Summary], proxy.Modes);
    }

    [Fact]
    public async Task Analysis_UsesCombinedRequest_AndRefreshInvalidatesResults()
    {
        var client = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)client;
        var activator = new StorageActivator();
        await using var services = Services(client, activator);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<StorageTab>());
        await renderer.Dispatcher.InvokeAsync(() => InvokeAsync(activator.Component, "AnalyzeStorageAsync"));
        Assert.Equal([DatabaseInspectionMode.Summary, DatabaseInspectionMode.FullWithIndexes], proxy.Modes);
        Assert.Equal(1, proxy.WalChecks);
        await renderer.Dispatcher.InvokeAsync(() => ((IComponent)activator.Component).SetParametersAsync(ParameterView.Empty));
        Assert.Contains("Analysis complete", await renderer.Dispatcher.InvokeAsync(root.ToHtmlString));
        await renderer.Dispatcher.InvokeAsync(() => InvokeAsync(activator.Component, "RefreshAllAsync"));
        Assert.Contains("Integrity checks have not run", await renderer.Dispatcher.InvokeAsync(root.ToHtmlString));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingAnalysis_CanBeCanceledOrClosed(bool closeTab)
    {
        var client = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)client;
        proxy.BlockAnalysis = true;
        var activator = new StorageActivator();
        await using var services = Services(client, activator);
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<StorageTab>());
        Task analysis = renderer.Dispatcher.InvokeAsync(() => InvokeAsync(activator.Component, "AnalyzeStorageAsync"));
        await proxy.Started.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await renderer.Dispatcher.InvokeAsync(() => ((IComponent)activator.Component).SetParametersAsync(ParameterView.Empty));
        string pending = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Cancel analysis", pending);
        Assert.Contains("DATABASE", pending.ToUpperInvariant());
        Assert.Contains("CSDB", pending); // Properties stay visible during the scan.
        if (closeTab)
            await renderer.DisposeAsync();
        else
            await renderer.Dispatcher.InvokeAsync(() => Invoke(activator.Component, "CancelAnalysis"));
        await analysis.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(proxy.Canceled);
        Assert.Equal(0, proxy.WalChecks);
        if (!closeTab)
        {
            await renderer.Dispatcher.InvokeAsync(() => ((IComponent)activator.Component).SetParametersAsync(ParameterView.Empty));
            Assert.Contains("Analysis canceled", await renderer.Dispatcher.InvokeAsync(root.ToHtmlString));
        }
    }

    private static ServiceProvider Services(ICSharpDbClient client, StorageActivator activator) => new ServiceCollection()
        .AddSingleton(client).AddSingleton<IComponentActivator>(activator)
        .AddSingleton<ToastService>().AddSingleton<DatabaseChangeService>()
        .AddSingleton<IConfiguration>(new ConfigurationBuilder().Build())
        .AddSingleton<IJSRuntime, NoJs>().BuildServiceProvider();

    private static object? Invoke(StorageTab component, string method)
        => typeof(StorageTab).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(component, null);
    private static Task InvokeAsync(StorageTab component, string method) => (Task)Invoke(component, method)!;

    private sealed class StorageActivator : IComponentActivator
    {
        public StorageTab Component { get; private set; } = null!;
        public IComponent CreateInstance(Type type) => Component = new StorageTab();
    }

    public class ClientProxy : DispatchProxy
    {
        public List<DatabaseInspectionMode> Modes { get; } = [];
        public int WalChecks;
        public bool BlockAnalysis;
        public bool Canceled;
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        protected override object? Invoke(MethodInfo? method, object?[]? args) => method?.Name switch
        {
            "get_DataSource" => "storage-test.db",
            "InspectStorageAsync" => InspectAsync((DatabaseInspectionMode)args![0]!, (CancellationToken)args[3]!),
            "CheckWalAsync" => CheckWal(),
            "GetTableNamesAsync" => Task.FromResult<IReadOnlyList<string>>([]),
            "GetIndexesAsync" => Task.FromResult<IReadOnlyList<CSharpDB.Client.Models.IndexSchema>>([]),
            "DisposeAsync" => ValueTask.CompletedTask,
            _ => throw new InvalidOperationException($"Unexpected client call: {method?.Name}"),
        };

        private async Task<DatabaseInspectReport> InspectAsync(DatabaseInspectionMode mode, CancellationToken ct)
        {
            Modes.Add(mode);
            if (mode == DatabaseInspectionMode.FullWithIndexes && BlockAnalysis)
            {
                Started.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
                catch (OperationCanceledException) { Canceled = true; throw; }
            }
            return new()
            {
                DatabasePath = "storage-test.db", IsSummary = mode == DatabaseInspectionMode.Summary,
                Header = new() { Magic = "CSDB", MagicValid = true, FileLengthBytes = 4096, PageSize = 4096 },
                PageTypeHistogram = [], Issues = [],
                IndexChecks = mode == DatabaseInspectionMode.FullWithIndexes
                    ? new() { DatabasePath = "storage-test.db", Indexes = [], Issues = [] } : null,
            };
        }

        private Task<WalInspectReport> CheckWal()
        {
            WalChecks++;
            return Task.FromResult(new WalInspectReport { DatabasePath = "storage-test.db", WalPath = "storage-test.db.wal", Issues = [] });
        }
    }

    private sealed class NoJs : IJSRuntime
    {
        public ValueTask<T> InvokeAsync<T>(string identifier, object?[]? args) => throw new NotSupportedException();
        public ValueTask<T> InvokeAsync<T>(string identifier, CancellationToken ct, object?[]? args) => throw new NotSupportedException();
    }
}

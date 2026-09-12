using System.Reflection;
using System.Text.Json;
using CSharpDB.Admin.Components.Shared;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class DataModelDependencyTests : IAsyncLifetime
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), "model_dependencies_" + Guid.NewGuid().ToString("N") + ".db");
    private ICSharpDbClient _inner = null!;
    private CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        _inner = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _path });
        Assert.Null((await _inner.ExecuteSqlAsync("CREATE TABLE Orders (Id INTEGER, Amount INTEGER)", Ct)).Error);
        Assert.Null((await _inner.ExecuteSqlAsync("CREATE TABLE Other (Id INTEGER)", Ct)).Error);
    }

    public async ValueTask DisposeAsync()
    {
        await _inner.DisposeAsync();
        foreach (string file in new[] { _path, _path + ".wal", _path + ".shm" }) if (File.Exists(file)) File.Delete(file);
    }

    [Fact]
    public async Task SharedAnalysis_ResolvesNativeAndIndirectUsesWithoutMatchingSqlLiterals()
    {
        var page = await CatalogAsync();
        var wrapper = Wrap(_ => Task.FromResult(page));
        var service = new DataModelService(wrapper);
        var before = await _inner.GetTableNamesAsync(Ct);
        var state = await service.BuildSelectionAsync(["Orders", "Other"], ct: Ct);
        var dependencies = state.Nodes.Single(n => n.Name == "Orders").Dependencies;
        Assert.Contains(dependencies, d => d.Kind == "View" && d.Name == "order_view" && !d.IsIndirect && d.Columns.Contains("Amount"));
        Assert.Contains(dependencies, d => d.Kind == "Saved query" && d.Name == "saved" && d.IsIndirect);
        Assert.Contains(dependencies, d => d.Kind == "Report" && d.Name == "report" && d.IsIndirect);
        Assert.Contains(dependencies, d => d.Kind == "Form" && d.Name == "entry" && !d.IsIndirect);
        Assert.Contains(dependencies, d => d.Kind == "Pipeline" && d.Name == "pipeline");
        Assert.Contains(dependencies, d => d.Kind == "Data model" && d.Relationship == "Model membership" && !d.IsIndirect);
        Assert.DoesNotContain(dependencies, d => d.Name == "literal_only");
        Assert.All(dependencies, d => Assert.NotEmpty(d.Evidence));
        Assert.Empty(state.Nodes.Single(n => n.Name == "Orders").DependencyWarnings);
        Assert.Equal(1, ((CatalogProxy)(object)wrapper).ReadCount);
        await service.GetSourceOptionsAsync(Ct);
        Assert.Equal(1, ((CatalogProxy)(object)wrapper).ReadCount);
        Assert.Equal(before, await _inner.GetTableNamesAsync(Ct));
    }

    [Fact]
    public async Task Inspection_IsBackgroundAndCancellationDoesNotBecomeCoverageWarning()
    {
        using var release = new ManualResetEventSlim();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var wrapper = Wrap(ct =>
        {
            started.TrySetResult();
            if (!release.Wait(TimeSpan.FromSeconds(10), ct)) throw new TimeoutException();
            return Task.FromResult(new DefinitionCatalogPage { CatalogVersion = "empty" });
        });
        var service = new DataModelService(wrapper);
        Task<DataModelState> loading = service.BuildSelectionAsync(["Orders"], ct: cancellation.Token);
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.False(loading.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => loading.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        }
        finally { release.Set(); }
    }

    [Fact]
    public async Task FailedInspection_PreservesModelWithRecoverableCoverageMessage()
    {
        var service = new DataModelService(Wrap(_ => throw new InvalidOperationException("Catalog temporarily unavailable")));
        var state = await service.BuildSelectionAsync(["Orders"], ct: Ct);
        var node = Assert.Single(state.Nodes);
        Assert.Equal(2, node.Columns.Count);
        Assert.Empty(node.Dependencies);
        Assert.Contains(node.DependencyWarnings, text => text.Contains("Catalog temporarily unavailable") && text.Contains("Refresh"));
    }

    [Fact]
    public async Task Refresh_RebindsChangedDefinitionsWithoutPersistingInspection()
    {
        var page = await CatalogAsync();
        var wrapper = Wrap(_ => Task.FromResult(page));
        var service = new DataModelService(wrapper);
        var state = await service.BuildSelectionAsync(["Orders"], ct: Ct);
        Assert.NotEmpty(state.Nodes[0].Dependencies);
        string json = DataModelGraphBuilder.SerializeState(state);
        Assert.DoesNotContain("order_view", json);
        Assert.DoesNotContain("dependencies", json, StringComparison.OrdinalIgnoreCase);
        var preview = DataModelSchemaProjection.ForEditing(state);
        Assert.Equal(state.Nodes[0].Dependencies.Count, preview.Find("Orders")!.Dependencies.Count);
        page = page with { Records = page.Records.Where(d => d.Kind == "Table").ToArray() };
        var refreshed = (await service.RefreshModelAsync(state, Ct))!;
        Assert.Empty(refreshed.Nodes[0].Dependencies);
        Assert.Equal(2, ((CatalogProxy)(object)wrapper).ReadCount);
    }

    [Fact]
    public void SavedDiagramTab_FollowsPickerAndSaveAsAndClearsIdentityAfterDeletion()
    {
        var tabs = new TabManagerService();
        var descriptor = tabs.OpenSavedDataModelTab("A");
        var component = new DataModelTab();
        typeof(DataModelTab).GetProperty(nameof(DataModelTab.Tab))!.SetValue(component, descriptor);
        var state = new DataModelState { DiagramName = "B", PendingOperations = [new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "Orders", ColumnName = "Draft" }] };
        typeof(DataModelTab).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(component, state);
        var save = typeof(DataModelTab).GetMethod("SaveState", BindingFlags.NonPublic | BindingFlags.Instance)!;
        save.Invoke(component, null);
        Assert.Equal("B", descriptor.State["InitialDiagramName"]);
        Assert.Same(descriptor, tabs.OpenSavedDataModelTab("B"));
        Assert.NotSame(descriptor, tabs.OpenSavedDataModelTab("A"));
        Assert.Single(DataModelGraphBuilder.DeserializeState(descriptor.DataModelStateJson!)!.PendingOperations);
        state.DiagramName = null;
        save.Invoke(component, null);
        Assert.False(descriptor.State.ContainsKey("InitialDiagramName"));
        Assert.Equal("Model: unsaved", descriptor.Title);
        state.DiagramName = "C";
        save.Invoke(component, null);
        Assert.Equal("C", descriptor.State["InitialDiagramName"]);
        Assert.Equal("Model: C", descriptor.Title);
    }

    [Fact]
    public async Task Inspector_UsesLiveIdentityAndClearlySeparatesRelationshipCategories()
    {
        var identity = Guid.NewGuid();
        var live = new DataModelNode { Name = "Orders", SchemaId = identity,
            Dependencies = [
                new("Data model", "Sales diagram", "") { ObjectId = "DataModel:1", Relationship = "Model membership" },
                new("Proposed change", "Rename amount", "") { ObjectId = "ModelChange:1:rename", Relationship = "Proposed change" },
                new("Archive relationship", "Archived orders", "") { ObjectId = "archive:1", Relationship = "External archive", NeedsReview = true }],
            DependencyWarnings = ["A module has a dynamic reference."] };
        var baseline = new DataModelState { Nodes = [live] };
        var preview = new DataModelNode { Name = "RenamedOrders", SchemaId = identity };
        await using var services = new ServiceCollection().AddSingleton<TabManagerService>().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var root = await renderer.Dispatcher.InvokeAsync(() => renderer.RenderComponentAsync<DataModelSchemaInspector>(
            ParameterView.FromDictionary(new Dictionary<string, object?>
            {
                [nameof(DataModelSchemaInspector.Baseline)] = baseline,
                [nameof(DataModelSchemaInspector.State)] = baseline,
                [nameof(DataModelSchemaInspector.Node)] = preview,
            })));
        string html = await renderer.Dispatcher.InvokeAsync(root.ToHtmlString);
        Assert.Contains("Known dependencies (3)", html);
        foreach (string text in new[] { "Model membership", "Proposed change", "External archive", "Needs review", "Incomplete coverage", "Open in SQL Search", "Explore all dependencies" })
            Assert.Contains(text, html);
    }

    private async Task<DefinitionCatalogPage> CatalogAsync()
    {
        var tables = new List<DefinitionCatalogRecord>();
        foreach (string table in new[] { "Orders", "Other" })
            tables.Add(Definition("Table", table, JsonSerializer.Serialize(await _inner.GetTableSchemaAsync(table, Ct)), "table"));
        var model = new DataModelState { Nodes = [new DataModelNode { Name = "Orders", Columns = [new DataModelColumn { Name = "Amount" }] }] };
        return new DefinitionCatalogPage
        {
            CatalogVersion = "model-test", CapturedUtc = DateTimeOffset.UtcNow,
            Records = [.. tables,
                Definition("View", "order_view", "SELECT Amount FROM Orders"),
                Definition("Saved query", "saved", "SELECT Amount FROM order_view"),
                Definition("View", "literal_only", "SELECT 'Orders.Amount' AS description FROM Other"),
                Definition("Form", "entry", """{"tableName":"Orders","fields":[{"fieldName":"Amount"}]}""", "form"),
                Definition("Report", "report", """{"source":{"kind":"SavedQuery","name":"saved"},"fields":[{"boundFieldName":"Amount"}]}""", "report"),
                Definition("Pipeline", "pipeline", """{"source":{"kind":"SqlQuery","queryText":"SELECT Amount AS Id FROM Orders"},"destination":{"tableName":"Other"}}""", "pipeline"),
                Definition("Data model", "Sales diagram", DataModelGraphBuilder.SerializeState(model), "dataModel")],
        };
    }

    private static DefinitionCatalogRecord Definition(string kind, string name, string source, string format = "sql") => new()
        { Id = kind + ":" + name, Kind = kind, Name = name, Source = source, Format = format };

    private ICatalogClient Wrap(Func<CancellationToken, Task<DefinitionCatalogPage>> read)
    {
        var client = DispatchProxy.Create<ICatalogClient, CatalogProxy>();
        var proxy = (CatalogProxy)(object)client; proxy.Inner = _inner; proxy.Read = read;
        return client;
    }

    public interface ICatalogClient : ICSharpDbClient, ICSharpDbDefinitionCatalogReader;
    public class CatalogProxy : DispatchProxy
    {
        public ICSharpDbClient Inner { get; set; } = null!;
        public Func<CancellationToken, Task<DefinitionCatalogPage>> Read { get; set; } = null!;
        public int ReadCount { get; private set; }
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(ICSharpDbDefinitionCatalogReader.ReadDefinitionCatalogAsync))
            {
                ReadCount++;
                return Read((CancellationToken)args![2]!);
            }
            try { return method.Invoke(Inner, args); }
            catch (TargetInvocationException ex) { System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException!).Throw(); throw; }
        }
    }
}

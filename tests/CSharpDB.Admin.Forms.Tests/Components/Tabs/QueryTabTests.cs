using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Helpers;
using CSharpDB.Admin.Models;
using CSharpDB.Primitives;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class QueryTabTests
{
    [Fact]
    public async Task ClosingTabCancelsPendingCompletionReadsWithoutBlockingRenderer()
    {
        var client = DispatchProxy.Create<ICSharpDbClient, PendingCompletionClient>();
        var proxy = (PendingCompletionClient)(object)client;
        using var services = new ServiceCollection().BuildServiceProvider();
        await using var renderer = new HtmlRenderer(services, NullLoggerFactory.Instance);
        var tab = new QueryTab();
        void Inject(string name, object value) => typeof(QueryTab).GetProperty(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, value);
        Inject("DbClient", client);
        Inject("Changes", new DatabaseChangeService());
        Inject("CallbackCatalog", new HostCallbackCatalogService(services));
        Task? loading = null;
        await renderer.Dispatcher.InvokeAsync((Action)(() => loading = (Task)typeof(QueryTab)
            .GetMethod("RefreshCompletionCatalogAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(tab, null)!));
        await proxy.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await renderer.Dispatcher.InvokeAsync(() => tab.DisposeAsync().AsTask()).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await loading!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(proxy.Token.IsCancellationRequested);
        Assert.False(proxy.CalledOnSynchronizationContext);
    }

    public class PendingCompletionClient : DispatchProxy
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationToken Token { get; private set; }
        public bool CalledOnSynchronizationContext { get; private set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            CalledOnSynchronizationContext |= SynchronizationContext.Current is not null;
            return targetMethod?.Name switch
            {
                nameof(ICSharpDbClient.GetTableNamesAsync) => Task.FromResult<IReadOnlyList<string>>([]),
                nameof(ICSharpDbClient.GetViewNamesAsync) => Task.FromResult<IReadOnlyList<string>>(["SlowView"]),
                nameof(ICSharpDbClient.GetProceduresAsync) => Task.FromResult<IReadOnlyList<ProcedureDefinition>>([]),
                nameof(ICSharpDbClient.ExecuteSqlAsync) => WaitForCancellationAsync((CancellationToken)args![1]!),
                _ => throw new NotSupportedException(targetMethod?.Name)
            };
        }
        private async Task<SqlExecutionResult> WaitForCancellationAsync(CancellationToken ct)
        {
            Token = ct;
            Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            throw new InvalidOperationException("The read should be cancelled when its tab closes.");
        }
    }

    [Fact]
    public void FormatQueryResultSummary_UsesExactTotalWhenAvailable()
    {
        string summary = InvokeFormatQueryResultSummary(new QueryResultsStatus
        {
            TotalRows = 1250000,
            VisibleRows = 25,
            Page = 1,
            PageSize = 25,
            HasExactTotal = true,
        });

        Assert.Equal("1250000 rows", summary);
    }

    [Fact]
    public void FormatQueryResultSummary_UsesVisibleRangeWhenTotalIsUnknown()
    {
        string summary = InvokeFormatQueryResultSummary(new QueryResultsStatus
        {
            VisibleRows = 25,
            Page = 2,
            PageSize = 25,
            HasExactTotal = false,
            HasNextPage = true,
        });

        Assert.Equal("Rows 26-50+", summary);
    }

    [Fact]
    public void BuildCompletionSources_ExcludesEveryRegisteredInternalBackingTable()
    {
        string[] internalTableNames = DbInternalTableRegistry.Descriptors
            .Select(static descriptor => descriptor.MatchKind == DbInternalTableMatchKind.Exact
                ? descriptor.Pattern
                : descriptor.Pattern + "completion_probe")
            .ToArray();

        IReadOnlyList<SqlCompletionSource> sources = QueryTab.BuildCompletionSources(
            [.. internalTableNames, "_custom_user_table", "__custom_user_table"],
            []);

        Assert.All(internalTableNames, tableName =>
            Assert.DoesNotContain(sources, source =>
                string.Equals(source.Name, tableName, StringComparison.OrdinalIgnoreCase)));
        Assert.Contains(sources, source =>
            source.Name == "_custom_user_table" && source.Kind == SqlCompletionSourceKind.Table);
        Assert.Contains(sources, source =>
            source.Name == "__custom_user_table" && source.Kind == SqlCompletionSourceKind.Table);
    }

    [Fact]
    public void BuildCompletionSources_SystemCatalogNamesAndAliasesWinPhysicalTableCollisions()
    {
        string[] catalogNamesAndAliases = DbSystemCatalogRegistry.Catalogs
            .SelectMany(static catalog => new[]
            {
                catalog.Name,
                DbSystemCatalogRegistry.GetUnderscoredAlias(catalog.Name),
            })
            .ToArray();

        IReadOnlyList<SqlCompletionSource> sources = QueryTab.BuildCompletionSources(
            catalogNamesAndAliases,
            []);

        Assert.All(catalogNamesAndAliases, name =>
        {
            SqlCompletionSource source = Assert.Single(
                sources,
                candidate => string.Equals(candidate.Name, name, StringComparison.OrdinalIgnoreCase));
            Assert.Equal(SqlCompletionSourceKind.SystemCatalog, source.Kind);
        });
        Assert.DoesNotContain(sources, source => source.Kind == SqlCompletionSourceKind.Table);
    }

    private static string InvokeFormatQueryResultSummary(QueryResultsStatus status)
    {
        MethodInfo method = typeof(QueryTab).GetMethod("FormatQueryResultSummary", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("FormatQueryResultSummary was not found.");
        return (string)method.Invoke(null, [status])!;
    }
}

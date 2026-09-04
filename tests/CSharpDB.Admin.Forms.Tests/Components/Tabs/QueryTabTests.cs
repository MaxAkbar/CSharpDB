using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Helpers;
using CSharpDB.Admin.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class QueryTabTests
{
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

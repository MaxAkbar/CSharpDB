using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Models;

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

    private static string InvokeFormatQueryResultSummary(QueryResultsStatus status)
    {
        MethodInfo method = typeof(QueryTab).GetMethod("FormatQueryResultSummary", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("FormatQueryResultSummary was not found.");
        return (string)method.Invoke(null, [status])!;
    }
}

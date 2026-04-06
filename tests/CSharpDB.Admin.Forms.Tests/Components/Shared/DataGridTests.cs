using System.Reflection;
using CSharpDB.Admin.Components.Shared;
using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class DataGridTests
{
    [Fact]
    public void UpdateFilterDraft_EmptyDraftOverridesCommittedFilterValue()
    {
        var component = new DataGrid();
        GetFilters(component)[0] = "alice";

        InvokeNonPublic(component, "UpdateFilterDraft", 0, string.Empty);

        string filterInput = (string)InvokeNonPublic(component, "GetFilterInput", 0)!;

        Assert.Equal(string.Empty, filterInput);
        Assert.True(GetFilterInputs(component).ContainsKey(0));
    }

    [Fact]
    public async Task ApplyFilterDraftAsync_EmptyDraftClearsActiveFilter()
    {
        var component = new DataGrid();
        SetProperty(component, nameof(DataGrid.Columns), new[] { "Name" });

        GetAllRows(component).Add(new DataGridRow(["Alice"], RowState.Modified));
        SetField(component, "_loadedAllRows", true);
        SetField(component, "_page", 2);
        GetFilters(component)[0] = "Alice";

        InvokeNonPublic(component, "UpdateFilterDraft", 0, string.Empty);
        await InvokeNonPublicAsyncAllowingUnrenderedState(component, "ApplyFilterDraftAsync", 0);

        Assert.False(GetFilters(component).ContainsKey(0));
        Assert.False(GetFilterInputs(component).ContainsKey(0));
        Assert.Equal(1, GetField<int>(component, "_page"));
        Assert.Single(GetRows(component));
    }

    [Fact]
    public void QueryPagingWithoutExactTotal_ShowsRangeAndKeepsNextEnabled()
    {
        var component = new DataGrid();
        SetField(component, "_page", 2);
        SetField(component, "_pageSize", 25);
        SetField(component, "_hasExactTotal", false);
        SetField(component, "_hasNextPage", true);
        SetField(component, "_rows", Enumerable.Range(0, 25)
            .Select(i => new DataGridRow([i]))
            .ToList());

        string pageInfo = (string)InvokeNonPublic(component, "GetPageInfoText")!;
        string rowCount = (string)InvokeNonPublic(component, "GetRowCountText")!;
        bool canGoNext = (bool)InvokeNonPublic(component, "CanGoToNextPage")!;
        bool canGoLast = (bool)InvokeNonPublic(component, "CanGoToLastPage")!;

        Assert.Equal("Page 2", pageInfo);
        Assert.Equal("Rows 26-50+", rowCount);
        Assert.True(canGoNext);
        Assert.False(canGoLast);
    }

    [Fact]
    public void ApplyOverfetchedPageRows_WithExtraRow_KeepsTotalUnknown()
    {
        var component = new DataGrid();
        SetField(component, "_page", 2);
        SetField(component, "_pageSize", 25);

        InvokeNonPublic(component, "ApplyOverfetchedPageRows", Enumerable.Range(0, 26)
            .Select(i => new object?[] { i })
            .ToList());

        Assert.False(GetField<bool>(component, "_hasExactTotal"));
        Assert.True(GetField<bool>(component, "_hasNextPage"));
        Assert.Equal(51, GetField<int>(component, "_totalRows"));
        Assert.Equal(25, GetAllRows(component).Count);
    }

    [Fact]
    public void ApplyOverfetchedPageRows_OnLastPage_ComputesExactTotal()
    {
        var component = new DataGrid();
        SetField(component, "_page", 2);
        SetField(component, "_pageSize", 25);

        InvokeNonPublic(component, "ApplyOverfetchedPageRows", Enumerable.Range(0, 7)
            .Select(i => new object?[] { i })
            .ToList());

        Assert.True(GetField<bool>(component, "_hasExactTotal"));
        Assert.False(GetField<bool>(component, "_hasNextPage"));
        Assert.Equal(32, GetField<int>(component, "_totalRows"));
        Assert.Equal(7, GetAllRows(component).Count);
    }

    private static Dictionary<int, string> GetFilters(DataGrid component)
        => GetField<Dictionary<int, string>>(component, "_filters");

    private static Dictionary<int, string> GetFilterInputs(DataGrid component)
        => GetField<Dictionary<int, string>>(component, "_filterInputs");

    private static List<DataGridRow> GetAllRows(DataGrid component)
        => GetField<List<DataGridRow>>(component, "_allRows");

    private static List<DataGridRow> GetRows(DataGrid component)
        => GetField<List<DataGridRow>>(component, "_rows");

    private static T GetField<T>(object instance, string fieldName)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        return (T)field.GetValue(instance)!;
    }

    private static void SetField(object instance, string fieldName, object? value)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        field.SetValue(instance, value);
    }

    private static object? InvokeNonPublic(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        return method.Invoke(instance, args);
    }

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
    }

    private static async Task InvokeNonPublicAsync(object instance, string methodName, params object?[] args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task?)method.Invoke(instance, args)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        await task;
    }

    private static async Task InvokeNonPublicAsyncAllowingUnrenderedState(object instance, string methodName, params object?[] args)
    {
        try
        {
            await InvokeNonPublicAsync(instance, methodName, args);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("render handle", StringComparison.OrdinalIgnoreCase))
        {
            // Direct component instances in these tests do not have a renderer; state mutation already happened.
        }
    }
}

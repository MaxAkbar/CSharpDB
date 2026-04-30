using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Services;

public sealed class TabManagerService
{
    private readonly List<TabDescriptor> _tabs = new();
    private int _queryCounter;
    private int _formDesignerCounter;
    private int _reportDesignerCounter;

    public IReadOnlyList<TabDescriptor> Tabs => _tabs;
    public TabDescriptor? ActiveTab { get; private set; }

    public event Action? StateChanged;

    public TabManagerService()
    {
        // Open the welcome tab by default
        var welcome = new TabDescriptor("welcome", "Welcome", "bi-house-door", TabKind.Welcome, closable: false);
        _tabs.Add(welcome);
        ActiveTab = welcome;
    }

    public void OpenTab(TabDescriptor tab)
    {
        // Deduplicate: if a tab with this ID already exists, just activate it
        var existing = _tabs.FirstOrDefault(t => t.Id == tab.Id);
        if (existing is not null)
        {
            ActivateTab(existing.Id);
            return;
        }

        _tabs.Add(tab);
        ActiveTab = tab;
        StateChanged?.Invoke();
    }

    public void ActivateTab(string tabId)
    {
        var tab = _tabs.FirstOrDefault(t => t.Id == tabId);
        if (tab is null) return;
        ActiveTab = tab;
        StateChanged?.Invoke();
    }

    public void CloseTab(string tabId)
    {
        var tab = _tabs.FirstOrDefault(t => t.Id == tabId);
        if (tab is null || !tab.Closable) return;

        int index = _tabs.IndexOf(tab);
        _tabs.Remove(tab);

        // If we closed the active tab, activate an adjacent one
        if (ActiveTab?.Id == tabId)
        {
            if (_tabs.Count == 0)
            {
                ActiveTab = null;
            }
            else
            {
                int newIndex = Math.Min(index, _tabs.Count - 1);
                ActiveTab = _tabs[newIndex];
            }
        }

        StateChanged?.Invoke();
    }

    public TabDescriptor OpenTableTab(string tableName)
    {
        var tab = new TabDescriptor($"table:{tableName}", tableName, "bi-table", TabKind.TableData)
        {
            ObjectName = tableName
        };
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenViewTab(string viewName)
    {
        var tab = new TabDescriptor($"view:{viewName}", viewName, "bi-eye", TabKind.ViewData)
        {
            ObjectName = viewName
        };
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenCollectionTab(string collectionName)
    {
        var tab = new TabDescriptor($"collection:{collectionName}", collectionName, "bi-braces", TabKind.CollectionData)
        {
            ObjectName = collectionName
        };
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenQueryTab(string? initialSql = null)
    {
        int num = Interlocked.Increment(ref _queryCounter);
        var tab = new TabDescriptor($"query:{num}", $"Query {num}", "bi-terminal", TabKind.Query)
        {
            SqlText = initialSql ?? string.Empty
        };
        OpenTab(tab);
        return tab;
    }

    public TabDescriptor OpenSystemCatalogTab(string catalogName, string sql)
    {
        var tab = new TabDescriptor($"system:{catalogName}", $"System: {catalogName}", "bi-hdd-stack", TabKind.Query)
        {
            SqlText = sql
        };

        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenProcedureTab(string procedureName)
    {
        var tab = new TabDescriptor($"procedure:{procedureName}", procedureName, "bi-gear-wide-connected", TabKind.Procedure)
        {
            ObjectName = procedureName
        };
        tab.State["IsNewProcedure"] = false;
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenNewProcedureTab()
    {
        var tab = new TabDescriptor("procedure:new", "New Procedure", "bi-gear-wide-connected", TabKind.Procedure);
        tab.State["IsNewProcedure"] = true;
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenStorageTab()
    {
        var tab = new TabDescriptor("storage:inspect", "Storage", "bi-hdd-stack", TabKind.Storage);
        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenPipelineTab()
    {
        int num = Interlocked.Increment(ref _queryCounter);
        var tab = new TabDescriptor($"pipeline:{num}", $"Pipeline {num}", "bi-diagram-3", TabKind.Pipeline);
        OpenTab(tab);
        return tab;
    }

    public TabDescriptor OpenTableDesignerTab()
    {
        int num = Interlocked.Increment(ref _queryCounter);
        var tab = new TabDescriptor($"designer:{num}", "New Table", "bi-table", TabKind.TableDesigner);
        OpenTab(tab);
        return tab;
    }

    public TabDescriptor OpenFormDesignerTab(string? formId = null, string? initialTableName = null, string? title = null)
    {
        TabDescriptor tab;
        if (!string.IsNullOrWhiteSpace(formId))
        {
            tab = new TabDescriptor($"form-designer:{formId}", title ?? "Form Designer", "bi-ui-checks-grid", TabKind.FormDesigner)
            {
                FormId = formId,
            };
        }
        else
        {
            int num = Interlocked.Increment(ref _formDesignerCounter);
            tab = new TabDescriptor($"form-designer:new:{num}", title ?? "New Form", "bi-ui-checks-grid", TabKind.FormDesigner)
            {
                InitialTableName = initialTableName,
            };
        }

        if (!string.IsNullOrWhiteSpace(initialTableName))
            tab.InitialTableName = initialTableName;

        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenFormEntryTab(
        string formId,
        string? title = null,
        object? initialRecordId = null,
        string? initialMode = null,
        string? initialFilterExpression = null,
        IReadOnlyDictionary<string, object?>? initialFilterParameters = null)
    {
        string tabId = $"form-entry:{formId}";
        TabDescriptor? existing = _tabs.FirstOrDefault(t => t.Id == tabId);
        if (existing is not null)
        {
            ApplyFormEntryInitialState(existing, initialRecordId, initialMode, initialFilterExpression, initialFilterParameters);
            ActivateTab(existing.Id);
            return existing;
        }

        var tab = new TabDescriptor(tabId, title ?? "Form Entry", "bi-file-earmark-text", TabKind.FormEntry)
        {
            FormId = formId,
        };
        ApplyFormEntryInitialState(tab, initialRecordId, initialMode, initialFilterExpression, initialFilterParameters);

        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    private static void ApplyFormEntryInitialState(
        TabDescriptor tab,
        object? initialRecordId,
        string? initialMode,
        string? initialFilterExpression,
        IReadOnlyDictionary<string, object?>? initialFilterParameters)
    {
        tab.InitialRecordId = initialRecordId;
        tab.InitialFormEntryMode = string.IsNullOrWhiteSpace(initialMode) ? null : initialMode.Trim();
        tab.InitialFilterExpression = string.IsNullOrWhiteSpace(initialFilterExpression) ? null : initialFilterExpression.Trim();
        tab.InitialFilterParameters = initialFilterParameters;
    }

    /// <summary>Open a table tab and switch it to Schema view.</summary>
    public TabDescriptor OpenTableSchemaTab(string tableName)
    {
        var tab = OpenTableTab(tableName);
        tab.State["ShowSchema"] = true;
        return tab;
    }

    /// <summary>Close any tabs referencing a specific object (e.g. after dropping a table).</summary>
    public void CloseTabsForObject(string objectName)
    {
        var toClose = _tabs.Where(t => t.ObjectName == objectName).ToList();
        foreach (var tab in toClose)
            CloseTab(tab.Id);
    }

    public void CloseTabsForForm(string formId)
    {
        var toClose = _tabs.Where(t => string.Equals(t.FormId, formId, StringComparison.Ordinal)).ToList();
        foreach (var tab in toClose)
            CloseTab(tab.Id);
    }

    public TabDescriptor OpenReportDesignerTab(string? reportId = null, string? sourceKind = null, string? sourceName = null, string? title = null)
    {
        TabDescriptor tab;
        if (!string.IsNullOrWhiteSpace(reportId))
        {
            tab = new TabDescriptor($"report-designer:{reportId}", title ?? "Report Designer", "bi-file-earmark-richtext", TabKind.ReportDesigner)
            {
                ReportId = reportId,
            };
        }
        else
        {
            int num = Interlocked.Increment(ref _reportDesignerCounter);
            tab = new TabDescriptor($"report-designer:new:{num}", title ?? "New Report", "bi-file-earmark-richtext", TabKind.ReportDesigner)
            {
                InitialReportSourceKind = sourceKind,
                InitialReportSourceName = sourceName,
            };
        }

        if (!string.IsNullOrWhiteSpace(sourceKind))
            tab.InitialReportSourceKind = sourceKind;
        if (!string.IsNullOrWhiteSpace(sourceName))
            tab.InitialReportSourceName = sourceName;

        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public TabDescriptor OpenReportPreviewTab(string reportId, string? title = null)
    {
        var tab = new TabDescriptor($"report-preview:{reportId}", title ?? "Report Preview", "bi-printer", TabKind.ReportPreview)
        {
            ReportId = reportId,
        };

        OpenTab(tab);
        return _tabs.First(t => t.Id == tab.Id);
    }

    public void CloseTabsForReport(string reportId)
    {
        var toClose = _tabs.Where(t => string.Equals(t.ReportId, reportId, StringComparison.Ordinal)).ToList();
        foreach (var tab in toClose)
            CloseTab(tab.Id);
    }
}

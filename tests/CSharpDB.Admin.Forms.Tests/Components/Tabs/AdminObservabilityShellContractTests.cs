using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Helpers;
using CSharpDB.Admin.Services;

namespace CSharpDB.Admin.Forms.Tests.Components.Tabs;

public sealed class AdminObservabilityShellContractTests
{
    [Fact]
    public void MainLayout_PassesActiveVisibilityToMountedObservabilityTab()
    {
        string source = ReadAdminSource("Components", "Layout", "MainLayout.razor");

        Assert.Contains("case TabKind.Observability:", source, StringComparison.Ordinal);
        Assert.Contains("<ObservabilityTab @key=\"tab.Id\" IsActive=\"isActive\" />", source, StringComparison.Ordinal);
        Assert.Contains("hidden=\"@(!isActive)\"", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Shell_OffersObservabilityWithoutExposingDataSourceWhileItIsActive()
    {
        string titleBar = ReadAdminSource("Components", "Layout", "TitleBar.razor");
        string statusBar = ReadAdminSource("Components", "Layout", "StatusBar.razor");
        string navigation = ReadAdminSource("Components", "Layout", "NavMenu.razor");
        string palette = ReadAdminSource("Components", "Layout", "CommandPalette.razor");
        string toasts = ReadAdminSource("Components", "Shared", "ToastContainer.razor");

        Assert.Contains("IsObservabilityActive ? \"Connected database\"", titleBar, StringComparison.Ordinal);
        Assert.Contains("defaultValue: IsObservabilityActive ? string.Empty : DbClient.DataSource", titleBar, StringComparison.Ordinal);
        Assert.Contains("Toast.Success(\"Database opened.\")", titleBar, StringComparison.Ordinal);
        Assert.Contains("Toast.Error(\"The database could not be opened.\")", titleBar, StringComparison.Ordinal);
        Assert.Contains("TabManager.ActiveTab?.Kind == TabKind.Observability", statusBar, StringComparison.Ordinal);
        Assert.Contains("Data source hidden in diagnostics view", statusBar, StringComparison.Ordinal);
        Assert.Contains("TabManager.ActiveTab?.Kind != TabKind.Observability", toasts, StringComparison.Ordinal);
        Assert.Contains("TabManager.OpenObservabilityTab()", navigation, StringComparison.Ordinal);
        Assert.Contains("TabManager.OpenObservabilityTab()", palette, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ObservabilityActivation_SynchronouslyCancelsAndClearsShellOverlays()
    {
        var modal = new ModalService();
        modal.OnChange += () =>
        {
            if (modal.IsVisible)
                modal.Cancel();
        };
        Task<string?> prompt = modal.PromptAsync(
            "Document key",
            "Enter a path-like key",
            defaultValue: @"server\private\record-key");

        Assert.False(modal.IsVisible);
        Assert.Null(modal.Current);
        Assert.Null(await prompt);

        string mainLayout = ReadAdminSource("Components", "Layout", "MainLayout.razor");
        string palette = ReadAdminSource("Components", "Layout", "CommandPalette.razor");
        string confirmModal = ReadAdminSource("Components", "Shared", "ConfirmModal.razor");
        string titleBar = ReadAdminSource("Components", "Layout", "TitleBar.razor");
        int handler = mainLayout.IndexOf("private void OnTabChanged()", StringComparison.Ordinal);
        int dismiss = mainLayout.IndexOf("DismissShellOverlays();", handler, StringComparison.Ordinal);
        int rerender = mainLayout.IndexOf("InvokeAsync(StateHasChanged)", dismiss, StringComparison.Ordinal);
        Assert.True(handler >= 0 && dismiss > handler && rerender > dismiss);
        Assert.Contains("if (Modal.IsVisible)", mainLayout, StringComparison.Ordinal);
        Assert.Contains("Modal.Cancel();", mainLayout, StringComparison.Ordinal);
        Assert.Contains("_commandPaletteVisible = false;", mainLayout, StringComparison.Ordinal);
        Assert.Contains("_commandPalette?.DismissImmediately();", mainLayout, StringComparison.Ordinal);
        Assert.Contains("if (IsObservabilityActive)", mainLayout, StringComparison.Ordinal);
        Assert.Contains("internal void DismissImmediately()", palette, StringComparison.Ordinal);
        Assert.Contains("_query = string.Empty;", palette, StringComparison.Ordinal);
        Assert.Contains("_items.Clear();", palette, StringComparison.Ordinal);
        Assert.Contains("generation != _loadGeneration", palette, StringComparison.Ordinal);
        Assert.Contains("TabManager.ActiveTab?.Kind != TabKind.Observability", palette, StringComparison.Ordinal);
        Assert.Contains("TabManager.ActiveTab?.Kind != TabKind.Observability", confirmModal, StringComparison.Ordinal);
        Assert.Contains("TabManager.ActiveTab?.Kind == TabKind.Observability", confirmModal, StringComparison.Ordinal);
        Assert.Contains("ModalService.Cancel();", confirmModal, StringComparison.Ordinal);
        Assert.Contains("if (ModalService.Current is null)", confirmModal, StringComparison.Ordinal);
        Assert.Contains("disabled=\"@IsObservabilityActive\"", titleBar, StringComparison.Ordinal);
    }

    [Fact]
    public void TabBar_UsesKeyboardNavigableAriaTabSemantics()
    {
        string source = ReadAdminSource("Components", "Layout", "TabBar.razor");

        Assert.Contains("role=\"tablist\"", source, StringComparison.Ordinal);
        Assert.Contains("role=\"tab\"", source, StringComparison.Ordinal);
        Assert.Contains("aria-selected=\"@isActive\"", source, StringComparison.Ordinal);
        Assert.Contains("\"ArrowLeft\"", source, StringComparison.Ordinal);
        Assert.Contains("\"ArrowRight\"", source, StringComparison.Ordinal);
        Assert.Contains("\"Home\"", source, StringComparison.Ordinal);
        Assert.Contains("\"End\"", source, StringComparison.Ordinal);
        Assert.Contains("aria-keyshortcuts=\"@(tab.Closable ? \"Delete\" : null)\"", source, StringComparison.Ordinal);
        Assert.Contains("CloseTabAndFocusAsync", source, StringComparison.Ordinal);
        Assert.Contains("tabindex=\"-1\"", source, StringComparison.Ordinal);
        Assert.Contains("aria-label=\"Close @tab.Title tab\"", source, StringComparison.Ordinal);
        Assert.Contains("adminTabInterop.register", source, StringComparison.Ordinal);
        Assert.Contains("adminTabInterop.unregister", source, StringComparison.Ordinal);
        Assert.Contains("if (!_interopRegistered)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("@onkeydown:preventDefault=\"true\"", source, StringComparison.Ordinal);

        string interop = ReadAdminSource("wwwroot", "js", "interop.js");
        Assert.Contains("event.key === 'Delete'", interop, StringComparison.Ordinal);
        Assert.Contains("event.key === 'Spacebar'", interop, StringComparison.Ordinal);
        Assert.Contains("removeEventListener('keydown'", interop, StringComparison.Ordinal);
        Assert.DoesNotContain("event.key === 'Tab'", interop[interop.IndexOf("window.adminTabInterop", StringComparison.Ordinal)..interop.IndexOf("// SQL editor scroll sync", StringComparison.Ordinal)], StringComparison.Ordinal);
    }

    [Fact]
    public void TabDomIds_AreSafeCollisionFreeAndSharedByBothIdRefConsumers()
    {
        string colonId = AdminTabDomIds.TabButtonId("table:a:b");
        string hyphenId = AdminTabDomIds.TabButtonId("table:a-b");
        string whitespaceId = AdminTabDomIds.PanelId("table: name with space");

        Assert.Equal("admin-tab-u007400610062006c0065003a0061003a0062", colonId);
        Assert.NotEqual(colonId, hyphenId);
        Assert.All(whitespaceId, character => Assert.False(char.IsWhiteSpace(character)));

        string mainLayout = ReadAdminSource("Components", "Layout", "MainLayout.razor");
        string tabBar = ReadAdminSource("Components", "Layout", "TabBar.razor");
        Assert.Contains("id=\"@AdminTabDomIds.PanelId(tab.Id)\"", mainLayout, StringComparison.Ordinal);
        Assert.Contains("aria-labelledby=\"@AdminTabDomIds.TabButtonId(tab.Id)\"", mainLayout, StringComparison.Ordinal);
        Assert.Contains("aria-controls=\"@AdminTabDomIds.PanelId(tab.Id)\"", tabBar, StringComparison.Ordinal);
        Assert.Contains("id=\"@AdminTabDomIds.TabButtonId(tab.Id)\"", tabBar, StringComparison.Ordinal);
        Assert.DoesNotContain("Replace(':', '-')", mainLayout, StringComparison.Ordinal);
        Assert.DoesNotContain("Replace(':', '-')", tabBar, StringComparison.Ordinal);
    }

    [Fact]
    public void OfflineHelp_ExplainsRuntimeVsPhysicalInspectionAndPrivacy()
    {
        string source = ReadAdminSource("wwwroot", "help", "operations.html");

        Assert.Contains("Polling runs only while the tab is active", source, StringComparison.Ordinal);
        Assert.Contains("Ordinary views never show SQL text or database paths", source, StringComparison.Ordinal);
        Assert.Contains("deeper physical inspection and can expose server-local paths", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ObservabilityTab_EnforcesVisibilityPrivacyAndAccessibleFallbacks()
    {
        string source = ReadAdminSource("Components", "Tabs", "ObservabilityTab.razor");

        Assert.Contains("[Parameter] public bool IsActive", source, StringComparison.Ordinal);
        Assert.Contains("await Observability.SetActiveAsync(IsActive)", source, StringComparison.Ordinal);
        Assert.Contains("Observability.ClearSensitiveDetail()", source, StringComparison.Ordinal);
        Assert.Contains("SQL text is not included", source, StringComparison.Ordinal);
        Assert.Contains("Separately authorized", source, StringComparison.Ordinal);
        Assert.Contains("detail.CaptureMode == SqlTextCaptureMode.Raw", source, StringComparison.Ordinal);
        Assert.Contains("!State.RevealedDetail.FieldsTruncated", source, StringComparison.Ordinal);
        Assert.Contains("State.HasPlanRequest &&", source, StringComparison.Ordinal);
        Assert.Contains("State.SelectedPlan.Availability != DiagnosticsAvailability.Available", source, StringComparison.Ordinal);
        Assert.Contains("State.HasDetailRequest &&", source, StringComparison.Ordinal);
        Assert.Contains("State.RevealedDetail.Availability != DiagnosticsAvailability.Available", source, StringComparison.Ordinal);
        Assert.Contains("EXPLAIN ESTIMATE FOR", source, StringComparison.Ordinal);
        Assert.Contains("does not execute the query", source, StringComparison.Ordinal);
        Assert.Contains("role=\"status\" aria-live=\"polite\"", source, StringComparison.Ordinal);
        Assert.Contains("<caption>", source, StringComparison.Ordinal);
        Assert.Contains("Chart data", source, StringComparison.Ordinal);
        Assert.Contains("TimeSpan.FromMilliseconds(250)", source, StringComparison.Ordinal);
        Assert.Contains("TimeSpan.FromSeconds(60)", source, StringComparison.Ordinal);
        Assert.Contains("Append(State.RefreshInterval)", source, StringComparison.Ordinal);
        Assert.Contains("The response record list was truncated.", source, StringComparison.Ordinal);
        Assert.Contains("Some fields in the returned records were truncated.", source, StringComparison.Ordinal);
        Assert.Contains("Bounded retained history dropped", source, StringComparison.Ordinal);
        Assert.Contains("aggregate data was not substituted", source, StringComparison.Ordinal);
        Assert.Contains("State.ScopeNotice", source, StringComparison.Ordinal);
        Assert.Contains("<span>WAL change</span>", source, StringComparison.Ordinal);
        Assert.Contains("FormatSignedByteRate", source, StringComparison.Ordinal);
        Assert.DoesNotContain("\"WAL growth\"", source, StringComparison.Ordinal);
        Assert.Contains("Where(static sample => sample.Value.HasValue)", source, StringComparison.Ordinal);
        Assert.Contains("No known samples", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DbClient.DataSource", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ExplainHandoff_BuildsEditorDraftWithoutExecutionDirective()
    {
        string draft = ObservabilityTab.BuildExplainEstimateDraft("SELECT * FROM widgets;");

        Assert.Equal("EXPLAIN ESTIMATE FOR SELECT * FROM widgets;", draft);
        Assert.DoesNotContain("EXECUTE", draft, StringComparison.OrdinalIgnoreCase);
    }

    private static string ReadAdminSource(params string[] segments)
    {
        string[] path = [FindRepositoryRoot(), "src", "CSharpDB.Admin", .. segments];
        return File.ReadAllText(Path.Combine(path));
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "CSharpDB.slnx")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate repository root from test base directory.");
    }
}

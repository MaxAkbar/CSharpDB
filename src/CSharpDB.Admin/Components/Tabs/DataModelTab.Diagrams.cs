using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;

namespace CSharpDB.Admin.Components.Tabs;

public partial class DataModelTab
{
    // Serialize saves with explicit lifecycle commands so a delayed viewport save
    // cannot recreate a deleted record or overwrite a newly selected diagram.
    private readonly SemaphoreSlim _diagramSaveGate = new(1, 1);
    private bool _showDiagramNamePanel, _creatingNewDiagram, _focusDiagramName, _focusDeleteCancel;
    private string? _diagramToDelete, _diagramCommandError;
    private int _diagramSelectorVersion;
    private ElementReference _diagramNameInput;
    private ElementReference _deleteCancelButton;

    private void OpenDiagramNamePanel(bool createNew)
    {
        CloseDiagramPanels();
        _creatingNewDiagram = createNew;
        _diagramName = createNew ? "" : _state.DiagramName is { Length: > 0 } name ? $"{name} copy" : "";
        _showDiagramNamePanel = true;
        _focusDiagramName = true;
    }

    private void CloseDiagramPanels()
    {
        _showDiagramNamePanel = false;
        _focusDiagramName = false;
        _focusDeleteCancel = false;
        _diagramToDelete = null;
        _diagramCommandError = null;
        _diagramName = _state.DiagramName ?? "";
    }

    private void OnDiagramPanelKeyDown(KeyboardEventArgs args)
    {
        if (args.Key == "Escape" && !_loading) CloseDiagramPanels();
    }

    private void RequestDeleteDiagram()
    {
        if (_loading || !HasPersistedActiveDiagram) return;
        CloseDiagramPanels();
        _diagramToDelete = _state.DiagramName;
        _focusDeleteCancel = true;
    }

    private Task SelectDiagramAsync(ChangeEventArgs args) => LoadDiagramAsync(args.Value?.ToString() ?? "");

    private Task SaveCurrentDiagramAsync()
    {
        if (string.IsNullOrWhiteSpace(_state.DiagramName))
        {
            OpenDiagramNamePanel(false);
            return Task.CompletedTask;
        }
        return SaveDiagramAsync();
    }

    private async Task SubmitDiagramNameAsync()
    {
        if (_loading || !_showDiagramNamePanel) return;
        string name = _diagramName.Trim();
        if (name.Length == 0) return;
        _loading = true;
        _diagramCommandError = null;
        CancelViewportPersistence();
        await _diagramSaveGate.WaitAsync();
        try
        {
            // Re-read, rather than relying on a possibly stale selector list.
            _savedDiagrams = await ActiveDiagrams.GetDiagramsAsync();
            if (_savedDiagrams.Any(diagram => diagram.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException("Choose a new diagram name. An existing diagram will not be overwritten.");

            if (_creatingNewDiagram || !string.IsNullOrWhiteSpace(_state.DiagramName))
                await PreserveDiagramBeforeSwitchAsync();

            DataModelState candidate = _creatingNewDiagram
                ? await ActiveDataModels.BuildSelectionAsync([])
                : CloneDiagram(_state);
            await ActiveDiagrams.SaveDiagramAsync(name, candidate);
            candidate.DiagramName = name;
            candidate.SavedLayoutName = name;
            _state = candidate;
            _diagramSaveError = null;
            _hasDiagramMutation = true;
            ResetDiagramInteraction();
            ResetHistory();
            SaveState();
            await RefreshSavedDiagramsAsync();
            Toast.Success($"Data model diagram '{name}' saved.");
        }
        catch (Exception ex)
        {
            // Saving a candidate must not change the current name, canvas, or
            // pending intent when validation, metadata loading, or storage fails.
            _diagramCommandError = ex.Message;
        }
        finally { _loading = false; _diagramSelectorVersion++; _diagramSaveGate.Release(); }
    }

    private async Task PreserveDiagramBeforeSwitchAsync()
    {
        if (!_hasDiagramMutation && _state.Nodes.Count == 0 && _state.PendingOperations.Count == 0 && _diagramSaveError is null)
            return;
        if (string.IsNullOrWhiteSpace(_state.DiagramName))
            throw new InvalidOperationException("Save the current unsaved diagram with Save As before creating or opening another diagram.");
        try { await SaveActiveDiagramCoreAsync(); }
        catch (Exception ex)
        {
            _diagramSaveError = $"Diagram changes were not saved: {ex.Message}";
            throw new InvalidOperationException("The current diagram could not be saved. Retry Save before switching diagrams.", ex);
        }
    }

    // Caller holds _diagramSaveGate. Save a snapshot: the service assigns a name
    // before its database write, which must not rename the live canvas on failure.
    private async Task SaveActiveDiagramCoreAsync()
    {
        string name = (_state.DiagramName ?? _diagramName).Trim();
        var snapshot = CloneDiagram(_state);
        await ActiveDiagrams.SaveDiagramAsync(name, snapshot);
        _state.DiagramName = name;
        _state.SavedLayoutName = name;
        if (!_showDiagramNamePanel) _diagramName = name;
        _hasDiagramMutation = true;
        _diagramSaveError = null;
        SaveState();
        await RefreshSavedDiagramsAsync();
    }

    private static DataModelState CloneDiagram(DataModelState state)
    {
        var copy = DataModelGraphBuilder.DeserializeState(DataModelGraphBuilder.SerializeState(state))!;
        copy.SchemaContext = state.SchemaContext;
        return copy;
    }

    private void ResetDiagramInteraction()
    {
        CloseDiagramPanels();
        ClearGroupSelection();
        _confirmTidyRoutes = false;
        _selectedWaypointId = null;
        _selectedNodeName = null;
        _selectedRelationshipId = null;
        _showNewTablePanel = false;
        _showInspectorPane = false;
        _showSourcesPane = _state.Nodes.Count == 0;
        _sourceSearch = "";
        _canvasSearch = "";
        _dataChecks = [];
        _dataCheckMessage = null;
        _dataCheckCancellation?.Cancel();
        _error = null;
    }

    private void CancelViewportPersistence()
    {
        _viewportPersistCts?.Cancel();
        _viewportPersistCts?.Dispose();
        _viewportPersistCts = null;
    }
}

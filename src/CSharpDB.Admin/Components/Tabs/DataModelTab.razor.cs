using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;
using System.Text.Json;

namespace CSharpDB.Admin.Components.Tabs;

public partial class DataModelTab
{
    [Inject] private IJSRuntime ModelJS { get; set; } = default!;
    private readonly DataModelHistory _history = new();
    private bool _historyReady, _restoringHistory, _savingDiagram, _showReview;
    private DataModelChangePlan? _reviewedPlan;
    private string _canvasSearch = "";
    private CancellationTokenSource? _dataCheckCancellation;
    private IReadOnlyList<DataModelDataCheckResult> _dataChecks = [];
    private string? _dataCheckMessage;
    private IEnumerable<DataModelNode> LocatedNodes => _state.Nodes.Where(node => node.Name.Contains(_canvasSearch, StringComparison.OrdinalIgnoreCase)).Take(15);
    private DataModelSchemaProjection? _projection;
    private DataModelState? _projectionBaseline;
    private string? _projectionIntent;
    private DataModelSchemaProjection EditingProjection
    {
        get
        {
            string intent = JsonSerializer.Serialize(_state.PendingOperations);
            if (_projection is null || _projectionBaseline != _state || _projectionIntent != intent)
            { _projection = DataModelSchemaProjection.ForEditing(_state); _projectionBaseline = _state; _projectionIntent = intent; }
            return _projection;
        }
    }
    private DataModelNode? EditingNode => SelectedNode is null ? null : EditingProjection.FromCanvas(SelectedNode);
    private IEnumerable<DataModelRelationship> InspectorRelationships => EditingProjection.State.Relationships.Where(relationship => SelectedNode is null || EditingNode is not null &&
        (relationship.LeftTable.Equals(EditingNode.Name, StringComparison.OrdinalIgnoreCase) || relationship.RightTable.Equals(EditingNode.Name, StringComparison.OrdinalIgnoreCase)));
    private string SaveStatus => _diagramSaveError is not null ? "Save failed" : _savingDiagram ? "Saving…" : HasPersistedActiveDiagram ? "Saved diagram" : "Unsaved diagram";
    private void OpenSelectedCatalog()
    {
        if (SelectedNode is not null) TabManager.OpenSystemCatalogTab("sys.columns", "SELECT * FROM sys.columns WHERE table_name = '" + SelectedNode.Name.Replace("'", "''") + "'");
    }

    private void ResetHistory() { _history.Reset(_state); _historyReady = true; _reviewedPlan = null; _showReview = false; }
    private async Task RestoreHistoryAsync(bool redo)
    {
        if (_loading) return;
        var restored = redo ? _history.Redo() : _history.Undo();
        if (restored is null) return;
        restored.DiagramName = _state.DiagramName; restored.SavedLayoutName = _state.SavedLayoutName;
        restored.Scale = _state.Scale; restored.ViewportX = _state.ViewportX; restored.ViewportY = _state.ViewportY;
        restored.SchemaContext = _state.SchemaContext;
        _state = restored; _restoringHistory = true;
        try { ClearGroupSelection(); _selectedNodeName = null; _selectedRelationshipId = null; _reviewedPlan = null; SaveState(); await PersistActiveDiagramAsync(); }
        finally { _restoringHistory = false; }
    }

    private async Task StageAdvancedAsync(IReadOnlyList<DataModelPendingOperation> operations)
    {
        DataModelService.ValidateBatchForStaging(_state, operations);
        foreach (var operation in operations)
        {
            operation.Description = $"{operation.Kind}: {operation.TableName} {operation.ColumnName ?? operation.ConstraintName ?? operation.IndexName}";
            var projectedNode = EditingProjection.Find(operation.TableName);
            var originalName = projectedNode is null ? operation.TableName : EditingProjection.OriginalName(projectedNode);
            var draftNode = FindNode(originalName);
            if (operation.Kind == DataModelPendingOperationKind.AddColumn && draftNode?.IsDraft == true &&
                !_state.PendingOperations.Any(op => op.TableName.Equals(originalName, StringComparison.OrdinalIgnoreCase) && op.Kind != DataModelPendingOperationKind.CreateTable))
            {
                var column = DataModelSchemaProjection.ColumnFromOperation(operation);
                var create = _state.PendingOperations.Single(op => op.Kind == DataModelPendingOperationKind.CreateTable && op.TableName == draftNode.Name);
                create.Columns.Add(column);
                if (!ReferenceEquals(create.Columns, draftNode.Columns)) draftNode.Columns.Add(column);
                continue;
            }
            _state.PendingOperations.Add(operation);
            if (operation.Kind == DataModelPendingOperationKind.AddForeignKey)
            {
                var pairs = operation.ColumnNames.Zip(operation.ReferencedColumnNames, (child, parent) => new DataModelColumnPair(child, parent)).ToList();
                if (pairs.Count == 0) pairs = [new(operation.ColumnName ?? "", operation.ReferencedColumnName ?? "")];
                _state.Relationships.Add(new() { Id = operation.Id, Kind = DataModelRelationshipKind.Draft, LeftTable = operation.TableName,
                    LeftColumn = pairs[0].ChildColumn, RightTable = operation.ReferencedTableName ?? "", RightColumn = pairs[0].ParentColumn,
                    ColumnPairs = pairs, ConstraintName = operation.ConstraintName, OnDelete = operation.OnDelete, OnUpdate = operation.OnUpdate });
            }
        }
        _reviewedPlan = null; SaveState(); await PersistActiveDiagramAsync();
    }

    private async Task ReviewChangesAsync()
    {
        _loading = true; _error = null; _reviewedPlan = null; _showReview = true;
        try { _reviewedPlan = await ActiveDataModels.ReviewChangesAsync(_state); }
        catch (Exception ex) { _error = ex.Message; }
        finally { _loading = false; }
    }

    private async Task ApplyReviewAsync()
    {
        if (_reviewedPlan?.CanApply != true) return;
        _loading = true; _error = null;
        try
        {
            var result = await ActiveDataModels.ApplyReviewedChangesAsync(_state, _reviewedPlan);
            if (!result.Succeeded) { _error = string.Join(" ", result.Messages); return; }
            ResetHistory(); SaveState();
            Changes?.NotifyChanged();
            if (!result.DiagramSaved) { _diagramSaveError = string.Join(" ", result.Messages); Toast.Warning(_diagramSaveError); }
            else { _diagramSaveError = null; Toast.Success("Reviewed schema changes applied."); }
            try { _sourceOptions = await ActiveDataModels.GetSourceOptionsAsync(); }
            catch (Exception ex) { _error = $"Schema changes were applied, but the source list could not refresh: {ex.Message}"; }
        }
        catch (Exception ex) { _error = $"Schema changes were not applied: {ex.Message}"; _reviewedPlan = null; }
        finally { _loading = false; }
    }

    private Task LocateAsync(string name)
    {
        var node = FindNode(name); if (node is null) return Task.CompletedTask;
        SelectTables(new(name, false));
        _state.ViewportX = Math.Max(0, (node.X + DataModelGroups.CanvasInset - 60) * CurrentScale);
        _state.ViewportY = Math.Max(0, (node.Y + DataModelGroups.CanvasInset - 60) * CurrentScale);
        return OnViewportChangedAsync(new(_state.ViewportX, _state.ViewportY, CurrentScale));
    }

    private async Task InspectKnownRelationshipAsync(DataModelRelationship relationship)
    {
        // A deliberate navigation action may add endpoint cards, but metadata inspection alone never does.
        foreach (string endpoint in new[] { relationship.LeftTable, relationship.RightTable }.Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var projected = EditingProjection.Find(endpoint);
            string live = projected is null ? endpoint : EditingProjection.OriginalName(projected);
            if (FindNode(live) is null) await AddSourceAsync(live, false);
        }
        if (_state.Relationships.Any(r => r.Id == relationship.Id)) SelectRelationship(relationship.Id);
    }

    private async Task ExportModelAsync(string format)
    {
        if (_loading) return;
        _loading = true;
        try
        {
            if (format == "sql")
            {
                var review = await ActiveDataModels.ReviewChangesAsync(_state);
                _reviewedPlan = review; _showReview = true;
                if (!review.CanApply) { _error = "Resolve the review errors before exporting pending SQL."; return; }
                await ModelJS.InvokeVoidAsync("modelerFiles.downloadText", "model-changes.sql", "text/plain", "BEGIN TRANSACTION;\n" + string.Join("\n", review.Steps.Select(step => step.Sql)) + "\nCOMMIT;");
            }
            else await ModelJS.InvokeVoidAsync("modelerFiles.exportCanvas", format, _schemaCanvas?.CanvasId);
        }
        catch (Exception ex) { _error = $"Export failed: {ex.Message}"; }
        finally { _loading = false; }
    }

    private async Task CheckSelectedDataAsync()
    {
        if (SelectedNode is null || _dataCheckCancellation is not null) return;
        string tableName = SelectedNode.Name;
        using var cancellation = new CancellationTokenSource(); _dataCheckCancellation = cancellation;
        _dataChecks = []; _dataCheckMessage = "Checking data. This may scan this table and related tables, including those outside the canvas; Cancel stops remaining work.";
        try { _dataChecks = await ActiveDataModels.CheckDataAsync(_state, tableName, cancellation.Token); _dataCheckMessage = $"{tableName}: {_dataChecks.Count(check => check.SkippedReason is null)} checks completed, {_dataChecks.Count(check => check.SkippedReason is not null)} skipped."; }
        catch (OperationCanceledException) { _dataCheckMessage = "Checks cancelled."; }
        catch (Exception ex) { _dataCheckMessage = $"Data check failed: {ex.Message}"; }
        finally { _dataCheckCancellation = null; }
    }
}

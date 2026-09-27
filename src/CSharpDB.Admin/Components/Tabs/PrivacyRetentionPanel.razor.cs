using System.Text.Json;
using CSharpDB.Admin.Services;
using CSharpDB.Client.Models;
using CSharpDB.DataPrivacy;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Components.Tabs;

public partial class PrivacyRetentionPanel
{
    [Parameter] public string? SeedTable { get; set; }
    [Inject] private PrivacyAdminService Privacy { get; set; } = null!;
    [Inject] private DatabaseClientHolder Holder { get; set; } = null!;
    [Inject] private DatabaseChangeService Changes { get; set; } = null!;
    [Inject] private ModalService Modal { get; set; } = null!;
    [Inject] private IJSRuntime JS { get; set; } = null!;
    private PrivacyPolicy _policy = new();
    private IReadOnlyList<TableSchema> _catalog = [];
    private IReadOnlyList<PrivacyPolicy> _policies = [];
    private IReadOnlyList<PrivacyReceipt> _runs = [];
    private PrivacyPreview? _preview;
    private PrivacyReceipt? _receipt;
    private PrivacyProgress? _progress;
    private Guid? _pending;
    private string? _error, _lastSeed;
    private string _selectedPolicy = "", _status = "", _addTable = "", _sourceTable = "", _sourceColumns = "", _targetTable = "", _targetColumns = "";
    private bool _dirty = true, _busy, _disposed;
    private CancellationTokenSource? _cts;
    private Task? _operation;
    private IProgress<PrivacyProgress> Progress => new Progress<PrivacyProgress>(p =>
    { if (!_disposed) _ = InvokeAsync(() => { _progress = p; StateHasChanged(); }); });

    protected override async Task OnInitializedAsync()
    {
        Holder.DatabaseChanged += DatabaseChanged; Changes.Changed += SchemaChanged;
        await ReloadAsync();
        if (_policy.RootTable.Length == 0 && !string.IsNullOrEmpty(SeedTable)) SetRoot(SeedTable);
    }
    protected override void OnParametersSet()
    {
        if (_lastSeed != SeedTable)
        {
            _lastSeed = SeedTable;
            if (!_busy && _policy.Revision == 0 && !string.IsNullOrEmpty(SeedTable)) SetRoot(SeedTable);
        }
    }
    private static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    private TableSchema? Schema(string name) => _catalog.FirstOrDefault(t => Same(t.TableName, name));
    private void Invalidate() { _preview?.Dispose(); _preview = null; _dirty = true; _status = "Preview required"; }
    private void NewPolicy() { Invalidate(); _policy = new(); _selectedPolicy = ""; _receipt = null; _error = null; SetRoot(SeedTable ?? _catalog.FirstOrDefault()?.TableName ?? ""); }
    private void Duplicate() { Invalidate(); _policy = _policy.Duplicate(); _selectedPolicy = ""; }
    private void LoadPolicy(PrivacyPolicy saved)
    {
        if (_busy) return;
        Invalidate(); _policy = PrivacyPolicy.FromJson(saved.ToJson()); _selectedPolicy = _policy.Id.ToString(); _dirty = false; _error = null; _receipt = null;
        _status = "Policy loaded; preview to see current matching records";
    }
    private void ChangeRoot(ChangeEventArgs e) => SetRoot(e.Value?.ToString() ?? "");
    private void SetRoot(string table)
    {
        Invalidate(); _policy.RootTable = table; _policy.Targets.Clear(); _policy.Relationships.Clear();
        _policy.Eligibility = new() { Children = [new() { Kind = PrivacyConditionKind.OlderThan,
            Column = Schema(table)?.Columns.FirstOrDefault(c => c.EffectiveType.Kind is SqlTypeKind.Date or SqlTypeKind.Timestamp or SqlTypeKind.TimestampWithTimeZone)?.Name ?? "" }] };
        _addTable = table;
    }
    private Task ReloadAsync() => RunAsync(async ct =>
    {
        if (!Privacy.IsSupported) return;
        var loaded = await Privacy.LoadAsync(ct); _catalog = loaded.Catalog; _policies = loaded.Policies; _runs = loaded.Runs; _pending = loaded.Pending;
        _preview?.Dispose(); _preview = null;
        if (_policy.RootTable.Length == 0) SetRoot(SeedTable ?? _catalog.FirstOrDefault()?.TableName ?? "");
        _status = "Policies and schema loaded";
    });
    private Task SaveAsync() => RunAsync(async ct =>
    {
        _preview?.Dispose(); _preview = null; _policy = await Privacy.SaveAsync(_policy, ct);
        _selectedPolicy = _policy.Id.ToString(); _dirty = false;
        var loaded = await Privacy.LoadAsync(ct); _policies = loaded.Policies; _status = "Policy saved";
    });
    private Task PreviewAsync() => RunAsync(async ct =>
    {
        _preview?.Dispose(); _preview = null;
        _preview = await Privacy.PreviewAsync(_policy, Progress, ct); _status = "Preview ready";
    });
    private Task ApplyAsync() => RunAsync(async ct =>
    {
        var reviewed = _preview;
        if (reviewed is null) return;
        string summary = $"Database: {Holder.DataSource}\nPolicy: {_policy.Name} (revision {_policy.Revision})\n"
            + string.Join("\n", reviewed.Tables.Select(t => $"{t.Table}: {t.ChangedRows:N0} rows"))
            + "\nSelected live values will be permanently replaced. All changes commit together.";
        var confirmation = Modal.ConfirmAsync("Apply privacy policy", summary, "Apply changes", isDanger: true);
        var ownedModal = Modal.Current;
        bool confirmed;
        try { confirmed = await confirmation.WaitAsync(ct); }
        finally { if (ct.IsCancellationRequested && ReferenceEquals(ownedModal, Modal.Current)) Modal.Cancel(); }
        if (!confirmed) return;
        ct.ThrowIfCancellationRequested();
        _receipt = await Privacy.ApplyAsync(reviewed, _policy, Progress, ct); _preview = null;
        _pending = _receipt.Status == "Unknown" ? _receipt.RunId : null;
        _status = _receipt.Status;
        if (_receipt.Status == "Committed") _runs = new[] { _receipt }.Concat(_runs).Take(100).ToArray();
    });
    private Task ReconcileAsync() => RunAsync(async ct =>
    {
        if (_pending is not { } id) return;
        var receipt = await Privacy.ReconcileAsync(id, ct);
        if (receipt is null) { _error = "No committed receipt was found. The outcome remains unresolved; retries remain blocked."; return; }
        _receipt = receipt; _pending = null; _status = $"Run reconciled: {receipt.Status}";
    });
    private async Task ExportPolicyAsync()
    {
        try { await JS.InvokeVoidAsync("fileInterop.downloadText", "privacy-policy.json", "application/json", _policy.ToJson()); }
        catch { _error = "The policy download failed."; }
    }
    private Task ImportAsync(InputFileChangeEventArgs e) => RunAsync(async ct =>
    {
        using var reader = new StreamReader(e.File.OpenReadStream(1024 * 1024, ct));
        var imported = PrivacyPolicy.FromJson(await reader.ReadToEndAsync(ct));
        Invalidate(); _policy = imported.Duplicate(); _selectedPolicy = ""; _status = "Imported as a new policy; review mappings and save";
    });
    private async Task ExportReceiptAsync()
    {
        if (_receipt is not null)
            try { await JS.InvokeVoidAsync("fileInterop.downloadText", "privacy-run.json", "application/json", JsonSerializer.Serialize(_receipt, new JsonSerializerOptions { WriteIndented = true })); }
            catch { _error = "The run summary download failed."; }
    }
    private void SelectReceipt(PrivacyReceipt receipt) => _receipt = receipt;
    private IEnumerable<PrivacyRelationship> Suggestions()
    {
        foreach (var child in _catalog)
            foreach (var fk in child.ForeignKeys)
            {
                var childColumns = fk.ColumnNames.Count > 0 ? fk.ColumnNames.ToList() : [fk.ColumnName];
                var parentColumns = fk.ReferencedColumnNames.Count > 0 ? fk.ReferencedColumnNames.ToList() : [fk.ReferencedColumnName];
                yield return new() { SourceTable = fk.ReferencedTableName, SourceColumns = parentColumns, TargetTable = child.TableName, TargetColumns = childColumns };
                yield return new() { SourceTable = child.TableName, SourceColumns = childColumns, TargetTable = fk.ReferencedTableName, TargetColumns = parentColumns };
            }
    }
    private void AddSuggestion(PrivacyRelationship relation)
    {
        if (_policy.Relationships.Any(r => Same(r.SourceTable, relation.SourceTable) && Same(r.TargetTable, relation.TargetTable)
            && r.SourceColumns.SequenceEqual(relation.SourceColumns, StringComparer.OrdinalIgnoreCase) && r.TargetColumns.SequenceEqual(relation.TargetColumns, StringComparer.OrdinalIgnoreCase))) return;
        _policy.Relationships.Add(relation); Invalidate();
    }
    private void AddMapping()
    {
        var source = _sourceColumns.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
        var target = _targetColumns.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries).ToList();
        if (source.Count == 0 || source.Count != target.Count || Schema(_sourceTable) is not { } from || Schema(_targetTable) is not { } to
            || source.Any(c => !from.Columns.Any(col => Same(col.Name, c))) || target.Any(c => !to.Columns.Any(col => Same(col.Name, c))))
        { _error = "Choose existing tables and equal ordered lists of existing columns."; return; }
        AddSuggestion(new() { SourceTable = _sourceTable, SourceColumns = source, TargetTable = _targetTable, TargetColumns = target }); _error = null;
    }
    private void RemoveMapping(PrivacyRelationship relation) { _policy.Relationships.Remove(relation); Invalidate(); }
    private List<List<string>> Paths(string target)
    {
        var result = new List<List<string>>();
        void Walk(string table, List<string> path, HashSet<string> visited)
        {
            if (result.Count >= 64 || path.Count > 8) return;
            if (Same(table, target)) { result.Add(path); return; }
            foreach (var relation in _policy.Relationships.Where(r => Same(r.SourceTable, table)))
            {
                if (visited.Contains(relation.TargetTable)) continue;
                Walk(relation.TargetTable, [..path, relation.Id], new(visited, StringComparer.OrdinalIgnoreCase) { relation.TargetTable });
            }
        }
        Walk(_policy.RootTable, [], new(StringComparer.OrdinalIgnoreCase) { _policy.RootTable }); return result;
    }
    private string PathLabel(List<string> path) => _policy.RootTable + string.Concat(path.Select(id => " → " + _policy.Relationships.First(r => r.Id == id).TargetTable));
    private void SetPath(PrivacyTarget target, string? value) { target.RelationshipPath = (value ?? "").Split('|', StringSplitOptions.RemoveEmptyEntries).ToList(); Invalidate(); }
    private void AddTarget()
    {
        if (Schema(_addTable) is null || _policy.Targets.Any(t => Same(t.Table, _addTable))) return;
        _policy.Targets.Add(new() { Table = _addTable, RelationshipPath = Paths(_addTable).FirstOrDefault() ?? [] }); Invalidate();
    }
    private void RemoveTarget(PrivacyTarget target) { _policy.Targets.Remove(target); Invalidate(); }
    private void ToggleColumn(PrivacyTarget target, ColumnDefinition column, bool selected)
    {
        if (selected) target.Columns.Add(new() { Column = column.Name, Kind = column.Nullable ? PrivacyMaskKind.Erase : PrivacyMaskKind.Constant });
        else target.Columns.RemoveAll(c => Same(c.Column, column.Name));
        Invalidate();
    }
    private static IEnumerable<PrivacyMaskKind> MaskOptions(ColumnDefinition column)
    {
        if (column.Nullable) yield return PrivacyMaskKind.Erase;
        yield return PrivacyMaskKind.Constant;
        if (column.Type == DbType.Text) { yield return PrivacyMaskKind.Anonymous; yield return PrivacyMaskKind.Partial; yield return PrivacyMaskKind.Email; }
    }
    private Task RunAsync(Func<CancellationToken, Task> action)
    {
        if (_busy || _disposed) return Task.CompletedTask;
        _operation = RunCoreAsync(action); return _operation;
    }
    private async Task RunCoreAsync(Func<CancellationToken, Task> action)
    {
        _busy = true; _error = null; _progress = null; _cts = new();
        try { await action(_cts.Token); }
        catch (OperationCanceledException) { _status = "Cancelled"; _preview?.Dispose(); _preview = null; }
        catch (PrivacyException ex) { _error = ex.Message; _preview?.Dispose(); _preview = null; }
        catch { _error = "The privacy operation could not complete. No original field values are included in this message."; _preview?.Dispose(); _preview = null; }
        finally { _cts.Dispose(); _cts = null; _busy = false; }
    }
    private void Cancel() => _cts?.Cancel();
    private void SchemaChanged()
    { if (!_busy && !_disposed) _ = InvokeAsync(() => { _preview?.Dispose(); _preview = null; StateHasChanged(); }); }
    private void DatabaseChanged()
    {
        Cancel();
        if (!_disposed) _ = InvokeAsync(async () =>
        {
            if (_operation is { } operation) await operation;
            if (_disposed) return;
            Invalidate(); _policy = new(); _catalog = []; _policies = []; _runs = []; _receipt = null; _pending = null; _selectedPolicy = "";
            await ReloadAsync(); StateHasChanged();
        });
    }
    public async ValueTask DisposeAsync()
    {
        _disposed = true; Holder.DatabaseChanged -= DatabaseChanged; Changes.Changed -= SchemaChanged; Cancel();
        if (_operation is { } operation) await operation;
        _preview?.Dispose(); _preview = null;
    }
}

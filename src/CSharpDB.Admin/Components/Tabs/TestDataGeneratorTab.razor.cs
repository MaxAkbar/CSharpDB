using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.DataGeneration;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Components.Tabs;

public partial class TestDataGeneratorTab
{
    [Parameter, EditorRequired] public TabDescriptor Tab { get; set; } = null!;
    [Inject] private TestDataGenerationAdminService Generator { get; set; } = null!;
    [Inject] private DatabaseClientHolder Holder { get; set; } = null!;
    [Inject] private DatabaseChangeService Changes { get; set; } = null!;
    [Inject] private IJSRuntime JS { get; set; } = null!;
    private GenerationProfile _profile = new() { ReferenceUtc = DateTime.UtcNow.Date };
    private IReadOnlyList<GenerationTableSnapshot> _catalog = [];
    private PreparedTestData? _prepared;
    private GenerationReceipt? _receipt;
    private GenerationProgress? _progress;
    private string? _error, _table, _field, _mappingChild;
    private string _filter = "", _section = "Fields", _previewTable = "", _mappingColumns = "", _mappingParent = "", _mappingParentColumns = "";
    private bool _busy, _disposed, _unknownOutcome;
    private object? _request;
    private CancellationTokenSource? _cts;
    private Task? _operation;
    private int _previewOffset;
    private IReadOnlyList<IReadOnlyDictionary<string, object?>> _previewRows = [];
    private TableGenerationRule? SelectedTable => FindTable(_table);
    private TableSchema? SelectedSchema => _catalog.FirstOrDefault(t => Same(t.Schema.TableName, _table))?.Schema;
    private ColumnGenerationRule? SelectedField => SelectedTable?.Columns.FirstOrDefault(c => Same(c.ColumnName, _field));
    private TableSchema PreviewSchema => _prepared!.Plan.Schema(_previewTable);
    private DateTime ReferenceDate { get => _profile.ReferenceUtc; set => _profile.ReferenceUtc = DateTime.SpecifyKind(value.Date, DateTimeKind.Utc); }

    protected override async Task OnInitializedAsync()
    {
        Holder.DatabaseChanged += DatabaseChanged;
        Changes.Changed += SchemaChanged;
        await ReloadAsync();
    }
    protected override void OnParametersSet()
    {
        if (!ReferenceEquals(_request, Tab.State.GetValueOrDefault("GeneratorRequest")) && !_busy)
        {
            _request = Tab.State.GetValueOrDefault("GeneratorRequest");
            if (Tab.State.GetValueOrDefault("GeneratorTable") is string table) AddParent(table);
        }
    }
    private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    private TableGenerationRule? FindTable(string? name) => _profile.Tables.FirstOrDefault(t => Same(t.TableName, name));
    private void SelectTable(string name) { _table = name; _field = null; _section = "Fields"; }
    private void Invalidate() { _prepared = null; _previewRows = []; _error = null; }
    private void ToggleTable(GenerationTableSnapshot table, bool selected)
    {
        if (selected && FindTable(table.Schema.TableName) is null)
        {
            var selectedNames = _profile.Tables.Select(t => t.TableName).ToHashSet(StringComparer.OrdinalIgnoreCase);
            selectedNames.Add(table.Schema.TableName);
            _profile.Tables.Add(GenerationPlan.Suggest(table, selectedNames));
            SelectTable(table.Schema.TableName);
        }
        else if (!selected) _profile.Tables.RemoveAll(t => Same(t.TableName, table.Schema.TableName));
        Invalidate();
    }
    private void AddParent(string table)
    {
        var snapshot = _catalog.FirstOrDefault(t => Same(t.Schema.TableName, table));
        if (snapshot is not null) ToggleTable(snapshot, true);
    }
    private static string Label(FieldGenerator value) => value switch
    {
        FieldGenerator.FirstName => "First name", FieldGenerator.LastName => "Last name", FieldGenerator.FullName => "Full name",
        FieldGenerator.PostalCode => "Postal code", FieldGenerator.ValueList => "Value list", FieldGenerator.DateTime => "Date / time",
        FieldGenerator.RowVersion => "Engine generated", _ => value.ToString(),
    };
    private static IEnumerable<FieldDistribution> Distributions(FieldGenerator generator)
    {
        yield return FieldDistribution.Uniform;
        if (generator == FieldGenerator.Number) yield return FieldDistribution.Normal;
        if (generator == FieldGenerator.ValueList) yield return FieldDistribution.Weighted;
        if (generator == FieldGenerator.DateTime) yield return FieldDistribution.Recent;
    }
    private void ChangeGenerator(ColumnGenerationRule field, string? value)
    {
        if (!Enum.TryParse(value, out FieldGenerator choice)) return;
        field.Generator = choice; field.Distribution = FieldDistribution.Uniform; _field = field.ColumnName; Invalidate();
    }
    private void SetNullRate(ColumnGenerationRule field, string? value)
    {
        Invalidate();
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out double percentage)) { field.NullRate = percentage / 100; Invalidate(); }
        else _error = "Enter a numeric null percentage.";
    }
    private void SetValues(ColumnGenerationRule field, string? value) { field.Values = (value ?? "").Replace("\r", "").Split('\n').ToList(); Invalidate(); }
    private void SetWeights(ColumnGenerationRule field, string? value)
    {
        Invalidate();
        try { field.Weights = (value ?? "").Split(',').Select(s => double.Parse(s, CultureInfo.InvariantCulture)).ToList(); Invalidate(); }
        catch (FormatException) { _error = "Enter numeric weights separated by commas."; }
    }
    private bool IsDeclared(TableGenerationRule table, RelationshipGenerationRule relation)
        => _catalog.First(t => Same(t.Schema.TableName, table.TableName)).Schema.ForeignKeys.Any(f => Same(f.ConstraintName, relation.Name));
    private void RemoveRelationship(TableGenerationRule table, RelationshipGenerationRule relation) { table.Relationships.Remove(relation); Invalidate(); }
    private void StartMapping(string table) { _mappingChild = table; _mappingColumns = _mappingParent = _mappingParentColumns = ""; }
    private void AddMapping()
    {
        var child = FindTable(_mappingChild);
        if (child is null || string.IsNullOrWhiteSpace(_mappingParent) || string.IsNullOrWhiteSpace(_mappingColumns) || string.IsNullOrWhiteSpace(_mappingParentColumns)) { _error = "Choose a parent and both sets of key columns."; return; }
        child.Relationships.Add(new RelationshipGenerationRule { Name = $"Logical relationship {child.Relationships.Count + 1}",
            Columns = _mappingColumns.Split(',', StringSplitOptions.TrimEntries).ToList(), ParentTable = _mappingParent,
            ParentColumns = _mappingParentColumns.Split(',', StringSplitOptions.TrimEntries).ToList(), Source = FindTable(_mappingParent) is null ? ParentKeySource.Existing : ParentKeySource.Generated });
        _mappingChild = null; Invalidate();
    }
    private async Task ReloadAsync()
    {
        if (_busy || Tab.HasRouteContext || !Generator.IsSupported) return;
        Invalidate();
        await RunAsync(async ct =>
        {
            _catalog = await Generator.ReadCatalogAsync(ct);
            if (Tab.State.GetValueOrDefault("GeneratorTable") is string table && _profile.Tables.Count == 0) AddParent(table);
        });
    }
    private async Task PreviewAsync()
    {
        Invalidate(); _receipt = null; _section = "Preview";
        await RunAsync(async ct =>
        {
            _prepared = await Generator.PreviewAsync(_profile, Progress(), ct);
            ChangePreviewTable(_prepared.Plan.Order.First());
        });
    }
    private async Task GenerateAsync()
    {
        if (_prepared is not { } prepared || _unknownOutcome) return;
        string hash = _profile.Hash;
        await RunAsync(async ct =>
        {
            _receipt = await Task.Run(() => Generator.ExecuteAsync(prepared, hash, Progress(), ct), ct);
            _unknownOutcome = _receipt.Status == "Unknown";
            _prepared = null; _previewRows = [];
            if (_receipt.Status == "Committed") _catalog = await Generator.ReadCatalogAsync(ct);
        });
    }
    private IProgress<GenerationProgress> Progress() => new Progress<GenerationProgress>(progress =>
    {
        if (!_disposed) _ = InvokeAsync(() => { if (!_disposed) { _progress = progress; StateHasChanged(); } });
    });
    private async Task RunAsync(Func<CancellationToken, Task> action)
    {
        if (_busy || _disposed) return;
        _busy = true; _error = null;
        using var cts = new CancellationTokenSource(); _cts = cts;
        try { _operation = action(cts.Token); await _operation; }
        catch (OperationCanceledException) { if (!_disposed) { _prepared = null; _error = "Operation cancelled."; } }
        catch (Exception error) { if (!_disposed) { _prepared = null; _error = error.Message; } }
        finally { _busy = false; _operation = null; _cts = null; }
    }
    private void ChangePreviewTable(string? table) { if (table is null) return; _previewTable = table; _previewOffset = 0; UpdatePreview(); }
    private void UpdatePreview() => _previewRows = _prepared?.Plan.Rows(_previewTable, _previewOffset, Generator.Limits.PreviewRows).ToArray() ?? [];
    private void PreviousPreview() { _previewOffset = Math.Max(0, _previewOffset - Generator.Limits.PreviewRows); UpdatePreview(); }
    private void NextPreview() { _previewOffset += Generator.Limits.PreviewRows; UpdatePreview(); }
    private string ParentPreview(string relation) => _prepared is null ? "" : string.Join("; ", _prepared.Plan.ReferencedKeys(_previewTable, relation, _previewOffset + 1)
        .Select(row => string.Join(", ", row.Select(p => $"{p.Key} = {GenerationValues.Display(p.Value)}"))));
    private async Task SaveProfileAsync()
    {
        try { await JS.InvokeVoidAsync("fileInterop.downloadText", "test-data-profile.json", "application/json", _profile.ToJson()); }
        catch (Exception error) { _error = error.Message; }
    }
    private async Task LoadProfileAsync(InputFileChangeEventArgs args)
    {
        await RunAsync(async ct =>
        {
            using var reader = new StreamReader(args.File.OpenReadStream(1_000_000, ct));
            var profile = GenerationProfile.FromJson(await reader.ReadToEndAsync(ct));
            foreach (var table in profile.Tables)
                if (!_catalog.Any(t => Same(t.Schema.TableName, table.TableName) && t.Schema.SchemaId == table.SchemaId))
                    throw new InvalidOperationException($"The profile's table '{table.TableName}' does not match this database. Create a new profile for this schema.");
            _profile = profile; _table = profile.Tables.FirstOrDefault()?.TableName; _field = null; Invalidate();
        });
    }
    private async Task SaveReceiptAsync()
    {
        if (_receipt is not null) await JS.InvokeVoidAsync("fileInterop.downloadText", "test-data-run.json", "application/json", JsonSerializer.Serialize(_receipt, new JsonSerializerOptions { WriteIndented = true }));
    }
    private void Cancel() => _cts?.Cancel();
    private void SchemaChanged() { if (!_busy && !_disposed) _ = InvokeAsync(() => { Invalidate(); StateHasChanged(); }); }
    private void DatabaseChanged()
    {
        Cancel();
        if (!_disposed) _ = InvokeAsync(async () =>
        {
            if (_operation is { } operation) { try { await operation; } catch { } }
            if (_disposed) return;
            _profile = new() { ReferenceUtc = DateTime.UtcNow.Date }; _catalog = []; _prepared = null; _table = null;
            _busy = false; await ReloadAsync(); StateHasChanged();
        });
    }
    public async ValueTask DisposeAsync()
    {
        _disposed = true; Holder.DatabaseChanged -= DatabaseChanged; Changes.Changed -= SchemaChanged; Cancel();
        if (_operation is { } operation) { try { await operation; } catch { } }
    }
}

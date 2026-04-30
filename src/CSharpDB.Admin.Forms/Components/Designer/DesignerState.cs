using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Components.Designer;

public class DesignerState
{
    private readonly List<ControlDefinition> _controls = [];
    private readonly List<FormEventBinding> _eventBindings = [];
    private readonly List<DbActionSequence> _actionSequences = [];
    private readonly List<ControlRuleDefinition> _rules = [];
    private readonly Stack<List<ControlDefinition>> _undoStack = new();
    private readonly Stack<List<ControlDefinition>> _redoStack = new();

    public string FormId { get; private set; } = "";
    public string FormName { get; set; } = "Untitled Form";
    public string TableName { get; private set; } = "";
    public int DefinitionVersion { get; private set; } = 1;
    public string SourceSchemaSignature { get; private set; } = string.Empty;
    public LayoutDefinition Layout { get; private set; } = new("absolute", 8, true, [new Breakpoint("md", 0, null)]);

    public IReadOnlyList<ControlDefinition> Controls => _controls;
    public IReadOnlyList<FormEventBinding> EventBindings => _eventBindings;
    public IReadOnlyList<DbActionSequence> ActionSequences => _actionSequences;
    public IReadOnlyList<ControlRuleDefinition> Rules => _rules;
    public HashSet<string> SelectedIds { get; } = [];

    // Active tool from toolbox (null = select mode)
    public string? ActiveTool { get; set; }

    // Tab Order overlay toggle
    public bool ShowTabOrder { get; set; }

    // Responsive breakpoint
    public string ActiveBreakpoint { get; set; } = "desktop";

    // Drag state
    public bool IsDragging { get; set; }
    public string? DraggingControlId { get; set; }
    public double DragStartPointerX { get; set; }
    public double DragStartPointerY { get; set; }
    public double DragOriginX { get; set; }
    public double DragOriginY { get; set; }

    // Resize state
    public bool IsResizing { get; set; }
    public string? ResizingControlId { get; set; }
    public string? ResizeHandle { get; set; }
    public Rect? ResizeOriginRect { get; set; }
    public double ResizeStartPointerX { get; set; }
    public double ResizeStartPointerY { get; set; }

    public event Action? OnChange;

    public double GridSize => Layout.GridSize;
    public bool SnapToGrid => Layout.SnapToGrid;

    public void SetLayoutMode(string layoutMode)
    {
        if (string.IsNullOrWhiteSpace(layoutMode) || string.Equals(Layout.LayoutMode, layoutMode, StringComparison.OrdinalIgnoreCase))
            return;

        Layout = Layout with { LayoutMode = layoutMode };
        NotifyChanged();
    }

    public void SetFormName(string? formName)
    {
        string normalized = string.IsNullOrWhiteSpace(formName)
            ? "Untitled Form"
            : formName.Trim();

        if (string.Equals(FormName, normalized, StringComparison.Ordinal))
            return;

        FormName = normalized;
        NotifyChanged();
    }

    public double Snap(double v)
    {
        if (!SnapToGrid) return v;
        return Math.Round(v / GridSize) * GridSize;
    }

    public void LoadForm(FormDefinition form)
    {
        _controls.Clear();
        _controls.AddRange(form.Controls);
        _eventBindings.Clear();
        _eventBindings.AddRange(form.EventBindings ?? []);
        _actionSequences.Clear();
        _actionSequences.AddRange(form.ActionSequences ?? []);
        _rules.Clear();
        _rules.AddRange(form.Rules ?? []);
        _undoStack.Clear();
        _redoStack.Clear();
        SelectedIds.Clear();
        FormId = form.FormId;
        FormName = form.Name;
        TableName = form.TableName;
        DefinitionVersion = form.DefinitionVersion;
        SourceSchemaSignature = form.SourceSchemaSignature;
        Layout = form.Layout ?? new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]);
        ActiveTool = null;
        NotifyChanged();
    }

    public void SetTableContext(FormTableDefinition? table)
    {
        TableName = table?.TableName ?? string.Empty;
        SourceSchemaSignature = table?.SourceSchemaSignature ?? string.Empty;
        NotifyChanged();
    }

    public FormDefinition ToFormDefinition()
    {
        return new FormDefinition(
            FormId, FormName, TableName, DefinitionVersion, SourceSchemaSignature,
            Layout, _controls.ToList(), EventBindings: _eventBindings.ToList(), ActionSequences: _actionSequences.ToList(), Rules: _rules.ToList());
    }

    public void UpdateEventBindings(IReadOnlyList<FormEventBinding> bindings)
    {
        _eventBindings.Clear();
        _eventBindings.AddRange(bindings);
        NotifyChanged();
    }

    public void UpdateActionSequences(IReadOnlyList<DbActionSequence> sequences)
    {
        _actionSequences.Clear();
        _actionSequences.AddRange(sequences);
        NotifyChanged();
    }

    public void UpdateRules(IReadOnlyList<ControlRuleDefinition> rules)
    {
        _rules.Clear();
        _rules.AddRange(rules);
        NotifyChanged();
    }

    public void UpdateControlEventBindings(string controlId, IReadOnlyList<ControlEventBinding> bindings)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        PushUndo();
        _controls[idx] = _controls[idx] with { EventBindings = bindings.ToList() };
        NotifyChanged();
    }

    public void PushUndo()
    {
        _undoStack.Push(_controls.Select(c => c).ToList());
        _redoStack.Clear();
    }

    public void Undo()
    {
        if (_undoStack.Count == 0) return;
        _redoStack.Push(_controls.Select(c => c).ToList());
        var snapshot = _undoStack.Pop();
        _controls.Clear();
        _controls.AddRange(snapshot);
        SelectedIds.Clear();
        NotifyChanged();
    }

    public void Redo()
    {
        if (_redoStack.Count == 0) return;
        _undoStack.Push(_controls.Select(c => c).ToList());
        var snapshot = _redoStack.Pop();
        _controls.Clear();
        _controls.AddRange(snapshot);
        SelectedIds.Clear();
        NotifyChanged();
    }

    public bool CanUndo => _undoStack.Count > 0;
    public bool CanRedo => _redoStack.Count > 0;

    public void AddControl(ControlDefinition control)
    {
        PushUndo();
        _controls.Add(control);
        SelectedIds.Clear();
        SelectedIds.Add(control.ControlId);
        NotifyChanged();
    }

    public void MoveControl(string controlId, double x, double y)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        var c = _controls[idx];
        _controls[idx] = c with { Rect = c.Rect with { X = x, Y = y } };
        NotifyChanged();
    }

    public void ResizeControl(string controlId, Rect newRect)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        _controls[idx] = _controls[idx] with { Rect = newRect };
        NotifyChanged();
    }

    public void DeleteSelected()
    {
        if (SelectedIds.Count == 0) return;
        PushUndo();
        _controls.RemoveAll(c => SelectedIds.Contains(c.ControlId));
        SelectedIds.Clear();
        NotifyChanged();
    }

    public void SelectControl(string controlId, bool addToSelection)
    {
        if (!addToSelection)
            SelectedIds.Clear();
        SelectedIds.Add(controlId);
        NotifyChanged();
    }

    public void ClearSelection()
    {
        if (SelectedIds.Count == 0) return;
        SelectedIds.Clear();
        NotifyChanged();
    }

    public void UpdateControlProp(string controlId, string key, object? value)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        PushUndo();
        var c = _controls[idx];
        var newValues = new Dictionary<string, object?>(c.Props.Values) { [key] = value };
        _controls[idx] = c with { Props = new PropertyBag(newValues) };
        NotifyChanged();
    }

    public void UpdateControlType(string controlId, string newType)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        PushUndo();
        _controls[idx] = _controls[idx] with { ControlType = newType };
        NotifyChanged();
    }

    public void UpdateControlBinding(string controlId, BindingDefinition? binding)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        PushUndo();
        _controls[idx] = _controls[idx] with { Binding = binding };
        NotifyChanged();
    }

    public ControlDefinition? GetSelectedControl()
    {
        if (SelectedIds.Count != 1) return null;
        var id = SelectedIds.First();
        return _controls.FirstOrDefault(c => c.ControlId == id);
    }

    public void BringToFront(string controlId)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0 || idx == _controls.Count - 1) return;
        PushUndo();
        var c = _controls[idx];
        _controls.RemoveAt(idx);
        _controls.Add(c);
        NotifyChanged();
    }

    public void SendToBack(string controlId)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx <= 0) return;
        PushUndo();
        var c = _controls[idx];
        _controls.RemoveAt(idx);
        _controls.Insert(0, c);
        NotifyChanged();
    }

    public void MoveUp(string controlId)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0 || idx >= _controls.Count - 1) return;
        PushUndo();
        (_controls[idx], _controls[idx + 1]) = (_controls[idx + 1], _controls[idx]);
        NotifyChanged();
    }

    public void MoveDown(string controlId)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx <= 0) return;
        PushUndo();
        (_controls[idx], _controls[idx - 1]) = (_controls[idx - 1], _controls[idx]);
        NotifyChanged();
    }

    public void MoveToIndex(string controlId, int newIndex)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0 || idx == newIndex) return;
        newIndex = Math.Clamp(newIndex, 0, _controls.Count - 1);
        PushUndo();
        var c = _controls[idx];
        _controls.RemoveAt(idx);
        _controls.Insert(newIndex, c);
        NotifyChanged();
    }

    // ===== Clipboard (Copy/Paste) =====
    private List<ControlDefinition> _clipboard = [];

    public bool HasClipboard => _clipboard.Count > 0;

    public void CopySelected()
    {
        _clipboard = _controls
            .Where(c => SelectedIds.Contains(c.ControlId))
            .ToList();
    }

    public void PasteClipboard()
    {
        if (_clipboard.Count == 0) return;
        PushUndo();
        SelectedIds.Clear();

        foreach (var original in _clipboard)
        {
            var pasted = original with
            {
                ControlId = Guid.NewGuid().ToString("N"),
                Rect = original.Rect with { X = original.Rect.X + 16, Y = original.Rect.Y + 16 }
            };
            _controls.Add(pasted);
            SelectedIds.Add(pasted.ControlId);
        }
        NotifyChanged();
    }

    public void DuplicateSelected()
    {
        CopySelected();
        PasteClipboard();
    }

    // ===== Alignment Tools =====
    public void AlignSelectedLeft()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).ToList();
        if (selected.Count < 2) return;
        PushUndo();
        var minX = selected.Min(c => c.Rect.X);
        foreach (var c in selected)
            MoveControlInternal(c.ControlId, minX, c.Rect.Y);
        NotifyChanged();
    }

    public void AlignSelectedTop()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).ToList();
        if (selected.Count < 2) return;
        PushUndo();
        var minY = selected.Min(c => c.Rect.Y);
        foreach (var c in selected)
            MoveControlInternal(c.ControlId, c.Rect.X, minY);
        NotifyChanged();
    }

    public void AlignSelectedRight()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).ToList();
        if (selected.Count < 2) return;
        PushUndo();
        var maxRight = selected.Max(c => c.Rect.X + c.Rect.Width);
        foreach (var c in selected)
            MoveControlInternal(c.ControlId, maxRight - c.Rect.Width, c.Rect.Y);
        NotifyChanged();
    }

    public void AlignSelectedBottom()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).ToList();
        if (selected.Count < 2) return;
        PushUndo();
        var maxBottom = selected.Max(c => c.Rect.Y + c.Rect.Height);
        foreach (var c in selected)
            MoveControlInternal(c.ControlId, c.Rect.X, maxBottom - c.Rect.Height);
        NotifyChanged();
    }

    public void DistributeHorizontally()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).OrderBy(c => c.Rect.X).ToList();
        if (selected.Count < 3) return;
        PushUndo();
        var left = selected.First().Rect.X;
        var right = selected.Last().Rect.X;
        var step = (right - left) / (selected.Count - 1);
        for (int i = 1; i < selected.Count - 1; i++)
            MoveControlInternal(selected[i].ControlId, left + step * i, selected[i].Rect.Y);
        NotifyChanged();
    }

    public void DistributeVertically()
    {
        var selected = _controls.Where(c => SelectedIds.Contains(c.ControlId)).OrderBy(c => c.Rect.Y).ToList();
        if (selected.Count < 3) return;
        PushUndo();
        var top = selected.First().Rect.Y;
        var bottom = selected.Last().Rect.Y;
        var step = (bottom - top) / (selected.Count - 1);
        for (int i = 1; i < selected.Count - 1; i++)
            MoveControlInternal(selected[i].ControlId, selected[i].Rect.X, top + step * i);
        NotifyChanged();
    }

    /// <summary>Internal move without undo push or notify — used by alignment helpers.</summary>
    private void MoveControlInternal(string controlId, double x, double y)
    {
        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        var c = _controls[idx];
        _controls[idx] = c with { Rect = c.Rect with { X = x, Y = y } };
    }

    // ===== Responsive Breakpoints =====

    public double GetCanvasWidth()
    {
        return ActiveBreakpoint switch
        {
            "mobile" => 375,
            "tablet" => 768,
            _ => 1200 // desktop
        };
    }

    public Rect GetEffectiveRect(ControlDefinition c)
    {
        if (ActiveBreakpoint == "desktop" || c.RendererHints is null) return c.Rect;

        var key = $"bp:{ActiveBreakpoint}";
        if (c.RendererHints.TryGetValue(key, out var hintObj) && hintObj is System.Text.Json.JsonElement je)
        {
            var x = je.TryGetProperty("x", out var xp) ? xp.GetDouble() : c.Rect.X;
            var y = je.TryGetProperty("y", out var yp) ? yp.GetDouble() : c.Rect.Y;
            var w = je.TryGetProperty("width", out var wp) ? wp.GetDouble() : c.Rect.Width;
            var h = je.TryGetProperty("height", out var hp) ? hp.GetDouble() : c.Rect.Height;
            return new Rect(x, y, w, h);
        }

        if (c.RendererHints.TryGetValue(key, out var dictObj) && dictObj is Dictionary<string, object?> dict)
        {
            var x = dict.TryGetValue("x", out var xv) ? Convert.ToDouble(xv) : c.Rect.X;
            var y = dict.TryGetValue("y", out var yv) ? Convert.ToDouble(yv) : c.Rect.Y;
            var w = dict.TryGetValue("width", out var wv) ? Convert.ToDouble(wv) : c.Rect.Width;
            var h = dict.TryGetValue("height", out var hv) ? Convert.ToDouble(hv) : c.Rect.Height;
            return new Rect(x, y, w, h);
        }

        return c.Rect; // fallback to desktop rect
    }

    public bool IsVisibleAtBreakpoint(ControlDefinition c)
    {
        if (ActiveBreakpoint == "desktop" || c.RendererHints is null) return true;

        var key = $"bp:{ActiveBreakpoint}";
        if (c.RendererHints.TryGetValue(key, out var hintObj))
        {
            if (hintObj is System.Text.Json.JsonElement je && je.TryGetProperty("visible", out var vp))
                return vp.GetBoolean();
            if (hintObj is Dictionary<string, object?> dict && dict.TryGetValue("visible", out var vv) && vv is bool b)
                return b;
        }

        return true; // visible by default
    }

    public void SetBreakpointOverride(string controlId, Rect rect)
    {
        if (ActiveBreakpoint == "desktop")
        {
            // Desktop: set directly on control Rect
            var idx = _controls.FindIndex(c => c.ControlId == controlId);
            if (idx < 0) return;
            _controls[idx] = _controls[idx] with { Rect = rect };
        }
        else
        {
            // Non-desktop: store in RendererHints
            var idx = _controls.FindIndex(c => c.ControlId == controlId);
            if (idx < 0) return;
            var c = _controls[idx];
            var key = $"bp:{ActiveBreakpoint}";
            var newHints = c.RendererHints is not null
                ? new Dictionary<string, object?>(c.RendererHints)
                : new Dictionary<string, object?>();

            var existing = new Dictionary<string, object?>();
            if (newHints.TryGetValue(key, out var prev) && prev is Dictionary<string, object?> prevDict)
                existing = new Dictionary<string, object?>(prevDict);

            existing["x"] = rect.X;
            existing["y"] = rect.Y;
            existing["width"] = rect.Width;
            existing["height"] = rect.Height;
            newHints[key] = existing;

            _controls[idx] = c with { RendererHints = newHints };
        }
        NotifyChanged();
    }

    public void SetBreakpointVisibility(string controlId, bool visible)
    {
        if (ActiveBreakpoint == "desktop") return; // desktop is always visible

        var idx = _controls.FindIndex(c => c.ControlId == controlId);
        if (idx < 0) return;
        var c = _controls[idx];
        var key = $"bp:{ActiveBreakpoint}";
        var newHints = c.RendererHints is not null
            ? new Dictionary<string, object?>(c.RendererHints)
            : new Dictionary<string, object?>();

        var existing = new Dictionary<string, object?>();
        if (newHints.TryGetValue(key, out var prev) && prev is Dictionary<string, object?> prevDict)
            existing = new Dictionary<string, object?>(prevDict);

        existing["visible"] = visible;
        newHints[key] = existing;

        _controls[idx] = c with { RendererHints = newHints };
        NotifyChanged();
    }

    // Override MoveControl and ResizeControl to use breakpoint overrides when not on desktop
    public void MoveControlForBreakpoint(string controlId, double x, double y)
    {
        if (ActiveBreakpoint == "desktop")
        {
            MoveControl(controlId, x, y);
        }
        else
        {
            var idx = _controls.FindIndex(c => c.ControlId == controlId);
            if (idx < 0) return;
            var currentRect = GetEffectiveRect(_controls[idx]);
            SetBreakpointOverride(controlId, currentRect with { X = x, Y = y });
        }
    }

    public void ResizeControlForBreakpoint(string controlId, Rect newRect)
    {
        if (ActiveBreakpoint == "desktop")
        {
            ResizeControl(controlId, newRect);
        }
        else
        {
            SetBreakpointOverride(controlId, newRect);
        }
    }

    public void NotifyChanged() => OnChange?.Invoke();
}

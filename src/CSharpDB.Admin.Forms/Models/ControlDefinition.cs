namespace CSharpDB.Admin.Forms.Models;

public sealed record ControlDefinition(
    string ControlId,
    string ControlType,
    Rect Rect,
    BindingDefinition? Binding,
    PropertyBag Props,
    ValidationOverride? ValidationOverride,
    IReadOnlyDictionary<string, object?>? RendererHints = null,
    IReadOnlyList<ControlEventBinding>? EventBindings = null);

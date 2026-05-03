using CSharpDB.Admin.Forms.Components.Designer;

namespace CSharpDB.Admin.Forms.Models;

public sealed record FormControlDesignContext(
    ControlDefinition Control,
    DesignerState State,
    bool IsSelected,
    Rect EffectiveRect,
    FormControlDescriptor Descriptor);

public sealed record FormControlRuntimeContext(
    FormDefinition Form,
    ControlDefinition Control,
    FormControlDescriptor Descriptor,
    FormTableDefinition? TableDefinition,
    Dictionary<string, object?> Record,
    string? FieldName,
    object? BoundValue,
    IReadOnlyList<EnumChoice> Choices,
    bool IsEnabled,
    bool IsReadOnly,
    string? ValidationError,
    int TabIndex,
    Func<object?, Task> SetValueAsync,
    Func<ControlEventKind, IReadOnlyDictionary<string, object?>?, Task> DispatchEventAsync);

public sealed record FormControlPropertyContext(
    ControlDefinition Control,
    FormControlDescriptor Descriptor,
    FormTableDefinition? SourceTableDefinition,
    IReadOnlyList<string>? AvailableTables,
    IReadOnlyList<FormDefinition>? AvailableForms,
    Func<string, object?, Task> SetPropertyAsync);

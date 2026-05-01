# Form Control Extensibility

Admin Forms controls are persisted as `ControlDefinition.ControlType` plus a
free-form `PropertyBag`. The extensibility registry turns that existing wire
shape into a developer API: a host can add designer toolbox entries, placement
defaults, property editing, designer previews, and runtime rendering without
changing saved form JSON.

## Registration

Register built-ins with `AddCSharpDbAdminForms()`, then add custom controls with
`AddCSharpDbAdminFormControls(...)`:

```csharp
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;

builder.Services.AddCSharpDbAdminForms();
builder.Services.AddCSharpDbAdminFormControls(controls =>
{
    controls.Add(new FormControlDescriptor
    {
        ControlType = "rating",
        DisplayName = "Rating",
        ToolboxGroup = "Custom",
        IconText = "*",
        DefaultWidth = 220,
        DefaultHeight = 48,
        SupportsBinding = true,
        ParticipatesInTabOrder = true,
        DefaultProps = new Dictionary<string, object?>
        {
            ["max"] = 5,
            ["displayMode"] = "buttons",
        },
        DesignerPreviewComponentType = typeof(RatingDesignerPreview),
        RuntimeComponentType = typeof(RatingRuntimeControl),
        PropertyEditorComponentType = typeof(RatingPropertyEditor),
    });
});
```

The same registration must exist anywhere the form is designed or rendered.
Unknown control types are preserved in form metadata and render as placeholders.

## Descriptor Fields

`FormControlDescriptor` defines the designer and runtime contract:

- `ControlType`: persisted control type string.
- `DisplayName`, `ToolboxGroup`, `IconText`, `Description`: toolbox and labels.
- `DefaultWidth`, `DefaultHeight`, `DefaultProps`: new-control placement.
- `SupportsBinding`: whether placement and the inspector create a field binding.
- `ParticipatesInTabOrder`: whether the control joins tab-order editing.
- `PropertyDescriptors`: generic property fields for simple custom props.
- `DesignerPreviewComponentType`: optional Blazor preview component.
- `RuntimeComponentType`: optional Blazor data-entry component.
- `PropertyEditorComponentType`: optional custom property editor.
- `ReplaceBuiltInRuntime`: lets a host explicitly replace a built-in runtime
  renderer while keeping the built-in designer metadata.

Duplicate custom control types fail when the registry is resolved. Built-in
runtime replacement also fails unless `ReplaceBuiltInRuntime = true`.

## Component Contexts

Custom components receive a single `Context` parameter.

Designer previews use `FormControlDesignContext`:

```csharp
[Parameter, EditorRequired]
public FormControlDesignContext Context { get; set; } = default!;
```

Runtime controls use `FormControlRuntimeContext`:

```csharp
[Parameter, EditorRequired]
public FormControlRuntimeContext Context { get; set; } = default!;
```

The runtime context includes the form, control metadata, current record, bound
field/value, resolved choices, enabled/read-only state, validation error,
tab-index, `SetValueAsync`, and `DispatchEventAsync`.

Custom property editors use `FormControlPropertyContext`:

```csharp
[Parameter, EditorRequired]
public FormControlPropertyContext Context { get; set; } = default!;
```

Use `Context.SetPropertyAsync(name, value)` to write into the control
`PropertyBag`.

## Generic Property Schema

For simple controls, skip a custom property editor and define
`PropertyDescriptors`:

```csharp
PropertyDescriptors =
[
    new FormControlPropertyDescriptor
    {
        Name = "max",
        Label = "Maximum Rating",
        Editor = FormControlPropertyEditor.Number,
        DefaultValue = 5,
        HelpText = "Allowed values are 1 through 10.",
    },
    new FormControlPropertyDescriptor
    {
        Name = "displayMode",
        Label = "Display Mode",
        Editor = FormControlPropertyEditor.Select,
        Options =
        [
            new FormControlPropertyOption("buttons", "Buttons"),
            new FormControlPropertyOption("compact", "Compact"),
        ],
    },
];
```

Generic editors support `Text`, `TextArea`, `Number`, `Checkbox`, and `Select`.

## Sample Rating Control

The Admin host includes a compiled sample custom control at:

- `src/CSharpDB.Admin/Components/Samples/FormControls/RatingDesignerPreview.razor`
- `src/CSharpDB.Admin/Components/Samples/FormControls/RatingRuntimeControl.razor`
- `src/CSharpDB.Admin/Components/Samples/FormControls/RatingPropertyEditor.razor`
- `src/CSharpDB.Admin/Components/Samples/FormControls/SampleRatingControlRegistration.cs`

It is disabled by default. Enable it for local testing with:

```powershell
$env:AdminForms__EnableSampleControls = 'true'
dotnet run --project src\CSharpDB.Admin --urls http://127.0.0.1:61818
```

The sample registers `sampleRating` under the `Custom` toolbox group. It binds to
a scalar field, writes the selected numeric rating through `SetValueAsync`, and
dispatches click events with runtime arguments.

## Current Limits

V1 custom controls are leaf controls. They can be placed inside existing
`tabControl` pages, but custom containers do not own or render child controls
yet. Custom components must be compiled into the host app or referenced
assemblies; form JSON never loads arbitrary component assemblies.

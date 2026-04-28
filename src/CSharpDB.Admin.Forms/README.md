# CSharpDB.Admin.Forms

Reusable Razor class library and service layer for CSharpDB form design and data
entry.

This project is consumed by `CSharpDB.Admin`. It is not a standalone web host.

## What This Project Provides

- visual form designer components
- data-entry components for generated forms
- form definition models and JSON serialization helpers
- schema discovery from `ICSharpDbClient`
- form persistence in CSharpDB
- record paging, search, create, update, and delete services
- validation rule inference and validation override support
- child table/tab support for related records
- trusted command-backed form events and command buttons
- trusted command-backed selected-control events
- declarative action sequences for form and selected-control events
- generated automation metadata for import/export host callback requirements

## Main Components

- `Pages/Designer.razor` - form designer page used inside admin tabs
- `Pages/DataEntry.razor` - generated form renderer and record editor
- `Components/Designer/*` - canvas, toolbox, property inspector, layer panel,
  child tabs, and record grid components
- `Services/DbFormRepository.cs` - form-definition persistence
- `Services/DbFormRecordService.cs` - table-backed record CRUD and navigation
- `Services/DbSchemaProvider.cs` - source table discovery and schema mapping
- `Services/DefaultFormGenerator.cs` - default form generation from table schema
- `Services/DefaultValidationInferenceService.cs` - validation rule inference

## Service Registration

Register the default implementation set in a Razor host that already provides
`ICSharpDbClient`:

```csharp
using CSharpDB.Admin.Forms.Services;

builder.Services.AddCSharpDbAdminForms();
```

Trusted command callbacks can be registered with the overload:

```csharp
builder.Services.AddCSharpDbAdminForms(commands =>
{
    commands.AddAsyncCommand(
        "AuditFormOpen",
        new DbCommandOptions(
            Description: "Writes a form audit entry.",
            Timeout: TimeSpan.FromSeconds(5),
            IsLongRunning: true),
        async (context, ct) =>
        {
            await WriteAuditAsync(context.Metadata["formName"], ct);
            return DbCommandResult.Success();
        });
});
```

The Forms runtime passes the command cancellation token to trusted callbacks.
If a command timeout elapses, the runtime reports the timeout through the same
form-event failure path as other command errors. Command buttons refresh their
executing state around async callbacks so the clicked button is disabled while
the callback is in flight.

The extension registers:

- `IFormRepository`
- `ISchemaProvider`
- `IFormRecordService`
- `IFormGenerator`
- `IValidationInferenceService`
- `IFormEventDispatcher`
- `DbCommandRegistry`

## Core Contracts

| Contract | Purpose |
| --- | --- |
| `IFormRepository` | Create, read, update, list, and delete form definitions. |
| `ISchemaProvider` | List source tables and map table schemas into form metadata. |
| `IFormRecordService` | Browse, search, navigate, create, update, and delete records. |
| `IFormGenerator` | Generate a default `FormDefinition` from a table definition. |
| `IValidationInferenceService` | Infer validation rules and evaluate records. |

## Data Model

The primary persisted model is `FormDefinition`:

```csharp
public sealed record FormDefinition(
    string FormId,
    string Name,
    string TableName,
    int DefinitionVersion,
    string SourceSchemaSignature,
    LayoutDefinition Layout,
    IReadOnlyList<ControlDefinition> Controls,
    IReadOnlyDictionary<string, object?>? RendererHints = null,
    IReadOnlyList<FormEventBinding>? EventBindings = null,
    DbAutomationMetadata? Automation = null);
```

Controls are stored as `ControlDefinition` records with geometry, binding,
properties, optional validation overrides, optional renderer hints, and optional
`ControlEventBinding` entries for selected control events such as `OnClick`,
`OnChange`, `OnGotFocus`, and `OnLostFocus`.

Form and control event bindings can reference a trusted command name and can
optionally include a `DbActionSequence`. Action sequences store declarative
steps such as `RunCommand`, `SetFieldValue`, `ShowMessage`, `Stop`, `NewRecord`,
`SaveRecord`, `DeleteRecord`, `RefreshRecords`, `PreviousRecord`, `NextRecord`,
and `GoToRecord`; they do not store C# source or serialized delegates. The
property inspector exposes a
visual action-sequence editor on form-level and selected-control event bindings;
JSON editing is limited to optional command argument payloads.

The built-in record actions run only in the rendered Forms data-entry runtime.
Headless form event dispatch can still run command, field, message, and stop
steps, but navigation and save/delete actions require a rendered form instance.

`DbFormRepository` regenerates `Automation` on save/load. The manifest records
trusted command and scalar-function names used by form events, command buttons,
selected-control events, action sequences, and computed formulas so exported
form JSON tells a host which callbacks it must register.

## Build

```powershell
dotnet build src/CSharpDB.Admin.Forms/CSharpDB.Admin.Forms.csproj
dotnet test tests/CSharpDB.Admin.Forms.Tests/CSharpDB.Admin.Forms.Tests.csproj
```

## Dependencies

- `CSharpDB.Client`
- `Microsoft.AspNetCore.App`

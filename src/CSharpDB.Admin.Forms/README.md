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

The extension registers:

- `IFormRepository`
- `ISchemaProvider`
- `IFormRecordService`
- `IFormGenerator`
- `IValidationInferenceService`

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
    IReadOnlyDictionary<string, object?>? RendererHints = null);
```

Controls are stored as `ControlDefinition` records with geometry, binding,
properties, optional validation overrides, and optional renderer hints.

## Build

```powershell
dotnet build src/CSharpDB.Admin.Forms/CSharpDB.Admin.Forms.csproj
dotnet test tests/CSharpDB.Admin.Forms.Tests/CSharpDB.Admin.Forms.Tests.csproj
```

## Dependencies

- `CSharpDB.Client`
- `Microsoft.AspNetCore.App`

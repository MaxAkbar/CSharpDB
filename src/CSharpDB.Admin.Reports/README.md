# CSharpDB.Admin.Reports

Reusable Razor class library and service layer for CSharpDB report design and
preview.

This project is consumed by `CSharpDB.Admin`. It is not a standalone web host.

## What This Project Provides

- visual report designer
- printable report preview surface
- report definition models and JSON serialization helpers
- report source discovery from tables, views, and supported sources
- report persistence in CSharpDB
- default report generation from source schema
- grouping, sorting, bands, bound text, calculated text, labels, lines, and box
  controls
- preview pagination and simple expression evaluation
- trusted command-backed preview lifecycle events
- generated automation metadata for import/export host callback requirements

## Main Components

- `Pages/Designer.razor` - report designer page used inside admin tabs
- `Pages/Preview.razor` - report preview and print surface
- `Services/DbReportRepository.cs` - report-definition persistence
- `Services/DbReportSourceProvider.cs` - source discovery and schema mapping
- `Services/DefaultReportGenerator.cs` - default report generation
- `Services/DefaultReportPreviewService.cs` - report rendering and pagination
- `Services/ReportPreviewQueryBuilder.cs` - query generation for preview data
- `Services/ReportFormulaEvaluator.cs` - calculated text expression support

## Service Registration

Register the default implementation set in a Razor host that already provides
`ICSharpDbClient`:

```csharp
using CSharpDB.Admin.Reports.Services;

builder.Services.AddCSharpDbAdminReports();
```

Hosts that want Access-style report automation can register trusted commands:

```csharp
using CSharpDB.Primitives;

builder.Services.AddCSharpDbAdminReports(commands =>
{
    commands.AddCommand("PublishReportRendered", static context =>
    {
        long pageCount = context.Arguments["pageCount"].AsInteger;
        return DbCommandResult.Success($"Rendered {pageCount} page(s).");
    });
});
```

The extension registers:

- `IReportRepository`
- `IReportSourceProvider`
- `IReportGenerator`
- `IReportEventDispatcher`
- `IReportPreviewService`

## Core Contracts

| Contract | Purpose |
| --- | --- |
| `IReportRepository` | Create, read, update, list, and delete report definitions. |
| `IReportSourceProvider` | List and describe reportable sources. |
| `IReportGenerator` | Generate a default `ReportDefinition` from a source. |
| `IReportPreviewService` | Render a report definition into preview pages. |

## Data Model

The primary persisted model is `ReportDefinition`:

```csharp
public sealed record ReportDefinition(
    string ReportId,
    string Name,
    ReportSourceReference Source,
    int DefinitionVersion,
    string SourceSchemaSignature,
    ReportPageSettings PageSettings,
    IReadOnlyList<ReportGroupDefinition> Groups,
    IReadOnlyList<ReportSortDefinition> Sorts,
    IReadOnlyList<ReportBandDefinition> Bands,
    IReadOnlyDictionary<string, object?>? RendererHints = null,
    IReadOnlyList<ReportEventBinding>? EventBindings = null,
    DbAutomationMetadata? Automation = null);
```

Report layout is band-based. Each `ReportBandDefinition` owns a list of
`ReportControlDefinition` records positioned within that band.

`EventBindings` can reference host-registered commands for `OnOpen`,
`BeforeRender`, and `AfterRender`. Report JSON stores event names, command
names, and optional arguments only; C# command bodies stay in the host process.
`DbReportRepository` regenerates `Automation` on save/load so exported report
JSON lists required trusted commands and scalar functions from event bindings
and calculated text expressions.

## Build

```powershell
dotnet build src/CSharpDB.Admin.Reports/CSharpDB.Admin.Reports.csproj
dotnet test tests/CSharpDB.Admin.Reports.Tests/CSharpDB.Admin.Reports.Tests.csproj
```

## Dependencies

- `CSharpDB.Client`
- `Microsoft.AspNetCore.App`

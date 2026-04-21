# CSharpDB.Admin

Blazor Server administration UI for CSharpDB databases.

`CSharpDB.Admin` is an application host, not a NuGet package. It wires the
shared `CSharpDB.Client` API into a tabbed browser UI for inspecting and editing
database objects.

## What This Project Provides

- object explorer for user tables, system tables, forms, reports, views,
  triggers, saved queries, and procedures
- table browsing with insert, update, delete, and schema views
- table designer for creating tables
- SQL query tabs with paged results
- procedure editor and execution surface
- storage inspection and maintenance views
- ETL pipeline designer, JSON package editor, stored pipeline catalog, and run
  history views
- form designer and data-entry tabs from `CSharpDB.Admin.Forms`
- report designer and preview tabs from `CSharpDB.Admin.Reports`

## Runtime Model

The host registers one `ICSharpDbClient` through `DatabaseClientHolder`.

Configuration supports two shapes:

- direct embedded database access through `ConnectionStrings:CSharpDB`
- remote access through `CSharpDB:Transport` plus `CSharpDB:Endpoint`

Default `appsettings.json` uses direct mode:

```json
{
  "CSharpDB": {
    "Transport": "direct"
  },
  "ConnectionStrings": {
    "CSharpDB": "Data Source=relational.db"
  }
}
```

The app opens the configured database during startup by calling
`ICSharpDbClient.GetInfoAsync()`, so invalid configuration fails before the UI
accepts requests.

## Running Locally

```powershell
dotnet run --project src/CSharpDB.Admin/CSharpDB.Admin.csproj
```

The development launch profile uses:

- `https://localhost:61816`
- `http://localhost:61817`

## Configuration Examples

Direct file-backed database:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\data\app.db"
$env:CSharpDB__Transport = "direct"
dotnet run --project src/CSharpDB.Admin/CSharpDB.Admin.csproj
```

gRPC daemon-backed database:

```powershell
$env:CSharpDB__Transport = "grpc"
$env:CSharpDB__Endpoint = "http://localhost:5820"
dotnet run --project src/CSharpDB.Admin/CSharpDB.Admin.csproj
```

## Project Layout

- `Program.cs` - Blazor host startup and database client wiring
- `Components/Layout` - shell layout, object explorer, tabs, and status UI
- `Components/Tabs` - table, query, procedure, storage, pipeline, form, and
  report tab surfaces
- `Components/Shared` - shared grid, editor, modal, context menu, and toast UI
- `Services` - tab manager, database holder, change notifications, modal, toast,
  and theme services
- `Helpers` - SQL formatting, highlighting, query paging, and query-designer SQL
  helpers

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.Admin.Forms`
- `CSharpDB.Admin.Reports`
- ASP.NET Core Razor Components

## Useful Commands

```powershell
dotnet build src/CSharpDB.Admin/CSharpDB.Admin.csproj
dotnet test tests/CSharpDB.Admin.Forms.Tests/CSharpDB.Admin.Forms.Tests.csproj
dotnet test tests/CSharpDB.Admin.Reports.Tests/CSharpDB.Admin.Reports.Tests.csproj
```

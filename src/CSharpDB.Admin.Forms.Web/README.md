# CSharpDB.Admin.Forms.Web

Blazor Server runtime host for stored CSharpDB forms.

`CSharpDB.Admin.Forms.Web` is an application host, not a NuGet package. It
loads form definitions from a target CSharpDB database and runs them through
the shared `DataEntry` runtime from `CSharpDB.Admin.Forms` without exposing the
form designer.

## What This Project Is For

Use this project when you want to:

- run stored forms as a focused data-entry web app
- point a simple web host at a database that already contains form metadata
- give operators a runnable form surface without the full Admin studio
- reuse the same stored forms in multiple hosts

Use `CSharpDB.Admin` when users also need schema browsing, query tabs,
procedures, pipelines, report design, and form design mode.

## What This Project Provides

- form list at `/`
- runnable form routes at `/forms/{formId}`
- shared form runtime from `CSharpDB.Admin.Forms`
- record create, update, delete, search, paging, navigation, child grids, and
  child tabs
- runtime-only host behavior with no visible `Edit Form` designer action

## Runtime Model

The host registers one `ICSharpDbClient` from configuration and then adds the
standard `AddCSharpDbAdminForms()` service set.

At runtime:

1. the host opens the configured database through `CSharpDbClient`
2. `IFormRepository` reads stored form definitions from the target database
3. the root page lists available forms
4. `/forms/{formId}` runs the selected form through `Pages/DataEntry.razor`

The host does not create forms. The target database must already contain them.

For example, the Fulfillment Hub sample seeds forms into:

```text
samples/fulfillment-hub/bin/Debug/net10.0/fulfillment-hub-demo.db
```

## Configuration

The host supports the same broad connection shapes as the other CSharpDB hosts:

- direct local database access through `CSharpDB:DataSource`
- direct access through `ConnectionStrings:CSharpDB`
- remote access through `CSharpDB:Transport` plus `CSharpDB:Endpoint`

Default `appsettings.json`:

```json
{
  "CSharpDB": {
    "Transport": "Direct",
    "DataSource": "csharpdb.db",
    "HostDatabase": {
      "OpenMode": "HybridIncrementalDurable",
      "UseWriteOptimizedPreset": true,
      "HotTableNames": [],
      "HotCollectionNames": []
    }
  }
}
```

In direct mode, the host uses the same direct/hybrid database option builder
pattern as Admin, including the write-optimized preset by default.

## Running Locally

Default run:

```powershell
dotnet run --project src/CSharpDB.Admin.Forms.Web/CSharpDB.Admin.Forms.Web.csproj
```

Run against a specific database:

```powershell
dotnet run --project src/CSharpDB.Admin.Forms.Web/CSharpDB.Admin.Forms.Web.csproj -- --CSharpDB:DataSource=C:\data\forms.db
```

Run against the Fulfillment Hub sample database on an explicit local URL:

```powershell
dotnet run --project src/CSharpDB.Admin.Forms.Web/CSharpDB.Admin.Forms.Web.csproj -- --urls http://127.0.0.1:5095 --CSharpDB:DataSource=C:\Users\maxim\source\Code\CSharpDB\samples\fulfillment-hub\bin\Debug\net10.0\fulfillment-hub-demo.db
```

After startup:

- `/` lists available stored forms
- `/forms/orders-workbench` opens the Fulfillment Hub order runtime

## Routes

| Route | Purpose |
| --- | --- |
| `/` | Lists all stored forms in the configured database. |
| `/forms/{formId}` | Runs one stored form in runtime-only mode. |

## Project Layout

- `Program.cs` - host startup and `ICSharpDbClient` wiring
- `Configuration/FormsHostClientOptionsBuilder.cs` - configuration binding and
  client option construction
- `Components/Pages/Home.razor` - root list of stored forms
- `Components/Pages/FormRuntime.razor` - runtime route wrapper around
  `DataEntry`
- `wwwroot/css/app.css` - small host-specific shell styling
- `wwwroot/js/forms-host.js` - runtime pane resize interop used by `DataEntry`

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.Admin.Forms`
- ASP.NET Core Razor Components

## Useful Commands

```powershell
dotnet build src/CSharpDB.Admin.Forms.Web/CSharpDB.Admin.Forms.Web.csproj
dotnet run --project src/CSharpDB.Admin.Forms.Web/CSharpDB.Admin.Forms.Web.csproj
```

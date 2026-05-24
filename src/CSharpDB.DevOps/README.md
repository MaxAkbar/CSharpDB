# CSharpDB.DevOps

Shared database DevOps services for CSharpDB schema compare, data compare,
drift reporting, and preview script generation.

`CSharpDB.DevOps` is the implementation layer used by the CLI and Admin
Compare / Deploy workspace. It keeps compare and script behavior in one place
so command-line, embedded, and Admin workflows produce the same reports and SQL
previews.

## Overview

The project compares database-like targets without introducing a new storage
format or public transport. Targets are loaded through existing CSharpDB client,
engine, and table archive APIs, then normalized into report models that can be
shown in Admin, serialized by the CLI, or rendered as SQL preview scripts.

Current target types:

- CSharpDB databases through `ICSharpDbClient`
- Direct engine-backed clients, with richer metadata access when available
- `.csdbtable` table archives for read-only schema and data compare inputs

Current report types:

- `SchemaDiffReport` for schema differences
- `DataDiffReport` for keyed row differences
- `DriftReport` for combined schema and optional data drift

## Main Components

### Schema Compare

`SchemaComparisonService` compares two `ISchemaCompareTarget` instances or two
already loaded `SchemaSnapshot` instances.

It currently compares:

- Tables
- Columns
- Foreign keys
- Indexes
- Views
- Triggers
- Stored procedure catalog entries

Schema changes are reported as `Added`, `Removed`, or `Changed`. Destructive
target-side removals are flagged with warnings instead of silently becoming
executable SQL.

### Schema Script Rendering

`SchemaScriptRenderer` renders two categories of SQL:

- Deployment preview scripts from a `SchemaDiffReport`
- Snapshot script-out output from a `SchemaSnapshot`

Snapshot script-out supports:

- Whole database scripts
- Selected table, view, index, trigger, or procedure scripts
- Table-related options for indexes, triggers, related views, and related
  procedures

Stored procedure catalog entries are emitted as commented metadata in V1 because
the engine does not yet expose executable procedure DDL.

### Data Compare

`DataComparisonService` compares rows for one table using stable key columns.
If no key columns are supplied, the service uses the table primary key when one
is available.

The report includes:

- Source row count
- Target row count
- Source-only rows
- Target-only rows
- Changed rows
- Preview rows capped by `MaxPreviewRows`
- Changed column names for modified rows

`RenderSyncScript` emits a preview SQL script with `INSERT`, `UPDATE`, and
`DELETE` statements. The script is intended for review and guarded execution,
not silent deployment.

### Drift Reports

`DriftReportService` combines schema comparison with optional table-level data
comparison. It is used for workflows that need to answer whether a current
database has drifted away from a known baseline.

## Usage

### Compare Schema

```csharp
using CSharpDB.DevOps;

var source = new ClientSchemaCompareTarget(sourceClient, "source");
var target = new ClientSchemaCompareTarget(targetClient, "target");

SchemaDiffReport report = await new SchemaComparisonService()
    .CompareAsync(source, target, ct);

string previewSql = SchemaScriptRenderer.RenderDeployScript(report);
```

### Script A Selected Table

```csharp
using CSharpDB.DevOps;

var target = new ClientSchemaCompareTarget(client, "current");
SchemaSnapshot snapshot = await target.LoadSchemaAsync(ct);

string tableScript = SchemaScriptRenderer.RenderSnapshotScript(
    snapshot,
    new SchemaScriptOptions
    {
        Scope = SchemaScriptScope.UserObjects,
        ObjectKind = SchemaObjectKind.Table,
        ObjectName = "customers",
        IncludeIndexes = true,
        IncludeTriggers = true,
        IncludeRelatedViews = false,
        IncludeRelatedProcedures = false,
    });
```

### Compare Data

```csharp
using CSharpDB.DevOps;

var source = new ClientDataCompareTarget(sourceClient, "source");
var target = new ClientDataCompareTarget(targetClient, "target");
var service = new DataComparisonService();

DataDiffReport report = await service.CompareAsync(
    source,
    target,
    new DataCompareOptions
    {
        TableName = "customers",
        KeyColumns = ["id"],
        MaxPreviewRows = 100,
    },
    ct);

string syncPreviewSql = service.RenderSyncScript(report);
```

### Create A Drift Report

```csharp
using CSharpDB.DevOps;

var baselineSchema = new ClientSchemaCompareTarget(baselineClient, "baseline");
var currentSchema = new ClientSchemaCompareTarget(currentClient, "current");
var baselineData = new ClientDataCompareTarget(baselineClient, "baseline");
var currentData = new ClientDataCompareTarget(currentClient, "current");

DriftReport report = await new DriftReportService().CreateAsync(
    baselineSchema,
    currentSchema,
    baselineData,
    currentData,
    new DriftReportOptions
    {
        DataTables =
        [
            new DataCompareOptions
            {
                TableName = "customers",
                KeyColumns = ["id"],
                MaxPreviewRows = 50,
            },
        ],
    },
    ct);
```

## Integration Points

This project is intentionally UI- and transport-neutral.

- `CSharpDB.Cli` uses it for `compare schema`, `compare data`, and `drift`.
- `CSharpDB.Admin` uses it for the Compare / Deploy workspace.
- `CSharpDB.Admin` also uses the snapshot script-out path for selected tables
  and object-level scripts.
- `CSharpDB.Client` exposes `InternalsVisibleTo` for this project so direct
  engine-backed clients can load richer metadata without changing public client
  contracts.

## Safety Model

- Services generate reports and preview SQL only.
- Script execution is owned by the caller.
- Table archive targets are read-only compare inputs.
- Destructive schema changes are commented or warned in preview output.
- Data sync scripts require stable key columns.
- Admin applies generated scripts only after explicit confirmation.

## Performance Notes

- Schema compare is proportional to the number of schema objects.
- Data compare loads source and target table rows into keyed dictionaries, so
  memory use is proportional to compared table size.
- Drift reports pay schema compare cost plus the cost of each requested data
  table comparison.
- Table archives are read from disk through the existing import/export archive
  path.

## Current Limits

- No automatic deployment orchestration lives in this project.
- No append or merge target abstraction is exposed for table archives.
- Data compare is table-at-a-time.
- Data sync script generation is preview-oriented and does not rewrite complex
  schema dependencies.
- Procedure script-out is metadata comments until executable procedure DDL is
  available.

## Tests

Focused coverage lives in `tests/CSharpDB.DevOps.Tests`.

```powershell
dotnet test .\tests\CSharpDB.DevOps.Tests\CSharpDB.DevOps.Tests.csproj -m:1
```

The full repository suite also covers the CLI and Admin consumers:

```powershell
dotnet test .\CSharpDB.slnx -m:1
```

# What's New

## version3.9.0

version3.9.0 adds SQL-first data hygiene workflows, session-scoped temporary
tables with explicit persistence, a new database DevOps toolkit, CSharpDB
Studio desktop packaging, and a consolidated website documentation set. It also
includes Admin usability fixes, English-only satellite resource output, NuGet
patch updates, and performance guardrail cleanup for the temp-table hot path.

### SQL Data Hygiene

- Added SQL support for finding duplicates, deduplicating rows, merging
  duplicate groups, creating audit-only validation rules, validating tables, and
  finding orphaned child rows.
- Added the hidden `__validation_rules` metadata table, created lazily when the
  first rule is added.
- Exposed validation metadata through `sys.validation_rules` and
  `sys_validation_rules` while keeping internal metadata hidden from normal
  table/object catalog listings.
- Kept all hygiene commands on normal SQL execution surfaces so embedded,
  ADO.NET, Admin, CLI, HTTP, and gRPC callers receive ordinary query-shaped
  results.
- Preserved the physical storage, page, and B-tree formats; no existing
  database migration is required.

### Temporary Tables

- Added session-scoped in-memory SQL temporary tables:
  ```sql
  CREATE TEMP TABLE name (...);
  CREATE TEMPORARY TABLE name (...);
  DROP TEMP TABLE name;
  PERSIST TEMP TABLE temp_name AS durable_name;
  ```
- Resolved unqualified names to temp tables first, then durable tables, so a
  temp table can intentionally shadow a durable table for the current session.
- Added `sys.temp_tables`, `sys_temp_tables`, `sys.temp_columns`, and
  `sys_temp_columns` for current-session temp metadata.
- Added explicit temp persistence that creates a new durable table and copies
  temp rows through the normal durable mutation path.
- Kept temp state out of durable catalogs, backups, checkpoints, and
  `SaveToFileAsync`; temp state is dropped when the owning connection/session
  is disposed unless explicitly persisted.
- Rejected unsupported temp-table features in v1 with clear errors, including
  foreign keys, triggers, secondary indexes, external tables, validation rules,
  and full-text indexes.

### Admin Data Hygiene Workspace

- Added a dedicated Data Hygiene tab in CSharpDB Admin with Duplicates,
  Validation, Orphans, and History/messages views.
- Added guided duplicate previews, `DEDUP`, and `MERGE DUPLICATES` flows using
  existing `ICSharpDbClient.ExecuteSqlAsync` SQL execution.
- Required preview plus danger confirmation before destructive dedup or merge
  actions.
- Added validation-rule creation and table validation against
  `sys.validation_rules`.
- Added declared and explicit relationship orphan scans.
- Wired Data Hygiene entry points into Object Explorer tools, table context
  menus, Data tab schema actions, Data Model relationship actions, the command
  palette, and Query tab SQL completions.
- Fixed the Data Hygiene result layout so large preview grids fill the workspace
  height and remain readable.

### Database DevOps Toolkit

- Added the `CSharpDB.DevOps` project with schema comparison, data comparison,
  drift report models, and SQL script rendering.
- Added a Compare / Deploy Admin workspace for schema and data comparison,
  source/target direction selection, and database-to-database comparison
  workflows.
- Added CLI DevOps commands backed by the shared DevOps project.
- Added selected-table scripting from Admin, including options for related
  objects such as indexes and triggers.
- Added DevOps tests and detailed project README files for the runtime and test
  projects.

### CSharpDB Studio Packaging

- Added `CSharpDB.Admin.Desktop`, a Windows desktop host for CSharpDB Studio.
- Added `CSharpDB.Admin.StorePackage` and a store package workflow for Windows
  packaging.
- Added desktop shell endpoints, packaging scripts, app assets, and in-app help
  content/screenshots for the Admin shell.

### Admin Usability And Schema Safety

- Improved Object Explorer behavior so database object groups start collapsed
  when Admin launches.
- Improved context-menu positioning so table menus can stay visible near the
  bottom of the side panel.
- Restricted table schema editing for existing columns when the requested change
  would conflict with index, relationship, or engine safety rules.
- Added integration coverage for indexed-column and referenced-column schema
  guardrails.

### Documentation And Website

- Consolidated the old markdown-heavy `docs` tree into the website HTML
  documentation under `www/docs`.
- Added or refreshed website docs for SQL data hygiene, temporary tables,
  database DevOps, configuration, trusted C# functions, validation rules,
  tutorials, Admin UI, Access-style macros, form extensibility, and performance
  guidance.
- Updated the root README comparison table with Microsoft Access context.
- Updated the SQL reference for the new hygiene and temp-table SQL commands.
- Removed duplicate markdown docs that now live as website pages.

### Build, Dependencies, And Performance

- Limited localized satellite resource output to English to reduce release
  folder size for the current English-only product surface.
- Updated NuGet package references to current patch versions.
- Fixed a temporary-table table-resolution hot-path regression by avoiding temp
  catalog lookups in normal durable sessions with no temp table context.
- Updated benchmark README, history, SQLite comparison, and manifest files with
  the May 31, 2026 performance guardrail close-out.

### Tests And Validation

- Added parser, tokenizer, classifier, engine, catalog, transport, and ADO.NET
  coverage for data hygiene and temporary table SQL.
- Added Admin unit and integration coverage for Data Hygiene, Compare / Deploy,
  command palette, tab management, SQL completions, context-menu behavior, and
  schema safety.
- Added DevOps service and CLI tests.
- Verified the temp-table focused engine tests:
  ```powershell
  dotnet test .\tests\CSharpDB.Tests\CSharpDB.Tests.csproj -m:1 --filter "FullyQualifiedName~TemporaryTable"
  ```
  Passed: 9 tests.
- Verified benchmark build and performance close-out:
  ```powershell
  dotnet build .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -c Release -m:1
  dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-transaction-diagnostics --repeat 3 --repro
  pwsh -NoProfile -ExecutionPolicy Bypass -File .\tests\CSharpDB.Benchmarks\scripts\Compare-Baseline.ps1 `
    -ReportPath .\tests\CSharpDB.Benchmarks\results\perf-guardrails-last.md `
    -ThresholdsPath .\tests\CSharpDB.Benchmarks\perf-thresholds.json `
    -CurrentMicroResultsDir .\tests\CSharpDB.Benchmarks\results\.tmp-current-micro-run
  ```
  Final guardrail result: `PASS=187, WARN=0, SKIP=0, FAIL=0`.

### Review Notes

- SQL validation rules are audit-only in this release; they are evaluated by
  `VALIDATE TABLE` and are not enforced during `INSERT` or `UPDATE`.
- Temp tables are session-scoped and in-memory. Remote HTTP/gRPC workflows
  should use transaction sessions for temp-table commands.
- `PERSIST TEMP TABLE` creates a new durable table in this release; appending or
  merging temp rows into an existing durable table can be added later.
- Data Hygiene, temporary tables, and DevOps tooling use existing SQL execution
  and mutation paths; no new public transport API is required.
- The published benchmark scorecard remains the May 6, 2026 release-core
  snapshot. The May 31, 2026 guardrail close-out is clean for current code after
  the temp-table hot-path fix.

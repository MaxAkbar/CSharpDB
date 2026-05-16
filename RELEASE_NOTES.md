# What's New

## version3.8.0

version3.8.0 adds native table archives, read-only external tables, Admin
Import / Export workflows, and a SQL Server-style Data Model diagram surface.
It also captures the next planning tracks for writable external tables, data
hygiene, developer experience, advanced differentiators, and database DevOps
tooling.

### Native Table Archives And Import / Export

- Added the shared `CSharpDB.ImportExport` runtime project and a dedicated
  `CSharpDB.Admin.ImportExport` Admin module.
- Added native `.csdbtable` archives using the `CSDBTBL3` seekable format with
  JSON schema/manifest sections, length-prefixed encoded rows, row counts,
  source table metadata, created timestamps, and optional integer primary-key
  archive indexes.
- Added direct table snapshot export through the engine transport path so large
  exports stream from a read snapshot instead of paging through the UI client.
- Added Admin Import / Export modes for table export, external table
  registration, and table restore.
- Added export destinations for browser download and server/database-local
  paths, including one-time server-side download packages so large archives do
  not need to be loaded into JavaScript memory.
- Added progress reporting and cancellation for long-running export and
  registration operations.
- Added `.csdbtable` to the ignore list so local table archive outputs do not
  get picked up by source control.

### External Tables

- Added SQL support for:
  ```sql
  CREATE EXTERNAL TABLE archived_customers FROM 'exports/customers.csdbtable';
  DROP EXTERNAL TABLE archived_customers;
  ```
- Stored external table registrations in the internal `__external_tables`
  metadata table and exposed read-only metadata through `sys.external_tables`.
- Resolved relative external archive paths from the database file directory.
- Made external `.csdbtable` tables usable in normal `SELECT` queries,
  projections, filters, ordering, and joins.
- Added external table scan, external index nested-loop join, and fast integer
  primary-key lookup paths over indexed archives.
- Kept external tables read-only in this release; writes, index creation, ALTER
  operations, and trigger targets are rejected against external registrations.
- Added external tables to Admin Object Explorer and Query tab system-catalog
  discovery.

### Data Model Diagrams

- Added a new Admin Data Model tab that visualizes user tables and external
  tables on an ERD-style canvas.
- Reused the Query Designer canvas behavior for draggable schema nodes,
  relationships, and zoom while keeping query-specific execution, filters, and
  grid state separate.
- Added Object Explorer, command palette, table context-menu, and external
  table context-menu entry points for the Data Model tab.
- Added saved diagram persistence through the internal
  `__data_model_diagrams` table instead of saved-query layout records.
- Exposed diagram metadata through `sys.diagrams`, including source/table and
  pending-operation counts, while hiding the internal table from normal table
  lists.
- Added a default global diagram so table placement, zoom, and source
  membership are remembered even before the user manually names a diagram.
- Added named diagram save/load/delete, table add/remove membership, node
  placement persistence, collapsed state, zoom, and stale/missing source
  warnings.
- Added preview-first staged schema operations for new table, drop table,
  rename table, add/drop/rename column, and add/drop relationship workflows.
  External tables remain display-only.

### Planning Docs And Website

- Added planning documents for writable external tables, the Data Hygiene
  Engine, the Developer Experience adoption track, Advanced Differentiators,
  and the Database DevOps Toolkit.
- Refreshed the roadmap to point at the new planning documents and to mark the
  user-defined functions/commands work as completed with sandbox opt-out
  coverage.
- Added the native table archives blog post and refreshed the website changelog,
  roadmap pages, blog index, and sitemap.

### Tests And Validation

- Added archive round-trip tests for schema, values, blobs, empty tables, and
  large table archive behavior.
- Added parser, engine, transport, and system-catalog coverage for external
  table registration, querying, joining, filtering, ordering, read-only
  enforcement, and metadata.
- Added Data Model graph and diagram service tests for placement persistence,
  saved membership, pending operations, hidden internal tables, and
  `sys.diagrams`.
- Added Admin tab manager and report-source tests for the new Data Model tab and
  removal of the old data-model saved-query layout path.
- Focused validation run for this release note:
  - `dotnet test tests\CSharpDB.Tests\CSharpDB.Tests.csproj --no-restore --filter "TableArchiveTests|ExternalTableTests|ParserTests|EngineTransportClientTests|DataModelGraphBuilderTests|DataModelDiagramServiceTests|SystemCatalogTests"`
    - Passed: 170 tests.
  - `dotnet test tests\CSharpDB.Admin.Reports.Tests\CSharpDB.Admin.Reports.Tests.csproj --no-restore --filter DbReportSourceProviderTests`
    - Passed: 4 tests.
  - `dotnet test tests\CSharpDB.Admin.Forms.Tests\CSharpDB.Admin.Forms.Tests.csproj --no-restore --filter TabManagerServiceTests`
    - Passed: 17 tests.
  - `dotnet build src\CSharpDB.Admin\CSharpDB.Admin.csproj`
  - `git diff --check`
  - Browser smoke test for opening Data Model and the New Table staging panel.

### Review Notes

- `.csdbtable` external tables are intentionally read-only in this release.
  Writable external tables are planned separately around an opt-in mutable
  `.csdbx` file.
- The table archive format is native and seekable, not a ZIP of JSON rows.
- The Data Model tab stages schema edits and previews SQL before applying
  changes; diagram membership and placement save independently.
- Drop-table actions still use the existing engine schema rules, so tables with
  foreign-key-owned support indexes may need relationship cleanup first.
- An initial parallel validation attempt hit a transient compiler output lock;
  the affected tests were rerun sequentially after shutting down the build
  server and passed.

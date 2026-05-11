# Database DevOps Toolkit Plan

This document captures the planned design for a first-party CSharpDB Database
DevOps Toolkit. The first implementation should focus on compare and deploy
workflows that help developers and operators understand differences between
databases, archives, and environments before applying changes.

## Summary And Goals

The toolkit should make database change management a normal CSharpDB workflow
instead of an external script collection. V1 should help users answer:

- What schema changed between two databases?
- What rows differ between a live table and another database or archive?
- Has a database drifted from a known baseline?
- What SQL would synchronize the target with the source?
- Which changes are destructive and need explicit review?

The goal is a scriptable CLI and visual Admin experience backed by the same
shared comparison services.

## Current Foundation

CSharpDB already has the primitives needed for a compare/deploy toolkit:

- `ICSharpDbClient` exposes schema, table, index, view, trigger, procedure,
  data, maintenance, backup, and inspection operations across direct and remote
  transports.
- Client backup and restore can create database-level snapshots.
- Native `.csdbtable` archives can export table schema and rows with manifest
  metadata.
- Admin already has object explorer, query tabs, storage/maintenance tooling,
  Import / Export, and table browsing.
- The `csharpdb` CLI already has interactive SQL, maintenance, inspection, ETL,
  backup, restore, and script execution commands.

The toolkit should build on those surfaces instead of creating separate
database access paths.

## V1 Toolkit Scope

V1 is focused on compare/deploy:

- Schema compare.
- Data compare.
- Drift reports.
- Generated schema deployment scripts.
- Generated data sync scripts.
- CLI commands for automation and CI.
- Admin Compare / Deploy workflow for visual review.

V1 should not ship masking, cloning, monitoring, SQL linting, dependency graphs,
or SQL unit testing as finished features. Those remain future toolkit phases.

## Compare Targets

The first implementation should support these target types:

| Target | Purpose |
| --- | --- |
| Live CSharpDB database | Compare an active database through `ICSharpDbClient`. |
| Backup database | Open or restore a database backup as a comparison target. |
| `.csdbtable` table archive | Compare a live table to an exported table snapshot. |
| SQL script target | Future source-control target for schema scripts and migrations. |

Target adapters should normalize schema and row access behind shared toolkit
interfaces so CLI and Admin use the same comparison behavior.

## Schema Compare

Schema compare should identify structural differences between source and target
schemas.

V1 should cover:

- Added, removed, and changed tables.
- Added, removed, and changed columns.
- Column type, nullability, identity, primary-key, collation, and foreign-key
  differences where metadata is available.
- Added, removed, and changed indexes.
- Added, removed, and changed views.
- Added, removed, and changed triggers.
- Added, removed, and changed procedures.

The output should group changes by object type and include warnings for
destructive changes such as dropped tables, dropped columns, type changes, and
nullable-to-not-null changes.

## Data Compare

Data compare should identify row-level differences for selected tables.

V1 should report:

- Rows present in source but missing in target.
- Rows present in target but missing in source.
- Rows present in both with changed column values.
- Rows that cannot be matched because no stable key is available.

Default key behavior:

- Use the table primary key when one exists.
- Require explicit `--key <columns>` when no primary key or stable key exists.
- Support composite keys when explicitly supplied.
- Treat `NULL`, text collation, and blobs consistently with CSharpDB value
  comparison semantics.

The comparison should stream rows where practical so large tables do not require
loading both sides fully into memory.

## Drift Reports

Drift reports compare a database against a known baseline and summarize whether
the current database still matches the expected state.

V1 baseline sources:

- Backup database.
- `.csdbtable` archive or archive manifest for table-level drift.
- Future SQL script/source-control target.

The report should include:

- Schema drift summary.
- Optional data drift summary for selected tables.
- Destructive or risky differences.
- Machine-readable JSON output for CI.
- Human-readable CLI and Admin summaries.

## Deployment Script Generation

Generated scripts should be preview-first. The toolkit should never silently
apply destructive changes.

Schema deployment scripts should:

- Use dependency-aware ordering where metadata allows.
- Create missing objects before dependent objects.
- Warn for destructive changes.
- Avoid pretending every destructive schema change is automatically safe.

Data sync scripts should:

- Use normal `INSERT`, `UPDATE`, and `DELETE` statements.
- Escape values correctly, including text, blobs, and nulls.
- Use primary key or explicit key predicates.
- Refuse script generation when no stable key exists.

CLI execution of generated scripts should require an explicit apply option in a
future runtime implementation. Admin execution should require a confirmation
step after preview.

## CLI And Admin Workflows

CLI shapes:

```powershell
csharpdb compare schema <source> <target> [--json] [--script-out <file>]
csharpdb compare data <source> <target> --table <name> [--key <columns>] [--json] [--script-out <file>]
csharpdb drift <dbfile> --baseline <archive-or-manifest> [--json]
```

Admin workflow:

- Add a `Compare / Deploy` entry under Tools or Object Explorer.
- Let users pick source and target from live database, backup, or archive
  targets.
- Show a schema diff view grouped by object type.
- Show a data diff view for selected tables with insert, update, and delete
  counts plus row previews.
- Show a warnings panel for destructive or risky changes.
- Show generated deployment script previews.
- Require explicit confirmation before executing generated scripts.

## Future Toolkit Phases

After compare/deploy, the toolkit can expand into:

- Data masking and anonymization.
- Lightweight clones for development and testing.
- Database monitoring and health history.
- SQL formatting and linting.
- Dependency and impact analysis for table/column changes.
- SQL-level unit tests.
- Source-control schema baselines and migration review.
- Single-row or table restore from archives.

These should reuse the same target adapters, diff models, archive readers, and
Admin/CLI surfaces where possible.

## Non-Goals

- No masking or anonymization in V1.
- No lightweight clone feature in V1.
- No production monitoring dashboard in V1.
- No SQL formatter or linter in V1.
- No dependency graph or impact analyzer in V1.
- No SQL unit testing framework in V1.
- No SQL script/source-control target required for V1.
- No silent execution of destructive generated scripts.

## Phased Implementation Plan

Phase 1: shared compare foundation.

- Add shared toolkit models and target adapters.
- Implement live database and `.csdbtable` archive target reads.
- Implement schema diff output for tables, columns, indexes, views, triggers,
  and procedures.
- Add JSON result contracts for CLI and Admin.

Phase 2: schema deploy scripts.

- Generate schema deployment script previews from schema diffs.
- Add destructive-change warnings.
- Add CLI schema compare command with JSON and script output.
- Add Admin schema diff preview.

Phase 3: data compare and sync scripts.

- Implement keyed row comparison for selected tables.
- Generate data sync script previews.
- Add CLI data compare command.
- Add Admin data diff preview.

Phase 4: drift reports.

- Add baseline comparison against backups and table archives.
- Add CLI drift command.
- Add Admin drift summary.
- Add CI-oriented JSON output and failure codes.

## Future Test Plan

- Schema diff tests for tables, columns, indexes, views, triggers, and
  procedures.
- Data diff tests for inserted, deleted, updated, null, blob, composite-key,
  missing-key, and empty-table cases.
- Archive target tests comparing live table data to `.csdbtable` exports.
- Script generation tests for ordering, escaping, destructive warnings, and
  invalid sync cases.
- CLI tests for schema compare, data compare, drift, JSON output, script output,
  and validation failures.
- Admin service tests for target loading, diff preview, script preview, and
  destructive execution confirmation.


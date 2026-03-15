# Backup, Export/Import, and Storage Maintenance Plan

> Note: the canonical storage diagnostics spec now lives in [../storage-inspector.md](../storage-inspector.md).  
> This document now focuses on the remaining backup/restore and logical mobility roadmap work that sits above the existing storage engine.

This document outlines the current state and remaining plan for:

- physical backup/restore flows built on committed-snapshot APIs
- logical export/import flows
- optional later reclaim/repair helpers beyond the maintenance tooling that already exists

The immediate goal is to land backup/restore on top of the current engine/client surface so that we do not need to modify pager, WAL, or B+tree internals for the first version.

---

## Recommended Scope

Backup/restore should be implemented as a top-layer workflow, not as new internal storage-engine behavior.

Use the surfaces that already exist:

- `Database.SaveToFileAsync(...)` and `CSharpDbConnection.SaveToFileAsync(...)` for consistent committed snapshot copies
- `Database.LoadIntoMemoryAsync(...)` for restore validation and file+WAL recovery into memory
- existing `inspect`, `inspect-page`, `check-wal`, and `check-indexes` commands for preflight/manifest validation
- existing maintenance report, `REINDEX`, and `VACUUM` flows when repair or deterministic reclaim is required

For the first backup/restore pass, avoid:

- page-level restore into an open database
- new pager or WAL file-format work
- internal B+tree repair logic beyond the maintenance operations that already ship

That keeps backup/restore isolated from storage-engine implementation changes.

---

## Already Implemented

The following pieces are already shipped and should be treated as foundation, not future work:

| Area | Current support | Status |
|------|-----------------|--------|
| **Storage diagnostics** | `inspect`, `inspect-page`, `check-wal`, `check-indexes` in CLI/API/native surfaces | Done |
| **Maintenance reporting** | Database size, page counts, freelist/tail-freelist, and fragmentation reporting | Done |
| **Index rebuild** | `REINDEX` for database, table, index, and collection scopes | Done |
| **Full rewrite compaction** | `VACUUM` with temp-file rewrite and backup/rollback guard path | Done |
| **Committed snapshot persistence** | `SaveToFileAsync(...)` from in-memory database/session surfaces | Done |
| **Disk-to-memory restore path** | `LoadIntoMemoryAsync(...)` loads `.db` plus committed WAL state into memory | Done |
| **Physical backup/restore** | `ICSharpDbClient.BackupAsync(...)` / `RestoreAsync(...)` with CLI, Admin, API, direct client, HTTP transport, and gRPC transport coverage | Done |
| **Logical metadata surfaces** | Tables, indexes, views, triggers, procedures, and saved queries are discoverable/copiable | Done |

---

## Current Engine Constraints

Based on current implementation:

- A freelist exists in the pager allocation path, but tail-only shrink is not exposed; deterministic reclaim currently comes from full rewrite `VACUUM`.
- Checkpoint copies committed WAL pages into the DB file but does not shrink file length on its own.
- WAL recovery/checkpoint invariants are explicit and can be validated offline.
- Metadata for tables, indexes, views, and triggers is available for logical export.
- SQL parameter binding currently rejects `byte[]`, so SQL-only BLOB round-trip is not yet complete.
- There is not yet a first-class `.export` or `.import` REPL/CLI surface.

---

## Proposed CLI Commands

### Already Shipped

- `csharpdb inspect <dbfile> [--json]`
- `csharpdb inspect-page <dbfile> <pageId> [--json] [--hex]`
- `csharpdb check-indexes <dbfile> [--index <name>] [--sample <n>]`
- `csharpdb check-wal <dbfile>`
- `csharpdb maintenance-report <dbfile> [--json]`
- `csharpdb reindex <dbfile> [--all|--table <name>|--index <name>] [--json]`
- `csharpdb vacuum <dbfile> [--json]`
- `.backup <dest.db>`
- `.backup <dest.db> --with-manifest`
- `.restore <src.db>`
- `.restore <src.db> --validate-only`

The Admin Storage tab and CLI now call the same `ICSharpDbClient` backup/restore surface. When the connection is remote, backup and restore paths are resolved on the connected host.

Behavior:

- backup should be a thin wrapper over the existing committed-snapshot save/copy path
- restore is an offline replace workflow that first normalizes the source through `LoadIntoMemoryAsync(...)` + `SaveToFileAsync(...)`
- optional validation should reuse `inspect` / `check-wal`
- backup manifests should be derived from existing inspection/report data rather than new low-level storage code

### Export / Import

- `.export sql <file.sql> [--schema-only|--data-only]`
- `.export json <file.json> [--schema-only|--data-only]`
- `.import sql <file.sql>` (can reuse `.read` statement execution path)
- `.import json <file.json> [--truncate]`

### Later Reclaim Helpers

- `csharpdb reclaim <dbfile> --dry-run`
- `csharpdb tail-trim <dbfile> --execute`

---

## Rollout Plan

### Phase 0: Existing Foundation

1. `inspect` / `inspect-page`
2. `check-indexes`
3. `check-wal`
4. maintenance report
5. `REINDEX`
6. `VACUUM`
7. committed snapshot save/load APIs

### Phase 1: Logical Export / Import

1. Implement `.export sql` / `.import sql`
2. Implement `.export json` / `.import json`

### Phase 2: Optional Reclaim Helpers

1. Implement `reclaim --dry-run`
2. Add tail-only shrink (`tail-trim`) for contiguous free high pages

---

## Safety Requirements

Write-mode operations (`backup`, `restore`, `tail-trim`, `import`) should enforce:

1. explicit write lock / single-writer guarantee
2. snapshot-reader awareness (avoid invalidating active readers)
3. checkpoint and WAL consistency prechecks
4. fail-fast behavior with no partial metadata updates
5. optional `--dry-run` or `--execute` confirmation flags where destructive

For `restore`, start with an offline requirement: the target database should not already be open for writes.

---

## BLOB Handling Policy

Until native SQL BLOB literal/binder support is added:

- SQL export/import should warn or block for tables containing BLOB data.
- JSON export/import should encode BLOB values as Base64 and decode on import.

This keeps logical backup/restore paths reliable while BLOB SQL round-trip remains incomplete.

---

## Suggested Implementation Order

1. `.export/.import` (SQL and JSON)
2. `reclaim --dry-run`
3. `tail-trim`

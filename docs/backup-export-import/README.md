# Backup, Export/Import, and Storage Maintenance Plan

This document outlines the proposed tooling plan for:

- database file structure checks
- index consistency checks
- WAL checks
- backup flows
- logical export/import flows
- page reclaim and resize (later phase)

The goal is to ship low-risk read-only diagnostics first, then controlled repair and shrink operations.

---

## Current Engine Constraints

Based on current implementation:

- A freelist exists in the pager allocation path, but full page reclamation is not yet wired through all drop/delete flows.
- Checkpoint copies committed WAL pages into the DB file but does not shrink file length.
- WAL recovery/checkpoint invariants are explicit and can be validated offline.
- Metadata for tables, indexes, views, and triggers is available for logical export.
- SQL parameter binding currently rejects `byte[]`, so SQL-only BLOB round-trip is not yet complete.

---

## Proposed CLI Commands

### Diagnostics (Read-Only First)

- `csharpdb inspect <dbfile> [--json]`
- `csharpdb check-indexes <dbfile> [--index <name>] [--sample <n>]`
- `csharpdb check-wal <dbfile>`
- `csharpdb reclaim <dbfile> --dry-run`

### Backup

- `.backup <dest.db>`
- `.backup <dest.db> --with-wal`

Behavior:

- force a checkpoint before copy when safe
- create a consistent snapshot copy
- optionally include `.wal`
- emit a small manifest (timestamp, page count, change counter, checksums)

### Export/Import

- `.export sql <file.sql> [--schema-only|--data-only]`
- `.export json <file.json> [--schema-only|--data-only]`
- `.import sql <file.sql>` (can reuse `.read` statement execution path)
- `.import json <file.json> [--truncate]`

---

## Rollout Plan

### Phase 1: Read-Only Validation

1. Implement `inspect`
2. Implement `check-indexes`
3. Implement `check-wal`
4. Implement `reclaim --dry-run`

### Phase 2: Safe Data Mobility

1. Implement `.backup` with checkpoint preflight and manifest
2. Implement `.export sql` / `.import sql`
3. Implement `.export json` / `.import json`

### Phase 3: Repair and Space Reclaim

1. Add index rebuild helper (`rebuild-index <name|all>`)
2. Add tail-only shrink (`tail-trim`) for contiguous free high pages
3. Add full rewrite compaction (`vacuum`) for deterministic reclaim

---

## Safety Requirements

Write-mode operations (`backup`, `rebuild-index`, `tail-trim`, `vacuum`, `import`) should enforce:

1. explicit write lock / single-writer guarantee
2. snapshot-reader awareness (avoid invalidating active readers)
3. checkpoint and WAL consistency prechecks
4. fail-fast behavior with no partial metadata updates
5. optional `--dry-run` or `--execute` confirmation flags where destructive

---

## BLOB Handling Policy

Until native SQL BLOB literal/binder support is added:

- SQL export/import should warn or block for tables containing BLOB data.
- JSON export/import should encode BLOB values as Base64 and decode on import.

This keeps logical backup/restore paths reliable while BLOB SQL round-trip remains incomplete.

---

## Suggested Implementation Order

1. `inspect`
2. `check-indexes`
3. `check-wal`
4. `.backup`
5. `.export/.import` (SQL and JSON)
6. `reclaim --dry-run`
7. `tail-trim`
8. `vacuum`

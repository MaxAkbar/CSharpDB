# Async I/O Batching Follow-Up

This note captures the remaining work behind the roadmap item currently marked
`In Progress`.

## Current Shipped State

The hot storage write path already has the main batching pieces in place:

- WAL commits can append dirty pages through `AppendFramesAndCommitAsync(...)`.
- Repeated `AppendFrameAsync(...)` calls inside a transaction are staged and emitted as chunked WAL writes during `CommitAsync(...)`.
- Checkpoint copying batches contiguous page writes back into the main database file.
- `SaveToFileAsync(...)` and backup-style snapshot copies use `StorageDeviceCopyBatcher`.
- Vacuum and foreign-key migration rewrites share `BTreeCopyUtility` instead of each owning a separate row-copy loop.

## Remaining Work

The remaining work is an audit and measurement pass, not a missing core WAL batching primitive.

1. Audit non-hot rewrite/export paths:
   - Backup and restore: `DatabaseBackupCoordinator`, `Database.SaveToFileAsync(...)`, `Pager.SaveToFileAsync(...)`.
   - Vacuum: `DatabaseMaintenanceCoordinator.CopyDatabaseAsync(...)`.
   - Foreign-key migration rewrites: `DatabaseForeignKeyMigrationCoordinator.ApplyPlanAsync(...)`.
   - Storage diagnostics and inspectors: `WalInspector`, `InspectorEngine`, and large sequential read paths.
   - Admin/UI export helpers only if they prove relevant to large database-file movement.

2. Decide whether `BTreeCopyUtility` should stay a logical row-copy helper or grow batching behavior:
   - Current behavior is cursor-read plus per-row `InsertAsync(...)`.
   - Possible follow-up: add row/page-local batching, reusable buffers, or progress hooks.
   - Avoid direct B-tree page copying unless catalog, root-page, schema, freelist, row-count, and index invariants are explicitly preserved.

3. Add benchmark/diagnostic coverage before optimizing further:
   - Large backup snapshot.
   - Large restore staging.
   - Large vacuum rewrite.
   - Large foreign-key migration rewrite.
   - Inspector/diagnostic scans over large DB/WAL files.

4. Define completion criteria:
   - The audit identifies every large sequential file-copy and logical table-copy path.
   - Each path is classified as already batched, intentionally unbatched, or worth optimizing.
   - Any optimized path has before/after timings and no crash-recovery or integrity regression.

## Non-Goals

These are separate from this follow-up:

- Durable group commit policy changes.
- Hot single-row insert fan-in.
- Public histogram/planner inspection.
- Query row-batch execution kernel specialization.

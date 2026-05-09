# Async I/O Batching Follow-Up

This note captures the close-out audit for the roadmap item now marked `Done`
for the current phase. It does not claim that every possible maintenance or
diagnostic path has been optimized forever; it records which broad storage paths
are covered and where future work should be limited.

## Shipped State

The hot storage write path and the main large-copy paths have the batching
pieces needed for this phase:

- WAL commits can append dirty pages through `AppendFramesAndCommitAsync(...)`.
- Repeated `AppendFrameAsync(...)` calls inside a transaction are staged and emitted as chunked WAL writes during `CommitAsync(...)`.
- Checkpoint copying batches contiguous page writes back into the main database file.
- `SaveToFileAsync(...)` and backup-style snapshot copies use `StorageDeviceCopyBatcher`.
- Vacuum and foreign-key migration rewrites share `BTreeCopyUtility` instead of each owning a separate row-copy loop.

## Close-Out Classification

| Path | Classification | Notes |
|---|---|---|
| WAL commit frame writes | Already batched | Dirty pages are staged and emitted through chunked WAL writes before commit publication. |
| Checkpoint page copies | Already batched | Contiguous page writes are grouped when checkpointing back into the main database file. |
| `Database.SaveToFileAsync(...)` / `Pager.SaveToFileAsync(...)` | Already batched | Snapshot copies use `StorageDeviceCopyBatcher` for large sequential movement. |
| `DatabaseBackupCoordinator` backup and restore staging | Already batched | Backup/save and restore staging stay on the snapshot-copy path rather than bespoke small writes. |
| Vacuum rewrite | Intentionally logical/unbatched | Uses `BTreeCopyUtility` to preserve catalog, root-page, schema, freelist, row-count, and index invariants. |
| Foreign-key migration rewrite | Intentionally logical/unbatched | Keeps row-logical copying so FK metadata and index rebuild invariants stay explicit. |
| `DatabaseInspector` / `WalInspector` | Benchmarked diagnostic path | Kept outside the WAL hot path; future work can tune specialized scans if diagnostics become a bottleneck. |
| Admin/UI export helpers | Already covered or out of scope | File-copy style exports should use the existing snapshot-copy helpers; UI orchestration is not a storage batching primitive. |

## Benchmark Coverage

The diagnostic close-out suite is available through:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --async-io-closeout --repro
```

The suite covers large save/backup, restore staging, vacuum/FK logical rewrite,
checkpoint-adjacent maintenance data movement, and inspector/WAL scan paths. The
rows remain diagnostic rather than release-blocking because they are sensitive to
local storage and dataset size.

The May 5, 2026 close-out run is recorded in
`tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/async-io-closeout-20260505-204638.csv`.
It measured save/backup/restore as `52,762`, `8,136`, and `9,996` pages/sec,
vacuum logical rewrite at `3,365` pages/sec, FK migration rewrite at `42,749`
rows/sec, database inspector scan at `18,600` pages/sec, and WAL inspector scan
at `2,310` frames/sec over a live 20-frame WAL.

## Future Boundaries

Future work should stay narrow:

- Tune specialized diagnostic or maintenance scans only when benchmark data shows material small-read overhead.
- Keep `BTreeCopyUtility` row-logical unless a specific path can preserve catalog, root-page, schema, freelist, row-count, and index invariants without special-case risk.
- Do not treat async I/O batching as a durable group-commit or insert fan-in substitute.

## Non-Goals

These are separate from this follow-up:

- Durable group commit policy changes.
- Hot single-row insert fan-in.
- Public histogram/planner inspection.
- Query row-batch execution kernel specialization.

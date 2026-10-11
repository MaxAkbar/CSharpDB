# Performance checkpoint: October 6, 2026

This is a historical checkpoint for product commit `860d7ee6`, not qualification
of the current branch head. Subsequent v4.6.6 changes fixed checkpoint finalization
racing with write-conflict WAL reads and removed the recorded xUnit1069 warning.
The original results and evidence identities below remain unchanged. The added
finalization synchronization has not received a new performance qualification.
See the [current release notes](../../RELEASE_NOTES.md) and
[checkpoint coordination](../../src/CSharpDB.Storage/README.md#checkpoint-coordination).

The current performance pass is complete. Product code at `860d7ee6` retains
the adopted improvements and its 3,803-test correctness qualification. The
combined representative comparison passed its declared local performance
screen. The remaining investigated candidates are rejected or parked; the
latest B-tree diagnostic does not justify another product change.

This checkpoint collects the research history, WAL design/model/prototype,
and two production recovery regression cases. It introduces no product code,
WAL format, default-setting or release-policy change. The separate website
roadmap edits are outside this checkpoint.

## Adopted product state

The product anchor is `860d7ee6ee1e0e0d4596befdc98ed06a9abc22b6` on
`version.4.6.6`. The detailed sequence and individual acceptance results remain
in [benchmark history](../../tests/CSharpDB.Benchmarks/HISTORY.md).

| Area | Retained changes and commit anchors |
|---|---|
| Prepared readers | Prepared plan reuse (`ebf0257e`), catalog snapshot correctness (`0dc0a36f`), bounded prepared ranges (`fcdfc7ea`), bounded read-ahead and planner work (`b0de2b6f`, `9af89853`) |
| Scans and batches | Avoid write-only snapshot allocations (`f82ed0b6`), allocate scan batches on demand (`d55c5d98`), forward intact LIMIT batches and correct mixed traversal (`92716dfa`, `30c3a01c`), reuse successful prefetch cancellation state (`f94ce04f`) |
| Mutations and ownership | Primary-key mutation improvements with page ownership preserved (`3156badc`), remove per-row observer allocations (`4e59bebf`) |
| Insert setup | Reuse statement column ordinals (`19ffabe5`), skip redundant canonical assignment conversions (`860d7ee6`) |

The final Release qualification passed **3,478 engine and 325 ADO tests**, with
no failures or skips. The recorded build has one existing xUnit1069 warning in
`PooledMemoryMappedSnapshotTests.cs:14`. Qualification output and the eleven
product DLL hashes are retained in
`.tmp/assignment-coercion-fastpath-20261006/qualification.json`.
The checkpoint audit checks these identities and the retained test results;
it does not repeat the unchanged product suite or performance workloads.

## Combined local result

Exact frozen `0dc0a36f` and `860d7ee6` binaries ran common harnesses in balanced
serial order under normal desktop use. All **14 declared cases passed**, with
30,800 measured durable commits and 183,600 query/cycle operations. Checks
covered row values, checksums, durable reopen, reader disposal and frozen inputs.
There were no measured retries or sample exclusions.

| Representative case | Current versus baseline, both balanced pairs |
|---|---|
| 10,000-row insertion phase median | 18.49% / 18.10% lower |
| 10,000-row write throughput including final drain | 12.06% / 11.92% higher |
| Four-row prepared range, one-table catalog, P50 | 53.56% / 55.22% lower |
| 4,096-row prepared range, one-table catalog, P50 | 21.60% / 22.71% lower |
| Sequential four-row UPDATE/commit plus pooled read, P50 | 70.45% / 69.34% lower |

Full paired P50/P95/P99, allocations, maxima and guard results are in
`.tmp/combined-validation-20261006/analysis-r2/Findings.md` and `analysis.json`.
The one-row write median changed +0.23% / +0.67%; the 1,000-row write P99 changed
+16.23% / -10.38%. Passing the declared screen does not mean every metric improved.

These are local representative results, not release qualification or published
benchmark-table replacements. The baseline already includes prepared-plan reuse
and checkpoint optimization, so this comparison does not measure those earlier
changes. Read fixtures have 10,000 rows and 1/256-table catalogs; mapping is off.
The mixed case is sequential, not many-client concurrency. The baseline's older
blocked performance qualification and earlier larger/concurrent studies retain
their own outcomes.

## Closed and parked investigations

| Investigation | Outcome and reason |
|---|---|
| Dirty-page callback allocation | Rejected: allocation falls 1.41%, but median paired throughput including drain falls 0.44%. |
| Shared row-ID advancing update | Rejected: insertion median improvements of 0.71% / 3.62% miss the required 3% in both pairs. Existing synchronization remains. |
| Local row-ID cache single lookup | Rejected: +0.11% / -1.45% insertion median changes miss the same target. |
| Known-key right-edge hint | Rejected: 0.15% / 2.11% insertion median improvements miss the 3% target. |
| Mapping with default automatic checkpoints | Rejected as a default change: standalone range gains accompany mixed point/range/cycle regressions. Mapping stays opt-in. |
| Deferred snapshot mapping | Rejected: mixed ranges slow by 2.52-6.49%, with additional tail/allocation failures. |
| Controlled durable-flush cadence | Parked: the pacing screen fails its tail target and reduces throughput by about 79%. Flush-path attribution does not identify a device or scheduler cause. |
| Larger WAL compaction buffers and read prefetch | Rejected; buffer-size tuning is parked. The 256-frame variant misses its P99 target and exceeds allocation limits. |
| Deferred logical WAL retirement shortcut | Rejected as unsafe: the corrupted-prefix model loses a later committed page; the current implementation preserves it. |
| Mirrored committed-tail WAL prototype | Correctness reference only: 179 tests pass, but three flushes per commit produce 2.61-3.45 times the floor-only prototype's P99 and 66.2-68.6% lower throughput. No production adoption. |
| Common B-tree insertion attribution | Diagnostic complete; no candidate selected. Small intervals overlap timer calibration, and dirty-page/buffer ownership cannot safely be bypassed from these results. |

The chronological history preserves each study's original next-step wording.
This table records the final disposition after those follow-ups. Rejected
patches and failed setup attempts remain in their evidence directories.
The first ordinal screen was rejected before the corrected version was adopted;
the observer allocation change was adopted after a separate fixed confirmation.
Their earlier failed screens remain part of the history.

## WAL research retained for review

The [WAL design](wal-reclamation.md) describes the isolated fresh-file `CWA2`
format, its limitations, and direct build/run instructions. The
[boundary model](wal_boundary_model.py) passed 1,740 bounded cases; its ordered
flush assumptions do not establish real power-loss durability.

The two `CSharpDB.WalPrototype.*` projects remain outside the product solution.
Their retained 179-test run includes modeled fault images and 90 targeted child
process terminations with the OS running. The floor-only mode can fail to reopen
after a torn append; the opt-in committed-tail mode addresses that bounded
recovery problem but fails the cost screen. Neither mode qualifies production
integration, migration, many-reader behavior or storage-device flush guarantees.

The retained
[checkpoint regressions](../../tests/CSharpDB.Tests/WalCheckpointBatchTests.cs)
verify that discarding an invalid indexed or superseded copied prefix preserves
a later committed suffix after reopen. Both cases are included in the final
engine qualification.

## Evidence and resumption

The [evidence inventory](performance-checkpoint-2026-10-06.json) records product
identity, result hashes, prototype source hashes and historical study manifests.
At the original checkpoint, raw captures, frozen binaries and crash fixtures
remained local under ignored `.tmp/` directories. On October 6, 2026, a later
user-requested cleanup removed the bulky databases, traces, per-event captures
and captured binaries. Compact result tables, summaries, source records and
test logs remain available. The original manifests are historical receipts;
complete artifact re-verification is no longer available for the pruned bundles.
Deletion details are recorded in
`.tmp/cleanup-20261006/retained-data-deletion.json` and its per-file journal.
This Git checkpoint preserves source and conclusions and does not back up raw
artifacts or make them available in a fresh clone.
Historical `committed: false` receipts describe the end of each study and remain
unchanged by this later checkpoint.

Resume with a distinct workload or mechanism and a fixed correctness/performance
gate. Do not reopen rejected candidates by relaxing thresholds, retrying for a
better sample or inferring savings from instrumented elapsed shares. Any new
product candidate needs independent uninstrumented evidence before adoption.

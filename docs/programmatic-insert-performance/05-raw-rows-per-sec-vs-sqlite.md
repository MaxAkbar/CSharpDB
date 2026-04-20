# Plan 5: Raw Rows/Sec Vs SQLite

## Goal

Answer one question cleanly:

What steady-state durable insert gap remains when CSharpDB is compared against a
matched SQLite baseline?

This plan is about raw durable throughput. It is not the hot right-edge
conflict-salvage plan.

## Fixed Controls

- File-backed durable mode first.
- One stable four-column schema for the matched bulk rows:
  `id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT`.
- Reuse the Plan 1 winning CSharpDB path as the engine baseline.
- Keep one primary CSharpDB batch size at `1000`, with `10000` as the large
  follow-up row.
- SQLite comparison rows must use:
  - `journal_mode=WAL`
  - `synchronous=FULL`
  - explicit transaction batching
  - prepared statement reuse
  - batch sizes `1000` and `10000`

## In Scope

- CSharpDB raw rows/sec against matched SQLite durable bulk rows
- row-width sensitivity
- secondary-index maintenance cost
- monotonic versus random primary-key locality
- rows-per-commit and flush amortization
- benchmark-table hygiene so both engines are compared under named matched rows

## Out Of Scope

- hot right-edge split-fallback recovery bugs from Plan 4
- buffered or in-memory semantics as the primary comparison story
- multi-writer contention as the main narrative
- read benchmarks or mixed read/write workloads

## Current Evidence

- The matched SQLite baseline already exists in
  `tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs`.
- That harness now includes:
  - `SQLite_WalFull_Sql_PreparedBulk4Col_B1000_5s`
  - `SQLite_WalFull_Sql_PreparedBulk4Col_B10000_5s`
- Those rows already use the intended controls:
  - four-column schema
  - `journal_mode=wal`
  - `synchronous=full`
  - explicit transaction batching
  - prepared statement reuse
- The CSharpDB durable batching harness already contains the attribution rows
  needed for this plan:
  - `RowWidth_*`
  - `IndexSweep_*`
  - `KeySweep_*`
- The recent Plan 4 recovery work improved structural correctness, but the hot
  rows still stayed pinned near `commitsPerFlush = 1.00`. That makes the
  remaining SQLite gap more likely to be fast-path cost or flush amortization
  rather than pure recovery failure.
- The April 19, 2026 same-runner Plan 5 refresh now gives the first clean
  post-fix matched bulk table:
  - CSharpDB `InsertBatch B1000` median-of-3:
    `181,468 rows/sec`
  - CSharpDB `InsertBatch B10000` median-of-3:
    `617,228 rows/sec`
  - SQLite prepared bulk `B1000` median-of-3:
    `188,463 rows/sec`
  - SQLite prepared bulk `B10000` median-of-3:
    `528,011 rows/sec`
- That refresh shows the first Plan 5 speed win clearly:
  skipping logical conflict-key generation on the single-writer bulk path when
  no explicit conflict-tracked transaction is active.
- Interpretation after that fix:
  - CSharpDB is now about `96%` of SQLite at `B1000`
  - CSharpDB is now about `117%` of SQLite at `B10000`
  - the remaining small-batch gap is narrower and no longer points to durable
    flush cost as the primary explanation
- A later row-table amortization follow-up then removed one more source of
  per-row allocation churn on the bulk path: multi-row inserts now reuse a
  caller-owned encoded-record buffer instead of allocating a new encoded row
  payload on every insert. The April 19, 2026 reproducible rerun on that
  version landed at:
  - CSharpDB `InsertBatch B1000` median-of-3:
    `185,289 rows/sec`
  - CSharpDB `InsertBatch B10000` median-of-3:
    `707,265 rows/sec`
  - SQLite prepared bulk `B1000` median-of-3:
    `206,936 rows/sec`
  - SQLite prepared bulk `B10000` median-of-3:
    `580,289 rows/sec`
- Interpretation after the row-table amortization follow-up:
  - CSharpDB improved to about `89.5%` of SQLite at `B1000`
  - CSharpDB improved to about `121.9%` of SQLite at `B10000`
  - the win is real on the matched bulk path, but it is not uniform:
    `B10000` improved materially while `B1000` only moved modestly
  - that asymmetry is useful attribution: once durable flush cost is heavily
    amortized, per-row encoded-record allocation still mattered a lot; at
    `B1000`, the remaining gap is more likely another row-table/right-edge
    leaf-write cost than more planner/setup or duplicate-bucket work
- A later same-day row-table follow-up then removed one more piece of duplicate
  work from the matched bulk path: when the insert planner already knows the
  encoded row length, the record encoder now reuses that known length instead
  of recomputing it during the write. That version also kept the right-edge
  leaf-split shortcut in place on the monotonic PK row. The April 19, 2026
  reproducible rerun on that version landed at:
  - CSharpDB `InsertBatch B1000` median-of-3:
    `218,029 rows/sec`
  - CSharpDB `InsertBatch B10000` median-of-3:
    `820,459 rows/sec`
  - SQLite prepared bulk `B1000` median-of-3:
    `206,936 rows/sec`
  - SQLite prepared bulk `B10000` median-of-3:
    `580,289 rows/sec`
- Interpretation after the known-length encode follow-up:
  - CSharpDB is now about `105.4%` of SQLite at `B1000`
  - CSharpDB is now about `141.4%` of SQLite at `B10000`
  - the matched durable monotonic bulk row is now ahead of SQLite at both
    published Plan 5 batch sizes on this runner
  - that closes the original parity question for the primary row: more
    row-table/right-edge work is now margin work rather than required catch-up
- The later April 19, 2026 attribution sweep on the same branch put numbers on
  the remaining slopes:
  - row width at `InsertBatch B1000`:
    - baseline: `180,753 rows/sec`
    - medium: `156,587 rows/sec`
    - wide: `72,567 rows/sec`
  - key locality at `InsertBatch B1000`:
    - monotonic: `179,618 rows/sec`
    - random: `15,795 rows/sec`
- A targeted follow-up on that same branch also trimmed indexed insert overhead
  by caching resolved per-index insert plans and reusing exact-sized
  key-component buffers across rows. Same-runner reruns moved the indexed rows
  to:
  - `Idx1`: `142,636 rows/sec`
  - `Idx2`: `125,366 rows/sec`
  - `Idx4`: `74,059 rows/sec`
- A later reproducible duplicate-bucket follow-up added a validated append
  context for repeated same-key appendable hashed-index updates. On the same
  durable `B1000` shape it landed at:
  - `Idx0`: `184,051 rows/sec`
  - `Idx1`: `139,730 rows/sec`
  - `Idx2`: `126,445 rows/sec`
  - `Idx4`: `74,837 rows/sec`
- A deeper duplicate-bucket follow-up then changed the appendable hashed-index
  storage shape itself: new duplicate-heavy buckets keep immutable key
  reference state in the B-tree leaf and move mutable append state into the
  overflow chain's first page. The April 19, 2026 reproducible rerun on that
  version landed at:
  - `Idx0`: `213,127 rows/sec`
  - `Idx1`: `162,952 rows/sec`
  - `Idx2`: `149,058 rows/sec`
  - `Idx4`: `83,233 rows/sec`
- A follow-up batch-level append-state amortization pass then staged repeated
  external-chain appends across each safe multi-row insert statement and
  flushed the chain header once at statement end. The April 19, 2026
  reproducible rerun on that version landed at:
  - `Idx0`: `211,069 rows/sec`
  - `Idx1`: `158,190 rows/sec`
  - `Idx2`: `148,167 rows/sec`
  - `Idx4`: `86,357 rows/sec`
- A deeper duplicate-bucket storage follow-up then changed the external-chain
  payload format itself for sorted appendable buckets: instead of storing
  fixed-width `8`-byte row ids in overflow pages, new duplicate-heavy chains
  delta-pack monotonic row ids with varint encoding. The April 19, 2026
  reproducible rerun on that version landed at:
  - `Idx0`: `215,932 rows/sec`
  - `Idx1`: `157,617 rows/sec`
  - `Idx2`: `158,267 rows/sec`
  - `Idx4`: `96,235 rows/sec`
- A later composite-index locality follow-up then changed newly created
  multi-column SQL indexes whose trailing column is `INTEGER` to use that
  trailing integer as the physical B-tree key while still keeping the full
  normalized key tuple in the hashed payload for exact-match validation. The
  April 19, 2026 reproducible rerun on that version landed at:
  - composite-only `idx_bench_category_value` row:
    `77,250 rows/sec -> 93,174 rows/sec`
  - full `Idx4` row:
    `54,675 rows/sec -> 64,323 rows/sec`
  - the structural attribution on `idx_bench_category_value` also flipped from
    mostly non-right-edge B-tree maintenance to pure right-edge growth on the
    composite-only row:
    `nre=41,473/41,566 -> 0/26,726`
- A focused median-of-3 indexed rerun was then completed on April 20, 2026
  after two more secondary-index cuts:
  - composite `HashedTrailingInteger` payloads now omit the trailing integer
    from the stored hashed payload when that integer is already the physical
    B-tree key
  - direct-integer secondary index inserts now reuse a caller-owned `8`-byte
    row-id payload buffer instead of allocating a new one for each row
  - the latest reproducible artifacts are:
    - `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx1_Monotonic-20260420-002155-median-of-3.csv`
    - `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx2_Monotonic-20260420-002259-median-of-3.csv`
    - `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_IdxCompositeCategoryValue_Monotonic-20260420-002401-median-of-3.csv`
    - `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-IndexSweep_InsertBatch_B1000_Baseline_Idx4_Monotonic-20260420-002502-median-of-3.csv`
  - that rerun landed at:
    - `Idx1`: `169,510 rows/sec`, `95.0 KiB/flush`, `P50 5.177 ms`,
      `P99 10.553 ms`
    - `Idx2`: `157,854 rows/sec`, `104.0 KiB/flush`, `P50 5.470 ms`,
      `P99 10.506 ms`
    - composite-only `idx_bench_category_value` row:
      `152,661 rows/sec`, `124.9 KiB/flush`, `P50 5.600 ms`,
      `P99 11.866 ms`
    - `Idx4`: `114,018 rows/sec`, `169.9 KiB/flush`, `P50 7.471 ms`,
      `P99 46.674 ms`
- Structural attribution from that April 20 median rerun stayed consistent:
  - `Idx1` and `Idx2` are still structurally dominated by the rowid table plus
    the direct `idx_bench_value` secondary index
  - the composite-only row is still dominated by `idx_bench_category_value`
  - the full `Idx4` row is still dominated by `idx_bench_category_value`
    first and `idx_bench_value` second
  - `Idx2` still reports heavy deferred hashed appends
    (`hashedAppendDeferred=1579000/1579`), which is useful negative evidence:
    duplicate-bucket append batching is no longer the main blocker on the
    current matrix
- Interpretation after the refreshed slopes and the indexed-path follow-up:
  - the next remaining Plan 5 win is no longer flush amortization on the
    matched monotonic bulk row
  - row encoding still matters for wider rows, but it is not the first blocker
    on the matched baseline because CSharpDB already wins the `B10000` row
  - the main remaining throughput cost on realistic insert shapes is now
    secondary-index maintenance, especially payload/page work after the cheap
    per-row metadata/setup cost has already been removed
  - the validated append-context follow-up was only a marginal gain, which is a
    useful negative result: the next step is deeper duplicate-bucket
    chain/page-state work, not more planner-side setup caching
  - the external-chain-state follow-up is structurally important because it
    removes the per-append B-tree payload rewrite from new duplicate-heavy
    buckets, but the same-session `Idx0` control also moved up. The indexed
    rows improved in raw throughput, yet the overall secondary-index slope is
    still substantial, so the next likely win is not another payload-parse
    shortcut. The later statement-level append-header amortization rerun was
    effectively flat on the current matrix: it was slightly down on `Idx0` /
    `Idx1`, roughly flat on `Idx2`, and only modestly up on `Idx4`. That is a
    useful negative result. Repeated header/state rewrites were not the main
    remaining slope, so the next likely win is another deeper
    write-amplification cut inside duplicate-bucket maintenance rather than
    more header batching
  - the delta-packed external-chain follow-up is the first Plan 5 result that
    isolates the duplicate-bucket storage cost cleanly: `Idx1` stayed roughly
    flat because it only indexes unique `value`, while `Idx2` and especially
    `Idx4` moved up materially. The same rerun also lowered WAL bytes per flush
    on those rows (`110.9 KiB -> 104.0 KiB` for `Idx2`, `250.0 KiB -> 236.2
    KiB` for `Idx4`), which confirms this was a real duplicate-bucket
    write-amplification cut rather than benchmark noise
  - the later trailing-integer composite-key follow-up is the first indexed
    row result that clearly cut B-tree locality cost without touching durable
    flush cadence or duplicate-bucket chain encoding. That matters because it
    narrows the remaining indexed-path slope again: the composite
    `category,value` index is no longer dominated by random hash distribution,
    so the next likely wins are now the remaining single-column `value` index
    slope and the row-table/right-edge write path rather than more work inside
    the composite hashed duplicate-bucket codec
  - the reusable encoded-row-buffer follow-up and the later known-length encode
    refinement together resolved the remaining matched-row parity question on
    this runner. CSharpDB is now ahead of the matched SQLite row at both
    `B1000` and `B10000`, so the monotonic PK-only bulk path is no longer the
    main Plan 5 blocker
  - that shifts the remaining Plan 5 priority again: if more throughput work is
    pursued, the larger unresolved slopes are still secondary-index maintenance
    on realistic schemas and the random-key locality cliff rather than primary
    monotonic-row parity
  - random-key primary inserts remain a separate major cliff, but that is a
    locality/split-maintenance problem rather than the current SQLite parity
    blocker for the matched monotonic baseline
  - the later April 20 median rerun strengthens that conclusion: the recent
    secondary-index gains are real and repeatable, but the remaining indexed
    slope is still conventional secondary B-tree maintenance rather than more
    duplicate-bucket append-context/header work

Relevant code:

- `tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/MasterComparisonBenchmark.cs`
- `src/CSharpDB.Engine/Database.cs`
- `src/CSharpDB.Engine/InsertBatch.cs`
- `src/CSharpDB.Execution/QueryPlanner.cs`
- `src/CSharpDB.Execution/IndexMaintenanceHelper.cs`
- `src/CSharpDB.Storage/Serialization/RecordEncoder.cs`

## Experiment Matrix

### 1. Matched Bulk Baseline

Compare the durable four-column bulk rows directly:

- CSharpDB `InsertBatch` `B1000`
- CSharpDB `InsertBatch` `B10000`
- SQLite prepared explicit-transaction `B1000`
- SQLite prepared explicit-transaction `B10000`

This establishes the real same-shape durable bulk gap before any secondary
explanations are introduced.

### 2. Row-Width Slope

Run the existing `RowWidth_*` CSharpDB rows:

- baseline
- medium
- wide

This isolates row encoding and serialization cost.

### 3. Secondary-Index Slope

Run the existing `IndexSweep_*` CSharpDB rows:

- `Idx0`
- `Idx1`
- `Idx2`
- `Idx4`

This isolates how much of the gap is secondary-index maintenance rather than the
primary-key insert itself.

### 4. Key-Locality Slope

Run the existing `KeySweep_*` CSharpDB rows:

- monotonic
- random

This separates right-edge append cost from split-heavy random-key maintenance.

### 5. Flush-Amortization Follow-Up

If the matched bulk gap still dominates after the slopes above, focus the next
iteration on:

- rows-per-commit
- `commitsPerFlush`
- `KiBPerFlush`
- `avgWalAppendMs`
- `avgDurableFlushMs`

That is the branch for deciding whether the next win is lower per-row overhead
or better durable flush amortization.

## Reporting Rules

For every published comparison row, report:

- commits/sec
- rows/sec
- `P50`, `P95`, `P99`
- schema and batch size in plain language
- transaction shape in plain language
- durability semantics in plain language

For every CSharpDB row, also report:

- `commitsPerFlush`
- `KiBPerFlush`
- `avgWalAppendMs`
- `avgDurableFlushMs`

Do not mix unmatched rows into the headline table. The primary table must only
compare like-for-like durable bulk rows.

## Deliverables

- one matched durable bulk table for CSharpDB versus SQLite
- one attribution table that separates row width, index maintenance, and key
  locality
- one short prioritized worklist that says whether the next throughput win is
  row encoding, index maintenance, or flush amortization

## Next Version Tasks

- [x] Add matched SQLite prepared-bulk `B1000` and `B10000` rows.
- [x] Keep the matched four-column schema and `WAL/FULL` durability settings in
      the SQLite harness.
- [x] Add `RowWidth_*`, `IndexSweep_*`, and `KeySweep_*` rows to the CSharpDB
      durable batching matrix.
- [x] Refresh one same-runner matched-bulk comparison table on the current
      branch.
- [x] Identify and remove unnecessary logical conflict tracking from the
      single-writer bulk path.
- [x] Refresh the row-width, index, and key-locality sweeps after the
      single-writer bulk-path fix.
- [x] Quantify whether the next remaining win is row encoding,
      secondary-index maintenance, or flush amortization from the refreshed
      post-fix slopes.
- [x] Try the next low-risk duplicate-bucket indexed-insert follow-up and
      refresh the affected indexed rows.
- [x] Push past planner/setup savings into deeper duplicate-bucket
      chain/page-update cost by moving mutable append state out of the B-tree
      payload for new duplicate-heavy buckets.
- [x] Measure the next duplicate-bucket follow-up from this new baseline.
      Statement-level append-header amortization was safe but effectively flat
      on the current `Idx0` / `Idx1` / `Idx2` / `Idx4` matrix.
- [x] Pursue the next deeper duplicate-bucket write-amplification cut now that
      header batching has been measured and mostly ruled out.
      Dense delta-packed external chains improved the duplicate-heavy indexed
      rows while leaving the unique-only `Idx1` control basically flat.
- [x] Shift back to the row-table bulk path and amortize per-row encoded-record
      allocation across multi-row inserts. That materially improved `B10000`
      and modestly improved `B1000`, which was enough to rule out encoded-row
      allocation as the main remaining small-batch blocker.
- [x] Pursue the next row-table/right-edge fast path cut on the matched
      monotonic bulk row. The later known-length encode follow-up plus the
      retained right-edge split shortcut moved the matched row to `218,029
      rows/sec` at `B1000` and `820,459 rows/sec` at `B10000`, ahead of the
      current matched SQLite rerun on this runner.
- [x] Refresh the indexed `Idx1` / `Idx2` / `IdxCompositeCategoryValue` /
      `Idx4` rows with a reproducible median-of-3 rerun before checkpointing
      the secondary-index slice. The April 20 artifacts confirmed the same
      story on repeat: duplicate-bucket append amortization is largely solved,
      while the remaining indexed slope is still the direct `value` index and
      the composite `category,value` B-tree maintenance.
- [x] Return to Plan 4 only if the measured throughput gap still traces back to
      unresolved hot right-edge structural rejects. The current matched
      monotonic row no longer needs that return path for SQLite parity.

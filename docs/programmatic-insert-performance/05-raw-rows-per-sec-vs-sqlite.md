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

Relevant code:

- `tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/MasterComparisonBenchmark.cs`
- `src/CSharpDB.Engine/Database.cs`
- `src/CSharpDB.Engine/InsertBatch.cs`

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
- [ ] Refresh one same-runner comparison table that includes those rows.
- [ ] Quantify whether the next win is row encoding, secondary-index
      maintenance, or flush amortization.
- [ ] Return to Plan 4 only if the measured throughput gap still traces back to
      unresolved hot right-edge structural rejects.

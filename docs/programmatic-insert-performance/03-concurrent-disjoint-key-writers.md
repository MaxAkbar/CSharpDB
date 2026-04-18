# Plan 3: Concurrent Disjoint-Key Writers

## Goal

Answer one question cleanly:

When does CSharpDB's shared-engine writer model help insert workloads, and what
key pattern is required?

This is an architectural concurrency plan. It is not a continuation of the
single-writer bulk-path race from Plan 1.

## Fixed Controls

- Shared `Database` instance.
- File-backed durable mode.
- One row per commit to isolate commit-fan-in behavior.
- Primary comparison uses explicit disjoint key ranges.
- Every result must name:
  - commit mode
  - implicit insert mode
  - key pattern
  - writer count
  - batch window

## In Scope

- `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`
- shared auto-commit inserts
- explicit `WriteTransaction` inserts
- `RunWriteTransactionAsync(...)`
- `BeginWriteTransactionAsync(...)`
- writer counts:
  - `1`
  - `2`
  - `4`
  - `8`
- durable group-commit windows:
  - `0`
  - `250us`
  - optional `500us` follow-up row if needed
- explicit ID versus auto-generated ID
- hot right-edge versus disjoint explicit key ranges

## Out Of Scope

- `InsertBatch` inside `WriteTransaction`
- any attempt to compare these numbers directly against the Plan 1 bulk insert
  numbers as if they are one workload
- buffered or in-memory variants

## Current Evidence

- The repo already has a dedicated insert-fan-in harness:
  `tests/CSharpDB.Benchmarks/Macro/InsertFanInDiagnosticsBenchmark.cs`.
- The current public API does not expose `InsertBatch` on `WriteTransaction`, so
  this plan is necessarily SQL-based for the concurrent writer rows.
- Existing checked-in results already suggest:
  - hot right-edge insert loops stay near one commit per flush
  - disjoint explicit-key inserts can benefit when concurrency is enabled
  - explicit `WriteTransaction` and auto-commit concurrent insert paths should
    be compared only inside this concurrency family

Relevant code:

- `src/CSharpDB.Engine/ImplicitInsertExecutionMode.cs`
- `src/CSharpDB.Engine/Database.cs`
- `src/CSharpDB.Engine/WriteTransaction.cs`
- `tests/CSharpDB.Benchmarks/Macro/InsertFanInDiagnosticsBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/ConcurrentDurableWriteBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/WriteTransactionDiagnosticsBenchmark.cs`

## Experiment Matrix

### 1. Shared Auto-Commit Serialized

Control rows:

- `ImplicitInsertExecutionMode.Serialized`
- explicit ID, hot right-edge
- explicit ID, disjoint writer range
- auto-generated ID, hot right-edge

These rows anchor the "legacy serialized insert" behavior.

### 2. Shared Auto-Commit Concurrent

Primary rows:

- `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`
- explicit ID, disjoint writer range
- writer counts `1/2/4/8`
- batch windows `0` and `250us`

These rows measure the actual upside of the shared auto-commit concurrent mode.

### 3. Explicit WriteTransaction

Primary rows:

- `RunWriteTransactionAsync(...)`
- explicit ID, disjoint writer range
- writer counts `1/2/4/8`
- batch windows `0` and `250us`

These rows answer whether explicit transaction ownership performs materially
better than shared auto-commit concurrency for the same disjoint insert shape.

### 4. Auto-Generated ID Follow-Up

Validation rows:

- auto-generated IDs under shared auto-commit concurrent mode
- auto-generated IDs under explicit `WriteTransaction`

These rows exist to validate correctness and retry behavior, not because
auto-generated IDs are expected to be the best-case fan-in shape.

## Reporting Rules

For every row, report:

- commits/sec
- rows/sec
- `P50`, `P95`, `P99`
- `commitsPerFlush`
- conflict/retry counts
- duplicate-key failures
- `maxPendingCommits`

This plan should be read through commit-path diagnostics, not throughput alone.

## Deliverables

- one crossover table that shows where concurrent insert writers help and where
  they do not
- one short note that says hot right-edge insert loops are still a separate
  design problem if they remain pinned near `commitsPerFlush = 1.00`
- one README-ready section for "concurrent inserts" that is clearly separate
  from the single-writer bulk-path guidance

## Next Version Tasks

- [ ] Reuse `InsertFanInDiagnosticsBenchmark` naming and structure instead of
      creating a second concurrency benchmark family.
- [ ] Refresh the `1/2/4/8` writer matrix with `0` and `250us` batch windows.
- [ ] Keep serialized auto-commit rows as explicit controls.
- [ ] Keep disjoint explicit-key rows as the primary success case.
- [ ] Keep hot right-edge rows as the primary failure-boundary case.
- [ ] Record `commitsPerFlush`, retries, and duplicate-key failures alongside
      throughput.
- [ ] Update `tests/CSharpDB.Benchmarks/README.md` with a concurrency section
      that does not get mixed into the single-writer bulk narrative.

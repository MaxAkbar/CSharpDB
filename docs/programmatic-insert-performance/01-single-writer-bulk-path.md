# Plan 1: Single-Writer Bulk Path

## Goal

Answer one question cleanly:

What is the fastest embedded single-writer bulk insert path when storage
semantics stay fixed?

This plan establishes the canonical bulk insert story for the embedded engine.
It does not try to answer durability trade-offs or multi-writer scaling.

## Fixed Controls

- One writer only.
- File-backed durable mode.
- `ImplicitInsertExecutionMode.Serialized`.
- One named baseline storage preset:
  `UseWriteOptimizedPreset()`.
- One stable table schema for the baseline:
  `id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT`.

Keep one optional `Default` row only as a historical control. Do not optimize
around it.

## In Scope

- `Database.PrepareInsertBatch(...)`
- `Database.ExecuteAsync(string sql)` with explicit multi-row `INSERT`
- explicit transaction batching via:
  - `Database.BeginTransactionAsync()`
  - `Database.CommitAsync()`
  - `Database.RollbackAsync()`
- batch-size effects
- row-width effects
- secondary-index write cost
- monotonic primary key versus random primary key

## Out Of Scope

- `DurabilityMode.Buffered`
- `OpenInMemoryAsync(...)`
- `LoadIntoMemoryAsync(...)`
- `OpenHybridAsync(...)`
- shared `Database` multi-writer fan-in
- `WriteTransaction` concurrency experiments

## Current Evidence

- `InsertBatch` is the engine-level reusable bulk helper and executes through
  `SimpleInsertSql`, which is the current fast path for simple full-row inserts.
- `Database.ExecuteAsync(string)` already detects simple inserts and routes them
  through the same simplified insert handling when the SQL shape qualifies.
- The checked-in durable SQL batching harness already shows that rows-per-commit
  dwarfs preset-level deltas for single-writer durable ingest.

Relevant code:

- `src/CSharpDB.Engine/InsertBatch.cs`
- `src/CSharpDB.Engine/Database.cs`
- `tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs`

## Experiment Matrix

### 1. API Path Comparison

Compare these paths under the same durable file-backed setup:

- `InsertBatch`
- explicit multi-row SQL:
  `INSERT INTO bench VALUES (...), (...), ...`
- per-row SQL inside one explicit transaction

Expected result:

- `InsertBatch` remains the recommended path.
- explicit multi-row SQL is a parity check, not a likely improvement.

### 2. Batch-Size Sweep

Run one baseline schema across these batch sizes:

- `1`
- `10`
- `100`
- `1000`
- `10000`

Report:

- commits/sec
- rows/sec
- `P50`, `P95`, `P99`

Expected result:

- a steep gain from `1` to `1000`
- a flatter curve from `1000` to `10000`

### 3. Row-Width Sweep

Hold the API path constant and vary row shape:

- narrow baseline row
- medium-width row
- wide row with one larger `TEXT` payload

This isolates serialization and row-encoding cost.

### 4. Secondary-Index Sweep

Hold the row shape constant and vary index count:

- primary key only
- primary key + 1 secondary index
- primary key + 2 secondary indexes
- primary key + 4 secondary indexes

This makes per-index maintenance cost visible instead of letting the primary
key-only case define the narrative.

### 5. Key-Pattern Sweep

Hold the API path constant and vary the insert key pattern:

- monotonic `id`
- random `id`

This separates right-edge append behavior from split-heavy behavior.

## Deliverables

- one recommended embedded bulk insert path
- one recommended batch-size band
- one README-ready chart for batch-size scaling
- one README-ready table for index and row-width sensitivity
- one short note that says which results are parity checks versus real tuning
  levers

## Next Version Tasks

- [ ] Extend the current durable SQL batching work instead of creating a new
      benchmark family with different naming.
- [ ] Add a dedicated `InsertBatch` versus multi-row SQL comparison row set.
- [ ] Add batch-size params for the baseline schema.
- [ ] Add row-width and secondary-index variants.
- [ ] Add monotonic versus random key variants.
- [ ] Update `tests/CSharpDB.Benchmarks/README.md` with the final recommended
      single-writer path.

# Plan 2: Durability And Residency Trade-Offs

## Goal

Answer one question cleanly:

How much of the insert throughput story changes when durability or residency
changes, while the Plan 1 bulk path stays fixed?

This plan is about storage semantics, not alternative insert APIs.

## Fixed Controls

- Reuse the winning single-writer bulk path from Plan 1.
- Keep one stable schema and one stable batch size.
- Keep one-writer execution only.
- Keep the same row shape and key pattern unless a row explicitly says
  otherwise.

## In Scope

- file-backed durable baseline
- file-backed buffered mode
- `OpenInMemoryAsync(...)`
- `LoadIntoMemoryAsync(...)`
- `OpenHybridAsync(...)`
- optional preset comparison inside the durable file-backed case:
  `UseWriteOptimizedPreset()` versus `UseLowLatencyDurableWritePreset()`

## Out Of Scope

- shared `Database` multi-writer fan-in
- `WriteTransaction` versus auto-commit concurrency
- alternate SQL/API path comparisons already covered by Plan 1

## Current Evidence

- The repo already has a checked-in storage-mode harness:
  `tests/CSharpDB.Benchmarks/Macro/HybridStorageModeBenchmark.cs`.
- The current hybrid API name is `HybridPersistenceMode`, with
  `IncrementalDurable` and `Snapshot`. Do not refer to a nonexistent
  `PersistenceMode.Lazy` surface.
- Existing checked-in benchmark summaries already suggest that:
  - buffered mode is a major write-throughput lever
  - in-memory mode is a major throughput lever
  - hybrid incremental-durable is close to file-backed for insert throughput and
    should be treated as a residency/design choice, not a likely big insert win

Relevant code:

- `src/CSharpDB.Engine/Database.cs`
- `src/CSharpDB.Engine/HybridDatabaseOptions.cs`
- `src/CSharpDB.Storage/StorageEngine/StorageEngineOptionsBuilder.cs`
- `tests/CSharpDB.Benchmarks/Macro/HybridStorageModeBenchmark.cs`
- `tests/CSharpDB.Benchmarks/README.md`

## Experiment Matrix

Use the same Plan 1 bulk path and run these modes:

### 1. Durable File-Backed

Baseline row:

- `Database.OpenAsync(...)`
- `UseWriteOptimizedPreset()`

Optional secondary row:

- `UseLowLatencyDurableWritePreset()`

### 2. Buffered File-Backed

Compare the same file-backed setup with:

- `UseDurabilityMode(DurabilityMode.Buffered)`

This is the explicit crash-durability trade-off row.

### 3. New Private In-Memory Database

Compare:

- `Database.OpenInMemoryAsync(...)`

This measures the maximum throughput case when persistence is intentionally
removed.

### 4. Load Existing Database Into Memory

Compare:

- `Database.LoadIntoMemoryAsync(...)`

This is a distinct workload from opening a brand-new in-memory database. It is
the right row for "load once, then ingest in RAM."

### 5. Hybrid Incremental-Durable

Compare:

- `Database.OpenHybridAsync(...)`
- `HybridDatabaseOptions { PersistenceMode = HybridPersistenceMode.IncrementalDurable }`

This row should be framed as a storage-shape decision, not assumed to be a
major write-throughput win.

## Reporting Rules

For every row, report:

- commits/sec
- rows/sec
- `P50`, `P95`, `P99`
- durability semantics in plain language next to the numbers

Do not publish the table as throughput-only. The semantics are the reason the
rows differ.

## Deliverables

- one decision table that says when to use:
  - file-backed durable
  - file-backed buffered
  - in-memory
  - load-into-memory
  - hybrid incremental-durable
- one short note that explicitly says hybrid incremental-durable is not a
  generic insert-speed substitute for in-memory mode
- one README-ready storage trade-off section

## Next Version Tasks

- [x] Reuse the Plan 1 winning bulk path for every row in this matrix.
- [x] Refresh the storage-mode insert numbers under one standardized harness.
- [x] Add a dedicated buffered row to the same matrix instead of leaving it as a
      separate side snapshot.
- [x] Keep `OpenInMemoryAsync(...)` and `LoadIntoMemoryAsync(...)` as separate
      rows.
- [x] Use the correct `HybridPersistenceMode` naming in the write-up.
- [x] Update `tests/CSharpDB.Benchmarks/README.md` with one durable-versus-
      buffered-versus-memory decision table.

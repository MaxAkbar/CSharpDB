# Multi-Writer Follow-Up Plan

This document tracks the work that remains after CSharpDB's initial multi-writer
support. It is intentionally narrower than the original exploratory work: the
engine already ships explicit `WriteTransaction` support, shared auto-commit
non-insert isolation, and opt-in `ConcurrentWriteTransactions` for shared
implicit inserts. What remains is the work needed to move from "initial support"
to "multi-writer is broadly done and predictable."

---

## Current State

What is already shipped:

- explicit multi-writer transactions through `BeginWriteTransactionAsync(...)`
  and `RunWriteTransactionAsync(...)`
- conflict detection and retry for isolated write-transaction attempts
- shared auto-commit `UPDATE`, `DELETE`, and DDL routed through isolated
  write-transaction state internally
- opt-in `ImplicitInsertExecutionMode = ConcurrentWriteTransactions` for shared
  auto-commit `INSERT`
- retained-WAL, checkpoint, commit-fan-in, and insert-fan-in benchmark coverage

What is still structurally true:

- the physical WAL publish/flush path is still serialized at the storage
  boundary
- hot single-row insert workloads still do not show update-style fan-in
- insert-side gains remain limited even when using explicit `WriteTransaction`
- durable flush cost is still the dominant fixed cost once pending-commit
  queuing works
- user-facing gains are workload-dependent rather than a blanket "all writes
  scale now"

The April 10-13, 2026 benchmark closeout supports this reading:

- low-conflict disjoint updates can coalesce durable flushes and benefit from
  the current queue shape
- hot insert loops still stay near `commitsPerFlush = 1.00`
- the explicit write-transaction path is stable enough to ship, but the insert
  path is still structurally limited

---

## Definition Of Done

We should only call multi-writer "done" when all of the following are true:

1. Hot insert workloads no longer collapse to the legacy single-commit shape by
   design alone.
2. Auto-generated IDs and explicit IDs both behave predictably under concurrent
   insert pressure without duplicate-key regressions or retry explosions.
3. Shared auto-commit write traffic and explicit write transactions have
   clearly documented and benchmarked performance envelopes.
4. Checkpoint, WAL retention, and snapshot readers stay stable under sustained
   write contention.
5. Guardrail benchmarks can detect regressions in both low-conflict and
   high-conflict multi-writer shapes.
6. The storage and engine docs can make a clean statement about what kinds of
   multi-writer workloads are expected to scale and which ones are still
   intentionally serialized.

Until then, the correct release language remains "initial multi-writer support."

---

## Non-Goals

This plan does not try to do the following in the same pass:

- distributed replication or change-feed work
- multi-process write coordination across separate daemon/engine processes
- network transport optimizations unrelated to write-path contention
- replacing the WAL durability model with a fundamentally different storage
  engine

---

## Workstreams

The remaining work breaks into five concrete workstreams.

### 1. Insert-Path Concurrency

Problem:

- concurrent inserts still collide structurally on row-id allocation,
  uniqueness checks, and right-edge leaf growth
- current fan-in data shows that changing the commit pipeline alone does not
  unlock insert-side gains

Primary goal:

- make hot insert traffic conflict less before commit publication, not just
  after it

Likely design areas:

- row-id reservation / allocation strategy for concurrent insert attempts
- separation of "logical key assignment" from "physical leaf placement"
- explicit handling of right-edge leaf split pressure
- stronger rebase/retry support for insert intents after page split movement

Likely code areas:

- `src/CSharpDB.Engine/Database.cs`
- `src/CSharpDB.Engine/WriteTransaction.cs`
- `src/CSharpDB.Execution/QueryPlanner.cs`
- `src/CSharpDB.Storage/Paging/Pager.cs`
- `src/CSharpDB.Storage/Paging/LeafInsertRebaseHelper.cs`
- `src/CSharpDB.Storage/Paging/InteriorInsertRebaseHelper.cs`

Exit criteria:

- insert fan-in benchmark no longer stays hard-pinned near
  `commitsPerFlush = 1.00` for all hot-insert shapes
- auto-id and explicit-id paths both remain correctness-stable
- no duplicate-key regressions in the retry path

### 2. Conflict Granularity And Rebase Stability

Problem:

- the system can detect conflicts, but insert and page-shape conflicts still
  force more retry or structural fallback than we want

Primary goal:

- reduce unnecessary retries by making conflict detection more logical and less
  page-incidental where correctness allows it

Work items:

- review current logical conflict key coverage
- tighten the distinction between true logical conflicts and page-layout
  movement
- harden rebase helpers for parent/child page movement and late split
  finalization
- document which operations are intentionally page-structural versus logically
  mergeable

Likely code areas:

- `src/CSharpDB.Storage/Transactions/LogicalConflictKey.cs`
- `src/CSharpDB.Storage/Transactions/TransactionCoordinator.cs`
- `src/CSharpDB.Storage/Transactions/PagerTransactionState.cs`
- `src/CSharpDB.Storage/Paging/LeafInsertRebaseHelper.cs`
- `src/CSharpDB.Storage/Paging/InteriorInsertRebaseHelper.cs`

Exit criteria:

- lower retry counts for disjoint-key insert/update scenarios
- no new corruption or phantom-success paths under `DatabaseConcurrencyTests`
- conflict behavior explainable in docs and diagnostics

### 3. Commit Pipeline And Durable Flush Sharing

Problem:

- once writes reach the pending-commit queue, durable flush still dominates
  cost
- the queue now helps low-conflict non-insert traffic, but the overall commit
  path still needs clearer stage boundaries

Primary goal:

- keep improving the publish/finalize pipeline without weakening durability or
  correctness

Work items:

- continue separating WAL append, publish, and durable finalization stages
- verify whether more publish-side batching is possible without reordering
  committed visibility incorrectly
- measure whether pending-commit queue depth can increase safely for the
  non-insert path
- validate whether `UseDurableGroupCommit(...)` needs smarter defaults or
  remains strictly opt-in

Likely code areas:

- `src/CSharpDB.Storage/Wal/WriteAheadLog.cs`
- `src/CSharpDB.Storage/Wal/IWriteAheadLog.cs`
- `src/CSharpDB.Storage/Paging/Pager.cs`
- `src/CSharpDB.Storage/Transactions/TransactionCoordinator.cs`
- `src/CSharpDB.Storage/StorageEngine/DurableGroupCommitOptions.cs`

Exit criteria:

- disjoint-update commit fan-in remains stable under reruns
- no regression in single-writer durability numbers beyond accepted thresholds
- commit stage diagnostics remain understandable and monotonic

### 4. Reader / Checkpoint / Retained-WAL Interaction

Problem:

- multi-writer throughput is not just a write path issue; checkpoint retention,
  reader snapshots, and retained WAL can become secondary bottlenecks or hide
  regressions

Primary goal:

- ensure that the multi-writer path stays stable when readers and checkpoints
  coexist with sustained write traffic

Work items:

- revalidate retained-WAL compaction behavior
- keep checkpoint finalization behavior explicit when snapshots still reference
  WAL frames
- stress test mixed read/write scenarios with hybrid mode enabled
- document acceptable retained-WAL growth and backpressure behavior

Likely code areas:

- `src/CSharpDB.Storage/Paging/Pager.cs`
- `src/CSharpDB.Storage/Wal/WriteAheadLog.cs`
- `tests/CSharpDB.Tests/WalTests.cs`
- `tests/CSharpDB.Tests/DatabaseConcurrencyTests.cs`

Exit criteria:

- no large manual-checkpoint tails after the writer blocker is released
- mixed read/write tests stay deterministic
- retained-WAL behavior remains bounded and diagnosable

### 5. Benchmarks, Guardrails, And Release Criteria

Problem:

- the engine now has enough write-path branches that correctness alone is not a
  sufficient release signal

Primary goal:

- turn the current benchmark knowledge into a durable release contract

Work items:

- keep `WriteTransactionDiagnosticsBenchmark` as the explicit transaction
  baseline
- keep `CommitFanInDiagnosticsBenchmark` for shared non-insert queue behavior
- keep `InsertFanInDiagnosticsBenchmark` as the insert-side structural truth
  source
- keep `ConcurrentDurableWriteBenchmark` for shared-database writer contention
- decide which rows are release-blocking versus informative only
- refresh perf thresholds only after same-runner reruns

Primary files:

- `tests/CSharpDB.Benchmarks/README.md`
- `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- benchmark classes under `tests/CSharpDB.Benchmarks/Macro`

Exit criteria:

- release guardrails match the actual shipped write contract
- stale thresholds do not block release for the wrong reasons
- docs can point to stable benchmark sources for each supported workload shape

---

## Recommended Execution Order

The clean order is:

1. Insert-path concurrency design
2. Conflict granularity and rebase hardening
3. Commit pipeline refinement
4. Mixed read/write + checkpoint validation
5. Benchmark threshold and docs refresh

Reasoning:

- the insert path is still the main structural gap
- commit-pipeline tuning without insert-side redesign will keep hitting the
  same ceiling
- benchmark work should validate the new shape, not guess it in advance

---

## Detailed Phase Plan

### Phase A: Insert Architecture Design

Deliverables:

- a written design for concurrent row-id reservation and insert intent handling
- a clear statement about whether the chosen path is optimistic reservation,
  preallocated ranges, or another model
- a correctness matrix for explicit-id versus auto-id inserts

Implementation notes:

- keep the design compatible with current WAL durability and page layout
- do not weaken uniqueness enforcement just to reduce retries
- prefer explicit diagnostics over silent fallback to the legacy path

Tests to add or strengthen:

- concurrent auto-id insert stress with retries and reopen validation
- concurrent explicit-id insert stress on disjoint keys
- split-heavy insert scenarios that force leaf and parent movement

Phase exit:

- design approved and minimally implemented behind the current public API

### Phase B: Insert Rebase / Retry Implementation

Deliverables:

- implemented row-id / insert-intent improvements
- hardened insert rebase helpers
- conflict/retry telemetry for the new path

Tests to add or strengthen:

- `DatabaseConcurrencyTests` coverage for repeated page splits
- WAL recovery coverage for partially progressed concurrent insert attempts
- long-running insert loops with seeded and unseeded tables

Benchmarks to rerun:

- `WriteTransactionDiagnosticsBenchmark`
- `InsertFanInDiagnosticsBenchmark`

Phase exit:

- insert-side numbers materially improve or the design is clearly proven not to
  help and is revised before moving on

### Phase C: Commit Pipeline Consolidation

Deliverables:

- clearer separation of append, publish, and durable finalize stages
- stable pending-commit queue behavior under multi-writer non-insert traffic
- updated diagnostics fields if stage timing changes

Tests to add or strengthen:

- queue-depth and flush-sharing assertions
- cancellation and disposal behavior while commits are pending
- crash-recovery coverage after queued but not yet finalized work

Benchmarks to rerun:

- `CommitFanInDiagnosticsBenchmark`
- `ConcurrentDurableWriteBenchmark`
- targeted durable-write comparisons against single-writer baselines

Phase exit:

- queue behavior is stable and does not regress single-writer durability

### Phase D: Mixed Workload Hardening

Deliverables:

- stable mixed read/write/checkpoint behavior under the new path
- retained-WAL and checkpoint guidance updated if necessary

Tests to add or strengthen:

- concurrent readers plus insert-heavy writers
- hybrid incremental-durable mixed workloads
- checkpoint-retention and post-checkpoint recovery scenarios

Benchmarks to rerun:

- `checkpoint-retention-diagnostics`
- relevant hybrid storage and concurrent read suites

Phase exit:

- no regression in mixed workload behavior or retained-WAL safety

### Phase E: Release Contract Refresh

Deliverables:

- benchmark README refresh
- perf threshold refresh where justified
- storage / engine / roadmap docs updated to the new supported boundary

Release gate:

- correctness tests green
- focused multi-writer benchmarks rerun on the same runner
- threshold updates justified by fresh captured artifacts, not anecdotes

---

## Testing Matrix

Every major multi-writer follow-up change should be validated against this
matrix:

| Area | Minimum Validation |
|------|--------------------|
| Correctness | `tests/CSharpDB.Tests/DatabaseConcurrencyTests.cs` plus WAL recovery coverage |
| Storage safety | `tests/CSharpDB.Tests/WalTests.cs` and checkpoint-related tests |
| Explicit multi-writer API | `WriteTransactionDiagnosticsBenchmark` and targeted transaction tests |
| Shared auto-commit non-insert | `CommitFanInDiagnosticsBenchmark` |
| Shared auto-commit insert | `InsertFanInDiagnosticsBenchmark` |
| Shared durable contention | `ConcurrentDurableWriteBenchmark` |
| Release guardrails | `tests/CSharpDB.Benchmarks/perf-thresholds.json` compare run on the canonical perf runner |

---

## Open Questions

These questions should be answered before we claim broader completion:

1. Should auto-generated row IDs reserve ranges per writer attempt, or should
   they remain globally coordinated but decoupled from page placement?
2. Can right-edge insert pressure be reduced enough with reservation/rebase
   alone, or does the tree need a more explicit append-friendly shape?
3. Is there a correctness-safe way to let more insert work reach the pending
   commit queue before leaf placement is finalized?
4. Which benchmark rows should become release-blocking for insert-side behavior,
   given that some shapes are intentionally still serialized today?
5. Do we want a separate public knob for insert-path strategy, or should this
   remain internal behind `ImplicitInsertExecutionMode`?

---

## Recommended Short Version

If we want the shortest accurate summary for future planning:

- initial multi-writer support is done
- low-conflict non-insert fan-in is meaningfully better
- hot insert fan-in is still the main unfinished structural problem
- the next serious phase should focus on row-id reservation, insert intent
  rebasing, and right-edge insert pressure before doing more queue tuning

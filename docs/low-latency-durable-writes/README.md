# Low-Latency Durable Writes — Roadmap & Design

> **Status (March 2026):** Planned. This document captures the recommended direction for improving file-backed durable auto-commit write latency in CSharpDB without relaxing exact per-commit durability for committed user data.
>
> **Release-note summary:** Reduce durable auto-commit overhead by removing advisory planner-stat persistence from the commit hot path, while preserving exact per-commit WAL durability for user rows.

CSharpDB's current durable write path is correct, but its single-row auto-commit throughput is still much lower than buffered mode because each successful durable commit forces a real WAL flush to disk. This document outlines a strict-durability optimization path that targets the remaining avoidable overhead around that flush, rather than weakening the durability contract itself.

---

## Problem

Today, file-backed durable single inserts are dominated by two realities:

1. Every successful durable auto-commit must force the WAL to disk before the commit completes.
2. The engine still performs advisory metadata work in the same write path as committed user data.

The first cost is fundamental to exact durable semantics. The second cost is not.

The current durable path still updates persisted planner statistics aggressively enough that auto-commit writes do more work than they need to for the single-row case. That is the main optimization target for this workstream.

---

## Current Measured Baseline

From the March 25-26, 2026 benchmark refreshes:

- Durable SQL single insert is about `287.5 ops/sec`
- Buffered SQL single insert is about `21.21K ops/sec`
- Durable write diagnostics show checkpoint tuning helps variance, but not the main durable single-insert ceiling:
  - best measured tuned durable variant is about `294.0 ops/sec`

That means checkpoint policy tuning is largely exhausted for this metric. The next gain has to come from reducing non-flush work inside the durable commit path.

---

## Goal

Improve file-backed durable auto-commit write latency, especially the single-row insert path, while keeping this contract unchanged:

- when a durable commit returns success, the committed user data is durably flushed through the WAL path

This work should produce a measurable uplift in the current durable single-insert benchmark shape without turning durable mode into buffered mode.

---

## Non-Goals

- changing durable mode into buffered mode
- adding intentional deferred WAL flush behavior
- weakening crash-safety guarantees for committed user rows
- replacing this work with group commit
- broad storage-format changes unrelated to commit hot-path cost

The existing `Group commit / deferred WAL flush` roadmap item remains a separate future direction with a different semantics tradeoff.

---

## Recommended Design

### 1. Keep user-data durability exact

The WAL durability contract stays unchanged:

- durable mode still forces the WAL flush before commit success
- committed rows remain crash-safe at the current semantics level

This plan only removes advisory metadata work from the same hot path.

### 2. Make advisory statistics persistence opt-in and deferrable

Add an opt-in mode that keeps planner statistics current in memory, but does not rewrite persisted stats on every durable auto-commit.

Recommended public model:

- `AdvisoryStatisticsPersistenceMode.Immediate`
- `AdvisoryStatisticsPersistenceMode.Deferred`

Default remains `Immediate`.

### 3. Stamp persisted stats with the durable DB change counter

Persisted table statistics should record the pager `ChangeCounter` value at the time they were last written.

Recommended addition:

- `TableStatistics.LastPersistedChangeCounter`

That gives the engine a durable freshness signal without requiring stats to be rewritten on every commit.

### 4. Split exact row-count use from estimate-only row-count use

The engine currently uses table row counts for both:

- exact `COUNT(*)` fast paths
- planner estimates and capacity hints

Those two uses must be separated.

Recommended rule:

- exact query results only use row counts that are known fresh against the current change counter
- planner heuristics may use older row counts as estimates

### 5. Treat column statistics as stale when table stats are behind

If persisted table stats lag the current durable change counter, the engine should conservatively treat that table's column statistics as stale after reopen or recovery.

That avoids writing per-column stale markers on every write while still keeping planner behavior safe.

### 6. Persist deferred advisory stats on maintenance boundaries, not every commit

In `Deferred` mode:

- normal write commits should skip persisted advisory-stats flushes
- deferred stats should be written on clean close
- explicit stats-maintenance flows such as `ANALYZE` should write fresh stats immediately

This keeps the hot path lean while still providing clean restart behavior after orderly shutdown.

---

## Public Surface Changes

Recommended additions:

- `AdvisoryStatisticsPersistenceMode`
- `StorageEngineOptions.AdvisoryStatisticsPersistenceMode`
- `StorageEngineOptionsBuilder.UseAdvisoryStatisticsPersistenceMode(...)`
- `StorageEngineOptionsBuilder.UseLowLatencyDurableWritePreset(...)`

`UseLowLatencyDurableWritePreset(...)` should be a convenience opt-in that combines:

- the existing write-oriented checkpoint behavior
- deferred advisory stats persistence

It should not replace or silently change the existing `UseWriteOptimizedPreset()`.

---

## Correctness Rules

The implementation must preserve these rules:

- committed durable user rows stay fully durable
- exact `COUNT(*)` must not trust stale persisted row counts
- planner estimates may use stale row counts only as estimates
- after crash/reopen in deferred mode, advisory stats may be less fresh, but user data correctness must not change
- clean close should preserve deferred stats so the next open gets the latest persisted advisory metadata available

---

## Benchmark and Test Plan

### Benchmarks

Measure before and after on the same machine for:

- README durable SQL single insert macro
- durable write diagnostics
- an analyzed-table durable insert case to prove that existing stats do not reintroduce hot-path churn

### Correctness tests

Add or update tests for:

- exact `COUNT(*)` in immediate mode
- exact `COUNT(*)` after writes in deferred mode within the same process
- crash/reopen behavior in deferred mode so stale persisted row counts are not used as exact results
- stale-column-stat behavior after reopen when persisted stats lag the current change counter
- clean-close persistence of deferred advisory stats

### Success criteria

The first implementation slice is successful if:

- durable single-insert throughput improves materially over the current `~287.5 ops/sec` baseline
- no SQL correctness regressions are introduced
- the gain comes without changing the durable commit contract

Buffered-mode throughput is not the target here. The physical flush remains the dominant floor in exact durable mode.

---

## Why This Is Separate From Group Commit

This plan and group commit solve different problems:

- **Low-latency durable writes:** preserve exact durable commit semantics and remove avoidable hot-path overhead
- **Group commit / deferred WAL flush:** deliberately share or delay flush boundaries across transactions to raise throughput further

Both may be worthwhile, but they should remain separate roadmap items because they have different correctness and product tradeoffs.

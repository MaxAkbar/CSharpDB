# CSharpDB Table/Index Statistics — Roadmap & Design

> **Status (March 2026):** Planned. This document captures the recommended v1 direction for table and index statistics in CSharpDB. The current planner is still rule-based and does not support `ANALYZE`.

CSharpDB already has strong fast paths for primary-key lookups, equality index lookups, integer range scans, and several join cases. What it does not have yet is persisted statistics that let the planner decide when an index is actually selective enough to beat a scan, or when a join strategy is likely to fan out badly.

---

## Problem

Today the planner makes access-path decisions with fixed heuristics:

- primary-key lookups outrank everything else
- unique secondary indexes outrank non-unique indexes
- non-unique indexes are chosen by structural rules rather than estimated selectivity
- join decisions rely on coarse row-count heuristics and, in some cases, runtime table counting

That leaves several known gaps:

- table row counts are not persisted, so some planner decisions may pay a first-use leaf walk
- low-NDV non-unique indexes can be chosen even when they touch a large fraction of the table
- range predicates do not yet benefit from persisted `min` / `max` boundaries
- duplicate-heavy join keys can push the planner toward poor build-side or index-nested-loop choices

The result is not that the planner is always slow. The result is that some queries can land on avoidably bad plans.

---

## Goal

Add first-class statistics support that gives CSharpDB:

1. exact persisted table `row_count`
2. persisted column `NDV`, `min`, and `max`
3. SQL-visible statistics through `ANALYZE` and `sys.*`
4. cost-based single-table access-path choice
5. better join sizing, build-side selection, and index-nested-loop fanout estimates

This is a planner-quality feature, not just a metadata feature.

---

## V1 Scope

### In scope

- `ANALYZE;`
- `ANALYZE table_name;`
- persisted exact `row_count`
- persisted column `NDV`, `min`, and `max`
- auto-maintained exact `row_count`
- auto-maintained column stats only where maintenance is cheap
- stale flags for column stats that can no longer be trusted after writes
- cost-based scan vs index choice for simple single-table predicates
- improved join build-side and index-nested-loop fanout decisions
- `COUNT(*)` row-count shortcut backed by persisted exact table counts

### Out of scope

- full join reordering
- histograms
- multi-column correlation statistics
- adaptive re-optimization
- covering-index costing

V1 should improve plan quality without turning the planner into a full optimizer rewrite.

---

## SQL Surface

The v1 SQL-visible additions should be:

```sql
ANALYZE;
ANALYZE users;
```

And these system catalogs:

- `sys.table_stats`
- `sys.column_stats`
- `sys.index_stats`

Recommended semantics:

- `row_count` is exact
- `NDV` / `min` / `max` are exact immediately after `ANALYZE`
- auto-maintained column stats may become stale after writes that are expensive to maintain exactly
- stale stats remain visible to SQL consumers
- stale stats must not be treated by the planner as if they were fresh

Recommended catalog shape:

### `sys.table_stats`

| Column | Meaning |
|--------|---------|
| `table_name` | Table name |
| `row_count` | Exact committed row count |
| `has_stale_columns` | Whether any tracked column stat is stale |

### `sys.column_stats`

| Column | Meaning |
|--------|---------|
| `table_name` | Table name |
| `column_name` | Column name |
| `data_type` | Declared column type |
| `ndv` | Distinct value count when fresh |
| `min_value_text` | Display-friendly min value |
| `max_value_text` | Display-friendly max value |
| `is_stale` | Whether the column stat can no longer be trusted |

### `sys.index_stats`

`sys.index_stats` should be planner-oriented and derived from persisted table and column stats rather than maintained as a separate authoritative source.

Suggested columns:

| Column | Meaning |
|--------|---------|
| `index_name` | Index name |
| `table_name` | Backing table |
| `estimated_distinct_keys` | Estimated distinct keys for the index |
| `estimated_avg_rows_per_lookup` | Estimated fanout |
| `is_stale` | Whether the estimate depends on stale column stats |

---

## Recommended Design

### 1. Use a dedicated stats catalog

Do not store statistics by rewriting `TableSchema` or `IndexSchema` on every write. That would couple high-frequency DML to schema metadata rewrites and make stats maintenance unnecessarily expensive.

Instead, add a dedicated stats catalog alongside the existing table, index, view, and trigger metadata catalogs.

Recommended persisted model:

- one table-stats record per physical table
- one column-stats record per tracked table column
- no separate persisted index-stats authority; derive index estimates from the table and column stats

### 2. Exact `row_count`, staleable column stats

The right v1 tradeoff is:

- keep `row_count` exact on every committed write
- keep `NDV`, `min`, and `max` exact when maintenance is cheap
- mark column stats stale when a write invalidates them in a way that is expensive to repair incrementally
- let `ANALYZE` recompute exact values and clear stale flags

This keeps the most important planner signal always available without pushing too much overhead into the write path.

### 3. `ANALYZE` is the exact refresh boundary

`ANALYZE` should perform a full scan of the target table or tables and rebuild exact:

- `row_count`
- `NDV`
- `min`
- `max`

After `ANALYZE`, those stats are authoritative again until later writes mark some of them stale.

---

## Planner Integration

The current planner uses fixed ranking and heuristics. V1 should introduce cost-based decisions where stats are available.

### Single-table access-path choice

Recommended selectivity inputs:

- PK equality: estimated rows = `1`
- unique index equality: estimated rows = `1`
- non-unique equality: estimated rows = `ceil(row_count / NDV)`
- composite equality: estimated rows from the product of component NDVs, capped by `row_count`
- integer range: estimate overlap from persisted `min` / `max`

Recommended planner behavior:

- compare scan cost against index cost using estimated rows
- keep PK and unique lookups effectively near-constant cost
- prefer scans when the estimated index fanout is too high
- fall back to the current heuristics when a required stat is missing or stale

### Join improvements

V1 should improve join planning without taking on full join reordering.

Recommended uses of stats:

- replace first-use runtime row counting with persisted exact table `row_count`
- use join-key NDV to estimate average fanout for index nested loops
- use NDV-informed estimates when sizing hash join build structures
- keep current join ordering behavior, but make the existing decisions less blind

This keeps v1 focused and tractable while still fixing real plan-quality gaps.

---

## Write-Path Maintenance

Recommended maintenance rules:

### `CREATE TABLE`

- initialize table stats with exact `row_count = 0`
- initialize empty column stats for tracked column types

### `INSERT`

- increment exact `row_count`
- update `min` / `max` when the new value obviously extends the boundary
- keep column stats exact only when maintenance is cheap
- otherwise mark the affected column stat stale

### `UPDATE`

- keep exact `row_count` unchanged
- if the updated value can invalidate `NDV`, `min`, or `max` and exact repair is not cheap, mark the column stat stale

### `DELETE`

- decrement exact `row_count`
- if the deleted value might have been the last distinct value or current boundary, mark the affected column stat stale unless exact repair is cheap

### Cheap exact maintenance

V1 can reasonably keep some stats exact on write when the underlying structure already helps:

- primary-key columns
- unique integer-like keys
- simple indexed columns where boundary checks or existence checks are cheap enough

Everything else should prefer correctness and bounded write overhead over over-engineered incremental maintenance.

---

## Expected Performance Impact

Yes, this feature should produce real performance gains.

The gains will be targeted, not uniform:

- biggest wins should come from avoiding bad non-unique index plans
- duplicate-heavy joins should benefit from better fanout and build-side estimates
- persisted exact `row_count` should improve cold-start planning after reopen
- `COUNT(*)` should benefit from exact persisted counts instead of leaf walking

Cases that should see the largest benefit:

- low-selectivity indexed predicates
- selective range predicates on columns with good `min` / `max`
- duplicate-heavy join-key workloads
- databases that reopen often and currently pay first-use counting costs

Cases that should be mostly neutral:

- PK lookups
- unique index equality lookups
- queries that already land on the correct plan today

Costs:

- some extra write-path work to keep `row_count` exact and track stale stats
- periodic full-scan `ANALYZE` work
- added planner complexity

### Short answer

Statistics are worth it for performance, but mainly because they help the planner avoid bad plans. They will not magically accelerate every query. The largest wins should show up on non-unique indexes, duplicate-heavy joins, and cold-start planning.

---

## Testing and Benchmarks

### Tests

Add coverage for:

- parser support for `ANALYZE`
- persistence across close/reopen
- exact `row_count` after insert, update, delete, and rollback
- stale-flag transitions after invalidating writes
- `ANALYZE` recomputation and stale-flag clearing
- planner-choice behavior with fresh stats
- fallback behavior when stats are stale or missing

### Benchmark additions

Extend the benchmark suite with scenarios that make the benefit measurable:

- equality lookups on low-NDV indexed columns
- equality lookups on high-NDV indexed columns
- selective and non-selective integer ranges
- duplicate-heavy joins
- reopen-and-query scenarios that show persisted `row_count` value

The point of the benchmark work is not just to prove that stats exist. It is to prove they cause better plan choices.

---

## Open Follow-Ups

Likely follow-up work after v1:

- histograms for skewed distributions
- join reordering
- covering-index costing
- multi-column correlation statistics
- adaptive or policy-driven refresh strategies

Those are good next steps, but they should not block a focused v1.

---

## Conclusion

The recommended first milestone is:

- exact persisted table `row_count`
- persisted column `NDV`, `min`, and `max`
- stale-aware auto-maintenance
- `ANALYZE` as the exact refresh command
- cost-based single-table index selection
- better join build and fanout decisions

That is a meaningful improvement to planner quality, answers the performance question positively, and stays within a scope that can be implemented without rewriting the entire optimizer.

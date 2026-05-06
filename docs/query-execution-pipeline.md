# Query Execution Pipeline

This document describes how CSharpDB processes a SQL query from text to results. It is
intended for contributors and advanced users who want to understand how the engine makes
planning decisions.

---

## Pipeline Overview

```
SQL Text
  │
  ▼
┌──────────────┐
│  Tokenizer   │  CSharpDB.Sql/Tokenizer.cs
└──────┬───────┘
       │ Token stream
       ▼
┌──────────────┐
│   Parser     │  CSharpDB.Sql/Parser.cs
└──────┬───────┘
       │ Abstract Syntax Tree (AST)
       ▼
┌──────────────┐
│Query Planner │  CSharpDB.Execution/QueryPlanner.cs
│              │  CSharpDB.Execution/CardinalityEstimator.cs
└──────┬───────┘
       │ Physical operator tree
       ▼
┌──────────────┐
│  Execution   │  CSharpDB.Execution/Operators.cs
└──────┬───────┘
       │ Row-at-a-time or batched iteration
       ▼
   Query Result
```

---

## 1. Tokenization

The tokenizer (`Tokenizer.cs`) performs a single pass over the SQL string, producing a
stream of `Token(TokenType, string value, int position)` objects.

- 120+ token types covering keywords, literals, operators, identifiers, and punctuation
- Case-insensitive keyword matching via dictionary lookup
- Handles SQL comments (`-- ...`), string escaping (`''`), and parameter tokens (`@name`)
- Classifies numeric literals as `IntegerLiteral` or `RealLiteral`

---

## 2. Parsing

The parser (`Parser.cs`) is a recursive-descent parser that consumes tokens and builds a
typed AST.

### AST Node Categories

**Statements** — top-level SQL commands:

- DDL: `CreateTableStatement`, `AlterTableStatement`, `DropTableStatement`,
  `CreateIndexStatement`, `DropIndexStatement`, `CreateViewStatement`, `DropViewStatement`,
  `CreateTriggerStatement`, `DropTriggerStatement`, `AnalyzeStatement`
- DML: `InsertStatement`, `UpdateStatement`, `DeleteStatement`
- Query: `SelectStatement`, `CompoundSelectStatement` (set operations), `WithStatement` (CTEs)

**Table references:**

- `SimpleTableRef(TableName, Alias)` — single table or view
- `JoinTableRef(Left, Right, JoinType, Condition)` — binary join tree

**Expressions:**

- `LiteralExpression`, `ParameterExpression`, `ColumnRefExpression`
- `BinaryExpression` — arithmetic, comparison, logical operators
- `UnaryExpression` — `NOT`, unary minus
- `FunctionCallExpression` — aggregates and scalar functions
- `LikeExpression`, `InExpression`, `BetweenExpression`, `IsNullExpression`
- `ScalarSubqueryExpression`, `InSubqueryExpression`, `ExistsExpression`
- `CollateExpression`

### Parser Fast Paths

The parser detects common query shapes and produces lightweight metadata instead of a full
AST, skipping unnecessary allocation:

| Fast Path | Pattern Detected | Benefit |
|-----------|-----------------|---------|
| Simple SELECT | Single table, simple WHERE | Skips full AST construction |
| Primary key lookup | `SELECT ... WHERE pk = value` | Direct lookup metadata |
| Simple INSERT | `INSERT INTO ... VALUES (...)` | Lightweight insert path |

---

## 3. Query Planning

The `QueryPlanner` dispatches statements to type-specific handlers. For `SELECT` queries,
it classifies the query into a `SelectPlanKind` to choose an execution strategy:

| Plan Kind | Description |
|-----------|-------------|
| `FastPrimaryKeyLookup` | Direct integer PK lookup — fastest path |
| `FastIndexedLookup` | Equality lookup via index |
| `FastSimpleTableScan` | Full scan with basic filtering |
| `SimpleCountStar` | `COUNT(*)` using cached row count (no scan) |
| `SimpleScalarAggregateColumn` | Single aggregate on a column |
| `SimpleGroupedIndexAggregate` | GROUP BY on indexed column (streaming) |
| `General` | Full operator tree for complex queries |

Plan classifications are cached (up to 1024 entries) so repeated queries skip re-analysis.

### Cardinality Estimation

When `ANALYZE` has been run on a table, the `CardinalityEstimator` uses collected statistics
to estimate result sizes and guide operator selection.

**Statistics collected by ANALYZE:**

| Statistic | Scope | Description |
|-----------|-------|-------------|
| Distinct count | Per column | Number of unique values |
| Non-NULL count | Per column | Number of non-NULL values |
| Min / Max value | Per column | Value range |
| Frequent values | Per column | Top 8 values by occurrence |
| Histogram buckets | Per column | 16 quantile-based buckets for range estimation |
| Prefix distinct counts | Per index | Distinct values for each prefix of a composite index |

These planner statistics are inspectable through stable SQL projections:
`sys.planner_histograms`, `sys.planner_heavy_hitters`, and
`sys.planner_index_prefix_stats`. `EXPLAIN ESTIMATE FOR <query>` reports
which of those sources were used or ignored for a selected query shape without
executing the query.

**Estimation methods:**

- **Lookup selectivity:** `tableRows / distinctCount` for uniform distribution, adjusted
  for skew when the lookup value matches a known frequent value
- **Filter selectivity:** Per-column selectivity multiplied across AND-conjuncts. Supports
  equality, range, NULL, and discrete value list predicates.
- **Join cardinality:** `(leftRows × rightRows) / max(leftDistinct, rightDistinct)`,
  adjusted by non-NULL fraction and outer join semantics.

Without statistics, the planner uses heuristic fallback estimates.

### Debugging Slow Queries With EXPLAIN ESTIMATE

The Admin query tab's **Estimate** button runs `EXPLAIN ESTIMATE FOR <query>` and shows
the returned diagnostic rows in the **Plan** tab. You can also run the statement directly:

```sql
EXPLAIN ESTIMATE FOR
SELECT *
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.status = 'open'
LIMIT 25;
```

This is a planner diagnostic, not a runtime profile. It does not execute the target query,
scan user rows to measure actual cardinality, or report per-operator wall-clock time. It
explains what information the planner used, what it ignored, and why it preferred or
rejected common access paths.

**Result columns:**

| Column | Meaning |
|--------|---------|
| `node_id` | Diagnostic row identifier. |
| `parent_node_id` | Parent diagnostic row, so related decisions can be grouped into a tree. |
| `node_kind` | Planner area being described, such as `source`, `filter`, `join`, `lookup`, `join-reorder`, or `estimate-source`. |
| `target` | Table, index, predicate, join, or query fragment for the row. |
| `decision` | The planner choice or finding. Examples include `table`, `index-lookup`, `hash-join`, `bounded-dp`, `heavy-hitter-equality`, `histogram-range`, and `ignored-stale-stats`. |
| `estimated_rows` | Estimated rows produced by this source, predicate, join, or decision. |
| `estimated_cost` | Relative planner cost used for comparison. It is not elapsed time. |
| `stats_source` | Statistics or metadata source behind the estimate, such as `sys.table_stats`, `sys.column_stats`, `sys.planner_heavy_hitters`, `sys.planner_histograms`, or `sys.planner_index_prefix_stats`. |
| `stats_state` | Whether the source was `fresh`, `exact`, `estimated`, `missing`, `stale-ignored`, `unsupported`, or otherwise informational. |
| `detail` | Stable text with the extra inputs behind the decision. |

**What a healthy estimate usually looks like:**

- Important base tables have `sys.table_stats` with `stats_state` of `exact` or
  `estimated`.
- Selective equality predicates use `sys.column_stats` or `sys.planner_heavy_hitters`.
- Numeric range predicates use `sys.planner_histograms`.
- Correlated multi-column filters or joins use `sys.planner_index_prefix_stats`.
- Large joins use `hash-join`, `index-lookup`, `bounded-dp`, or another intentional
  strategy rather than falling all the way to an unconstrained nested-loop fallback.
- `estimated_rows` drops after selective filters and after joins that should narrow the
  result.

**Red flags:**

| Symptom in Plan rows | Likely meaning | First action |
|----------------------|----------------|--------------|
| `stats_state = missing` on important tables or columns | The planner is guessing. | Run `ANALYZE` for the table or database. |
| `stats_state = stale-ignored` or `decision = ignored-stale-stats` | Stats exist but were not trusted after table changes. | Run `ANALYZE table_name;`. |
| Large table source followed by `table-scan` when the predicate should be selective | Missing index, unsupported predicate shape, or missing stats. | Check indexes and rewrite predicate into a simple equality/range shape if possible. |
| Large join estimated with `nested-loop` fallback | No usable equi-join keys, no usable lookup index, or hash analysis could not extract keys. | Check join predicates and indexes on join keys. |
| `estimated_rows` stays close to the base table size after a selective predicate | Stats do not capture the selectivity, the predicate is unsupported, or values are stale. | Run `ANALYZE`, inspect heavy hitters/histograms, and check predicate shape. |
| `stats_source` is missing or heuristic on the expensive part of the query | Planner has little evidence for that choice. | Add stats with `ANALYZE`; consider an index if the predicate is selective. |
| Join reorder rows show fallback/greedy when a small inner-join chain was expected | Query shape is outside bounded reorder support or estimates are missing. | Check for outer joins, views/CTEs, unsupported predicates, or missing stats. |

**Debugging workflow:**

1. Run the query normally and note elapsed time and row count.
2. Click **Estimate** in Admin, or run `EXPLAIN ESTIMATE FOR <query>` manually.
3. Check `stats_state` first. If important rows are `missing` or `stale-ignored`, run:

   ```sql
   ANALYZE;
   ```

   or target the affected table:

   ```sql
   ANALYZE orders;
   ```

4. Re-run **Estimate** and look for whether the planner now uses column stats,
   heavy hitters, histograms, or composite-prefix stats.
5. Inspect large sources and joins:
   - Is a large table scanned even though the predicate is selective?
   - Is a join using hash or indexed lookup?
   - Does `estimated_rows` shrink where the query should become selective?
6. Inspect the backing stats directly when an estimate looks wrong:

   ```sql
   SELECT * FROM sys.table_stats WHERE table_name = 'orders';
   SELECT * FROM sys.column_stats WHERE table_name = 'orders';
   SELECT * FROM sys.planner_heavy_hitters WHERE table_name = 'orders';
   SELECT * FROM sys.planner_histograms WHERE table_name = 'orders';
   SELECT * FROM sys.planner_index_prefix_stats WHERE table_name = 'orders';
   ```

7. If stats are fresh but the plan still looks wrong, check query shape and indexes:
   simple equality/range predicates and equi-joins are easiest for the planner to cost.
   Expressions around indexed columns, non-equality joins, and unsupported predicates can
   force fallback estimates or operators.

### Public Planner Observability Phases

The current `EXPLAIN ESTIMATE` and `sys.planner_*` surfaces are phase 1 of public planner
observability. They make the optimizer's existing statistics and costing choices visible,
but they intentionally do not change normal query planning or execution behavior.

| Phase | Scope | Runtime performance impact |
|-------|-------|----------------------------|
| Phase 1: SQL-first planner diagnostics | Stable `sys.planner_histograms`, `sys.planner_heavy_hitters`, `sys.planner_index_prefix_stats`, and `EXPLAIN ESTIMATE FOR <query>` rowsets. | No direct gain expected. The value is explaining slow or surprising plans without adding overhead to ordinary queries. |
| Phase 2: Admin query debugger UX | Plan-tab warnings for missing/stale stats, clearer index chosen/rejected explanations, join-order visualization, copyable reports, and guided `ANALYZE` actions. | No direct engine gain expected. It shortens diagnosis time and reduces guesswork. |
| Phase 3: Runtime actuals / `EXPLAIN ANALYZE` | Execute the query and report actual rows, elapsed time, and estimate-vs-actual gaps per plan node. | Small overhead while profiling. The main value is identifying where estimates diverge from reality. |
| Phase 4: Adaptive re-optimization | Re-plan or adapt long-running queries when observed cardinality diverges materially from persisted statistics. | Direct gains are possible for stale-stat, skewed, and parameter-sensitive joins or filters, but this changes execution behavior and needs careful guardrails. |
| Phase 5: Broader public stats management | Typed .NET APIs, richer stats health reports, explicit stats refresh helpers, and expanded multi-column statistics inspection. | Mostly operational value. Performance gains come indirectly from keeping stats healthy and making bad plans easier to prevent. |

Phase 1 is complete for the current public surface. The next performance-enabling phase is
runtime actuals, because adaptive re-optimization needs evidence about where the estimate
was wrong before it can safely change an executing query.

### Index Selection

For each AND-separated predicate in the WHERE clause, the planner checks whether an
available index can satisfy it:

| Candidate | Rank | Description |
|-----------|:----:|-------------|
| Integer primary key equality | 0 | Best — direct B-tree lookup |
| Unique index equality | 1 | Single-row guarantee |
| Non-unique index equality | 2 | Accepted only if estimated rows ≤ 25% of table |

When multiple candidates match, the planner picks the lowest rank. Ties are broken by
estimated row count (fewer is better). Composite indexes are used when equality predicates
match the index prefix columns.

### Join Operator Selection

The planner tries join operators in preference order:

**1. Index Nested-Loop Join**

Requirements: INNER or LEFT join, right side is a simple table, equi-join condition exists,
and the right table has an index on the join key.

Cost decision (when statistics are available):
- Unique index: use if `leftRows ≤ rightRows × 8` (PK) or `leftRows ≤ rightRows × 2`
  (unique)
- Non-unique index: use if estimated lookup cost < hash join cost

Produces `IndexNestedLoopJoinOperator` (integer PK) or `HashedIndexNestedLoopJoinOperator`
(text/composite keys).

**2. Hash Join**

Requirements: equi-join condition with extractable key columns.

Build side selection: for INNER joins, builds the smaller estimated side. For LEFT/RIGHT
joins, respects outer table preservation.

**3. Nested-Loop Join (fallback)**

Cartesian product with join condition filtering. Used when no index or equi-join condition
is available.

### Join Reordering

For queries joining 4–6 tables, the planner applies dynamic-programming-based join
reordering to find the lowest-cardinality ordering. Results are cached per query shape.
Views, CTEs, and system tables are excluded from reordering.

---

## 4. Execution

### Iterator Model

Operators implement a pull-based iterator protocol:

```csharp
interface IOperator : IAsyncDisposable
{
    ColumnDefinition[] OutputSchema { get; }
    ValueTask OpenAsync();
    ValueTask<bool> MoveNextAsync();
    DbValue[] Current { get; }
}
```

The root operator is opened, then `MoveNextAsync` is called repeatedly until it returns
`false`. Results flow from leaf operators (scans) upward through the tree.

### Batch Execution

Performance-critical operators also implement `IBatchOperator`, which yields `RowBatch`
objects containing up to 64 rows per batch. This enables SIMD-friendly memory layout and
reduces per-row overhead.

```csharp
interface IBatchOperator : IAsyncDisposable
{
    ValueTask OpenAsync();
    ValueTask<bool> MoveNextBatchAsync();
    RowBatch CurrentBatch { get; }
}
```

### Expression Evaluation

Expressions are evaluated through two paths:

| Path | Used When | How |
|------|-----------|-----|
| Interpreter | One-off evaluation | Recursive AST walk with per-row schema lookups |
| Compiled | Hot paths (filters, projections) | AST compiled to `Func<DbValue[], DbValue>` delegate with bound column indices. Cached (up to 4096 entries). |

### Operator Catalog

#### Scan Operators

| Operator | Description |
|----------|-------------|
| `TableScanOperator` | Sequential B+tree scan. Supports pre-decode filtering and projection pushdown. |
| `IndexScanOperator` | Ordered index scan with optional range bounds. |
| `IndexOrderedScanOperator` | Prefix scan with secondary range filtering. |
| `PrimaryKeyLookupOperator` | Direct row lookup by integer PK. Fastest scan path. |
| `UniqueIndexLookupOperator` | Single-row lookup via unique index. |

Projection variants (`PrimaryKeyProjectionLookupOperator`,
`IndexScanProjectionOperator`, etc.) fuse column selection into the scan to skip
unnecessary deserialization.

#### Join Operators

| Operator | Description |
|----------|-------------|
| `HashJoinOperator` | Hash table on build side, probed by probe side. Supports INNER, LEFT, RIGHT, CROSS. |
| `IndexNestedLoopJoinOperator` | Probes right table via integer PK index for each left row. |
| `HashedIndexNestedLoopJoinOperator` | Probes via hash-based index (text/composite keys). |
| `NestedLoopJoinOperator` | Cartesian product with condition filtering. |

#### Filter and Projection

| Operator | Description |
|----------|-------------|
| `FilterOperator` | Applies WHERE predicates. Optional compiled filter for hot paths. |
| `ProjectionOperator` | Column selection and expression evaluation. |
| `FilterProjectionOperator` | Fused filter + projection in a single pass. |

#### Aggregation

| Operator | Description |
|----------|-------------|
| `HashAggregateOperator` | Grouped aggregation via hash table (GROUP BY). |
| `ScalarAggregateOperator` | Single-row aggregation (no GROUP BY). |
| `IndexGroupedAggregateOperator` | Streaming GROUP BY on indexed column (no hash table). |
| `CompositeIndexGroupedAggregateOperator` | Streaming GROUP BY on composite index prefix. |
| `CountStarTableOperator` | Returns cached row count — no scan needed. |

Additional specialized variants exist for specific aggregate patterns
(`ScalarAggregateLookupOperator`, `FilteredScalarAggregateTableOperator`, etc.).

#### Sort and Limit

| Operator | Description |
|----------|-------------|
| `SortOperator` | Full sort (in-memory for small sets, merge sort for larger). |
| `TopNSortOperator` | Heap-based top-N for `ORDER BY ... LIMIT N` — avoids full sort. |
| `LimitOperator` | Truncates output after N rows. |
| `OffsetOperator` | Skips first N rows. |

#### Other

| Operator | Description |
|----------|-------------|
| `DistinctOperator` | Removes duplicates via hash set. |
| `MaterializedOperator` | Wraps pre-materialized rows (CTEs, subquery results). |

### Optimization Interfaces

Operators expose capability interfaces that enable cross-operator optimization:

| Interface | Purpose |
|-----------|---------|
| `IPreDecodeFilterSupport` | Push equality filters into the decode layer, eliminating rows before full deserialization |
| `IProjectionPushdownTarget` | Declare needed output columns so scans skip unused columns |
| `IEstimatedRowCountProvider` | Expose cardinality to parent operators for cost decisions |
| `IRowBufferReuseController` | Control whether row buffers are reused or cloned |

---

## 5. Worked Example

```sql
SELECT c.name, SUM(o.amount) AS total
FROM customers c
LEFT JOIN orders o ON o.cust_id = c.id
WHERE c.country = 'USA'
GROUP BY c.id, c.name
ORDER BY total DESC
LIMIT 10;
```

**Planning decisions:**

1. **Index check on `customers.country`:** Non-unique index found. Estimated 300K rows out
   of 1M — selectivity passes the 25% threshold → use `IndexScanOperator`.

2. **Join strategy:** Index exists on `orders.cust_id`. Estimated left rows (300K) ≤
   right rows × 8 → use `IndexNestedLoopJoinOperator`.

3. **Aggregation:** GROUP BY columns not aligned with an index → `HashAggregateOperator`.

4. **Sort + limit:** `ORDER BY total DESC LIMIT 10` → `TopNSortOperator` (heap of size 10,
   avoids sorting all groups).

**Resulting operator tree:**

```
LimitOperator (10)
  └─ TopNSortOperator (total DESC, N=10)
       └─ HashAggregateOperator (GROUP BY c.id, c.name; SUM(o.amount))
            └─ IndexNestedLoopJoinOperator (LEFT, o.cust_id = c.id)
                 ├─ IndexScanOperator (customers, country = 'USA')
                 └─ orders.cust_id index
```

**Execution:**

1. `IndexScanOperator` iterates the `country` index for `'USA'` entries, yielding customer
   rows in batches of 64.
2. For each customer, `IndexNestedLoopJoinOperator` probes the orders index on `cust_id`,
   yielding joined rows (or a NULL-padded row for LEFT JOIN when no orders exist).
3. `HashAggregateOperator` accumulates `SUM(amount)` per `(id, name)` group in a hash
   table, then emits all groups.
4. `TopNSortOperator` maintains a min-heap of 10 entries by `total DESC`, discarding rows
   below the current 10th largest.
5. `LimitOperator` yields the final 10 rows to the caller.

---

## Constants

| Constant | Value | Purpose |
|----------|-------|---------|
| Default batch size | 64 rows | `RowBatch` capacity |
| Expression cache | 4096 entries | Compiled expression delegate cache |
| Plan cache | 1024 entries | SELECT plan classification cache |
| Histogram buckets | 16 | Quantile buckets per column in ANALYZE |
| Frequent values | 8 | Top-N tracked per column in ANALYZE |
| Max join reorder leaves | 6 | DP-based reordering table limit |
| Max trigger depth | 16 | Recursive trigger invocation limit |
| Max FK cascade depth | 64 | Foreign key cascade recursion limit |

# CSharpDB Performance Guide

This guide is about getting real performance out of CSharpDB, not just turning knobs.
It is organized by workload scenario and grounded in the current benchmark and documentation set in this repo.

## Benchmark Basis

This guide uses the latest current sources:

- The latest perf guardrail report on March 29, 2026 passed `184/184` checks on the benchmark runner.
- The broader macro, direct-client, and hybrid comparison captures were refreshed on March 28, 2026.
- The published micro spot checks in `tests/CSharpDB.Benchmarks/README.md` and `BenchmarkDotNet.Artifacts/results/` provide the query-shape details used below.
- The benchmark README still includes some earlier March 29 narrative from a failing midday rerun; this guide uses the latest passing guardrail report as the current benchmark status.

Benchmark environment for the published numbers:

- CPU: Intel Core i9-11900K
- OS: Windows 11 (`10.0.26300` benchmark runner)
- Runtime: .NET 10
- Disk: NVMe SSD

Treat the numbers here as directional for decision-making, then rerun the benchmark suite on your hardware before locking in a tuning choice.

## Start Here

Use this table first. Most CSharpDB performance wins come from choosing the right mode and access path before touching advanced settings.

| Scenario | Start with | Why |
|----------|------------|-----|
| Durable local app, mostly point reads | File-backed + `UseDirectLookupOptimizedPreset()` | Best simple baseline for hot local reads |
| Durable app with a known hot working set | Hybrid incremental-durable + `HotTableNames` / `HotCollectionNames` | Best when you can pay open cost once and then hammer the same hot objects |
| Ephemeral cache or periodic snapshot workflow | In-memory + `SaveToFileAsync` when needed | Biggest write throughput win by far |
| Burst of related SQL reads | Reuse one `ReaderSession` per reader burst | Snapshot reuse is much cheaper than per-query session creation |
| Ordered/range SQL queries | Build indexes for the filter/sort columns and project only what you need | Covered and compact paths are major wins |
| Join/reporting workload | Add join indexes and run `ANALYZE` after bulk changes | Planner can use stats for build-side choice, selective lookups, and limited reordering |
| Write-heavy ingest | `UseWriteOptimizedPreset()` plus explicit transaction batching | Batching is the biggest durable-write lever in the current suite |
| Document/path queries | `Collection<T>` + `EnsureIndexAsync(...)` on fields and paths you query | Indexed collection paths are fast; unindexed predicate scans are not |
| ADO.NET app with frequent opens | Enable pooling explicitly | Pooling dominates open/close overhead in the current provider benchmarks |

## The Biggest Levers

If you remember only a few rules, use these:

1. Choose the right storage mode first.
2. Batch durable writes instead of auto-committing row-by-row.
3. Reuse `ReaderSession` for bursts of related SQL reads.
4. Create indexes that match the filter, join, and `ORDER BY` shape.
5. Prefer covered or narrow projections over `SELECT *`.
6. Run `ANALYZE` after bulk loads or major data-distribution changes.
7. Measure before enabling advanced knobs like caching indexes, durable batch windows, or WAL preallocation.

## Scenario 1: Hot Point Reads In A Local Embedded App

This is the default "application database" scenario: the app is already open, the database is local, and the hot path is `GetAsync`, primary-key lookups, or small indexed reads.

### What to do

- Start file-backed with `UseDirectLookupOptimizedPreset()`.
- Use `Collection<T>.GetAsync(...)` for pure key/document access.
- Use `SELECT ... WHERE id = ...` or equivalent indexed equality lookups for relational paths.
- Reuse a `ReaderSession` when you are doing many related reads from the same snapshot.

### Why

Current published steady-state numbers are already strong without exotic tuning:

- File-backed SQL point lookups in the hot steady-state harness were about `1.10M` ops/sec.
- File-backed collection point gets were about `1.78M` ops/sec.
- Hot-cache micro references in the benchmark README still reach roughly `4.16M` SQL PK lookups/sec and `2.63M` collection gets/sec on the benchmark runner.

For direct file-backed opens, the benchmark README's current recommendation is:

- `builder.UseDirectLookupOptimizedPreset()` for hot local lookup workloads

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseDirectLookupOptimizedPreset());

await using var db = await Database.OpenAsync("app.db", options);

await using var result = await db.ExecuteAsync(
    "SELECT id, name FROM users WHERE id = 42");
```

### When to use `Collection<T>`

Use the collection API when the workload is fundamentally:

- key by id
- document get/put/delete
- indexed field equality
- indexed path equality/range

That path skips the SQL front door entirely.

## Scenario 2: Burst SQL Reads While Writes Continue

This is the common local service pattern: a writer is active, but readers want a stable snapshot and tend to issue several related queries together.

### What to do

- Create one `ReaderSession` per concurrent reader.
- Reuse that session for a burst of related reads.
- Dispose each `QueryResult` before issuing the next query on the same session.

```csharp
using var reader = db.CreateReaderSession();

await using (var users = await reader.ExecuteReadAsync(
    "SELECT id, name FROM users WHERE tier = 'gold'"))
{
    while (await users.MoveNextAsync())
    {
    }
}

await using (var orders = await reader.ExecuteReadAsync(
    "SELECT id, total FROM orders WHERE customer_id = 42"))
{
    while (await orders.MoveNextAsync())
    {
    }
}
```

### Why

The current dedicated `ReaderSessionBenchmarks` show the setup penalty clearly:

- per-query reader-session point lookups are roughly `3x` slower than reused-session lookups
- reused reader-session lookups are effectively at the same floor as direct `ExecuteAsync`

The practical reading is simple:

- `ReaderSession` is the right tool for concurrent snapshot reads.
- Reusing it is almost always better than recreating it for every small query.

## Scenario 3: Ordered And Range SQL Queries

This is where query shape matters a lot. The main question is whether the engine can stay on index data, or whether it has to keep fetching wide base rows.

### What to do

- Index the filter or sort column.
- If possible, project only the indexed columns plus the row id.
- Prefer narrow projections over `SELECT *`.
- For multi-column filters, use composite indexes that match the left-to-right predicate shape.

```sql
CREATE INDEX idx_orders_created_at ON orders(created_at);

SELECT id, created_at
FROM orders
WHERE created_at BETWEEN @from AND @to
ORDER BY created_at
LIMIT 100;
```

### Why

The current `OrderByIndexBenchmarks` and README spot checks show large wins when the query stays on index data:

- `ORDER BY value LIMIT 100` on `100K` rows:
  - index-order scan: about `64.79 us`
  - covered index-order scan: about `23.17 us`
- `WHERE value BETWEEN ...` on `100K` rows:
  - row fetch path: about `59.48 ms`
  - covered projection: about `18.49 ms`
- Composite covered projection on `100K` rows:
  - about `2.523 us`

This is one of the clearest themes in the repo's benchmarks:

- the engine is fast at finding qualifying rows
- the expensive part is often row fetch and materialization
- covered and compact projections are a major lever

### Practical rule

If the business endpoint only needs `id`, `status`, and `created_at`, do not ask CSharpDB to materialize twelve more columns.

## Scenario 4: Reporting, Joins, And Selective Filters

This is the "real SQL" scenario: non-trivial joins, selective predicates, and planner choice starts to matter.

### What to do

- Create indexes on join keys and on selective filter columns.
- Run `ANALYZE` after bulk loads and after major distribution changes.
- Inspect `sys.table_stats` and `sys.column_stats` when a plan is not behaving as expected.

```sql
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_order_items_order ON order_items(order_id);

ANALYZE;

SELECT * FROM sys.table_stats ORDER BY table_name;
SELECT * FROM sys.column_stats ORDER BY table_name, ordinal_position;
```

### Why

Current planner work already uses persisted stats for:

- selective lookup choice
- join method choice
- hash build-side choice
- limited inner-join reordering

The benchmarks show this matters:

- `INNER JOIN 1Kx20K (planner swap build side)`: about `7.16 ms`
- `INNER JOIN 1Kx20K (no swap via view)`: about `11.43 ms`
- `INNER JOIN on right PK (index nested-loop)`: about `482.70 us`
- `INNER JOIN on right PK (forced hash)`: about `833.54 us`

The practical rule:

- if a reporting or join workload matters, index it and analyze it
- if you skip `ANALYZE`, you are leaving planner quality on the table

## Scenario 5: Durable Write-Heavy Ingest

This is where the wrong default usage hurts most. If you write one durable row at a time, the fixed WAL/fsync cost dominates.

### What to do

- Start with `UseWriteOptimizedPreset()`.
- Batch writes in explicit transactions.
- Measure with your real batch size before touching advanced durable-write knobs.

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

await using var db = await Database.OpenAsync("ingest.db", options);

await db.BeginTransactionAsync();
try
{
    foreach (var row in rows)
    {
        await db.ExecuteAsync(
            $"INSERT INTO events VALUES ({row.Id}, {row.TimestampTicks})");
    }

    await db.CommitAsync();
}
catch
{
    await db.RollbackAsync();
    throw;
}
```

### Why

The current durable batching benchmarks show that batching is the largest lever in the repo:

- auto-commit single-row SQL: about `270.5` ops/sec
- explicit transaction, `10` rows/commit: about `2,695.8` rows/sec
- explicit transaction, `100` rows/commit: about `26,999.5` rows/sec
- explicit transaction, `1000` rows/commit: about `197,256.9` rows/sec

That is the core write-performance message for CSharpDB:

- if you can batch, batch
- if you cannot batch, expect durable single-row throughput to be much lower

### Preset guidance

Current repo guidance is:

- start with `UseWriteOptimizedPreset()`
- do not assume `UseLowLatencyDurableWritePreset()` is faster on your workload

On the current benchmark runner:

- analyzed `UseWriteOptimizedPreset()`: about `267.8` ops/sec
- analyzed `UseLowLatencyDurableWritePreset()`: about `261.4` ops/sec

So the "low latency" preset is measure-first, not the default recommendation.

## Scenario 6: Multi-Writer Durable Contention

This is narrower than normal ingest: several writer tasks are hitting the same shared `Database` instance.

### What to do

- Keep the baseline at `UseWriteOptimizedPreset()`.
- For `8` in-process writers, benchmark WAL preallocation first.
- For `4` in-process writers, benchmark a small durable batch window first.
- Do not cargo-cult batch-window tuning from the shared-writer case into the single-writer case.

### Why

The current concurrent write diagnostics say:

- best `8`-writer row: `W8_Batch0_Prealloc1MiB` at about `1078.6` commits/sec
- close followers: `W8_Batch250us` at `1070.4`, `W8_Batch0` at `1068.2`
- best `4`-writer row: `W4_Batch250us` at about `553.4` commits/sec

But the single-writer durable diagnostics say the opposite for batch windows:

- `BatchWindow(250us)`: about `267.2` ops/sec
- `BatchWindow(1ms)`: about `65.8` ops/sec

So:

- shared-writer contention can justify small wait windows or preallocation
- single-writer durable paths usually cannot

## Scenario 7: Cold File Reads, Tooling, And Cache-Pressured Workloads

This is the "open a big local file and probe around" scenario: admin tools, inspection tools, one-shot analytics, or workloads that do not stay hot.

### What to do

- Start with `UseDirectColdFileLookupPreset()` for direct file-backed opens.
- If your hot pages are still living in the WAL, benchmark a small WAL read cache.
- For deliberately bounded-cache workloads, start around `UseHybridFileCachePreset(2048)` scale rather than tiny caches.

```csharp
var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseDirectColdFileLookupPreset());

await using var db = await Database.OpenAsync("cold.db", options);
```

### Why

The current cold-read benchmarks show:

- SQL cold lookup, copy-based path: `28.704 us`
- SQL cold lookup, WAL-backed with `128`-page WAL cache: `19.83 us`

The current file-backed collection tuning matrix also shows cache size matters:

- indexed collection lookup at `16` pages: `54.15 us`
- indexed collection lookup at `2048` pages: `20.57 us`

Two practical takeaways:

- cold local reads benefit from the mapped-file read path
- tiny bounded caches are expensive unless the workload is truly memory-constrained

### What not to do

Do not assume caching indexes are automatically a win.

The current tuning matrix found `UseCachingIndexes` neutral-to-negative on these lookup workloads, including a case where a reused reader-session SQL lookup worsened from `41.36 us` to `212.94 us`.

## Scenario 8: Known Hot Set With Durable Backing

This is the hybrid-mode sweet spot: the database must stay durable on disk, but the same tables or collections are hot enough that prewarming them once is worth it.

### What to do

- Use `OpenHybridAsync(...)` with `IncrementalDurable`.
- Set `HotTableNames` and/or `HotCollectionNames` for the truly hot objects.
- Use this only when the process is long-lived enough to amortize open cost.

```csharp
await using var db = await Database.OpenHybridAsync(
    "app.db",
    new DatabaseOptions(),
    new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.IncrementalDurable,
        HotTableNames = ["users", "orders"],
        HotCollectionNames = ["sessions"]
    });
```

### Why

In the dedicated resident hot-set harness:

- hybrid hot-set incremental-durable SQL burst: `625.76K` ops/sec
- hybrid hot-set incremental-durable collection burst: `707.38K` ops/sec

But the trade-off is open cost:

- hybrid hot-set SQL open only: about `87.39 ms`
- hybrid hot-set collection open only: about `128.59 ms`

This mode is excellent when:

- open happens once
- the process stays up
- the same objects stay hot

It is a bad fit when:

- open latency is user-visible and frequent
- the hot set is not stable

### Constraints

Current hot-set warming is intentionally narrow:

- supported only for `IncrementalDurable`
- rejected for snapshot mode
- rejected for bounded caches and custom page-cache factories

## Scenario 9: In-Memory Database As A Performance Tool

This is the right answer when durability is optional during the hot path and you can snapshot explicitly.

### What to do

- Use `OpenInMemoryAsync()` for new ephemeral stores.
- Use `LoadIntoMemoryAsync()` when you want to import a file, work in memory, then save back later.
- Use `SaveToFileAsync()` at explicit persistence boundaries.

```csharp
await using var db = await Database.OpenInMemoryAsync();

// hot path
await db.ExecuteAsync("INSERT INTO cache VALUES (1, 'hot')");

// persistence boundary
await db.SaveToFileAsync("cache.db");
```

### Why

In-memory mode is the strongest write-throughput lever in the repo:

- hot steady-state in-memory SQL single insert: roughly `315K` ops/sec
- hot steady-state in-memory collection single put: roughly `294K` ops/sec
- in-memory batched SQL rows/sec is far above file-backed durable mode in the current macro suite

Use it when the workflow is:

- cache
- local-first scratch workspace
- import, process, export
- periodically checkpointed embedded store

Do not use it when every commit must already be durable before control returns.

## Scenario 10: Collection And Document Workloads

The collection API can be very fast, but only if you use the indexed surfaces when the workload needs them.

### What to do

- Use `GetAsync` for key lookups.
- Use `EnsureIndexAsync(...)` for fields or paths that you query repeatedly.
- Use `FindByIndexAsync(...)`, `FindByPathAsync(...)`, and `FindByPathRangeAsync(...)` for indexed access.
- Keep the number of indexes honest; every extra index adds write maintenance cost.

```csharp
var users = await db.GetCollectionAsync<User>("users");

await users.EnsureIndexAsync(x => x.Email);
await users.EnsureIndexAsync("$.address.city");
await users.EnsureIndexAsync("$.tags[]");

await foreach (var match in users.FindByPathAsync("$.address.city", "Seattle"))
{
}
```

### Why

Current indexed collection path numbers are strong:

- `FindByPath("$.address.city")`: about `568.9 ns`
- `FindByPath("$.tags[]")`: about `474.8 ns`
- `FindByPath("$.orders[].sku")`: about `599.3 ns`
- integer path range with `1024` matches: about `552.79 us`
- text path range with `1000` matches: about `545.69 us`

But writes do pay for indexes:

- `PutAsync` with secondary indexes, insert case: about `12.453 us`
- `PutAsync` with secondary indexes, update case: about `29.079 us`
- `DeleteAsync` with secondary indexes: about `47.314 us`

### Practical rule

`FindAsync(predicate)` is convenient, but it is still a scan.
If the predicate matters to latency, promote it to an indexed field or indexed path.

## Scenario 11: ADO.NET-Heavy Apps

This is the integration scenario: existing .NET data-access code, ORMs, utilities, or apps that open and close connections frequently.

### What to do

- Turn pooling on explicitly if you open and close connections often.
- Use private `:memory:` when you do not need cross-connection sharing.
- Use named shared `:memory:name` only when you actually need multiple live connections against the same in-process memory database.
- Reuse command objects when it keeps your application code cleaner, but do not expect command preparation alone to be your biggest win.

```text
Data Source=myapp.db;Pooling=true;Max Pool Size=16
```

```text
Data Source=:memory:
```

```text
Data Source=:memory:shared-cache
```

### Why

The current ADO.NET benchmark results are clear:

- open+close with pooling off: about `8.9 ms`
- open+close with pooling on: about `1.9 us`

That is the main provider-level lever.

For in-memory ADO.NET:

- `ExecuteScalar` on private `:memory:`: `238.7 ns`
- `ExecuteScalar` on named shared `:memory:name`: `347.1 ns`
- insert on private `:memory:`: `2.722 us`
- insert on named shared `:memory:name`: `2.903 us`

Prepared commands were roughly neutral in the current provider microbenchmarks:

- parameterized select: about `298.1 us`
- prepared select with reused command: about `296.7 us`

So for most ADO.NET apps:

- pooling matters a lot
- memory mode choice matters a little
- command preparation is not the first lever to chase

## What Usually Hurts

These are the common ways to leave performance on the floor in CSharpDB:

- Auto-committing every durable row when batching is possible.
- Recreating a `ReaderSession` for every small read.
- Using `SELECT *` on range and ordered queries that could be covered or at least narrow.
- Skipping `ANALYZE` after bulk loads, then blaming the planner.
- Using `FindAsync(...)` on large collections for fields that should be indexed.
- Turning on advanced knobs like caching indexes, durable batch windows, or WAL preallocation without measuring the exact scenario they are meant for.
- Paying hybrid hot-set open cost for short-lived processes.
- Using named shared `:memory:` when private `:memory:` would do.

## Recommended Workflow

When tuning a real CSharpDB workload, use this order:

1. Pick the storage mode: file-backed, hybrid, or in-memory.
2. Pick the API surface: SQL, collection API, or ADO.NET.
3. Add the right indexes.
4. Shape queries so they stay narrow and covered when possible.
5. Run `ANALYZE`.
6. Reuse `ReaderSession` or pooled connections where appropriate.
7. Batch writes.
8. Only then benchmark advanced knobs.

## Source Map

Primary references for this guide:

- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md)
- [Latest Guardrail Report](../tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md)
- [Engine README](../src/CSharpDB.Engine/README.md)
- [ADO.NET Provider README](../src/CSharpDB.Data/README.md)
- [Architecture Guide](architecture.md)
- [Roadmap](roadmap.md)

# CSharpDB Versus SQLite: How We Compare Them Fairly, and How To Get the Most Out of CSharpDB

If you want to compare an embedded database against SQLite, it is very easy to
publish a misleading chart without meaning to.

The trap is always the same: one side gets measured at the engine layer, the
other side gets measured through a provider or ORM, durability settings do not
match, batching rules differ, and the final table looks precise while answering
the wrong question.

This post is the practical version of how we compare CSharpDB and SQLite in
this repository. It also doubles as a guide for getting the best insert
performance out of CSharpDB when you are using it in a real application.

The short version is:

- Compare engine to engine, provider to provider, and ORM to ORM.
- Keep durability contracts aligned.
- Reuse prepared work on both sides.
- Treat single-writer bulk ingest and concurrent writers as different
  workloads.
- Use the CSharpDB surfaces that avoid per-row SQL and planner overhead when
  you care about write throughput.

## Why Fair Comparisons Are Hard

The phrase "CSharpDB versus SQLite" sounds like one benchmark, but in practice
it is at least four different comparisons:

1. CSharpDB engine API versus SQLite engine API.
2. CSharpDB ADO.NET provider versus SQLite ADO.NET provider.
3. CSharpDB EF Core provider versus SQLite EF Core provider.
4. Single-writer durable bulk ingest versus concurrent writer contention.

Those are not interchangeable.

If one result uses the raw engine on one side and ADO.NET on the other, the
chart is already mixing surfaces. If one result uses file-backed durable WAL
and the other uses shared memory, it is no longer a storage-engine comparison.
If one side prepares a command once and reuses it while the other reparses SQL
for every row, the result mostly measures statement setup rather than insert
throughput.

That is why the CSharpDB benchmark suite splits these questions on purpose.

## The Comparison Rules We Use

The repo follows a few rules consistently.

### Rule 1: Match the Layer

We keep these pairings separate:

- Engine versus engine.
- ADO.NET versus ADO.NET.
- EF Core versus EF Core.

That means:

- If we compare CSharpDB directly through `Database` and `InsertBatch`, we
  compare it against SQLite's native C API rather than against
  `Microsoft.Data.Sqlite`.
- If we compare `CSharpDB.Data`, we compare it against
  `Microsoft.Data.Sqlite`.
- If we compare the EF Core provider, we compare it against EF Core on SQLite.

### Rule 2: Match the Durability Contract

For the primary durable write comparison, both sides are file-backed and
durable.

For the matched SQLite baseline in
[SqliteComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs),
SQLite uses:

- `journal_mode=WAL`
- `synchronous=FULL`
- explicit transaction batching
- prepared statement reuse

For the corresponding CSharpDB bulk path in
[DurableSqlBatchingBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs),
the comparison uses the engine's durable file-backed path with the write-heavy
storage preset and the same logical batch shape.

### Rule 3: Match the Statement Shape

SQLite absolutely does support the classic "prepare once, bind many times"
workflow. In the native API that is `sqlite3_prepare_v2`, followed by repeated
binds and `sqlite3_step` / reset cycles. In ADO.NET the equivalent is
`DbCommand.Prepare()` plus reusing parameters.

CSharpDB has two comparable fast paths, depending on the layer:

- At the engine layer, use `Database.PrepareInsertBatch(...)`.
- At the ADO.NET layer, use `DbCommand.Prepare()` through `CSharpDB.Data`.

If you compare SQLite with prepared reuse against CSharpDB with one fresh SQL
string per row, you are not comparing the engines fairly.

### Rule 4: Keep Single-Writer and Multi-Writer Results Separate

Single-writer durable ingest answers one question:

How fast can each engine append committed rows when batching and durability are
held constant?

Concurrent writers answer a different question:

How does each engine behave when several local writers are inserting at the
same time against the same database?

Those are different workloads with different limits. They should not be merged
into one headline number.

## What the Current Durable Bulk Numbers Say

The cleanest apples-to-apples insert comparison in this repo is still the
single-writer durable four-column bulk row:

- schema:
  `id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT`
- monotonic primary keys
- explicit transaction batching
- file-backed durable mode
- median-of-3 reruns on the same runner

As of April 21, 2026 (promoted from the release-core run with
`PASS=185, WARN=0, SKIP=0, FAIL=0`), the latest matched rerun on this machine
produced:

| Scenario | Rows/sec | P50 | P99 |
|---|---:|---:|---:|
| CSharpDB `InsertBatch B1000` | `204,028` | 4.1034 ms | 9.5599 ms |
| SQLite prepared bulk `B1000` | `192,059` | 4.7479 ms | 18.1078 ms |
| CSharpDB `InsertBatch B10000` | `798,254` | 8.7019 ms | 119.0664 ms |
| SQLite prepared bulk `B10000` | `539,562` | 16.6275 ms | 43.3034 ms |

That means, on this runner:

- CSharpDB is about `106.2%` of SQLite at `B1000`.
- CSharpDB is about `147.9%` of SQLite at `B10000`.

Source artifacts:

- [durable-sql-batching-20260421-214227-median-of-3.csv](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260421-214227-median-of-3.csv)
- [sqlite-compare-20260421-222824-median-of-3.csv](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260421-222824-median-of-3.csv)
- Full scorecard: [tests/CSharpDB.Benchmarks/SQLITE_COMPARISON.md](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/SQLITE_COMPARISON.md)

The important part is not just that CSharpDB is ahead on this matched row. The
important part is what that row actually proves:

- It proves the primary durable monotonic bulk path is now competitive.
- It does not prove every insert shape is faster.
- It does not erase the cost of secondary indexes.
- It does not erase the random-key locality cliff.
- It does not answer hot right-edge multi-writer contention.

That distinction matters if you want the public write-up to stay honest.

## The Three Real Benchmark Surfaces

The benchmark suite now lets us talk about three comparison layers clearly.

### 1. Direct Engine Versus SQLite C API

This comparison lives in
[ConcurrentSqliteCApiComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentSqliteCApiComparisonBenchmark.cs).

On the CSharpDB side:

- one shared `Database` instance is opened for the scenario
- one writer task is created per logical writer
- each writer loops for the measurement window
- each loop performs one insert commit
- the surface is either raw `Database.ExecuteAsync(sql)` or a reused
  one-row `InsertBatch`
- concurrent insert routing can opt into
  `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`

On the SQLite side:

- one native SQLite connection handle is opened per writer
- one prepared native insert statement is created per writer
- each loop rebinds values, calls `sqlite3_step`, then resets the statement
- SQLite runs with `WAL`, `FULL`, and a `busy_timeout`

This is the right layer for answering engine-versus-engine concurrency
questions.

### 2. ADO.NET Versus ADO.NET

This comparison lives in
[ConcurrentAdoNetComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentAdoNetComparisonBenchmark.cs)
and
[StrictInsertComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/StrictInsertComparisonBenchmark.cs).

The provider rules are intentionally symmetrical:

- one connection per writer
- one prepared command per writer
- one `ExecuteNonQueryAsync()` call per insert

The strict single-writer comparison also covers:

- raw SQL single inserts
- prepared single inserts
- raw SQL batched inserts
- prepared batched inserts
- file-backed CSharpDB ADO.NET opened with
  `Storage Preset=WriteOptimized` and `Embedded Open Mode=Direct`
- file-backed SQLite ADO.NET with `journal_mode=WAL` and `synchronous=FULL`

This is the right layer for answering provider overhead questions. It is not
the right layer for making storage-engine claims unless the durability contract
also matches.

### 3. EF Core Versus EF Core

This comparison lives in
[EfCoreComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/EfCoreComparisonBenchmark.cs).

Today it covers:

- `--efcore-compare`: single-row and `100`-row `SaveChangesAsync()` with one
  open connection held for the timed run
- `--efcore-compare-hybrid-shared-connection`: the same insert shapes with
  one externally-owned open connection reused across short-lived `DbContext`
  instances
- `--efcore-compare-auto-open-close`: the same insert shapes with EF-managed
  auto-open/close left in the measurement
- CSharpDB EF Core provider versus SQLite EF Core provider
- CSharpDB EF Core configured with `UseStoragePreset(WriteOptimized)` and
  `UseEmbeddedOpenMode(Direct)`

It does not currently provide a dedicated concurrent EF Core benchmark. So if
you are writing a blog post, the fair statement is:

We have an EF Core comparison surface, but the concurrent comparison story is
currently strongest at the engine and ADO.NET layers.

## How Concurrent Writer Tests Differ Between the Two Databases

This is usually where comparison posts become hand-wavy, so it is worth being
explicit.

For CSharpDB concurrent engine tests:

- writers share one in-process engine
- the engine decides whether inserts stay serialized or route through isolated
  write transactions
- disjoint explicit-key workloads can benefit from the concurrent insert path
- hot right-edge workloads still behave very differently from disjoint-key
  workloads

For SQLite concurrent engine tests:

- each writer has its own native handle to the same database file
- every writer still competes inside SQLite's WAL write rules
- prepared statements are reused, but the engine remains effectively
  single-writer at the durable file boundary

So the concurrency comparison is not "who has more threads." It is "how does
each engine's write architecture behave when several local writers are active."

## How To Get the Best Performance Out of CSharpDB

This is the part that matters if you are not just comparing benchmarks but also
trying to ship a fast application.

### Start With the Engine Fast Path

If you are using the engine directly, the highest-value insert surface is
`PrepareInsertBatch(...)`, not one dynamically built SQL string per row.

```csharp
using CSharpDB.Engine;
using CSharpDB.Primitives;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

await using var db = await Database.OpenAsync("ingest.db", options);

await db.ExecuteAsync("""
    CREATE TABLE IF NOT EXISTS bench (
        id INTEGER PRIMARY KEY,
        value INTEGER,
        text_col TEXT,
        category TEXT
    )
    """);

var batch = db.PrepareInsertBatch("bench", initialCapacity: 1000);

await db.BeginTransactionAsync();
try
{
    for (int block = 0; block < 100; block++)
    {
        for (int i = 0; i < 1000; i++)
        {
            int id = (block * 1000) + i;
            batch.AddRow(
                DbValue.FromInteger(id),
                DbValue.FromInteger(id),
                DbValue.FromText("durable_batch"),
                DbValue.FromText("Alpha"));
        }

        await batch.ExecuteAsync();
    }

    await db.CommitAsync();
}
catch
{
    await db.RollbackAsync();
    throw;
}
```

The reason this matters is straightforward: it keeps the workload on the engine
bulk path instead of paying repeated SQL parse and planner cost for every row.

### Batch Commits Aggressively Before You Reach for Exotic Knobs

The first major durable-write lever is still rows per commit.

The practical starting guidance from the current benchmark matrix is:

- Start at `1000` rows per commit.
- Measure `10000` only for dedicated ingest jobs that can tolerate larger
  commit latency and larger WAL bursts.

Single-row durable auto-commit inserts are valid for correctness tests and for
latency-sensitive write APIs, but they are not the right baseline for bulk
ingest.

### Use the Write-Heavy Storage Preset

For durable file-backed ingest, start with
`UseWriteOptimizedPreset()`.

That is the current recommended storage baseline for write-heavy engine
workloads and is documented in both:

- [CSharpDB.Engine README](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Engine/README.md)
- [CSharpDB.Storage README](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Storage/README.md)

### Prefer Monotonic Primary Keys

Monotonic primary keys are materially cheaper than random primary keys.

Why:

- they keep the table B-tree growing on the right edge
- they reduce split-heavy locality loss
- they make the primary insert path more predictable

The durable batching matrix in this repo continues to show that random-key
primary inserts are still a major throughput cliff relative to monotonic keys.

### Keep Secondary Indexes Off the Hot Ingest Path When You Can

If you only remember one design rule besides batching, it should be this one:

Every extra secondary index costs you per-row write work.

The recent Plan 5 work materially improved duplicate-bucket maintenance and
composite-index locality, but the remaining slope on realistic insert shapes is
still mostly secondary-index maintenance rather than the PK-only bulk path.

If your workflow allows it, the easy win is still:

1. load data first
2. build or refresh indexes after the ingest step

### Use Prepared Commands at the Provider Layer

If you are not using the engine directly and instead go through `CSharpDB.Data`,
the equivalent rule is to reuse prepared commands.

That looks like ordinary ADO.NET:

- create one `DbCommand`
- add parameters once
- call `Prepare()`
- update parameter values per execution

This is the right analogue to SQLite's prepare-once, bind-many workflow.

### Use Concurrent Insert Mode Only for the Right Shape

CSharpDB does have a concurrent insert mode:

`ImplicitInsertExecutionMode.ConcurrentWriteTransactions`

But it is not a magic global acceleration switch.

The measured guidance is:

- Keep the default serialized insert mode for hot right-edge insert loops.
- Measure concurrent insert mode when several tasks insert into disjoint
  explicit key ranges on one shared `Database`.
- If each writer needs multiple statements to commit atomically, use the
  explicit write-transaction APIs instead.

That is the difference between using a feature because it exists and using it
because the workload shape actually matches it.

### Treat Durable Group Commit as a Measure-First Lever

`UseDurableGroupCommit(...)` can help when several writers are already
overlapping and you are willing to trade some latency for better flush sharing.

But it should come after:

- batching
- prepared work reuse
- key-shape cleanup
- avoiding unnecessary indexes

In other words, do not use group commit as a substitute for basic workload
hygiene.

## The Honest Public Conclusion

If you are turning this into a blog post, the cleanest conclusion is:

- CSharpDB is now ahead of the matched SQLite baseline on the primary durable
  monotonic bulk row on this runner.
- That result is real because the comparison was aligned at the schema,
  batching, and durability levels.
- The remaining hard performance work is no longer the PK-only monotonic bulk
  path.
- The bigger remaining costs are secondary-index maintenance, random-key
  locality, and hot right-edge multi-writer contention.

That is a much stronger statement than "CSharpDB beats SQLite" because it is
specific enough to survive scrutiny.

## Reproducing the Main Comparisons

Matched durable bulk rows:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching-scenario BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching-scenario BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --sqlite-compare --repeat 3 --repro
```

Concurrent engine versus SQLite C API:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-sqlite-capi-compare --repeat 3 --repro
```

Concurrent ADO.NET provider comparison:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-adonet-compare --repeat 3 --repro
```

EF Core comparison:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare-hybrid-shared-connection --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare-auto-open-close --repeat 3 --repro
```

## Related Source Material

If you want the lower-level internal notes behind this write-up, start here:

- [Existing comparison guide](/C:/Users/maxim/source/Code/CSharpDB/docs/query-and-durable-write-performance/csharpdb-vs-sqlite-performance-guide.md)
- [Plan 5: Raw Rows/Sec Vs SQLite](/C:/Users/maxim/source/Code/CSharpDB/docs/programmatic-insert-performance/05-raw-rows-per-sec-vs-sqlite.md)
- [Benchmark suite README](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/README.md)
- [CSharpDB.Engine README](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Engine/README.md)
- [CSharpDB.Storage README](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Storage/README.md)

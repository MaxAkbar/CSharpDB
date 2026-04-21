# How To Compare CSharpDB and SQLite Fairly

This note is the practical comparison guide behind the benchmark matrix in this
repo.

It has two goals:

1. explain how to compare CSharpDB and SQLite without mixing unlike surfaces
2. show which CSharpDB features matter most when you want maximum write
   throughput

The short version is simple:

- compare engine to engine, provider to provider, and ORM to ORM
- keep durability and batching rules aligned
- use the CSharpDB insert surfaces that avoid per-row SQL/planner overhead
- separate disjoint-key concurrency from hot right-edge contention

## Why This Matters

It is easy to make an unfair database comparison by accident.

These are not equivalent:

- CSharpDB engine API versus SQLite through ADO.NET
- durable file-backed inserts versus shared-memory non-durable inserts
- single-writer monotonic primary keys versus many writers fighting over the
  same right edge
- prepared and reused insert commands on one side versus ad-hoc SQL strings on
  the other

The benchmark suite in this repo deliberately splits those questions apart so
the comparison stays defensible.

## The Four Comparison Surfaces

### 1. Durable Single-Writer Bulk Inserts

This is the primary apples-to-apples insert comparison.

- CSharpDB side:
  [DurableSqlBatchingBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs)
- SQLite side:
  [SqliteComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs)

Controls:

- file-backed durable mode
- same four-column schema
  `id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT`
- same monotonic primary-key insert pattern
- explicit transaction batching
- prepared statement reuse on the SQLite side
- batch sizes `1000` and `10000`
- SQLite uses `journal_mode=WAL` and `synchronous=FULL`

This surface answers:

What is the raw durable insert throughput gap once the comparison is aligned?

### 2. Direct Engine vs SQLite C API Under Concurrent Writers

This is the engine-level multi-writer comparison.

- Harness:
  [ConcurrentSqliteCApiComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentSqliteCApiComparisonBenchmark.cs)

Controls:

- one durable file-backed database per run
- one local writer task per writer
- same writer counts, key patterns, and run durations
- CSharpDB is exercised directly through the engine
- SQLite is exercised directly through the native C API

This surface answers:

How do the two engines behave when multiple local writers are issuing inserts at
the same time against the same database?

### 3. ADO.NET vs ADO.NET

This is the provider-layer comparison.

- Harness:
  [StrictInsertComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/StrictInsertComparisonBenchmark.cs)
- Harness:
  [ConcurrentAdoNetComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentAdoNetComparisonBenchmark.cs)

Controls:

- `StrictInsertComparisonBenchmark` is the file-backed single-writer provider comparison
- on the CSharpDB side it now opens through `CSharpDB.Data` with
  `Storage Preset=WriteOptimized` and `Embedded Open Mode=Direct`
- SQLite stays file-backed through `Microsoft.Data.Sqlite` with
  `journal_mode=WAL` and `synchronous=FULL`
- one writer task per connection
- one prepared command per writer
- same explicit key patterns
- CSharpDB through `CSharpDB.Data`
- SQLite through `Microsoft.Data.Sqlite`

Important limitation:

- `ConcurrentAdoNetComparisonBenchmark` is intentionally shared-memory and
  non-durable on both sides, so it does not use the new CSharpDB embedded
  tuning surface

This surface answers:

How much provider overhead and contention behavior comes from the ADO.NET layer
rather than the durable storage engine?

### 4. EF Core vs EF Core

This is the ORM-layer comparison.

- Harness:
  [EfCoreComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/EfCoreComparisonBenchmark.cs)

Current scope:

- `--efcore-compare`: single-row and `100`-row `SaveChangesAsync` with one
  open connection held for the timed run
- `--efcore-compare-hybrid-shared-connection`: the same insert shapes with
  one externally-owned open connection reused across short-lived `DbContext`
  instances
- `--efcore-compare-auto-open-close`: the same insert shapes with EF-managed
  auto-open/close left in the measurement
- CSharpDB EF Core provider versus SQLite EF Core provider in both cases

Important limitation:

- as of April 20, 2026 there is not a dedicated concurrent EF Core comparison
  harness in this repo

This surface answers:

What happens once you move up to EF Core instead of using the engine or ADO.NET
directly?

## Current Durable Insert Snapshot

As of April 20, 2026, the latest matched single-writer durable rerun on this
runner produced:

| Surface | Rows/sec |
|---|---:|
| CSharpDB `InsertBatch B1000` | `208,094` |
| SQLite prepared bulk `B1000` | `200,092` |
| CSharpDB `InsertBatch B10000` | `846,718` |
| SQLite prepared bulk `B10000` | `566,977` |

Interpretation:

- at `B1000`, CSharpDB is about `104.0%` of SQLite
- at `B10000`, CSharpDB is about `149.3%` of SQLite

Those numbers came from:

- [CSharpDB B1000 median CSV](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic-20260420-011131-median-of-3.csv)
- [CSharpDB B10000 median CSV](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-scenario-BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic-20260420-011235-median-of-3.csv)
- [SQLite median CSV](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260420-011334-median-of-3.csv)

That does not mean CSharpDB wins every insert shape. It means the matched
durable monotonic bulk row is now ahead on this runner.

The remaining harder cases are mostly:

- secondary-index maintenance
- random-key locality
- hot right-edge multi-writer contention

## How Concurrent Requests Are Actually Issued

This is the part that usually gets hand-waved in blog posts. In this repo it is
explicit in code.

### CSharpDB Engine vs SQLite C API

The concurrent engine harness is in
[ConcurrentSqliteCApiComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentSqliteCApiComparisonBenchmark.cs).

For CSharpDB:

- one shared `Database` instance is opened for the scenario
- one `Task.Run(...)` is created per writer
- a start gate releases all writers at the same time
- each writer loops for the measured duration
- each loop performs one insert commit
- the writer surface is either:
  - `Database.ExecuteAsync(sql)` for raw SQL, or
  - `PrepareInsertBatch("bench")` reused as a one-row batch
- for concurrent insert scenarios, the engine can use
  `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`

For SQLite C API:

- one `Task.Run(...)` is created per writer
- each writer opens its own native SQLite connection handle to the same file
- each writer prepares one native insert statement once
- each loop rebinds `id`, calls `sqlite3_step`, then resets the statement
- each loop is one auto-commit insert
- SQLite uses `WAL`, `FULL`, and a `busy_timeout`

The critical difference is:

- CSharpDB shares one in-process engine object and decides internally how to
  route the write path
- SQLite uses one connection handle per writer and relies on SQLite's own
  single-writer WAL rules

### ADO.NET Comparison

The provider harness is in
[ConcurrentAdoNetComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/ConcurrentAdoNetComparisonBenchmark.cs).

For both providers:

- one `Task.Run(...)` per writer
- one connection per writer
- one prepared command per writer
- each loop performs one `ExecuteNonQueryAsync()`

The important difference here is the storage target:

- CSharpDB ADO.NET uses a shared-memory in-process target
- SQLite ADO.NET uses shared in-memory SQLite with `journal_mode=MEMORY`

That is intentional. This harness is not trying to answer the durable engine
question again. It is isolating provider-layer overhead.

### EF Core Comparison

The EF Core harness is in
[EfCoreComparisonBenchmark.cs](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/Macro/EfCoreComparisonBenchmark.cs).

Today it is single-writer only:

- either:
  - one open connection held for the whole timed run in `--efcore-compare`, or
  - one externally-owned open connection reused across short-lived contexts in
    `--efcore-compare-hybrid-shared-connection`, or
  - EF-managed auto-open/close in `--efcore-compare-auto-open-close`
- one `SaveChangesAsync()` loop
- either one row or `100` rows per save
- on the CSharpDB side the benchmark now uses `UseStoragePreset(WriteOptimized)`
  and `UseEmbeddedOpenMode(Direct)`
- SQLite EF Core stays file-backed with `journal_mode=WAL` and
  `synchronous=FULL`

So if you are writing about concurrency, do not imply the repo already has a
parallel EF Core benchmark. It does not yet.

## How To Get the Most Performance Out of CSharpDB

If your goal is high write throughput, the highest-value rules are these.

### 1. Use the Right Surface

If you are inside the engine layer, prefer `PrepareInsertBatch(...)` over
building one SQL string per row.

Example:

```csharp
using CSharpDB.Engine;
using CSharpDB.Primitives;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
    });

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

If you are using `CSharpDB.Data`, the equivalent optimization is the familiar
ADO.NET path:

- create one `DbCommand`
- bind parameters
- call `Prepare()`
- reuse that command

### 2. Batch Commits

The current practical starting point for durable ingest is:

- start at `1000` rows per commit
- measure `10000` only for dedicated ingest jobs that can tolerate larger
  commit latency and larger WAL bursts

Single-row durable auto-commit inserts are the wrong baseline if your real
application naturally batches work.

### 3. Use the Write-Heavy Storage Preset

For durable file-backed ingest, start from
`UseWriteOptimizedPreset()`.

That preset is the intended baseline for write-heavy embedded workloads and is
documented in:

- [src/CSharpDB.Storage/README.md](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Storage/README.md)
- [src/CSharpDB.Engine/README.md](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Engine/README.md)

### 4. Keep Primary Keys Monotonic When You Can

Monotonic primary keys are materially cheaper than random keys.

Why:

- the table B-tree stays on the right edge
- leaf splits are predictable
- locality is much better

The current Plan 5 data shows that random-key primary inserts are still a major
cliff relative to monotonic inserts.

### 5. Avoid Secondary Indexes During the Hot Ingest Path Unless You Need Them

Every extra secondary index adds work on every insert.

The recent tuning work showed:

- PK-only monotonic bulk is no longer the main limiter
- realistic indexed ingest is now dominated by secondary-index maintenance
- duplicate-bucket work has improved a lot, but ordinary secondary B-tree
  maintenance is still expensive

The simplest win is still:

- load first
- add indexes after, if your workflow allows it

### 6. Choose the Right Multi-Writer Mode

For shared `Database` insert workloads, the key question is whether writers are
mostly disjoint or all fighting over the same hot end of the table.

Recommended guidance:

- keep the default serialized implicit insert mode for hot right-edge insert
  loops
- measure `ConcurrentWriteTransactions` when many tasks insert into disjoint
  explicit key ranges on one shared `Database`
- if each writer needs several statements to commit atomically, use explicit
  write-transaction APIs instead of trying to fake it with independent
  auto-commit inserts

Example:

```csharp
using CSharpDB.Engine;
using CSharpDB.Execution;

var options = new DatabaseOptions
{
    ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
}.ConfigureStorageEngine(builder =>
{
    builder.UseWriteOptimizedPreset();
});

await using var db = await Database.OpenAsync("ingest.db", options);
```

This is a measure-first setting. It is not a blanket "turn this on and all
concurrent inserts get faster" switch.

### 7. Treat Durable Group Commit as an Expert Knob

`UseDurableGroupCommit(...)` exists, but it should be driven by benchmark
evidence, not by assumption.

Use it when:

- several writers are already overlapping
- you want to trade some latency for better flush sharing

Do not assume it is the first or biggest lever. In this repo, batching and key
shape mattered earlier and more often.

## Common Comparison Mistakes

If you are turning this into a public comparison, avoid these mistakes.

### Comparing Different Layers

Bad:

- CSharpDB engine API versus SQLite ADO.NET

Good:

- engine versus C API
- ADO.NET versus ADO.NET
- EF Core versus EF Core

### Comparing Different Durability Contracts

Bad:

- durable file-backed CSharpDB versus in-memory SQLite

Good:

- same durability on both sides
- same journal/WAL assumptions
- same transaction boundaries

### Comparing Different Statement Shapes

Bad:

- one side uses prepared statements and batching
- the other side reparses one SQL string per row

Good:

- both sides reuse prepared work where that surface supports it
- both sides batch at the same logical point

### Mixing Up Disjoint Writers and Hot Right-Edge Writers

These are different workloads.

- disjoint writers test fan-in and conflict avoidance
- hot right-edge writers test structural contention

If you collapse them into one headline number, the story becomes misleading.

## Recommended Reproduction Commands

Matched durable single-writer bulk comparison:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching-scenario BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching-scenario BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --sqlite-compare --repeat 3 --repro
```

Concurrent direct engine versus SQLite C API:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-sqlite-capi-compare --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-sqlite-capi-compare-scenario CSharpDB_InsertBatch_DisjointConcurrent_W8_Batch250us --repro
```

Concurrent ADO.NET provider comparison:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-adonet-compare --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-adonet-compare-scenario SQLite_AdoNet_Disjoint_W8 --repro
```

EF Core comparison:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare-hybrid-shared-connection --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --efcore-compare-auto-open-close --repeat 3 --repro
```

## Suggested Blog Framing

If you are turning this into a public blog post, the cleanest narrative is:

1. start with the fairness rules
2. show the matched durable single-writer bulk result
3. explain why that does not answer the concurrency question
4. split concurrency into:
   - engine versus engine
   - provider versus provider
5. explain which CSharpDB features matter most:
   - `PrepareInsertBatch(...)`
   - `UseWriteOptimizedPreset()`
   - monotonic keys
   - fewer hot-path secondary indexes
   - `ConcurrentWriteTransactions` only for the right insert shape
6. finish with the current honest conclusion:
   - CSharpDB is now ahead on the matched durable monotonic bulk row on this
     runner
   - the harder remaining work is mostly indexed ingest and hot-contention
     behavior, not the PK-only bulk path

## Source Material

If you want to cross-link the deeper internal notes, start with:

- [programmatic-insert-performance/05-raw-rows-per-sec-vs-sqlite.md](/C:/Users/maxim/source/Code/CSharpDB/docs/programmatic-insert-performance/05-raw-rows-per-sec-vs-sqlite.md)
- [tests/CSharpDB.Benchmarks/README.md](/C:/Users/maxim/source/Code/CSharpDB/tests/CSharpDB.Benchmarks/README.md)
- [src/CSharpDB.Engine/README.md](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Engine/README.md)
- [src/CSharpDB.Storage/README.md](/C:/Users/maxim/source/Code/CSharpDB/src/CSharpDB.Storage/README.md)

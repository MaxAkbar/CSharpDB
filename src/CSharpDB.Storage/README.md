# CSharpDB.Storage

`CSharpDB.Storage` is the page-oriented durability layer used by the CSharpDB embedded database engine. It owns:

- physical file I/O through `IStorageDevice`
- page caching and dirty tracking through `Pager`
- write-ahead logging and crash recovery through `WriteAheadLog`
- row-id keyed B+trees for table and index storage
- schema metadata persistence through `SchemaCatalog`

This package is usually consumed indirectly through `CSharpDB.Engine`, but it also supports direct low-level use for tooling, diagnostics, and storage experiments.

## Most users: configure storage through `Database`

If you are using SQL or the engine layer, customize storage like this:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseDirectLookupOptimizedPreset();
    });

await using var db = await Database.OpenAsync("app.cdb", options);
```

`UseDirectLookupOptimizedPreset()` is the current recommended opt-in preset for direct file-backed lookup workloads. It keeps the existing page-cache shape and read path, and keeps the standard B-tree index provider, so hot local workloads stay close to default behavior.

For cache-pressured or cold-file direct lookups, use the explicit cold-file preset:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseDirectColdFileLookupPreset();
    });

await using var db = await Database.OpenAsync("cold-read.cdb", options);
```

`UseDirectColdFileLookupPreset()` keeps the existing cache shape but enables memory-mapped reads for clean main-file pages when the storage device supports them.

For explicit bounded file-cache scenarios, use the explicit hybrid file-cache preset instead:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseHybridFileCachePreset();
    });

await using var db = await Database.OpenAsync("app.cdb", options);
```

`UseHybridFileCachePreset()` is the current recommended opt-in preset for explicit bounded file-cache runs. It sets `MaxCachedPages = 2048`, adds a small bounded WAL read cache (`MaxCachedWalReadPages = 256`), keeps sequential B-tree leaf read-ahead enabled, enables memory-mapped reads for clean main-file pages when the storage device supports them, and keeps the standard B-tree index provider, which outperformed the caching index wrapper in the current tuning matrix.

For sustained durable writes, use the write-heavy preset instead:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);
```

`UseWriteOptimizedPreset()` is the current recommended opt-in preset for file-backed write-heavy workloads. It keeps the existing cache and index configuration, raises the auto-checkpoint frame threshold to `4096`, and runs auto-checkpoints in background slices instead of blocking the triggering commit. `PagerOptions.AutoCheckpointMaxPagesPerStep` controls how much work each background slice performs; the default remains `64` pages. Treat this as the stable baseline preset, not a promise that the frame-count/background row is always the top line in every harness. In the latest March 27 durable SQL batching median, auto-commit single-row SQL on this preset measured about `270.5 ops/sec`, and the analyzed-table row measured about `267.8 ops/sec`. In the stable March 28 single-writer diagnostics rerun, the preset's `FrameCount(4096)+Background(256 pages/step)` row measured about `275.9 ops/sec`, effectively tied with the top single-writer rows (`FrameCount(4096)` at `276.3` and `WalSize(8 MiB)` at `273.8`) while still keeping checkpoint work off the triggering commit.

If you want to experiment with moving advisory statistics persistence off the ordinary durable commit path, the storage builder also exposes `UseLowLatencyDurableWritePreset()`:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseLowLatencyDurableWritePreset();
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);
```

Treat this as a measure-first preset rather than a new baseline. The preset now deliberately separates exact committed-row durability from advisory planner-stat persistence: committed user rows remain WAL-durable per commit, while `sys.table_stats.row_count_is_exact` and stale column-stat tracking make any deferred planner metadata explicit after reopen/recovery. In the latest `durable-sql-batching` median-of-3 run, analyzed single-row durable SQL measured about `267.8 ops/sec` on `UseWriteOptimizedPreset()` and about `261.4 ops/sec` on `UseLowLatencyDurableWritePreset()`. The current biggest durable ingest win is still explicit transaction batching, not the low-latency preset by itself.

If you want to experiment with durable group commit, the storage builder now exposes `UseDurableGroupCommit(...)`:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
        builder.UseDurableGroupCommit(TimeSpan.FromMilliseconds(0.25));
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);
```

Keep this at `TimeSpan.Zero` unless you have benchmark data for your workload. The delay only affects file-backed `Durable` commits and trades commit latency for more opportunity to share one OS flush across multiple writers. The flush leader now skips or short-circuits that wait once the pending commit queue is already large enough, so the option behaves more like "batch briefly when lightly contended" than "always sleep before every durable flush." In the April 10, 2026 shared-engine closeout rerun, the best no-preallocation rows were effectively tied rather than clearly batch-window-driven: `W4` landed at about `468.1-468.3 commits/sec` with `0` or `500us`, and `W8` landed at about `466.7-467.3 commits/sec` with `0` or `250us`. Treat this as an opt-in knob for measured contention rather than a new default. When you test it, look at queue depth, commits per flush, and latency percentiles in addition to raw throughput. `UseDurableCommitBatchWindow(...)` remains available as a compatibility alias for the same setting.

The current engine shape is more nuanced than the earlier phase-3 guidance. After the April 11, 2026 phase-4 fan-in work, shared-`Database` implicit auto-commit non-insert SQL writes no longer hold the engine write gate all the way through `PagerCommitResult` completion. Those `UPDATE` / `DELETE` / other non-insert auto-commit writes now run on isolated `WriteTransaction` state, so overlapping writers can build a real pending WAL commit queue. In the focused `commit-fan-in-diagnostics-20260411-141949.csv` rerun, shared auto-commit disjoint updates reached about `525 commits/sec` / `1.99 commitsPerFlush` at `W4` and about `743 commits/sec` / `3.37 commitsPerFlush` at `W8`.

That does not mean every shared auto-commit workload now coalesces equally. The dedicated `insert-fan-in-diagnostics-20260411-165557.csv` rerun shows the remaining boundary clearly: hot insert workloads stayed in a narrow `413-458 commits/sec` band with `commitsPerFlush = 1.00` across shared auto-commit and explicit `WriteTransaction` variants. The shared row-id reservation pass removed the earlier duplicate-key failures from the explicit auto-generated-id rows, but it still did not unlock commit fan-in for hot inserts. Treat `UseDurableGroupCommit(...)` as a measured knob for overlapping explicit `WriteTransaction` commits plus shared auto-commit non-insert contention, not as a guaranteed throughput switch for hot single-row insert loops. For insert-heavy workloads, application-level batching and explicit transaction shape are still the first levers.

If you want to benchmark the shared auto-commit `INSERT` path itself, the engine-level switch is `DatabaseOptions.ImplicitInsertExecutionMode`. The default `Serialized` mode is still the right baseline for hot right-edge insert workloads. `ConcurrentWriteTransactions` routes each shared auto-commit `INSERT` through isolated `WriteTransaction` state, which can be worth measuring for disjoint-key insert workloads, but the current April 11 hot-insert rerun still stayed in the same `413-458 commits/sec` band and did not unlock `commitsPerFlush > 1.00` on the measured runner.

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions
{
    ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
}.ConfigureStorageEngine(builder =>
{
    builder.UseWriteOptimizedPreset();
});
```

For sustained file-backed ingest, the builder also exposes `UseWalPreallocationChunkBytes(...)`:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
        builder.UseWalPreallocationChunkBytes(1 * 1024 * 1024);
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);
```

Keep this at `0` by default. In the April 10, 2026 shared-engine closeout rerun it regressed the `8`-writer rows on the measured runner: `WalPrealloc(1MiB)` landed at about `420.0-420.9 commits/sec`, below the `466.7-467.3` no-preallocation rows. Treat it as an experimental opt-in for specific local-disk ingest workloads rather than a general preset.

If you are trying to reproduce the concurrent durable-write benchmark shape, the key detail is that the writers share one `Database` instance in-process:

```csharp
using System.Threading;
using CSharpDB.Engine;
using CSharpDB.Execution;

static async ValueTask ExecuteNonQueryAsync(Database db, string sql, CancellationToken ct = default)
{
    await using QueryResult result = await db.ExecuteAsync(sql, ct);
}

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);

await ExecuteNonQueryAsync(
    db,
    "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");

int nextId = 0;
Task[] writers = new Task[8];

for (int writerId = 0; writerId < writers.Length; writerId++)
{
    int localWriterId = writerId;
    writers[writerId] = Task.Run(async () =>
    {
        for (int i = 0; i < 10_000; i++)
        {
            int id = Interlocked.Increment(ref nextId);
            await ExecuteNonQueryAsync(
                db,
                $"INSERT INTO bench (id, value, text_col, category) VALUES ({id}, {localWriterId}, 'durable', 'Alpha')");
        }
    });
}

await Task.WhenAll(writers);
```

With the current multi-writer engine, `Task.Run(...)` is only the outer shape.
The important part is which write path each task uses:

- Shared auto-commit `UPDATE` / `DELETE` / DDL statements already run on
  isolated `WriteTransaction` state internally.
- Shared auto-commit `INSERT` statements only switch to isolated
  write-transaction commits when `DatabaseOptions.ImplicitInsertExecutionMode`
  is set to `ConcurrentWriteTransactions`.
- If each task needs a multi-statement atomic unit, use
  `RunWriteTransactionAsync(...)` or `BeginWriteTransactionAsync(...)` instead
  of the legacy `BeginTransactionAsync()` / `CommitAsync()` pair.

For example, this is the current explicit multi-writer pattern when each task
needs to commit more than one statement together:

```csharp
using System.Threading;
using CSharpDB.Engine;

var options = new DatabaseOptions
{
    ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
}.ConfigureStorageEngine(builder =>
{
    builder.UseWriteOptimizedPreset();
});

var txOptions = new WriteTransactionOptions
{
    MaxRetries = 10,
    InitialBackoff = TimeSpan.FromMilliseconds(0.25),
    MaxBackoff = TimeSpan.FromMilliseconds(20),
};

await using var db = await Database.OpenAsync("ingest.cdb", options);

await ExecuteNonQueryAsync(
    db,
    "CREATE TABLE IF NOT EXISTS ingest_log (id INTEGER PRIMARY KEY, worker_id INTEGER, status TEXT)");
await ExecuteNonQueryAsync(
    db,
    "CREATE TABLE IF NOT EXISTS worker_stats (worker_id INTEGER PRIMARY KEY, commits INTEGER)");

for (int workerId = 0; workerId < 8; workerId++)
{
    await ExecuteNonQueryAsync(
        db,
        $"INSERT INTO worker_stats VALUES ({workerId}, 0)");
}

int nextLogId = 0;
Task[] transactionalWriters = new Task[8];

for (int writerId = 0; writerId < transactionalWriters.Length; writerId++)
{
    int localWriterId = writerId;
    transactionalWriters[writerId] = Task.Run(async () =>
    {
        for (int i = 0; i < 1_000; i++)
        {
            await db.RunWriteTransactionAsync(
                async (tx, ct) =>
                {
                    int logId = Interlocked.Increment(ref nextLogId);
                    await tx.ExecuteAsync(
                        $"INSERT INTO ingest_log (id, worker_id, status) VALUES ({logId}, {localWriterId}, 'queued')",
                        ct);
                    await tx.ExecuteAsync(
                        $"UPDATE worker_stats SET commits = commits + 1 WHERE worker_id = {localWriterId}",
                        ct);
                },
                txOptions);
        }
    });
}

await Task.WhenAll(transactionalWriters);
```

For better write-heavy numbers, start with these rules:

- `UseWriteOptimizedPreset()` first. It is the baseline recommendation for file-backed durable ingest.
- If your workload can batch multiple logical writes into one explicit transaction, do that before tuning microsecond batch windows. In the latest durable SQL batching median, that scaled from about `270 rows/sec` at auto-commit to about `2.7K`, `27K`, and `197K rows/sec` at `10`, `100`, and `1000` rows per commit.
- If your shared workload is mostly low-conflict `UPDATE` / `DELETE` traffic on one `Database`, benchmark the current shared auto-commit path directly. The latest `commit-fan-in-diagnostics` rerun reached about `743 commits/sec` / `3.37 commitsPerFlush` at `W8`, effectively in the same band as the explicit `WriteTransaction` disjoint-update row on the same runner.
- If you have hot single-row `INSERT` loops, treat batching or explicit transaction shape as the first levers. The current insert-fan-in rerun still kept shared auto-commit and explicit `WriteTransaction` inserts in the same `413-458 commits/sec` band with `commitsPerFlush = 1.00`.
- If you have `8` in-process durable writers issuing shared auto-commit inserts, benchmark `TimeSpan.Zero` and `TimeSpan.FromMilliseconds(0.25)` first. Those rows were effectively tied in the latest closeout rerun, and `UseWalPreallocationChunkBytes(1 * 1024 * 1024)` regressed on that runner.
- If you have `4` in-process durable writers issuing shared auto-commit inserts, benchmark `TimeSpan.Zero` and `TimeSpan.FromMilliseconds(0.5)` first. Those rows were effectively tied at the top of the latest closeout rerun.
- Measure `UseLowLatencyDurableWritePreset()` on your own workload rather than assuming it helps. On the current perf runner it did not beat `UseWriteOptimizedPreset()` for analyzed single-row durable SQL.

### Recommended Read/Write Topology

- In one process, prefer one long-lived `Database` instance for writes and create `ReaderSession`s from that same instance for snapshot reads.
- Avoid opening the same `.cdb` file twice in one process just to split "read DB" and "write DB". That duplicates engine state instead of using the intended shared-instance coordination path.
- If you need multiple callers or transports, put one warm `Database` behind your host/service boundary and route both reads and writes through that owner.

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("app.cdb", options);

using var reader = db.CreateReaderSession();
await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench");
```

Separately from durable flush tuning, the storage write path now does partial async I/O batching on its own. Direct `AppendFramesAndCommitAsync(...)` already writes WAL frames in chunks, checkpoint copies already batch contiguous page writes back into the main database file, repeated `AppendFrameAsync(...)` calls inside one transaction are now staged and emitted as chunked WAL writes at `CommitAsync(...)` time, and the snapshot/export-style copy paths now share one batched storage-device copy helper. The remaining roadmap work here is to audit the remaining export/rewrite paths and decide which ones are worth batching further.

The current crash-level durability coverage is process-based rather than mock-based. The test suite now verifies recovery after a real process crash at four points: immediately after commit returns, at checkpoint start, after checkpoint page copies have been flushed to the main DB file, and after WAL checkpoint finalization but before pager state refresh completes.

## Low-level use: open the storage graph directly

If you need direct access to `Pager`, `SchemaCatalog`, or `BTree`, use the default storage engine factory:

```csharp
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

var storageOptions = new StorageEngineOptionsBuilder()
    .UsePagerOptions(new PagerOptions { MaxCachedPages = 1024 })
    .UseBTreeIndexes()
    .Build();

var factory = new DefaultStorageEngineFactory();
var context = await factory.OpenAsync("lowlevel.cdb", storageOptions);
await using var pager = context.Pager;

await pager.BeginTransactionAsync();
try
{
    uint rootPageId = await BTree.CreateNewAsync(pager);
    var tree = new BTree(pager, rootPageId);

    await tree.InsertAsync(1, new byte[] { 1, 2, 3, 4 });
    byte[]? payload = await tree.FindAsync(1);

    await pager.CommitAsync();
}
catch
{
    await pager.RollbackAsync();
    throw;
}
```

## Key extension points

- `IStorageDevice` for alternate storage backends
- `IPageCache` through `PagerOptions.PageCacheFactory`
- `ICheckpointPolicy` for auto-checkpoint decisions
- `IPageOperationInterceptor` for diagnostics and fault injection
- `IPageChecksumProvider` for WAL checksum behavior
- `IIndexProvider` for index-store composition
- `ISerializerProvider` for record and schema serialization
- `ICatalogStore` for catalog payload encoding
- `IStorageEngineFactory` for replacing the default storage composition root

## Related docs

- [Storage tutorial index](../../samples/storage-tutorials/README.md)
- [Storage architecture](../../samples/storage-tutorials/architecture.md)
- [Usage and extensibility guide](../../samples/storage-tutorials/extensibility.md)
- [Runnable study examples](../../samples/storage-tutorials/examples/README.md)

## Related packages

| Package | Description |
|---|---|
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | SQL/engine layer built on this storage package |
| [CSharpDB.Storage.Diagnostics](https://www.nuget.org/packages/CSharpDB.Storage.Diagnostics) | Read-only inspection and integrity tooling |
| [CSharpDB.Execution](https://www.nuget.org/packages/CSharpDB.Execution) | Query execution layer that reads/writes through storage |

## Installation

```bash
dotnet add package CSharpDB.Storage
```

For the all-in-one package:

```bash
dotnet add package CSharpDB
```

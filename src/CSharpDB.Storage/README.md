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

`UseWriteOptimizedPreset()` is the recommended opt-in preset for file-backed write-heavy workloads. It keeps the existing cache and index configuration, raises the auto-checkpoint frame threshold to `4096`, and runs auto-checkpoints in background slices instead of blocking the triggering commit. `PagerOptions.AutoCheckpointMaxPagesPerStep` controls each slice; the default is `64` pages. Measure throughput and commit latency with your application's transaction shape.

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

Measure `UseLowLatencyDurableWritePreset()` on your workload before selecting it. It separates committed-row durability from advisory planner-stat persistence: committed user rows remain WAL-durable per commit, while `sys.table_stats.row_count_is_exact` and stale column-stat tracking make deferred metadata explicit after recovery. Explicit transaction batching remains an important option for durable ingest.

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

Keep this at `TimeSpan.Zero` unless your workload benefits from group commit. The delay affects file-backed `Durable` commits and trades commit latency for opportunities to share one OS flush across multiple writers. The flush leader can end the wait early when the pending commit queue is already large enough. Measure queue depth, commits per flush, and latency percentiles along with throughput. `UseDurableCommitBatchWindow(...)` is a compatibility alias.

Shared-`Database` implicit auto-commit `UPDATE`, `DELETE`, and other non-insert SQL writes use isolated `WriteTransaction` state while their durable commits complete. Overlapping writers can build a pending WAL commit queue and share a durable flush.

Commit fan-in depends on key contention and transaction shape. Hot single-row inserts can serialize even when several writers are active. For insert-heavy workloads, start with application-level batching and explicit transactions, then measure group commit under representative concurrency.

`DatabaseOptions.ImplicitInsertExecutionMode` controls the shared auto-commit `INSERT` path. The default `Serialized` mode provides a baseline for hot right-edge insert workloads. `ConcurrentWriteTransactions` routes inserts through isolated `WriteTransaction` state and can be useful for disjoint-key workloads. Compare both modes using your application's key distribution.

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

Keep WAL preallocation at `0` by default. Evaluate it on your own disk and workload before enabling it; reducing file extensions can trade additional allocation and flush costs against throughput.

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

For write-heavy workloads:

- Start with `UseWriteOptimizedPreset()` for file-backed durable ingest.
- Batch multiple logical writes in one explicit transaction before tuning commit windows.
- Measure low-conflict `UPDATE` / `DELETE` workloads and hot `INSERT` workloads separately; their contention patterns differ.
- Compare serialized and concurrent insert execution for your key distribution.
- Start with a zero group-commit window, then measure small windows such as `TimeSpan.FromMilliseconds(0.25)` or `TimeSpan.FromMilliseconds(0.5)` under concurrent writes.
- Leave WAL preallocation disabled unless measurements on your storage justify it.
- Evaluate `UseLowLatencyDurableWritePreset()` on your own workload before adopting it.

### Recommended Read/Write Topology

- In one process, prefer one long-lived `Database` instance for writes and create `ReaderSession`s from that same instance for snapshot reads.
- Avoid opening the same `.cdb` file twice in one process just to split "read DB" and "write DB". That duplicates engine state instead of using the intended shared-instance coordination path.
- If you need multiple callers or transports, put one warm `Database` behind your host/service boundary and route both reads and writes through that owner.

`Database.OpenAsync(...)` remains a physical engine open; disposing the
`Database` remains its physical close. The warm transaction-handle handoff used
by the direct `CSharpDB.Client` is implemented at the client ownership boundary
and is not inherited by callers of the raw engine API. Retain and share the
owning `Database` when its concurrency model fits the workload.

Specialized single-owner workflows can configure
`StorageEngineOptionsBuilder.UsePrimaryFileShare(FileShare.Read)`. The primary
database handle still reads and writes through its existing owner, but the OS
refuses any second read/write opener until that owner is disposed. The staged
migration target uses this opt-in barrier so no independent writer can change
the file between validation and activation. The default remains
`FileShare.ReadWrite` for compatibility.

On Unix, .NET maps non-exclusive file-share modes to the same shared OS lock.
CSharpDB therefore uses a stronger exclusive OS lock for this opt-in writer
barrier. Independent file readers are also refused there; use a
`ReaderSession` from the owning `Database`. Windows retains the exact
allow-readers/deny-writers behavior.

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("app.cdb", options);

using var reader = db.CreateReaderSession();
await using var result = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM bench");
```

Separately from durable flush tuning, the storage write path now does partial async I/O batching on its own. Direct `AppendFramesAndCommitAsync(...)` already writes WAL frames in chunks, checkpoint copies already batch contiguous page writes back into the main database file, repeated `AppendFrameAsync(...)` calls inside one transaction are now staged and emitted as chunked WAL writes at `CommitAsync(...)` time, and the snapshot/export-style copy paths now share one batched storage-device copy helper. The remaining roadmap work here is to audit the remaining export/rewrite paths and decide which ones are worth batching further.

The current crash-level durability coverage is process-based rather than mock-based. The test suite now verifies recovery after a real process crash at four points: immediately after commit returns, at checkpoint start, after checkpoint page copies have been flushed to the main DB file, and after WAL checkpoint finalization but before pager state refresh completes.

## Checkpoint coordination

Use `Database.CheckpointAsync()` or the owning `Pager.CheckpointAsync()` for a
live database. Checkpoint copying preserves registered readers' page visibility;
finalization waits until no registered snapshot retains WAL frames. Finalization
resets an empty tail or compacts frames committed after the captured copy boundary.

Finalization acquires the same commit mutex used by write-conflict resolution
before the begin barrier, writer barrier, and checkpoint admission. It holds those
reservations through WAL reset or compaction and pager state refresh, then releases
them in reverse order on completion, cancellation, or error. Pending durable
commits defer finalization because their unpublished frames still own physical
offsets, including legacy commits that already released the writer lock.

Direct `WalIndex.TakeSnapshot()` and `WriteAheadLog.ReadPageAsync(offset)` calls do
not register a reader with the pager. Low-level callers must coordinate those
reads and all pending writes with checkpointing themselves: a retained offset can
become invalid or point to a different page after reset or compaction. Prefer
`ReaderSession` at the engine layer or the pager's registered snapshot API.

The [deterministic regressions](../../tests/CSharpDB.Tests/CheckpointConflictReadTests.cs)
cover reset, retained-tail compaction, stale offsets within file bounds,
cancellation, lock release, and subsequent commit/checkpoint progress. The original
CI interleaving was not captured; these tests reproduce the failure mechanism.
The production database and WAL formats remain unchanged.

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

Every `IStorageEngineFactory.OpenAsync(...)` call composes a new physical
storage graph. Disposing its `Pager` closes that graph; the factory does not
implicitly pool or recycle it. For repeated low-level work against one file,
retain the opened context and pager for the intended ownership scope instead of
opening and disposing them per operation.

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

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

Keep this at `TimeSpan.Zero` unless you have benchmark data for your workload. The delay only affects file-backed `Durable` commits and trades commit latency for more opportunity to share one OS flush across multiple writers. The flush leader now skips or short-circuits that wait once the pending commit queue is already large enough, so the option behaves more like "batch briefly when lightly contended" than "always sleep before every durable flush." In the stable March 28 concurrent median-of-3 rerun, `250us` was the best `4`-writer row at about `553.4 commits/sec` and the narrow best pure batch-window `8`-writer row at about `1070.4 commits/sec`, while the single-writer harness still regressed to about `267.2 ops/sec`. This should remain an opt-in knob for measured in-process contention rather than a new default. When you test it, look at queue depth, commits per flush, and latency percentiles in addition to raw throughput. `UseDurableCommitBatchWindow(...)` remains available as a compatibility alias for the same setting.

One important constraint on the current engine shape: shared-`Database` implicit auto-commit writes still hold the engine write gate until `PagerCommitResult` completion, so they do not build a meaningful pending WAL commit queue today. The durable group-commit knob is therefore most relevant to overlapping explicit `WriteTransaction` commit paths and other flows that can reach the WAL pending-commit queue concurrently, not to a single shared `Database.ExecuteAsync(...)` hot loop.

That constraint is also the next performance target. Phase-3 checkpoint work is in a good place; the next meaningful write-path gain is to reduce engine-side serialization above the WAL pending-commit queue so shared auto-commit workloads can actually build commit fan-in before durable flush. Until that changes, treat `UseDurableGroupCommit(...)` as a measured explicit-transaction / overlapping-commit knob rather than a general shared-auto-commit throughput switch.

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

Keep this at `0` by default. In the stable March 28 concurrent rerun it was helpful on the `8`-writer rows, where `WalPrealloc(1MiB)` with `BatchWindow(0)` was the best measured row at about `1078.6 commits/sec`, but it was still not a general single-writer answer: on the latest single-writer diagnostics it moved the `FrameCount(4096)+Background(256 pages/step)` row from about `275.9` to `273.7 ops/sec`, while `WalSize(8 MiB)` measured about `273.8 ops/sec` and plain `FrameCount(4096)` remained the top row at about `276.3 ops/sec`. Treat it as an experimental opt-in for specific local-disk ingest workloads rather than a general preset.

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
        builder.UseWalPreallocationChunkBytes(1 * 1024 * 1024); // Best measured 8-writer row on the current perf runner
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

For better write-heavy numbers, start with these rules:

- `UseWriteOptimizedPreset()` first. It is the baseline recommendation for file-backed durable ingest.
- If your workload can batch multiple logical writes into one explicit transaction, do that before tuning microsecond batch windows. In the latest durable SQL batching median, that scaled from about `270 rows/sec` at auto-commit to about `2.7K`, `27K`, and `197K rows/sec` at `10`, `100`, and `1000` rows per commit.
- If you have `8` in-process durable writers sharing one `Database`, benchmark `UseWalPreallocationChunkBytes(1 * 1024 * 1024)` first with the batch window left at `0`; that was the best measured `8`-writer row on the current perf runner.
- If you want to tune the batch window under `8`-writer contention, benchmark `TimeSpan.FromMilliseconds(0.25)` next; that was the narrow best pure batch-window row in the latest median-of-3 rerun.
- If you have `4` in-process durable writers, benchmark `TimeSpan.FromMilliseconds(0.25)` first.
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

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

`UseWriteOptimizedPreset()` is the current recommended opt-in preset for file-backed write-heavy workloads. It keeps the existing cache and index configuration, raises the auto-checkpoint frame threshold to `4096`, and runs auto-checkpoints in background slices instead of blocking the triggering commit. `PagerOptions.AutoCheckpointMaxPagesPerStep` controls how much work each background slice performs; the default remains `64` pages. In the current durable-write diagnostics, the `64`-page background preset was the best measured background variant at `33.30K ops/sec`, slightly ahead of `256` pages at `33.24K` and foreground `FrameCount(4096)` at `33.13K`. The important difference is that background sliced mode kept checkpoint work off essentially all commits while the foreground policy still had `246` commits pay checkpoint cost in the median run.

If you want to experiment with durable group commit, the storage builder now exposes `UseDurableCommitBatchWindow(...)`:

```csharp
using CSharpDB.Engine;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UseWriteOptimizedPreset();
        builder.UseDurableCommitBatchWindow(TimeSpan.FromMilliseconds(0.25));
    });

await using var db = await Database.OpenAsync("ingest.cdb", options);
```

Keep this at `TimeSpan.Zero` unless you have benchmark data for your workload. The delay only affects file-backed `Durable` commits and trades commit latency for more opportunity to share one OS flush across multiple writers. The flush leader now skips or short-circuits that wait once the pending commit queue is already large enough, so the option behaves more like "batch briefly when lightly contended" than "always sleep before every durable flush." In the final post-fix median-of-3 diagnostics, `250us` was only a narrow `4`-writer win and was not a consistent `8`-writer win, so it should stay opt-in.

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

Keep this at `0` by default. In the final post-fix diagnostics it was effectively flat to slightly negative: not a single-writer win, and basically neutral on the `8`-writer durable commit benchmark. Treat it as an experimental opt-in for specific local-disk ingest workloads rather than a general preset.

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

- [Storage tutorial index](../../docs/tutorials/storage/README.md)
- [Storage architecture](../../docs/tutorials/storage/architecture.md)
- [Usage and extensibility guide](../../docs/tutorials/storage/extensibility.md)
- [Runnable study examples](../../docs/tutorials/storage/examples/README.md)

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

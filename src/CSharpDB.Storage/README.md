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

# CSharpDB.Storage Usage and Extensibility Guide

This guide is about the storage API that exists today in this repository. It focuses on two questions:

1. How do you use `CSharpDB.Storage` from application code?
2. Which parts of the storage stack are intentionally replaceable, and which are still concrete internals?

If you want the architectural walkthrough first, read [architecture.md](./architecture.md) before this guide.

---

## 1. Start at the right level

There are two realistic entry points into the storage layer.

### Most applications: configure storage through `Database`

If you are building on the SQL/engine layer, the normal path is:

```csharp
using CSharpDB.Engine;
using CSharpDB.Storage.Caching;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UsePagerOptions(new PagerOptions
        {
            MaxCachedPages = 2048,
            CheckpointPolicy = new FrameCountCheckpointPolicy(500),
            WriterLockTimeout = TimeSpan.FromSeconds(10),
        });

        builder.UseCachingBTreeIndexes(findCacheCapacity: 4096);
    });

await using var db = await Database.OpenAsync("app.cdb", options);
```

Use this path when you want SQL execution, collections, transactions, and storage customization without manually composing the storage graph.

### Low-level tooling or experiments: open the storage graph directly

If you want direct access to `Pager`, `SchemaCatalog`, or the serializer/index provider graph, use `DefaultStorageEngineFactory`.

```csharp
using CSharpDB.Storage.BTrees;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.StorageEngine;

var storageOptions = new StorageEngineOptionsBuilder()
    .UsePagerOptions(new PagerOptions
    {
        MaxCachedPages = 1024,
    })
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
    await pager.CommitAsync();
}
catch
{
    await pager.RollbackAsync();
    throw;
}
```

Use this path when you are writing diagnostics, storage-focused benchmarks, or experiments that bypass the SQL layer.

---

## 2. Current composition model

The current composition chain is:

```text
Database.OpenAsync(...)
  -> IStorageEngineFactory.OpenAsync(...)
       -> StorageEngineContext
            -> Pager
            -> SchemaCatalog
            -> IRecordSerializer
            -> ISchemaSerializer
            -> IIndexProvider
            -> IPageChecksumProvider
```

`StorageEngineOptionsBuilder` configures the default composition root. It does not create a `StorageEngine` service object. The concrete factory type in this repository is `DefaultStorageEngineFactory`.

Internal helpers such as `PageBufferManager`, `PageAllocator`, `TransactionCoordinator`, and `CheckpointCoordinator` are useful to understand when reading the source, but they are not the public customization surface.

---

## 3. Extension points that exist today

The current storage layer is partially provider-driven. These are the real seams you can replace without rewriting `Pager` or `BTree`.

| Concern | API | Default | How to configure |
|---|---|---|---|
| Storage backend | `IStorageDevice` | `FileStorageDevice` | Use a custom `IStorageEngineFactory`, or construct `Pager` directly in tests/tools |
| Page cache | `IPageCache` | `DictionaryPageCache` or `LruPageCache` | `PagerOptions.MaxCachedPages` or `PagerOptions.PageCacheFactory` |
| Auto-checkpoint policy | `ICheckpointPolicy` | `FrameCountCheckpointPolicy` | `PagerOptions.CheckpointPolicy` |
| Lifecycle hooks | `IPageOperationInterceptor` | `NoOpPageOperationInterceptor` | `PagerOptions.Interceptors` |
| WAL checksum | `IPageChecksumProvider` | `AdditiveChecksumProvider` | `StorageEngineOptionsBuilder.UseChecksumProvider(...)` |
| Index implementation | `IIndexProvider` | `BTreeIndexProvider` | `UseBTreeIndexes()`, `UseCachingBTreeIndexes(...)`, or `UseIndexProvider(...)` |
| Record/schema serialization | `ISerializerProvider` | `DefaultSerializerProvider` | `UseSerializerProvider(...)` |
| Catalog payload codec | `ICatalogStore` | `CatalogStore` | `UseCatalogStore(...)` |
| Storage composition root | `IStorageEngineFactory` | `DefaultStorageEngineFactory` | `DatabaseOptions.StorageEngineFactory` |
| Clock for time-based policies | `IClock` | `SystemClock` | Pass to `TimeIntervalCheckpointPolicy` |

Two important boundaries:

- `IWriteAheadLog` is an interface, but the builder does not expose WAL replacement directly. Replacing WAL behavior means supplying a custom `IStorageEngineFactory`.
- `BTree`, `Pager`, `SlottedPage`, and the on-disk page format are concrete implementation details today, not strategy interfaces.

---

## 4. What the builder actually controls

`StorageEngineOptionsBuilder` currently controls five top-level registrations:

```csharp
var storageOptions = new StorageEngineOptionsBuilder()
    .UsePagerOptions(new PagerOptions
    {
        MaxCachedPages = 1000,
        CheckpointPolicy = new FrameCountCheckpointPolicy(500),
    })
    .UseSerializerProvider<DefaultSerializerProvider>()
    .UseBTreeIndexes()
    .UseCatalogStore<CatalogStore>()
    .UseChecksumProvider<AdditiveChecksumProvider>()
    .Build();
```

That `StorageEngineOptions` instance is then passed into `DefaultStorageEngineFactory.OpenAsync(filePath, options)`.

From the engine layer, the equivalent pattern is:

```csharp
using CSharpDB.Engine;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Paging;

var options = new DatabaseOptions()
    .ConfigureStorageEngine(builder =>
    {
        builder.UsePagerOptions(new PagerOptions
        {
            MaxCachedPages = 1000,
            CheckpointPolicy = new FrameCountCheckpointPolicy(500),
        });

        builder.UseCachingBTreeIndexes(findCacheCapacity: 2048);
    });

await using var db = await Database.OpenAsync("mydb.cdb", options);
```

There is no `new Database("path", options => ...)` constructor in the current codebase.

---

## 5. Usage patterns by scenario

### Tune cache behavior

Use `MaxCachedPages` when you just want the built-in LRU cache:

```csharp
builder.UsePagerOptions(new PagerOptions
{
    MaxCachedPages = 5000,
});
```

Use `PageCacheFactory` when you want a custom cache implementation:

```csharp
builder.UsePagerOptions(new PagerOptions
{
    PageCacheFactory = () => new MetricsPageCache(maxPages: 5000),
});
```

### Tune checkpoint behavior

All automatic checkpointing decisions flow through `ICheckpointPolicy`.

```csharp
builder.UsePagerOptions(new PagerOptions
{
    CheckpointPolicy = new AnyCheckpointPolicy(
        new FrameCountCheckpointPolicy(500),
        new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5))),
});
```

If you want to disable auto-checkpointing during a bulk import, provide a policy that always returns `false`, then call `Database.CheckpointAsync()` or `Pager.CheckpointAsync()` explicitly when the import is finished.

### Add diagnostics or fault injection

Attach one or more `IPageOperationInterceptor` implementations through `PagerOptions.Interceptors`.

```csharp
builder.UsePagerOptions(new PagerOptions
{
    Interceptors =
    [
        new ConsoleLoggingInterceptor(),
        new LatencyTrackingInterceptor(),
    ],
});
```

When no interceptors are configured, `Pager` uses `NoOpPageOperationInterceptor` and skips interceptor dispatch on the hot path.

### Swap index strategy

The default index provider is B+tree-backed:

```csharp
builder.UseBTreeIndexes();
```

For a cache-decorated variant:

```csharp
builder.UseCachingBTreeIndexes(findCacheCapacity: 4096);
```

For a custom implementation:

```csharp
builder.UseIndexProvider(new MyIndexProvider());
```

### Replace serializers or catalog encoding

These are already interface-based in the current codebase:

```csharp
builder.UseSerializerProvider(new MySerializerProvider());
builder.UseCatalogStore(new MyCatalogStore());
```

This is one of the main differences between the current repository and older mental models of the storage layer: serializer and index composition are already first-class builder concerns.

---

## 6. What is still concrete

These pieces are still concrete implementation details in the default storage stack:

- `Pager` internals (`PageBufferManager`, `PageAllocator`, dirty-page bookkeeping)
- `WriteAheadLog` behavior selected by `DefaultStorageEngineFactory`
- `BTree` page layout and split/merge logic
- `SlottedPage` binary format
- File-header and WAL-frame layouts in `PageConstants`

You can still replace large portions of the stack, but once you want to change WAL orchestration or pager composition, you have moved beyond builder-level customization and into custom factory territory.

---

## 7. Diagnostics live in a separate project

Inspection helpers are real, but they do not live under `src/CSharpDB.Storage/`.

Current layout:

```text
src/CSharpDB.Storage/             core storage engine
src/CSharpDB.Storage.Diagnostics/ inspectors and integrity tooling
```

That distinction matters because a reader following a file map should not go looking for `DatabaseInspector`, `WalInspector`, or `IndexInspector` inside the core storage project.

---

## 8. Suggested reading order

If your goal is understanding:

1. Read [architecture.md](./architecture.md) for the mental model.
2. Read [examples/README.md](./examples/README.md) for the runnable extension examples project.
3. Then inspect the source in this order:
   - `StorageEngine/DefaultStorageEngineFactory.cs`
   - `Paging/Pager.cs`
   - `Wal/WriteAheadLog.cs`
   - `BTree/BTree.cs`
   - `Catalog/SchemaCatalog.cs`

If your goal is customization:

1. Start with `DatabaseOptions.ConfigureStorageEngine(...)`.
2. Only drop to `DefaultStorageEngineFactory` when you truly need direct `Pager` access.
3. Reach for a custom `IStorageEngineFactory` only when builder-level seams are not enough.

---

## 9. Practical summary

Use `DatabaseOptions.ConfigureStorageEngine(...)` for normal application customization.

Use `DefaultStorageEngineFactory` when you need direct access to `Pager`, `SchemaCatalog`, or low-level components.

Treat `IStorageDevice`, `IPageCache`, `ICheckpointPolicy`, `IPageOperationInterceptor`, `IPageChecksumProvider`, `IIndexProvider`, `ISerializerProvider`, `ICatalogStore`, and `IStorageEngineFactory` as the supported extension surface today.

Treat pager internals, WAL orchestration, and B+tree page algorithms as concrete implementation details unless you are intentionally replacing the storage composition root.

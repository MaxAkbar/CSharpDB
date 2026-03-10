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
        builder.UseLookupOptimizedPreset();
    });

await using var db = await Database.OpenAsync("app.cdb", options);
```

`UseLookupOptimizedPreset()` is the current recommended opt-in preset for file-backed lookup-heavy workloads. It sets `MaxCachedPages = 2048` and keeps the standard B-tree index provider, which outperformed the caching index wrapper in the current tuning matrix.

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

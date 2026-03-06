# CSharpDB.Storage

B+tree storage engine with page cache, write-ahead log (WAL), crash recovery, and concurrent snapshot-isolated readers for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Storage)](https://www.nuget.org/packages/CSharpDB.Storage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Storage` is the disk-level storage layer for CSharpDB. It manages all physical I/O through a page-oriented architecture with B+tree data structures, a write-ahead log for durability, and snapshot isolation for concurrent readers. Single-file storage, zero external dependencies.

## Architecture

```
┌─────────────────────────────┐
│       CatalogService        │  Schema catalog (B+tree-backed)
├─────────────────────────────┤
│          BTree              │  Key-value storage (long -> byte[])
├─────────────────────────────┤
│      Pager + PageCache      │  Page I/O, LRU cache, transactions
├──────────────┬──────────────┤
│  WAL (Write  │  Storage     │  Durability & file I/O
│  Ahead Log)  │  Device      │
└──────────────┴──────────────┘
```

## Key Components

### B+Tree
- Keyed by `long` rowid with variable-length payloads
- Leaf-hint cache for fast sequential access
- Stack allocation for small cells (<=256 bytes), `ArrayPool` for splits
- Cursor-based iteration with `MoveNextAsync` and `SeekAsync`

### Pager
- 4 KB page-oriented I/O with 100-byte file header
- LRU page cache with configurable capacity
- Single-writer transactions via `SemaphoreSlim`
- Page allocation and freelist management

### Write-Ahead Log (WAL)
- Append-only log with frame-level checksums
- Batch commit for multi-page writes
- Automatic checkpoint with configurable threshold (default: 1000 frames)
- Crash recovery with salt and checksum validation

### Concurrent Access
- Single writer + multiple concurrent readers
- Snapshot isolation via `WalIndex` snapshots
- WAL backpressure to protect long-running readers
- `TransactionCoordinator` with timeout support

### Catalog Service
- B+tree-backed schema storage for tables, indexes, views, and triggers
- In-memory schema caching with root-page tracking
- Handles root page updates when B+trees split

## Usage

```csharp
using CSharpDB.Storage;

// Open or create a database file
var options = new StorageEngineOptions { DatabasePath = "mydb.db" };
var context = await StorageEngine.OpenAsync(options);

// Access the pager for page-level operations
var pager = context.Pager;
await pager.BeginTransactionAsync();

// Create a B+tree
var tree = await BTree.CreateNewAsync(pager);

// Insert data
await tree.InsertAsync(1, recordBytes);

// Read data
var data = await tree.FindAsync(1);

// Iterate with a cursor
var cursor = tree.CreateCursor();
while (await cursor.MoveNextAsync())
{
    var key = cursor.CurrentKey;
    var value = cursor.CurrentValue;
}

await pager.CommitAsync();
```

## Page Layout

```
File:   [Header:100][Page0:4096][Page1:4096][Page2:4096]...
Page:   [PageHeader:9][CellPointers:2*N][  free  ][cells←]
WAL:    [WalHeader:32][Frame:24+4096][Frame:24+4096]...
```

## Installation

```
dotnet add package CSharpDB.Storage
```

## Dependencies

- `CSharpDB.Primitives` - shared type system and schema definitions

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine built on this storage layer |
| [CSharpDB.Storage.Diagnostics](https://www.nuget.org/packages/CSharpDB.Storage.Diagnostics) | Read-only inspection and integrity checking |
| [CSharpDB.Execution](https://www.nuget.org/packages/CSharpDB.Execution) | Query operators that read/write through this layer |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.

# CSharpDB.Storage

A low-level, high-performance storage engine for .NET 10 built on top of `RandomAccess` and `SafeFileHandle`. It provides random-access, async I/O to a binary file and is the foundation for the B+tree page cache and write-ahead log (WAL) layers.

For the guided storage tutorial track, start with [docs/tutorials/storage/README.md](../tutorials/storage/README.md).

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [FileStorageDevice](#filestoragedevice)
- [IStorageDevice Interface](#istoragedevice-interface)
- [Device Scenarios](#device-scenarios)
  - [1. Create a New File](#1-create-a-new-file)
  - [2. Open an Existing File](#2-open-an-existing-file)
  - [3. Write Raw Bytes at an Offset](#3-write-raw-bytes-at-an-offset)
  - [4. Read Raw Bytes from an Offset](#4-read-raw-bytes-from-an-offset)
  - [5. Read Past End of File (Zero-Fill Behavior)](#5-read-past-end-of-file-zero-fill-behavior)
  - [6. Pre-allocate / Extend File Length](#6-pre-allocate--extend-file-length)
  - [7. Flush to Disk (fsync)](#7-flush-to-disk-fsync)
  - [8. Check File Length](#8-check-file-length)
  - [9. Dispose Synchronously](#9-dispose-synchronously)
  - [10. Dispose Asynchronously](#10-dispose-asynchronously)
  - [11. Cancellation Support](#11-cancellation-support)
  - [12. Injecting via IStorageDevice (Testability)](#12-injecting-via-istoragedevice-testability)
  - [13. Writing Fixed-Size Pages (4 KB)](#13-writing-fixed-size-pages-4-kb)
  - [14. Reading Fixed-Size Pages (4 KB)](#14-reading-fixed-size-pages-4-kb)
  - [15. Appending Sequential Pages](#15-appending-sequential-pages)
- [Pager](#pager)
  - [P1. Create a New Database](#p1-create-a-new-database)
  - [P2. Open and Recover an Existing Database](#p2-open-and-recover-an-existing-database)
  - [P3. Read and Write Pages](#p3-read-and-write-pages)
  - [P4. Allocate and Free Pages](#p4-allocate-and-free-pages)
  - [P5. Transaction Lifecycle](#p5-transaction-lifecycle)
  - [P6. Snapshot Isolation (Concurrent Readers)](#p6-snapshot-isolation-concurrent-readers)
  - [P7. Manual Checkpoint](#p7-manual-checkpoint)
  - [P8. Configure Checkpoint Policy](#p8-configure-checkpoint-policy)
- [B+Tree](#btree)
  - [B1. Create a New B+Tree](#b1-create-a-new-btree)
  - [B2. Insert a Key-Value Pair](#b2-insert-a-key-value-pair)
  - [B3. Point Lookup](#b3-point-lookup)
  - [B4. Cache-Only Fast Path](#b4-cache-only-fast-path)
  - [B5. Delete a Key](#b5-delete-a-key)
  - [B6. Forward Cursor Scan](#b6-forward-cursor-scan)
  - [B7. Seek to a Specific Key](#b7-seek-to-a-specific-key)
  - [B8. Count Entries](#b8-count-entries)
- [Write-Ahead Log (WAL)](#write-ahead-log-wal)
  - [W1. Open or Create a WAL](#w1-open-or-create-a-wal)
  - [W2. Write Transaction to WAL](#w2-write-transaction-to-wal)
  - [W3. Take a Reader Snapshot](#w3-take-a-reader-snapshot)
  - [W4. Checkpoint WAL to Database File](#w4-checkpoint-wal-to-database-file)
- [Slotted Page Layout](#slotted-page-layout)
  - [S1. Initialize a Page](#s1-initialize-a-page)
  - [S2. Insert and Read Cells](#s2-insert-and-read-cells)
  - [S3. Delete a Cell and Defragment](#s3-delete-a-cell-and-defragment)
- [Indexing](#indexing)
  - [I1. Create an Index Store](#i1-create-an-index-store)
  - [I2. Insert and Lookup Index Entries](#i2-insert-and-lookup-index-entries)
  - [I3. Range Scan with Cursor](#i3-range-scan-with-cursor)
  - [I4. Add Caching to an Index](#i4-add-caching-to-an-index)
- [Record Serialization](#record-serialization)
  - [R1. Encode and Decode a Row](#r1-encode-and-decode-a-row)
  - [R2. Selective Column Projection](#r2-selective-column-projection)
  - [R3. Fast Filter Without Materialization](#r3-fast-filter-without-materialization)
  - [R4. Varint Encoding](#r4-varint-encoding)
- [Schema Catalog](#schema-catalog)
  - [C1. Initialize the Catalog](#c1-initialize-the-catalog)
  - [C2. Create and Query Tables](#c2-create-and-query-tables)
  - [C3. Create and Query Indexes](#c3-create-and-query-indexes)
  - [C4. Views and Triggers](#c4-views-and-triggers)
  - [C5. Persist Root Page Changes](#c5-persist-root-page-changes)
- [Folder & File Storage](#folder--file-storage)
  - [Domain Models](#domain-models)
  - [F1. Bootstrap the Storage](#f1-bootstrap-the-storage)
  - [F2. Create a Folder](#f2-create-a-folder)
  - [F3. Create a File Inside a Folder](#f3-create-a-file-inside-a-folder)
  - [F4. Read a File](#f4-read-a-file)
  - [F5. List All Files in a Folder](#f5-list-all-files-in-a-folder)
  - [F6. List All Folders](#f6-list-all-folders)
  - [F7. Update File Content](#f7-update-file-content)
  - [F8. Delete a File](#f8-delete-a-file)
  - [F9. Delete a Folder and Its Contents](#f9-delete-a-folder-and-its-contents)
  - [F10. Rename or Move a File](#f10-rename-or-move-a-file)
  - [F11. Search Files by Predicate](#f11-search-files-by-predicate)
  - [F12. Bulk Create with an Explicit Transaction](#f12-bulk-create-with-an-explicit-transaction)
  - [F13. SQL-Based Approach](#f13-sql-based-approach)
  - [F14. One Database File per Folder (Multi-Volume)](#f14-one-database-file-per-folder-multi-volume)
- [Key Design Notes](#key-design-notes)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Application                        │
│          (SQL Engine / Collection API)               │
├──────────────────────────────────────────────────────┤
│                 SchemaCatalog                        │
│    Tables ─ Indexes ─ Views ─ Triggers               │
├──────────────┬───────────────┬───────────────────────┤
│    BTree     │  IndexStore   │  RecordEncoder        │
│  (data)      │  (secondary)  │  (row format)         │
├──────────────┴───────────────┴───────────────────────┤
│                    Pager                             │
│   PageCache ─ DirtyTracking ─ PageAllocator          │
├──────────────────────┬───────────────────────────────┤
│   WriteAheadLog      │   CheckpointCoordinator       │
│   (WAL + WalIndex)   │   (policy-driven)             │
├──────────────────────┴───────────────────────────────┤
│              IStorageDevice                          │
│         (FileStorageDevice / memory)                 │
└──────────────────────────────────────────────────────┘
```

**Page Layout:**
```
Page 0 (File Header):
  [Magic: 4 bytes "CSDB"]
  [FormatVersion: 4 bytes]
  [PageSize: 4 bytes = 4096]
  [PageCount: 4 bytes]
  [SchemaRootPage: 4 bytes]
  [FreelistHead: 4 bytes]
  [ChangeCounter: 4 bytes]
  [... reserved to 100 bytes ...]
  [Slotted page content: 3996 bytes]

Pages 1+:
  [SlottedPage: 4096 bytes]
    [Header: 9 bytes]
      PageType (1) ─ CellCount (2) ─ CellContentStart (2) ─ RightChild/NextLeaf (4)
    [CellPointers: 2 bytes each]
    [Free space]
    [Cells: growing backward from page end]
```

---

## FileStorageDevice

`FileStorageDevice` wraps a `SafeFileHandle` opened with `FileOptions.Asynchronous | FileOptions.RandomAccess`, giving you:

- **True async I/O** via `RandomAccess.ReadAsync` / `RandomAccess.WriteAsync`
- **Position-independent reads and writes** -- no seek, no shared file pointer
- **Concurrent reads** from other processes (`FileShare.Read`)
- **Direct fsync** to durable storage via `RandomAccess.FlushToDisk`

```csharp
public FileStorageDevice(string filePath, bool createNew = false)
```

| Parameter    | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `filePath`   | Path to the database file.                                                  |
| `createNew`  | `true` -> `FileMode.CreateNew` (fails if file exists). `false` -> `OpenOrCreate`. |

---

## IStorageDevice Interface

All storage operations go through `IStorageDevice`, making it easy to swap implementations (e.g., for in-memory testing).

```csharp
public interface IStorageDevice : IAsyncDisposable, IDisposable
{
    long Length { get; }
    ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default);
    ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default);
    ValueTask FlushAsync(CancellationToken ct = default);
    ValueTask SetLengthAsync(long length, CancellationToken ct = default);
}
```

---

## Device Scenarios

### 1. Create a New File

Creates the file, failing if it already exists.

```csharp
await using var device = new FileStorageDevice("mydb.cdb", createNew: true);
Console.WriteLine($"File created. Length: {device.Length}"); // 0
```

### 2. Open an Existing File

Opens the file if it exists, or creates it if it does not.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
Console.WriteLine($"Opened. Length: {device.Length}");
```

### 3. Write Raw Bytes at an Offset

Writes are position-independent. Multiple writes at different offsets can be issued concurrently without locking.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
byte[] payload = "Hello, CSharpDB!"u8.ToArray();
await device.WriteAsync(offset: 0, payload);
```

### 4. Read Raw Bytes from an Offset

`ReadAsync` loops internally until the buffer is fully filled or EOF is reached.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
var buffer = new byte[16];
int bytesRead = await device.ReadAsync(offset: 0, buffer);
Console.WriteLine($"Read {bytesRead} byte(s): {System.Text.Encoding.UTF8.GetString(buffer, 0, bytesRead)}");
```

### 5. Read Past End of File (Zero-Fill Behavior)

If you read a range that extends beyond the current file length, `ReadAsync` zero-fills the remainder of the buffer and returns the number of bytes that were actually on disk. This is useful for treating uninitialized pages as zeroed memory.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
// File is currently 16 bytes; request 4096 bytes
var buffer = new byte[4096];
int bytesRead = await device.ReadAsync(offset: 0, buffer);
Console.WriteLine($"Bytes on disk: {bytesRead}");          // 16
Console.WriteLine($"Remainder is zeros: {buffer[16] == 0}"); // true
```

### 6. Pre-allocate / Extend File Length

Pre-allocating avoids fragmentation on spinning disks and is required before writing pages beyond the current end-of-file on some file systems.

```csharp
await using var device = new FileStorageDevice("mydb.cdb", createNew: true);
const int PageSize = 4096;
const int InitialPages = 8;
await device.SetLengthAsync(PageSize * InitialPages);
Console.WriteLine($"Pre-allocated: {device.Length} bytes"); // 32768
```

### 7. Flush to Disk (fsync)

`FlushAsync` calls `RandomAccess.FlushToDisk`, which issues a full fsync/FlushFileBuffers. Call this after committing a transaction to guarantee durability.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
byte[] data = new byte[4096];
await device.WriteAsync(offset: 0, data);
await device.FlushAsync(); // durable on disk after this returns
```

### 8. Check File Length

The `Length` property reads the current file size directly from the OS without seeking.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
long pages = device.Length / 4096;
Console.WriteLine($"Database has {pages} page(s).");
```

### 9. Dispose Synchronously

`FileStorageDevice` implements `IDisposable` for non-async contexts such as unit tests or top-level using statements.

```csharp
using var device = new FileStorageDevice("mydb.cdb");
// ... operations ...
// Disposed when leaving the scope.
```

### 10. Dispose Asynchronously

Prefer `await using` in async code paths to align with the async I/O model.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
// ... operations ...
// DisposeAsync called automatically.
```

### 11. Cancellation Support

All async methods accept a `CancellationToken`. Pass one to abort long reads or writes cleanly.

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
await using var device = new FileStorageDevice("mydb.cdb");
var buffer = new byte[4096];
try
{
    await device.ReadAsync(offset: 0, buffer, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine("Read was cancelled.");
}
```

### 12. Injecting via IStorageDevice (Testability)

Program against `IStorageDevice` so you can substitute a fake or in-memory implementation in tests without touching the file system.

```csharp
// Production wiring
IStorageDevice device = new FileStorageDevice("mydb.cdb");
var pager = await Pager.CreateAsync(device, wal, walIndex);

// In a unit test -- swap in your own IStorageDevice implementation
public sealed class MemoryStorageDevice : IStorageDevice
{
    private byte[] _data = [];
    public long Length => _data.Length;

    public ValueTask<int> ReadAsync(long offset, Memory<byte> buffer, CancellationToken ct = default)
    {
        int available = (int)Math.Max(0, _data.Length - offset);
        int toCopy = Math.Min(buffer.Length, available);
        _data.AsMemory((int)offset, toCopy).CopyTo(buffer);
        buffer[toCopy..].Span.Clear();
        return ValueTask.FromResult(toCopy);
    }

    public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        long needed = offset + buffer.Length;
        if (needed > _data.Length)
            Array.Resize(ref _data, (int)needed);
        buffer.CopyTo(_data.AsMemory((int)offset));
        return ValueTask.CompletedTask;
    }

    public ValueTask FlushAsync(CancellationToken ct = default) => ValueTask.CompletedTask;

    public ValueTask SetLengthAsync(long length, CancellationToken ct = default)
    {
        Array.Resize(ref _data, (int)length);
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public void Dispose() { }
}
```

### 13. Writing Fixed-Size Pages (4 KB)

The storage engine works in 4 096-byte pages (see `PageConstants.PageSize`). Write a page at a computed offset.

```csharp
await using var device = new FileStorageDevice("mydb.cdb", createNew: true);
const int PageSize = 4096;

// Pre-allocate space for 4 pages
await device.SetLengthAsync(PageSize * 4);

// Build a page payload
byte[] page = new byte[PageSize];
System.Text.Encoding.UTF8.GetBytes("page-0 data").CopyTo(page.AsSpan());

// Write page 0 at offset 0, page 1 at offset 4096, etc.
uint pageId = 0;
await device.WriteAsync(offset: (long)pageId * PageSize, page);
```

### 14. Reading Fixed-Size Pages (4 KB)

Read a page back by computing its byte offset from its page number.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
const int PageSize = 4096;

uint pageId = 0;
byte[] page = new byte[PageSize];
int read = await device.ReadAsync(offset: (long)pageId * PageSize, page);
Console.WriteLine($"Page {pageId}: read={read}, first bytes={System.Text.Encoding.UTF8.GetString(page, 0, 10)}");
```

### 15. Appending Sequential Pages

Grow the file by one page at a time and write content into each new page.

```csharp
await using var device = new FileStorageDevice("mydb.cdb", createNew: true);
const int PageSize = 4096;
int pagesToAppend = 3;

for (int i = 0; i < pagesToAppend; i++)
{
    long newLength = device.Length + PageSize;
    await device.SetLengthAsync(newLength);

    byte[] page = new byte[PageSize];
    BitConverter.TryWriteBytes(page, i); // store page index in first 4 bytes
    await device.WriteAsync(offset: newLength - PageSize, page);
}

Console.WriteLine($"Final file size: {device.Length} bytes"); // 12288
await device.FlushAsync();
```

---

## Pager

The `Pager` sits between the B+tree layer and the storage device. It owns the page cache, tracks dirty pages, coordinates transactions, manages WAL integration, and drives checkpointing.

```csharp
public static async ValueTask<Pager> CreateAsync(
    IStorageDevice device,
    IWriteAheadLog wal,
    WalIndex walIndex,
    PagerOptions? options = null,
    CancellationToken ct = default)
```

### P1. Create a New Database

```csharp
await using var device = new FileStorageDevice("mydb.cdb", createNew: true);
var walIndex = new WalIndex();
await using var wal = new WriteAheadLog("mydb.cdb", walIndex);
await wal.OpenAsync(currentDbPageCount: 0);

var pager = await Pager.CreateAsync(device, wal, walIndex);
await pager.InitializeNewDatabaseAsync(); // writes file header (page 0)

Console.WriteLine($"Pages: {pager.PageCount}"); // 1 (the file header page)
```

### P2. Open and Recover an Existing Database

On startup, call `RecoverAsync` to redo any committed WAL frames that were not yet checkpointed.

```csharp
await using var device = new FileStorageDevice("mydb.cdb");
var walIndex = new WalIndex();
await using var wal = new WriteAheadLog("mydb.cdb", walIndex);

var pager = await Pager.CreateAsync(device, wal, walIndex);
await pager.RecoverAsync(); // replays committed WAL frames
```

### P3. Read and Write Pages

```csharp
// Read a page (checks cache -> WAL -> device)
byte[] page = await pager.GetPageAsync(pageId: 1);

// Modify the page buffer in-place, then mark dirty
page[0] = 0xFF;
await pager.MarkDirtyAsync(pageId: 1); // tracked for WAL write on commit
```

### P4. Allocate and Free Pages

```csharp
// Allocate a new page (extends file or reuses from freelist)
uint newPageId = await pager.AllocatePageAsync();

// Free a page (adds to freelist for reuse)
await pager.FreePageAsync(newPageId);
```

### P5. Transaction Lifecycle

Single writer per database. Reads do not require transactions.

```csharp
await pager.BeginTransactionAsync();
try
{
    // ... modify pages via B+tree ...
    await pager.CommitAsync(); // writes dirty pages to WAL, fsync
}
catch
{
    await pager.RollbackAsync(); // discards uncommitted WAL frames
    throw;
}
```

### P6. Snapshot Isolation (Concurrent Readers)

Multiple readers can run concurrently with a single writer. Each reader sees a consistent snapshot.

```csharp
// Writer thread: acquire snapshot for a reader
WalSnapshot snapshot = pager.AcquireReaderSnapshot();

// Reader thread: create a snapshot pager that sees only committed data
Pager snapshotPager = pager.CreateSnapshotReader(snapshot);
byte[] page = await snapshotPager.GetPageAsync(pageId: 1);

// When reader is done:
pager.ReleaseReaderSnapshot();
```

### P7. Manual Checkpoint

Copy all committed WAL frames to the main database file, then reset the WAL.

```csharp
await pager.CheckpointAsync();
```

### P8. Configure Checkpoint Policy

```csharp
var options = new PagerOptions
{
    CheckpointPolicy = new AnyCheckpointPolicy(
        new FrameCountCheckpointPolicy(threshold: 500),
        new TimeIntervalCheckpointPolicy(TimeSpan.FromMinutes(5))
    )
};

var pager = await Pager.CreateAsync(device, wal, walIndex, options);
// Auto-checkpoint triggers after 500 frames OR 5 minutes, whichever comes first
```

Built-in policies:

| Policy | Triggers When |
|--------|---------------|
| `FrameCountCheckpointPolicy(n)` | Committed frame count exceeds `n` |
| `WalSizeCheckpointPolicy(bytes)` | Estimated WAL size exceeds `bytes` |
| `TimeIntervalCheckpointPolicy(span)` | Elapsed time since last checkpoint exceeds `span` |
| `AnyCheckpointPolicy(...)` | Any sub-policy triggers |

---

## B+Tree

B+tree keyed by `long` rowid. Leaf pages store `(key, payload)` pairs; interior pages store routing keys and child pointers. Supports forward-only cursor iteration and cache-only fast paths.

### B1. Create a New B+Tree

```csharp
// Allocates a root page and returns its ID
uint rootPageId = await BTree.CreateNewAsync(pager);

// Open the tree
var tree = new BTree(pager, rootPageId);
```

### B2. Insert a Key-Value Pair

Payload is raw bytes -- the B+tree has no opinion on format.

```csharp
byte[] payload = System.Text.Encoding.UTF8.GetBytes("Hello, B+tree!");
await tree.InsertAsync(key: 42, payload);
```

If the leaf page is full, the tree automatically splits the leaf and propagates the split key up to interior pages.

### B3. Point Lookup

```csharp
byte[]? result = await tree.FindAsync(key: 42);
if (result is not null)
    Console.WriteLine(System.Text.Encoding.UTF8.GetString(result));
```

### B4. Cache-Only Fast Path

Avoids async I/O when all pages are cached (50-70% hit rate typical).

```csharp
if (tree.TryFindCached(key: 42, out byte[]? payload))
{
    // Hit: payload is definitive (null = not found, non-null = value)
    Console.WriteLine($"Cache hit: {payload is not null}");
}
else
{
    // Miss: need to call FindAsync for full traversal
    payload = await tree.FindAsync(key: 42);
}
```

### B5. Delete a Key

```csharp
bool deleted = await tree.DeleteAsync(key: 42);
Console.WriteLine(deleted ? "Deleted." : "Key not found.");
```

### B6. Forward Cursor Scan

Iterate all entries in key order. The cursor follows leaf-to-leaf next pointers (no interior page I/O after the first leaf).

```csharp
var cursor = tree.CreateCursor();
while (await cursor.MoveNextAsync())
{
    long key = cursor.CurrentKey;
    ReadOnlyMemory<byte> value = cursor.CurrentValue;
    Console.WriteLine($"Key={key}, PayloadSize={value.Length}");
}
```

### B7. Seek to a Specific Key

Position the cursor at the first key >= target, then iterate forward.

```csharp
var cursor = tree.CreateCursor();
if (await cursor.SeekAsync(targetKey: 100))
{
    do
    {
        Console.WriteLine($"Key={cursor.CurrentKey}");
    } while (await cursor.MoveNextAsync());
}
```

### B8. Count Entries

Walks all leaf pages and sums cell counts.

```csharp
long count = await tree.CountEntriesAsync();
Console.WriteLine($"Tree contains {count} entries.");
```

---

## Write-Ahead Log (WAL)

Redo-style WAL for crash recovery and concurrent snapshot-isolated readers. Each commit writes dirty pages as frames to the WAL file. On checkpoint, committed frames are copied to the main database file.

```
WAL File Format:
  [WAL Header: 32 bytes]
    Magic ─ Version ─ PageSize ─ Checksum salt
  [Frame 0: 4120 bytes]
    [FrameHeader: 24 bytes] ─ PageId ─ DbPageCount ─ Checksum
    [PageData: 4096 bytes]
  [Frame 1: 4120 bytes]
    ...
```

### W1. Open or Create a WAL

```csharp
var walIndex = new WalIndex();
await using var wal = new WriteAheadLog("mydb.cdb", walIndex);
await wal.OpenAsync(currentDbPageCount: pager.PageCount);
// WAL file: "mydb.cdb.wal"
```

### W2. Write Transaction to WAL

```csharp
wal.BeginTransaction();

// Append modified pages as frames
await wal.AppendFrameAsync(pageId: 1, pageData);
await wal.AppendFrameAsync(pageId: 5, pageData);

// Commit: the last frame gets dbPageCount > 0, marking the commit boundary
await wal.CommitAsync(newDbPageCount: pager.PageCount);

// Or rollback: truncates uncommitted frames
await wal.RollbackAsync();
```

### W3. Take a Reader Snapshot

Freeze the WAL index at a point in time for a concurrent reader.

```csharp
WalSnapshot snapshot = walIndex.TakeSnapshot();

// Reader uses snapshot to resolve page lookups:
if (snapshot.TryGet(pageId: 1, out long walOffset))
{
    byte[] page = await wal.ReadPageAsync(walOffset);
    // page is the committed version at snapshot time
}
```

### W4. Checkpoint WAL to Database File

Copy all committed WAL pages to the main database file, then reset the WAL.

```csharp
await wal.CheckpointAsync(device, pageCount: pager.PageCount);
walIndex.Reset();
// WAL is now empty; all data is in the main file
```

---

## Slotted Page Layout

`SlottedPage` is a struct that overlays a `byte[4096]` buffer, providing structured access to variable-size cells within a fixed-size page.

```
[Header: 9 bytes]
  PageType (1) ─ CellCount (2) ─ CellContentStart (2) ─ RightChild/NextLeaf (4)
[Cell Pointers: 2 bytes each, growing forward]
  Offset to cell data for each cell
[Free Space]
[Cell Data: growing backward from page end]
  Variable-size cells packed from the end
```

### S1. Initialize a Page

```csharp
byte[] buffer = new byte[4096];
var sp = new SlottedPage(buffer, pageId: 1);
sp.Initialize(PageConstants.PageTypeLeaf);

Console.WriteLine($"Type: {sp.PageType}");        // Leaf
Console.WriteLine($"Cells: {sp.CellCount}");      // 0
Console.WriteLine($"Free: {sp.FreeSpace} bytes");  // ~4085
```

### S2. Insert and Read Cells

```csharp
byte[] cellData = new byte[] { 0x01, 0x02, 0x03, 0x04 };
bool inserted = sp.InsertCell(index: 0, cellData);
Console.WriteLine($"Inserted: {inserted}"); // true

Span<byte> cell = sp.GetCell(index: 0);
Console.WriteLine($"Cell[0] length: {cell.Length}"); // 4
```

### S3. Delete a Cell and Defragment

```csharp
sp.DeleteCell(index: 0);
Console.WriteLine($"Cells after delete: {sp.CellCount}"); // 0

// After many inserts/deletes, free space may be fragmented
sp.Defragment(); // rewrites cells contiguously at end of page
```

---

## Indexing

Secondary B+tree-backed indexes with optional caching and ordered range scan support.

```csharp
public interface IIndexStore
{
    uint RootPageId { get; }
    ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default);
    ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default);
    IIndexCursor CreateCursor(IndexScanRange range);
}
```

### I1. Create an Index Store

```csharp
uint indexRootPage = await BTree.CreateNewAsync(pager);
var indexTree = new BTree(pager, indexRootPage);
IIndexStore index = new BTreeIndexStore(indexTree);
```

### I2. Insert and Lookup Index Entries

Index payload typically contains the rowid(s) of matching rows.

```csharp
// Insert: key = hashed column value, payload = rowid (8 bytes)
byte[] rowIdPayload = BitConverter.GetBytes(42L);
await index.InsertAsync(key: hashOfColumnValue, rowIdPayload);

// Lookup
byte[]? result = await index.FindAsync(key: hashOfColumnValue);
if (result is not null)
{
    long rowId = BitConverter.ToInt64(result);
    Console.WriteLine($"Found rowid: {rowId}");
}
```

### I3. Range Scan with Cursor

```csharp
var range = new IndexScanRange(
    LowerBound: 100, LowerInclusive: true,
    UpperBound: 200, UpperInclusive: false);

var cursor = index.CreateCursor(range);
while (await cursor.MoveNextAsync())
{
    Console.WriteLine($"IndexKey={cursor.CurrentKey}");
}

// Full scan:
var fullCursor = index.CreateCursor(IndexScanRange.All);

// Point lookup as cursor:
var pointCursor = index.CreateCursor(IndexScanRange.At(42));
```

### I4. Add Caching to an Index

Wrap any `IIndexStore` with an LRU cache for repeated lookups.

```csharp
IIndexStore cached = new CachingIndexStore(
    inner: new BTreeIndexStore(indexTree),
    capacity: 2048);

// Lookups check cache first; inserts/deletes update cache
byte[]? result = await cached.FindAsync(key: 42);
```

---

## Record Serialization

Compact binary encoding for database rows. Supports selective column projection and fast filter evaluation without materializing managed strings.

```
Binary Format:
  [columnCount: varint]
  [col0_typeTag: 1 byte] [col0_data: ...]
  [col1_typeTag: 1 byte] [col1_data: ...]
  ...

Type Tags:
  Null (0x00)    -> no data
  Integer (0x01) -> 8 bytes, little-endian long
  Text (0x02)    -> [length: varint] [UTF-8 bytes]
  Real (0x03)    -> 8 bytes, little-endian double (IEEE 754)
  Blob (0x04)    -> [length: varint] [raw bytes]
```

### R1. Encode and Decode a Row

```csharp
var values = new DbValue[]
{
    DbValue.FromInteger(1),
    DbValue.FromText("Alice"),
    DbValue.FromInteger(30)
};

byte[] encoded = RecordEncoder.Encode(values);
DbValue[] decoded = RecordEncoder.Decode(encoded);

Console.WriteLine($"Id={decoded[0].AsInteger}, Name={decoded[1].AsText}, Age={decoded[2].AsInteger}");
```

### R2. Selective Column Projection

Decode only the columns you need -- avoids materializing unused fields.

```csharp
// Decode only columns 0 and 1 (skip column 2)
DbValue[] partial = RecordEncoder.DecodeUpTo(encoded, maxColumnIndexInclusive: 1);

// Decode a single column by index
DbValue age = RecordEncoder.DecodeColumn(encoded, columnIndex: 2);
```

### R3. Fast Filter Without Materialization

Evaluate filters on encoded rows without allocating managed strings.

```csharp
// Check if column 1 equals "Alice" without creating a string
byte[] expectedUtf8 = "Alice"u8.ToArray();
if (RecordEncoder.TryColumnTextEquals(encoded, columnIndex: 1, expectedUtf8, out bool equals))
{
    Console.WriteLine($"Column 1 is Alice: {equals}");
}

// Check numeric column for comparison
if (RecordEncoder.TryDecodeNumericColumn(encoded, columnIndex: 2,
    out long intValue, out double realValue, out bool isReal))
{
    Console.WriteLine($"Age: {intValue}");
}

// Check for null
bool isNull = RecordEncoder.IsColumnNull(encoded, columnIndex: 0);
```

### R4. Varint Encoding

Variable-length unsigned integer encoding (LEB128-style). Small values encode in 1 byte; up to 64-bit values supported.

```csharp
Span<byte> buffer = stackalloc byte[10];
int bytesWritten = Varint.Write(buffer, 300UL);

ulong value = Varint.Read(buffer, out int bytesRead);
Console.WriteLine($"Value: {value}, Bytes: {bytesRead}"); // 300, 2

int predictedSize = Varint.SizeOf(300UL); // 2
```

---

## Schema Catalog

B+tree-backed metadata store for tables, indexes, views, and triggers. Provides in-memory caching with a schema version counter for cache invalidation.

### C1. Initialize the Catalog

```csharp
var catalog = await SchemaCatalog.CreateAsync(pager);
Console.WriteLine($"Schema version: {catalog.SchemaVersion}");
```

### C2. Create and Query Tables

```csharp
// Create a table
var schema = new TableSchema
{
    TableName = "users",
    Columns = new[]
    {
        new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true },
        new ColumnDefinition { Name = "name", Type = DbType.Text },
        new ColumnDefinition { Name = "age", Type = DbType.Integer },
    }
};
await catalog.CreateTableAsync(schema);

// Query table metadata
TableSchema? users = catalog.GetTable("users");
uint rootPage = catalog.GetTableRootPage("users");
BTree tableTree = catalog.GetTableTree("users");

// List all tables
IReadOnlyCollection<string> tableNames = catalog.GetTableNames();

// For snapshot readers
BTree snapshotTree = catalog.GetTableTree("users", snapshotPager);
```

### C3. Create and Query Indexes

```csharp
var indexSchema = new IndexSchema
{
    IndexName = "idx_users_name",
    TableName = "users",
    Columns = new[] { "name" },
    IsUnique = false,
};
await catalog.CreateIndexAsync(indexSchema);

// Get index store
IIndexStore indexStore = catalog.GetIndexStore("idx_users_name");

// List indexes for a table
IReadOnlyList<IndexSchema> indexes = catalog.GetIndexesForTable("users");

// For snapshot readers
IIndexStore snapshotIndex = catalog.GetIndexStore("idx_users_name", snapshotPager);
```

### C4. Views and Triggers

```csharp
// Views
await catalog.CreateViewAsync("active_users", "SELECT * FROM users WHERE age > 18");
string? viewSql = catalog.GetViewSql("active_users");
bool isView = catalog.IsView("active_users");

// Triggers
var trigger = new TriggerSchema
{
    TriggerName = "trg_users_audit",
    TableName = "users",
    Event = TriggerEvent.AfterInsert,
    Body = "INSERT INTO audit_log (table_name, action) VALUES ('users', 'INSERT')",
};
await catalog.CreateTriggerAsync(trigger);

IReadOnlyList<TriggerSchema> triggers = catalog.GetTriggersForTable("users");
```

### C5. Persist Root Page Changes

After B+tree operations that split the root page, persist the new root page ID in the catalog.

```csharp
// Persist for a single table + its indexes (fast)
await catalog.PersistRootPageChangesAsync("users");

// Persist for all tables and indexes (slower, used during batch operations)
await catalog.PersistAllRootPageChangesAsync();
```

---

## Folder & File Storage

`FileStorageDevice` is a raw byte device. To build a **folder/file storage system** on top of it, use the higher-level `Database` + `Collection<T>` API from `CSharpDB.Engine`. A single `.cdb` file (backed by one `FileStorageDevice`) holds all folders and files as typed JSON documents in B+tree-backed collections.

> **Add the Engine reference** to your project if it is not already there:
> ```xml
> <ProjectReference Include="..\CSharpDB.Engine\CSharpDB.Engine.csproj" />
> ```

---

### Domain Models

Define records that represent a folder and a file entry. These are serialized as JSON by `Collection<T>`.

```csharp
public record FolderEntry(
    string Name,
    string Path,              // e.g. "/documents/reports"
    DateTime CreatedAt,
    string? Description = null);

public record FileEntry(
    string Name,
    string FolderPath,        // parent folder path
    string Content,           // UTF-8 text content (or Base64 for binary)
    string ContentType,       // e.g. "text/plain", "application/json"
    long SizeBytes,
    DateTime CreatedAt,
    DateTime UpdatedAt);
```

Keys follow a **path convention**:
- Folders -> `"/documents/reports"`
- Files   -> `"/documents/reports/summary.txt"`

### F1. Bootstrap the Storage

Open (or create) a single `.cdb` file and obtain the two collections.

```csharp
await using var db = await Database.OpenAsync("storage.cdb");
var folders = await db.GetCollectionAsync<FolderEntry>("folders");
var files   = await db.GetCollectionAsync<FileEntry>("files");
```

Everything -- the B+tree pages, the WAL, and the page cache -- is managed by the single `FileStorageDevice` that `Database.OpenAsync` creates internally.

### F2. Create a Folder

Use the folder's path as the collection key so lookups are O(1) hash-based.

```csharp
async Task CreateFolderAsync(string path, string? description = null)
{
    var entry = new FolderEntry(
        Name:        Path.GetFileName(path.TrimEnd('/')),
        Path:        path,
        CreatedAt:   DateTime.UtcNow,
        Description: description);

    await folders.PutAsync(path, entry);
}

await CreateFolderAsync("/documents");
await CreateFolderAsync("/documents/reports", description: "Monthly reports");
await CreateFolderAsync("/images");
```

### F3. Create a File Inside a Folder

The key is the full file path, which guarantees uniqueness across all folders.

```csharp
async Task CreateFileAsync(string folderPath, string fileName, string content, string contentType = "text/plain")
{
    string key = $"{folderPath}/{fileName}";
    var entry = new FileEntry(
        Name:        fileName,
        FolderPath:  folderPath,
        Content:     content,
        ContentType: contentType,
        SizeBytes:   System.Text.Encoding.UTF8.GetByteCount(content),
        CreatedAt:   DateTime.UtcNow,
        UpdatedAt:   DateTime.UtcNow);

    await files.PutAsync(key, entry);
}

await CreateFileAsync("/documents/reports", "q1.txt",  "Q1 earnings: $1.2M");
await CreateFileAsync("/documents/reports", "q2.txt",  "Q2 earnings: $1.5M");
await CreateFileAsync("/documents",          "notes.md", "# Notes\nTodo list...", "text/markdown");
await CreateFileAsync("/images",             "logo.svg", "<svg>...</svg>",        "image/svg+xml");
```

### F4. Read a File

Retrieve a file by its full path key.

```csharp
FileEntry? file = await files.GetAsync("/documents/reports/q1.txt");

if (file is not null)
{
    Console.WriteLine($"Name:    {file.Name}");
    Console.WriteLine($"Type:    {file.ContentType}");
    Console.WriteLine($"Size:    {file.SizeBytes} bytes");
    Console.WriteLine($"Content: {file.Content}");
}
else
{
    Console.WriteLine("File not found.");
}
```

### F5. List All Files in a Folder

`Collection<T>.FindAsync` performs a full scan with an in-memory predicate -- suitable for small-to-medium collections.

```csharp
string targetFolder = "/documents/reports";
await foreach (var kvp in files.FindAsync(f => f.FolderPath == targetFolder))
{
    Console.WriteLine($"  {kvp.Value.Name}  ({kvp.Value.SizeBytes} bytes)  [{kvp.Value.UpdatedAt:u}]");
}
```

### F6. List All Folders

```csharp
await foreach (var kvp in folders.ScanAsync())
{
    var f = kvp.Value;
    Console.WriteLine($"{f.Path,-40} created {f.CreatedAt:u}");
}
```

### F7. Update File Content

`PutAsync` is an upsert -- it replaces the document at the key if it already exists.

```csharp
async Task UpdateFileAsync(string filePath, string newContent)
{
    FileEntry? existing = await files.GetAsync(filePath);
    if (existing is null) throw new FileNotFoundException($"File not found: {filePath}");

    var updated = existing with
    {
        Content   = newContent,
        SizeBytes = System.Text.Encoding.UTF8.GetByteCount(newContent),
        UpdatedAt = DateTime.UtcNow
    };

    await files.PutAsync(filePath, updated);
}

await UpdateFileAsync("/documents/reports/q1.txt", "Q1 earnings: $1.4M (revised)");
```

### F8. Delete a File

```csharp
bool deleted = await files.DeleteAsync("/documents/reports/q2.txt");
Console.WriteLine(deleted ? "File deleted." : "File not found.");
```

### F9. Delete a Folder and Its Contents

There is no cascading delete built in, so collect the child keys first, then delete in a single transaction.

```csharp
async Task DeleteFolderAsync(string folderPath)
{
    // Collect all file keys under this folder
    var toDelete = new List<string>();
    await foreach (var kvp in files.FindAsync(f => f.FolderPath.StartsWith(folderPath, StringComparison.Ordinal)))
        toDelete.Add(kvp.Key);

    await db.BeginTransactionAsync();
    try
    {
        foreach (var key in toDelete)
            await files.DeleteAsync(key);
        await folders.DeleteAsync(folderPath);
        await db.CommitAsync();
    }
    catch
    {
        await db.RollbackAsync();
        throw;
    }
}

await DeleteFolderAsync("/documents/reports");
```

### F10. Rename or Move a File

CSharpDB does not have a rename primitive; copy the document to the new key and delete the old one inside a transaction.

```csharp
async Task MoveFileAsync(string sourcePath, string destinationPath)
{
    FileEntry? source = await files.GetAsync(sourcePath);
    if (source is null) throw new FileNotFoundException($"Source not found: {sourcePath}");

    string newFolder   = Path.GetDirectoryName(destinationPath)!.Replace('\\', '/');
    string newFileName = Path.GetFileName(destinationPath);
    var moved = source with
    {
        Name       = newFileName,
        FolderPath = newFolder,
        UpdatedAt  = DateTime.UtcNow
    };

    await db.BeginTransactionAsync();
    try
    {
        await files.PutAsync(destinationPath, moved);
        await files.DeleteAsync(sourcePath);
        await db.CommitAsync();
    }
    catch
    {
        await db.RollbackAsync();
        throw;
    }
}

await MoveFileAsync("/documents/notes.md", "/documents/reports/notes.md");
```

### F11. Search Files by Predicate

Find all Markdown files larger than 100 bytes modified after a given date.

```csharp
DateTime since = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
await foreach (var kvp in files.FindAsync(f =>
    f.ContentType == "text/markdown" &&
    f.SizeBytes   > 100             &&
    f.UpdatedAt   > since))
{
    Console.WriteLine($"{kvp.Key}  ({kvp.Value.SizeBytes} bytes)");
}
```

### F12. Bulk Create with an Explicit Transaction

Wrap multiple writes in a single transaction so they all succeed or all roll back together.

```csharp
string[] reportNames = ["jan.txt", "feb.txt", "mar.txt", "apr.txt"];

await db.BeginTransactionAsync();
try
{
    await CreateFolderAsync("/archive/2025");
    foreach (var name in reportNames)
        await CreateFileAsync("/archive/2025", name, $"Report: {name}");
    await db.CommitAsync();
    Console.WriteLine($"Committed {reportNames.Length} files in one transaction.");
}
catch
{
    await db.RollbackAsync();
    throw;
}
```

### F13. SQL-Based Approach

If you prefer a relational model, create `folders` and `files` tables with SQL and use `ExecuteAsync`.

```csharp
await using var db = await Database.OpenAsync("storage.cdb");

// Create schema
await db.ExecuteAsync("""
    CREATE TABLE IF NOT EXISTS folders (
        id          INTEGER PRIMARY KEY,
        path        TEXT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT,
        created_at  TEXT NOT NULL
    )
    """);

await db.ExecuteAsync("""
    CREATE TABLE IF NOT EXISTS files (
        id           INTEGER PRIMARY KEY,
        folder_path  TEXT NOT NULL,
        name         TEXT NOT NULL,
        content      TEXT NOT NULL,
        content_type TEXT NOT NULL,
        size_bytes   INTEGER NOT NULL,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL
    )
    """);

// Insert a folder
await db.ExecuteAsync("""
    INSERT INTO folders (path, name, created_at)
    VALUES ('/documents', 'documents', '2025-01-01T00:00:00Z')
    """);

// Insert a file
await db.ExecuteAsync("""
    INSERT INTO files (folder_path, name, content, content_type, size_bytes, created_at, updated_at)
    VALUES ('/documents', 'readme.txt', 'Hello world', 'text/plain', 11,
            '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
    """);

// Query files in a folder
var result = await db.ExecuteAsync("SELECT name, size_bytes FROM files WHERE folder_path = '/documents'");
foreach (var row in result.Rows)
    Console.WriteLine($"{row[0]}  ({row[1]} bytes)");
```

### F14. One Database File per Folder (Multi-Volume)

Map each top-level folder to its own `.cdb` file. Each file gets its own `FileStorageDevice` instance, giving you independent WAL, checkpoint, and locking per folder.

```csharp
// Each folder is a separate database file
var volumes = new Dictionary<string, Database>(StringComparer.Ordinal);

async ValueTask<Database> GetVolumeAsync(string folderName)
{
    if (!volumes.TryGetValue(folderName, out var db))
    {
        db = await Database.OpenAsync($"{folderName}.cdb");
        volumes[folderName] = db;
    }
    return db;
}

// Write to the "documents" volume
var docsDb   = await GetVolumeAsync("documents");
var docsFiles = await docsDb.GetCollectionAsync<FileEntry>("files");
await docsFiles.PutAsync("readme.txt", new FileEntry(
    Name:        "readme.txt",
    FolderPath:  "/",
    Content:     "Welcome to the documents volume.",
    ContentType: "text/plain",
    SizeBytes:   32,
    CreatedAt:   DateTime.UtcNow,
    UpdatedAt:   DateTime.UtcNow));

// Write to the "images" volume
var imagesDb    = await GetVolumeAsync("images");
var imagesFiles = await imagesDb.GetCollectionAsync<FileEntry>("files");
await imagesFiles.PutAsync("logo.svg", new FileEntry(
    Name:        "logo.svg",
    FolderPath:  "/",
    Content:     "<svg>...</svg>",
    ContentType: "image/svg+xml",
    SizeBytes:   14,
    CreatedAt:   DateTime.UtcNow,
    UpdatedAt:   DateTime.UtcNow));

// Dispose all volumes on shutdown
foreach (var (_, volume) in volumes)
    await volume.DisposeAsync();
```

> **When to use multi-volume:** large datasets where you want per-folder backup, different checkpoint intervals, or parallel writes to disjoint folders. For most use-cases a single `.cdb` file is simpler.

---

## Key Design Notes

| Concern | Detail |
|---|---|
| **No shared file pointer** | `RandomAccess` APIs are stateless w.r.t. position, so concurrent reads at different offsets are safe without locking. |
| **Async-first** | All I/O is issued via `RandomAccess.ReadAsync` / `WriteAsync`, wiring directly into the OS async I/O (IOCP on Windows, io_uring on Linux). |
| **Zero-fill on short reads** | `ReadAsync` always fills the entire buffer. Pages beyond EOF are returned as zeros, matching an uninitialized page convention used by the `Pager`. |
| **fsync on flush** | `FlushAsync` calls `RandomAccess.FlushToDisk` which maps to `FlushFileBuffers` (Windows) or `fsync` (Linux/macOS), guaranteeing crash durability. |
| **FileShare.Read** | Other processes can open the file read-only concurrently; write access is exclusive to the owning `FileStorageDevice` instance. |
| **IDisposable + IAsyncDisposable** | Both patterns are supported; prefer `await using` in async code. |
| **4 KB page size** | All pages are `PageConstants.PageSize` (4096 bytes). Page 0 reserves 100 bytes for the file header. |
| **Single writer, multiple readers** | The `TransactionCoordinator` enforces a single writer via `SemaphoreSlim`. Readers use WAL snapshots for isolation. |
| **B+tree leaf linking** | Leaf pages are linked via `RightChildOrNextLeaf` pointers, enabling efficient forward-only cursor scans without interior I/O. |
| **Pluggable checkpoint policies** | `ICheckpointPolicy` allows frame-count, WAL-size, time-interval, or custom composite triggers. |
| **Schema versioning** | `SchemaCatalog.SchemaVersion` increments on every DDL operation, enabling cache invalidation in upper layers. |
| **Interceptor pipeline** | `IPageOperationInterceptor` provides hooks for diagnostics, metrics, and custom behavior on page reads, writes, transactions, and checkpoints. |

---

## See Also

- [Architecture Guide](../architecture.md)
- [Collection Optimization Plan](../collection-optimization/README.md)
- [Benchmark Suite](../../tests/CSharpDB.Benchmarks/README.md)
- [Roadmap](../roadmap.md)

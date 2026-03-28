# CSharpDB.Storage Architecture

## Audience and Intent
This document explains how `CSharpDB.Storage` is designed and how its components collaborate. It is written for a mid-level C# engineer who is comfortable with .NET async patterns, interfaces, dependency injection concepts, and clean architecture principles.

---

## 1) Role of `CSharpDB.Storage` in the CSharpDB ecosystem

`CSharpDB.Storage` is the durability and physical layout layer for CSharpDB. It is responsible for:

- Persisting pages to a backing store via `IStorageDevice`.
- Managing in-memory page caching and dirty tracking (`Pager`).
- Providing crash safety and commit durability through a write-ahead log (`WriteAheadLog`).
- Offering a row-id keyed B+tree implementation used for table and index data (`BTree`, `BTreeCursor`, `SlottedPage`).
- Encoding row values and schema metadata into compact byte payloads (`RecordEncoder`, `SchemaSerializer`).
- Maintaining a system catalog for tables, indexes, views, and triggers (`SchemaCatalog`).

The higher layers (`CSharpDB.Engine`, `CSharpDB.Execution`, and SQL planner/executor components) treat storage as a stable persistence substrate. The engine creates/open storage objects during database open and delegates transaction lifecycle methods (`BeginTransactionAsync`, `CommitAsync`, `RollbackAsync`) to the `Pager`.

---

## 2) High-level architecture

At runtime, write/read operations flow through this stack:

1. **Engine / planner** decides what logical records to read/write.
2. **SchemaCatalog + BTree** resolve table/index trees and perform key-level operations.
3. **Pager** provides page access, cache, dirty bookkeeping, page allocation/freelist, transaction boundaries.
4. **WriteAheadLog + WalIndex** provide durability, commit visibility, recovery, and checkpointing.
5. **IStorageDevice** performs physical file reads/writes/flushes.

Conceptually:

- **BTree is logical page consumer** (structure + algorithms).
- **Pager is transactional page manager** (state + coordination).
- **WAL is durability journal** (redo log with commit markers).
- **Storage device is pluggable I/O boundary**.

---

## 3) File format and on-disk layout

### 3.1 Database pages

- Fixed page size: **4096 bytes** (`PageConstants.PageSize`).
- Page 0 contains a **100-byte file header**, then usable page content.
- Remaining pages are fully usable for B+tree or freelist content.

File header tracks key metadata:

- magic/version/page size
- total page count
- schema catalog root page id
- freelist head page id
- change counter

### 3.2 Slotted page structure

Each B+tree page uses a slotted layout:

- Header fields: page type, cell count, content-start pointer, and right-child/next-leaf pointer.
- Pointer array grows from the front.
- Cell content grows from the end.
- Free space is in the middle.

`SlottedPage` encapsulates this structure and provides:

- cell insert/delete
- free-space accounting
- defragmentation
- typed access to page header fields

### 3.3 WAL file layout

WAL file (`<db>.wal`) has:

- 32-byte WAL header (magic/version/page size/salts)
- repeated frames, each:
  - 24-byte frame header
  - full 4096-byte page image

Frames are appended for dirty pages. A transaction is committed by marking the **last frame** with non-zero `dbPageCount` and flushing the WAL stream.

---

## 4) Core components and responsibilities

## 4.1 I/O abstraction: `IStorageDevice` and `FileStorageDevice`

- `IStorageDevice` defines async primitives for random reads/writes, flushing, resizing, and disposal.
- `FileStorageDevice` is the default implementation using `RandomAccess` APIs and asynchronous random I/O.
- Read behavior zero-fills unread portions when reading beyond file end, simplifying caller logic.

**Why it matters:** this is the main extensibility seam for alternate backends (in-memory, encrypted, remote block device, etc.) without changing pager/BTree logic.

## 4.2 Page manager and transaction coordinator: `Pager`

`Pager` is the central coordinator.

Primary responsibilities:

- Caches pages in memory via `IPageCache` (`DictionaryPageCache` by default, `LruPageCache` when bounded, or a custom cache via `PagerOptions`).
- Tracks dirty page ids within a write transaction.
- Manages page allocation and freelist reuse.
- Reads file header / writes file header state.
- Owns transaction flow: begin, commit, rollback.
- Delegates WAL appends/commit/rollback/checkpoint.
- Supports concurrent snapshot readers and checkpoint coordination.

Concurrency model inside pager:

- **Single writer lock** (`SemaphoreSlim`) with timeout.
- **Checkpoint lock** to serialize checkpoint operations.
- **Active reader counter** prevents checkpoint while snapshot readers are alive.
- Snapshot readers use immutable `WalSnapshot` state and a read-only pager instance.

Transaction behavior:

- `BeginTransactionAsync`: acquires writer lock and starts WAL tx.
- `CommitAsync`: writes all dirty pages as WAL frames, commits WAL, clears dirty set, releases lock.
- `RollbackAsync`: truncates uncommitted WAL frames, clears cache/dirty state, rehydrates header state.

Checkpoint behavior:

- Triggered manually or automatically when WAL frame count threshold is reached.
- Skips if active readers hold snapshots.
- Applies committed WAL pages back into main DB file and resets WAL/index.

## 4.3 Durability and recovery: `WriteAheadLog` + `WalIndex`

`WriteAheadLog` responsibilities:

- Create/open WAL file.
- Append page frames for dirty pages.
- Mark commit by rewriting final frame header (`dbPageCount`) + checksum + flush.
- Recover by scanning WAL, validating frame salts/checksums, rebuilding committed index, truncating partial/corrupt tail.
- Checkpoint committed pages to DB file and reset WAL.

`WalIndex` responsibilities:

- Map `pageId -> latest committed wal offset`.
- Track committed frames/count.
- Produce `WalSnapshot` copies for readers.

`WalSnapshot` enables snapshot isolation:

- Readers resolve page lookups through a frozen page-to-offset map.
- New commits do not affect an existing snapshot.

## 4.4 Data structure layer: `BTree` and `BTreeCursor`

`BTree` is a row-id keyed B+tree (`long` key).

- Leaf cells: key + payload bytes.
- Interior cells: left child + separator key.
- Interior pages also use header right-child pointer.
- Leaf pages chain via `nextLeaf` pointer for scans.

Supported operations:

- point lookup (`FindAsync`, plus cache-only fast path `TryFindCached`)
- insert with recursive split propagation
- delete (simplified; no underflow rebalance/merge)
- full count by leaf traversal
- cursor creation

Performance-oriented details:

- Leaf hint cache (`_hintLeafPageId`, min/max key range) can bypass root-to-leaf traversal for clustered lookups.
- Binary search utilities (`LowerBoundLeaf`, `UpperBoundInterior`) on per-page cells.

`BTreeCursor`:

- Forward-only iterator.
- Starts at leftmost leaf and advances through leaf-linked list.
- Supports `SeekAsync(targetKey)` then incremental `MoveNextAsync`.

## 4.5 Serialization utilities: `RecordEncoder`, `Varint`, `SchemaSerializer`

### Record encoding (`RecordEncoder`)

Rows are encoded as:

- column count varint
- repeated `[typeTag][valueData]`

Includes optimized APIs beyond full decode:

- `DecodeInto` (reuse destination span)
- `DecodeUpTo` (prefix decode)
- `DecodeColumn` (single-column extraction)
- `TryColumnTextEquals` (UTF-8 equality without string allocation)
- `IsColumnNull`
- `TryDecodeNumericColumn`

These are useful for query execution paths that avoid materializing full rows.

### Varint (`Varint`)

- Unsigned LEB128-style encoding.
- Used by record and schema serializers for compact lengths/sizes.

### Schema metadata serialization (`SchemaSerializer`)

Serializes/deserializes:

- `TableSchema`
- `IndexSchema`
- `TriggerSchema`

Also maps names to `long` keys via deterministic hash functions for catalog storage.

## 4.6 Catalog and metadata subsystem: `SchemaCatalog`

`SchemaCatalog` stores and caches metadata in B+trees.

Design:

- Main catalog tree stores table definitions keyed by hashed table name.
- Sentinel keys in main catalog point to separate subtrees for:
  - indexes
  - views
  - triggers
- In-memory caches mirror catalog for fast lookups.

Responsibilities:

- create/drop/update table schemas
- create/drop indexes, views, triggers
- provide table/index BTree instances
- track table/index root page ids
- persist root-page changes after BTree root splits

Important detail: root page ids can change after splits, so catalog entries are rewritten when detected (`PersistRootPageChangesAsync`).

---

## 5) Data flow walkthroughs

## 5.1 Database open / recovery

1. Engine creates `FileStorageDevice`, `WalIndex`, `WriteAheadLog`, `Pager`.
2. If new DB: initialize page 0 header, create/open WAL.
3. If existing DB: pager loads header then WAL recovery scans WAL.
4. If committed WAL frames exist after recovery, checkpoint is run to bring DB file current.
5. `SchemaCatalog` loads metadata trees and warms in-memory schema caches.

## 5.2 Write transaction (insert/update/delete/DDL)

1. `Pager.BeginTransactionAsync` acquires writer lock and starts WAL transaction.
2. Planner/catalog/BTree mutate page byte arrays.
3. Modified pages are marked dirty in pager.
4. `Pager.CommitAsync`:
   - updates page 0 header state
   - appends all dirty pages to WAL as frames
   - WAL commits by marking final frame and flushing
   - WAL index becomes visible to future readers
   - writer lock released
5. Optional auto-checkpoint may run when threshold reached.

## 5.3 Rollback

1. WAL is truncated to tx start offset.
2. Pager clears dirty state and cache.
3. Header state is reloaded from DB file (or latest committed WAL page 0 if present).
4. Writer lock is released.

## 5.4 Read path (normal and snapshot)

Normal read:

- BTree asks pager for pages.
- Pager resolves from cache first, then latest WAL frame (if any), else DB file.

Snapshot read:

1. Reader acquires `WalSnapshot` from pager.
2. A read-only snapshot pager is created.
3. Page resolution uses snapshot mapping only; later commits are invisible.
4. On session disposal, reader count decrements, allowing future checkpoint.

## 5.5 Query scans

- `BTreeCursor` begins at leftmost leaf and walks `nextLeaf` pointers.
- Row payloads are decoded via `RecordEncoder` as needed by execution operators.

---

## 6) Architectural patterns used

- **Abstraction boundary**: `IStorageDevice` decouples persistence medium from page/WAL logic.
- **Coordinator pattern**: `Pager` centralizes transactional lifecycle and page state.
- **Redo logging**: WAL stores post-image page frames and commit markers.
- **Snapshot pattern**: `WalSnapshot` provides immutable read view for concurrent readers.
- **Repository-like metadata service**: `SchemaCatalog` encapsulates schema persistence and in-memory indexing.
- **Cursor iteration pattern**: `BTreeCursor` for ordered streaming traversal.
- **Format utility classes**: `RecordEncoder`, `SchemaSerializer`, `Varint` isolate binary format concerns.

Note on plugin-driven architecture: this project is **partially pluggable** today. Several seams are first-class providers already, including storage device, page cache, checkpoint policy, lifecycle interceptors, checksum provider, index provider, serializer provider, catalog store, and the storage engine factory. Pager/WAL/BTree internals remain concrete.

---

## 7) Extensibility model

### 7.1 Current extensibility points

1. **Custom storage device**
   - Implement `IStorageDevice` and use it when creating the pager/engine plumbing.
   - Useful for encryption, memory-backed testing, cloud/object storage adaptation.

2. **Custom page cache**
   - Supply `PagerOptions.MaxCachedPages` for built-in LRU behavior or `PagerOptions.PageCacheFactory` for a custom `IPageCache`.
   - Useful for bounded caches, metrics collection, and specialized eviction strategies.

3. **Custom checkpoint policy and lifecycle hooks**
   - Implement `ICheckpointPolicy` to control auto-checkpoint timing.
   - Implement `IPageOperationInterceptor` for diagnostics, metrics, or fault injection.

4. **Alternative record/schema encoding**
   - `ISerializerProvider` is already part of `StorageEngineOptions`.
   - This allows swapping `IRecordSerializer` and `ISchemaSerializer` together.

5. **Alternative indexing structures**
   - `IIndexProvider` is already part of the composition model.
   - Built-in choices are `BTreeIndexProvider` and `CachingBTreeIndexProvider`, but custom providers can be registered.

6. **Catalog/checksum/factory replacement**
   - `ICatalogStore` controls low-level catalog payload encoding.
   - `IPageChecksumProvider` controls WAL checksums.
   - `IStorageEngineFactory` lets advanced callers replace the default composition root entirely.

### 7.2 What to add for first-class plugin support

If contributors want broader provider-style extensibility beyond the seams above:

- Expose WAL replacement through `StorageEngineOptionsBuilder` instead of requiring a custom `IStorageEngineFactory`.
- Introduce a higher-level abstraction around pager/WAL orchestration if alternate journaling schemes are a goal.
- Consider whether page layout and B+tree algorithms should ever be swappable, or whether those should remain intentionally concrete.

---

## 8) Tradeoffs and rationale

1. **Whole-page WAL frames (simple, robust) vs logical WAL (smaller, complex)**
   - Current design favors implementation clarity and recovery simplicity.
   - Tradeoff: larger write amplification for small updates.

2. **Single-writer lock model (predictable) vs multi-writer concurrency (complex)**
   - Simplifies conflict and consistency handling.
   - Tradeoff: write throughput bottleneck under heavy concurrent writes.

3. **Snapshot readers via copied map (easy isolation) vs shared lock-free index structures**
   - Snapshot creation is cheap for moderate map sizes and easy to reason about.
   - Tradeoff: copy cost grows with WAL page map size.

4. **B+tree delete without rebalance (simplicity) vs full balancing invariants**
   - Easier implementation and fewer edge-case bugs.
   - Tradeoff: potential space inefficiency and page under-utilization over time.

5. **Hash-based catalog keys (simple lookup keying) vs collision-free identifiers**
   - Deterministic and lightweight.
   - Tradeoff: theoretical collision risk; mitigated in practice but still a design consideration.

6. **Static utility serializers (low overhead) vs DI-pluggable serializers**
   - Fast and straightforward in current architecture.
   - Tradeoff: reduced runtime configurability.

---

## 9) “How to extend this module” (Contributor quick guide)

### A) Add a new storage backend

1. Implement `IStorageDevice`.
2. Ensure read semantics match expectations (especially zero-fill beyond EOF behavior).
3. Validate with WAL + pager tests (transaction commit/rollback/recovery).

### B) Add row encoding optimizations

1. Extend `RecordEncoder` with focused decode helpers for execution hot paths.
2. Keep wire format backward-compatible unless versioning header strategy is introduced.
3. Add tests for mixed type decoding and edge cases (missing columns, null semantics).

### C) Improve BTree behavior

1. Add rebalance/merge logic for delete underflow.
2. Preserve cursor and lookup correctness with split/merge interactions.
3. Ensure root-page updates are persisted through `SchemaCatalog.PersistRootPageChangesAsync`.

### D) Introduce pluggable index providers (larger refactor)

1. Define an interface for index operations used by planner/catalog.
2. Provide adapter for existing `BTree` implementation.
3. Migrate factory points in engine/database construction.

### E) Concurrency changes

1. Be cautious around writer/checkpoint/reader interactions.
2. Any checkpoint policy changes must preserve snapshot guarantees.
3. Re-run WAL recovery and concurrent reader tests after modifications.

---

## 10) Practical mental model

A useful way to reason about this storage engine:

- **BTree answers “where is this key/value logically?”**
- **Pager answers “which page image is authoritative right now?”**
- **WAL answers “what is durable and recoverable after a crash?”**
- **SchemaCatalog answers “which trees represent which database objects?”**

Those four concerns are intentionally separated, and that separation is the main architectural strength of `CSharpDB.Storage`.

# Collection/Document Storage Optimization

Separate storage path, direct binary hydration, and expression-based document field indexing for the Collection<T> API.

---

## Problem

CSharpDB's `Collection<T>` currently piggybacks on the SQL table infrastructure. A single `Put()` traverses layers that exist only because collections pretend to be 2-column SQL tables:

```
Current PUT path:
T -> JsonSerializer.Serialize(T)          // allocates intermediate string
  -> DbValue[] { key, json }              // wraps in type-tagged structs (~64 bytes)
  -> RecordEncoder.Encode(values)         // varint headers, type tags, buffer copy
  -> BTree.InsertAsync                    // actual storage (efficient)
  -> _catalog.PersistAllRootPageChangesAsync  // scans ALL tables, not just this one
  -> Pager.CommitAsync                    // WAL write + fsync (necessary)

Current GET path:
BTree.FindAsync -> byte[]
  -> RecordEncoder.Decode -> DbValue[]    // type dispatch, allocation
  -> values[1].AsText -> string           // UTF-8 decode to string
  -> JsonSerializer.Deserialize(string)   // re-encodes string back to UTF-8 internally
  -> T
```

The BTree already accepts raw `byte[]` payloads (`InsertAsync(long key, ReadOnlyMemory<byte> payload)`) -- the RecordEncoder wrapping and DbValue allocation are unnecessary for collections.

Additionally, `FindAsync()` performs a full table scan with in-memory filtering. There is no secondary index support for document fields.

## Design Decisions

- **Serialization**: Optimized JSON using `JsonSerializer.SerializeToUtf8Bytes` and `Deserialize<T>(ReadOnlySpan<byte>)` -- no new NuGet dependencies, eliminates intermediate string allocation
- **Index API**: Expression-based -- `collection.EnsureIndexAsync(x => x.Age)` with compile-time type safety
- **Backward compatibility**: Format marker byte (`0xC1`) distinguishes new payload format from old RecordEncoder format (which starts with `0x02` column count)

## Performance Targets

| Metric | Current | Target | Approach |
|--------|---------|--------|----------|
| Collection Put (single) | 12,590 ops/sec | 18,000+ ops/sec | Eliminate DbValue[], RecordEncoder, string allocation, full-catalog persist |
| Collection Get 10K | 1,015,385 ops/sec | 1,500,000+ ops/sec | Eliminate RecordEncoder decode, DbValue[], intermediate string |
| FindByIndex | O(n) full scan | O(log n) | Secondary B+tree indexes on document fields |

---

## Phase 1: Separate Storage Path + Direct Hydration

### 1A. New binary payload format

Replace RecordEncoder-based encoding with a simple direct format stored in BTree leaf cells:

```
[0xC1: format marker][keyLength:varint][keyUtf8:N bytes][documentJsonUtf8:remaining bytes]
```

The `0xC1` marker distinguishes from the old RecordEncoder format (which starts with column count varint `0x02`), allowing backward-compatible reads during migration.

**Eliminated overhead per operation:**
- `DbValue[]` allocation (~64 bytes)
- RecordEncoder varint headers + type tags (~4-10 bytes + CPU)
- Intermediate `string` from `JsonSerializer.Serialize()` -- `SerializeToUtf8Bytes` goes directly to UTF-8
- Re-encoding from string to UTF-8 inside `Deserialize(string)` -- `Deserialize(ReadOnlySpan<byte>)` reads UTF-8 directly

### 1B. Rewrite Collection.cs encoding/decoding

Remove `IRecordSerializer` dependency from `Collection<T>`. Rewrite three internal methods:

**EncodeDocument** (was: JSON -> string -> DbValue[] -> RecordEncoder.Encode):
```csharp
private static byte[] EncodeDocument(string key, T document)
{
    byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions);
    int keyByteCount = Encoding.UTF8.GetByteCount(key);
    int keyLenSize = Varint.SizeOf((ulong)keyByteCount);
    byte[] payload = new byte[1 + keyLenSize + keyByteCount + jsonUtf8.Length];
    payload[0] = 0xC1; // format marker
    int pos = 1 + Varint.Write(payload.AsSpan(1), (ulong)keyByteCount);
    Encoding.UTF8.GetBytes(key, payload.AsSpan(pos, keyByteCount));
    pos += keyByteCount;
    jsonUtf8.CopyTo(payload.AsSpan(pos));
    return payload;
}
```

**DecodeDocument** (was: RecordEncoder.Decode -> DbValue[] -> string -> Deserialize(string)):
```csharp
private (string key, T document) DecodeDocument(ReadOnlySpan<byte> payload)
{
    // Check format marker for backward compatibility
    if (payload[0] == 0xC1)
    {
        // New direct format
        int keyLen = (int)Varint.Read(payload.Slice(1), out int bytesRead);
        int keyStart = 1 + bytesRead;
        string key = Encoding.UTF8.GetString(payload.Slice(keyStart, keyLen));
        ReadOnlySpan<byte> jsonSpan = payload.Slice(keyStart + keyLen);
        T doc = JsonSerializer.Deserialize<T>(jsonSpan, s_jsonOptions)!;
        return (key, doc);
    }
    else
    {
        // Legacy RecordEncoder format (column count varint = 0x02)
        // ... fall back to RecordEncoder.Decode for migration
    }
}
```

**DecodeKey** (was: RecordEncoder.DecodeUpTo(0) -> DbValue[]):
```csharp
private static string DecodeKey(ReadOnlySpan<byte> payload)
{
    if (payload[0] == 0xC1)
    {
        int keyLen = (int)Varint.Read(payload.Slice(1), out int bytesRead);
        return Encoding.UTF8.GetString(payload.Slice(1 + bytesRead, keyLen));
    }
    // Legacy fallback...
}
```

### 1C. Optimize AutoCommitAsync

Replace `_catalog.PersistAllRootPageChangesAsync()` (scans ALL tables/indexes) with `_catalog.PersistRootPageChangesAsync(_catalogTableName)` (single table + its indexes only). Skip even that when the BTree root page hasn't changed:

```csharp
private async ValueTask AutoCommitAsync(Func<ValueTask> action, CancellationToken ct)
{
    if (_isInTransaction()) { await action(); return; }

    await _pager.BeginTransactionAsync(ct);
    try
    {
        uint rootBefore = _tree.RootPageId;
        await action();
        if (_tree.RootPageId != rootBefore || _indexes.Count > 0)
            await _catalog.PersistRootPageChangesAsync(_catalogTableName, ct);
        await _pager.CommitAsync(ct);
    }
    catch { await _pager.RollbackAsync(ct); throw; }
}
```

### 1D. Update Database.cs

Remove `IRecordSerializer` parameter from the `Collection<T>` constructor call in `GetCollectionAsync<T>`.

### Existing infrastructure reused (no changes needed)
- `Varint.Write/Read/SizeOf` (`src/CSharpDB.Storage/Serialization/Varint.cs`)
- `BTree.InsertAsync/FindAsync/DeleteAsync` -- already accepts raw `ReadOnlyMemory<byte>`
- `Pager` transaction/WAL -- unchanged
- `SchemaCatalog.PersistRootPageChangesAsync(tableName)` -- already exists

---

## Phase 2: Expression-Based Document Field Indexing

### 2A. Index architecture

Each document index is a secondary B+tree mapping:

```
Key:     hash(fieldValue) -> long
Payload: [docHash1:8 bytes][docHash2:8 bytes]...  (non-unique: multiple docs per value)
```

This follows the same pattern as SQL secondary indexes (`IIndexStore`, `BTreeIndexStore`).

### 2B. New interface: ICollectionIndex<T>

```csharp
// src/CSharpDB.Engine/ICollectionIndex.cs (CREATE)
internal interface ICollectionIndex<T>
{
    string FieldName { get; }
    BTree IndexTree { get; }
    ValueTask InsertAsync(long docHash, T document, CancellationToken ct);
    ValueTask DeleteAsync(long docHash, T document, CancellationToken ct);
}
```

### 2C. New class: CollectionIndex<T, TField>

```csharp
// src/CSharpDB.Engine/CollectionIndex.cs (CREATE)
internal sealed class CollectionIndex<T, TField> : ICollectionIndex<T>
{
    private readonly Func<T, TField> _fieldExtractor;  // compiled from expression
    private readonly BTree _indexTree;
    ...
}
```

Key methods:
- `InsertAsync(docHash, document)` -- extract field value via compiled expression, hash it, insert/append docHash in index BTree
- `DeleteAsync(docHash, document)` -- extract field, hash, find in index, remove docHash from payload
- `FindDocHashesAsync(value)` -- hash value, lookup index BTree, return list of docHashes

### 2D. Collection<T> new public methods

**EnsureIndexAsync** -- create or reattach a secondary index:

```csharp
public async ValueTask EnsureIndexAsync<TField>(
    Expression<Func<T, TField>> fieldSelector,
    CancellationToken ct = default)
```

Flow:
1. Extract field name from expression (e.g., `x => x.Age` -> `"age"`)
2. Check if already loaded in `_indexes` list -- return if yes
3. Index name: `"_colidx_{collectionTableName}_{fieldName}"`
4. Check `_catalog.GetIndex(indexName)` -- if exists, reattach to existing BTree (no rebuild)
5. If new: `_catalog.CreateIndexAsync(schema)` -> create BTree -> backfill from existing docs
6. Compile expression and wrap in `CollectionIndex<T, TField>`

**FindByIndexAsync** -- O(log n) lookup using a secondary index:

```csharp
public async IAsyncEnumerable<KeyValuePair<string, T>> FindByIndexAsync<TField>(
    Expression<Func<T, TField>> fieldSelector,
    TField value,
    [EnumeratorCancellation] CancellationToken ct = default)
```

Flow: find matching index -> `FindDocHashesAsync(value)` -> for each docHash, `_tree.FindAsync(docHash)` -> decode -> yield. Falls back to full scan if no matching index exists.

### 2E. Index maintenance in Put/Delete

**PutAsync** -- after inserting/upserting document data:
```csharp
// On insert (new slot)
foreach (var idx in _indexes)
    await idx.InsertAsync(probeHash, document, ct);

// On upsert (existing key)
var (_, oldDoc) = DecodeDocument(existing);
foreach (var idx in _indexes)
    await idx.DeleteAsync(probeHash, oldDoc, ct);
// ... delete + insert data ...
foreach (var idx in _indexes)
    await idx.InsertAsync(probeHash, document, ct);
```

**DeleteAsync** -- before deleting document data:
```csharp
var (_, doc) = DecodeDocument(payload);
foreach (var idx in _indexes)
    await idx.DeleteAsync(probeHash, doc, ct);
await _tree.DeleteAsync(probeHash, ct);
```

### 2F. Index persistence

**Catalog stores**: index name, table name, field name, BTree root page -- survives restart.

**Runtime binding**: The compiled `Func<T, TField>` is not serializable. Users must call `EnsureIndexAsync()` after each database open. The catalog metadata allows skipping the expensive backfill on reopen (BTree already populated).

**Crash safety**: Index updates happen within the same WAL transaction as document updates -- atomically committed or rolled back together.

### 2G. Catalog additions

Add `GetIndexRootPage(string indexName)` to `SchemaCatalog` and `CatalogService` -- the data already exists in the `_indexRootPages` dictionary; this just exposes it.

---

## Phase 3: Tests & Benchmarks

### New test file: `tests/CSharpDB.Tests/CollectionDirectPathTests.cs`

| # | Test | Validates |
|---|------|-----------|
| 1 | Basic CRUD with new format | Put, Get, Update, Delete, Count, Scan |
| 2 | Backward compatibility | Reading old RecordEncoder-format data |
| 3 | EnsureIndex + FindByIndex (int) | Index on numeric field |
| 4 | EnsureIndex + FindByIndex (string) | Index on string field |
| 5 | Index backfill | Insert docs first, then create index, verify lookups |
| 6 | Index persistence | Create index, reopen DB, EnsureIndex again (no rebuild), verify |
| 7 | Multiple indexes | Two indexes on same collection |
| 8 | Index on upsert | Update indexed field, verify old+new values |
| 9 | Index on delete | Delete doc, verify removed from index |
| 10 | Transaction rollback | Put within transaction, rollback, verify index clean |

### Benchmark additions: `tests/CSharpDB.Benchmarks/Macro/CollectionBenchmark.cs`

- `Collection_FindByIndex_10k_15s` -- pre-indexed collection, measure indexed lookups vs full scan
- `Collection_Put_WithIndex_15s` -- Put throughput when maintaining one index

### Compatibility

All 19 existing tests in `CollectionTests.cs` must pass unchanged -- the public API is preserved.

---

## Files Summary

| File | Action | Phase |
|------|--------|-------|
| `src/CSharpDB.Engine/Collection.cs` | MODIFY -- new encoding, remove IRecordSerializer, add indexing | 1, 2 |
| `src/CSharpDB.Engine/Database.cs` | MODIFY -- update Collection constructor call | 1 |
| `src/CSharpDB.Engine/ICollectionIndex.cs` | CREATE -- non-generic index interface | 2 |
| `src/CSharpDB.Engine/CollectionIndex.cs` | CREATE -- secondary index implementation | 2 |
| `src/CSharpDB.Storage/Catalog/SchemaCatalog.cs` | MODIFY -- add GetIndexRootPage | 2 |
| `src/CSharpDB.Storage/Catalog/CatalogService.cs` | MODIFY -- add GetIndexRootPage | 2 |
| `tests/CSharpDB.Tests/CollectionDirectPathTests.cs` | CREATE -- new test suite | 3 |
| `tests/CSharpDB.Benchmarks/Macro/CollectionBenchmark.cs` | MODIFY -- add indexed benchmarks | 3 |

---

## Optimized Data Flow (After Implementation)

```
PUT (new path):
T -> JsonSerializer.SerializeToUtf8Bytes(T)  // direct to UTF-8 bytes, no string
  -> [0xC1][varint][keyUtf8][jsonUtf8]        // single byte[] allocation
  -> BTree.InsertAsync                         // raw payload storage
  -> foreach index: extract field, hash, insert in index BTree
  -> PersistRootPageChangesAsync(thisTable)    // only if root page changed
  -> Pager.CommitAsync                         // WAL write + fsync

GET (new path):
BTree.FindAsync -> byte[]
  -> read 0xC1 marker -> varint -> skip key bytes
  -> JsonSerializer.Deserialize<T>(ReadOnlySpan<byte>)  // direct from UTF-8
  -> T

FIND BY INDEX (new):
hash(fieldValue) -> index BTree.FindAsync -> [docHash1, docHash2, ...]
  -> for each: main BTree.FindAsync -> DecodeDocument -> yield
  = O(log n) instead of O(n)
```

---

## Verification

1. **All existing tests pass** -- `dotnet test` (355 tests + 19 collection tests)
2. **New collection tests pass** -- `CollectionDirectPathTests.cs`
3. **Run macro benchmarks** -- `dotnet run -c Release -- --macro`
   - Collection Put Single > 18,000 ops/sec (baseline: 12,590)
   - Collection Get 10K > 1,500,000 ops/sec (baseline: 1,015,385)
4. **FindByIndex benchmark** -- indexed lookup should be orders of magnitude faster than full-scan FindAsync

---

## See Also

- [Architecture Guide](../architecture.md)
- [Benchmark Suite](../../tests/CSharpDB.Benchmarks/README.md)
- [Roadmap](../roadmap.md)

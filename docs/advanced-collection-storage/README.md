# Advanced Collection Storage Plan

> **Status (March 2026):** Planned for `v3`. This document captures the intended breaking-change direction for collection storage: move from UTF-8 JSON-backed document payloads to a native binary document format with direct binary hydration, richer path-based indexing, and an explicit migration tool for existing databases.

CSharpDB's current collection fast path already avoids the older `DbValue[]` row wrapper, but it still stores document bodies as UTF-8 JSON and still deserializes them through `System.Text.Json` on reads. That keeps the format simple and debuggable, but it leaves performance on the table for document-heavy workloads and limits the index model to direct top-level field equality.

---

## Problem

Today, the collection path still has four important constraints:

1. Documents are persisted as UTF-8 JSON payloads.
2. Reads that return `T` still deserialize through JSON.
3. Secondary indexes are limited to direct field/property selectors such as `x => x.Age`.
4. `FindAsync` remains a full scan because there is no general path/expression index model.

That means collection workloads still pay JSON parse/serialize cost in the hot path, and many document-style queries cannot become indexed lookups.

---

## v3 Direction

`v3` should treat advanced collection storage as a deliberate format transition, not a small internal optimization.

The target position is:

- Collection payloads are stored in a CSharpDB-owned binary format.
- Hydration reads fields directly from that binary payload instead of deserializing JSON.
- Indexes can target richer field paths, not only top-level scalar members.
- Existing JSON-backed collection payloads are migrated explicitly with tooling.
- The `v3` runtime no longer depends on JSON-backed collection payload support for normal operation.

This is intentionally a breaking change for collection storage. The compatibility story should be a migration tool, not indefinite dual-format complexity in the steady-state runtime.

---

## Goals

1. Remove JSON serialization/deserialization from the steady-state collection read/write path.
2. Support direct binary hydration for `Collection<T>` reads with lower allocation and CPU overhead.
3. Introduce a versioned binary document format owned by CSharpDB.
4. Support path-based field extraction and richer persisted indexes.
5. Provide an explicit, safe migration path for existing databases.
6. Preserve predictable crash recovery, WAL behavior, and diagnostics after migration.

---

## Non-Goals

For the first `v3` milestone, this plan does not require:

- changing SQL row storage formats
- general-purpose ad hoc expression compilation for every predicate shape
- full document-database query language parity
- retaining legacy JSON collection payload reads forever
- automatic on-open in-place upgrades

---

## Recommended Design

### 1. Introduce a native binary collection payload format

Add a new versioned collection payload format specifically for documents.

The format should be owned by CSharpDB and optimized for:

- fast field lookup by path
- direct hydration into `T`
- compact scalar encoding
- forward-compatible evolution
- stable index extraction

Recommended characteristics:

- small header with format marker and payload version
- length-prefixed key storage
- typed field encoding for primitive values
- nested object/array support through structured segments
- deterministic field ordering or an offset table for fast lookup

A custom format is preferred over a generic serializer because CSharpDB needs stable on-disk semantics, direct field access, and predictable indexing behavior.

### 2. Separate field access from full hydration

Add an internal binary document reader over `ReadOnlySpan<byte>`.

That reader should support operations such as:

- `TryGetInt64(path, out value)`
- `TryGetString(path, out value)`
- `TryGetBoolean(path, out value)`
- `TryGetDocument(path, out slice)`

This enables:

- index maintenance without full object construction
- path-based predicate evaluation
- future projection APIs
- lower-overhead diagnostics and migration tooling

### 3. Replace JSON deserialization with direct binary hydration

For `Collection<T>.GetAsync`, `ScanAsync`, and indexed fetches that must return `T`, hydrate directly from the binary payload.

Recommended approach:

- cache per-type metadata
- compile setters/constructors once per `T`
- populate objects directly from decoded field values
- avoid intermediate JSON text or DOM materialization

This keeps the existing `Collection<T>` programming model while removing the current JSON hot path.

### 4. Expand indexing from direct fields to path-based indexes

Replace the current “top-level string/int only” model with a richer path-based index definition.

Examples:

- `x => x.Address.City`
- `x => x.Tags[0]`
- string path forms such as `$.address.city`

Initial `v3` support should stay disciplined:

- scalar paths first
- equality indexes first
- deterministic path grammar
- clear unsupported cases for arrays or polymorphic shapes

Range indexes, multi-value array indexes, and broader computed expressions can follow after the basic path-index model is stable.

### 5. Keep migration explicit

Do not silently rewrite legacy collection payloads when opening a database.

Instead:

- `v2` and earlier databases with JSON-backed collection payloads require migration before `v3` use
- migration runs as an explicit offline tool or command
- the tool rewrites collection rows and rebuilds collection indexes
- the tool validates the result before replacing the source database

This keeps the breaking change intentional and operationally safe.

---

## Migration Tool

`v3` should ship with a first-class migration utility for collection payloads.

Recommended command shape:

```bash
csharpdb migrate-collections --input app.db --output app.v3.db
```

Recommended behavior:

1. Open the source database read-only.
2. Enumerate all collection catalogs.
3. Read each legacy JSON-backed document.
4. Re-encode it into the binary collection format.
5. Rebuild all collection indexes using the new path/index extractor logic.
6. Run validation passes:
   - row/document counts match
   - collection keys match
   - indexed lookup samples match
   - metadata/catalog state is consistent
7. Emit a new destination database instead of mutating the source in place.

Optional follow-up modes:

- `--in-place` only after the copy-based tool is proven
- `--backup <path>`
- `--verify-only`
- `--collections <name1,name2,...>`

The safest initial release is copy-and-verify, not in-place rewrite.

---

## API Surface Changes

The public collection API can remain familiar, but the storage assumptions change.

Recommended additions:

- path-based index creation APIs
- raw/lite document access APIs for advanced callers
- migration command support in CLI/admin/service tooling

Recommended compatibility stance:

- `Collection<T>` remains the main typed API
- binary payloads become the only supported collection persistence format in `v3`
- legacy JSON payloads are handled by the migrator, not by the steady-state runtime

---

## Rollout Phases

### Phase 1: Format and runtime prototype

- define the binary document format
- implement binary reader primitives
- benchmark field lookup and hydration against the current JSON path
- validate schema evolution behavior

### Phase 2: Binary hydration path

- add binary encode/decode for `Collection<T>`
- replace JSON-based hydration in the typed collection path
- keep focused microbenchmarks for encode, decode, get, put, and scan

### Phase 3: Path-based indexes

- introduce persisted path index descriptors
- backfill/rebuild index logic over binary payloads
- add path-based lookup APIs or overloads
- validate mixed scalar and nested path cases

### Phase 4: Migration tooling

- add offline migrator command
- add verification/reporting output
- add backup and rollback guidance
- test migration on realistic databases

### Phase 5: `v3` cutover

- remove runtime dependence on legacy JSON collection payload reads
- update release notes and upgrade guidance
- publish migration documentation and compatibility matrix

---

## Performance Expectations

These are directional expectations, not guarantees:

- direct binary hydration should provide a meaningful reduction in collection read/write CPU cost and allocations
- point `GetAsync` / indexed fetches that currently deserialize full JSON objects should see moderate gains
- scan/filter workloads that can inspect fields without constructing `T` should see larger gains
- path-based indexes should deliver the largest end-user win by converting current full scans into indexed probes

Reasonable rough expectations:

- typed collection get/put paths: often `1.3x` to `2x` better, depending on document shape
- scan/filter/projection paths that avoid full hydration: sometimes higher than `2x`
- path-indexed lookups versus full scans: potentially orders-of-magnitude better on supported predicates

Page-level compression is still valuable for I/O reduction, but binary hydration and richer indexing should be prioritized first because they attack the current CPU and query-shape bottlenecks directly.

---

## Risks

- breaking collection on-disk compatibility in `v3`
- migration complexity for large databases
- schema/version evolution for typed documents
- keeping the binary format debuggable and diagnosable
- avoiding excessive per-type reflection overhead in the hydration layer
- ensuring NativeAOT-friendly behavior for long-term typed hydration strategies

---

## Validation Matrix

Minimum coverage should include:

- create binary-format collections and round-trip typed documents
- update/delete documents and verify index maintenance
- nested path extraction and indexed lookup correctness
- crash-style reopen with WAL recovery on binary collection payloads
- migration of existing JSON-backed collections to the new format
- validation of collection counts, keys, and index matches before/after migration
- performance comparison against the current JSON-backed collection path

---

## Recommended Position

For `v3`, CSharpDB should make a clean cut:

- binary document payloads become the supported collection storage format
- direct binary hydration replaces JSON deserialization in the main collection path
- richer path-based indexes become part of the collection story
- migration is explicit, tool-driven, and well documented

That gives the project a simpler long-term runtime, better document-query performance, and a clearer foundation for future collection features than continuing to optimize around JSON payloads.

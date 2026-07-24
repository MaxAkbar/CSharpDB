# Migration JSON Document-Collection Projection

## Scope

This contract defines the explicit SDK projection of complete top-level JSON
values into one real CSharpDB document collection. It covers immutable
inspection, catalog creation, fail-fast replay, crash-resumable staged apply,
schema/count/checksum validation, and activation.

It does not change the object-table JSON package v1 or typed-table package v2.
A retained collection package and CLI route remain a separate follow-up slice.

## Versioned contracts

| Concern | Contract |
| --- | --- |
| Projection | `csharpdb-json-collection-projection/v1` |
| Catalog schema | `csharpdb-json-collection-schema/v1` |
| Migration row bridge | `csharpdb-migration-collection-document-row/v1` |
| Key policy | `csharpdb-json-source-ordinal-key/v1` |
| Resume cursor | `csharpdb-json-collection-cursor/v1` |
| Document bytes | `csharpdb-json-ordered-value/v1` |

The catalog contains one `Collection` and exactly two non-null columns:

| Field | Logical type | Migration value | Meaning |
| --- | --- | --- | --- |
| `_key` | `Text` | `Text` | Generated source ordinal key |
| `_doc` | `Json` | `Json` | Complete ordered-canonical JSON value |

The collection, fields, native markers, roles, mappings, and version facets
must match this contract exactly. Arbitrary `Collection` catalogs remain
unsupported.

## Projection

Collection projection is explicit. Every top-level input record becomes one
document:

- a root-array element in `RootArray` framing; or
- a complete top-level value in `MultipleValues` framing, including ordinary
  NDJSON.

Objects, arrays, strings, numbers, booleans, and `null` are all valid
documents. Empty root arrays and blank multiple-value inputs produce an empty
collection with the same fixed schema.

Documents use the existing ordered-canonical JSON representation. It retains
object encounter order and exact valid number lexemes, rejects duplicate
decoded property names at every depth, minimally escapes strings, and writes
other valid Unicode characters literally. The migration preserves these
canonical bytes; it does not preserve insignificant source whitespace or an
alternate escape spelling.

Inspection scans the complete snapshot. The catalog records total and
per-kind counts plus the maximum canonical document size. It does not retain
source property names or document values.

Logical collection names are SQL identifiers no longer than 123 characters.
The limit reserves the Engine-owned `_col_` physical prefix within CSharpDB's
128-character identifier ceiling.

## Keys and rows

Version 1 never infers a key from document content. A zero-based source ordinal
produces:

```text
json-ordinal-v1:{zeroBasedOrdinal:D20}
```

For example, the first document key is
`json-ordinal-v1:00000000000000000000`.

The key is emitted as both `MigrationDataRow.StableKey` and `_key`. `_doc`
contains the ordered-canonical JSON text. Reads require the exact key-then-
document projection; partial, reordered, duplicate, or unknown fields fail.

The cursor binds the immutable snapshot, framing, catalog, collection name,
projection and key contracts, requested row/value/batch bounds, and fail-fast
policy. Table-v1, typed-v2, foreign-snapshot, renamed-collection, and changed-
bound cursors fail closed.

## Planning

CSharpDB's generic collection capability remains conditional. The planner
admits only the exact contract above:

- `_key` maps exactly to target `Text`;
- `_doc` maps losslessly to target `Text` through
  `canonical-text` version 1;
- no extra children, members, dependencies, indexes, keys, or constraints are
  present;
- the physical `_col_` name fits and does not collide case-insensitively with
  an included table; and
- row handling is `FailFast`.

Unknown versions and lookalikes are excluded. Deterministic-reject mode does
not apply because every successfully parsed top-level JSON kind is a valid
document; syntax, encoding, duplicate-name, integrity, and resource failures
remain fatal.

## Staged target

The staged adapter creates a real Engine `JsonElement` collection rather than
lowering `Collection` to ordinary SQL DDL. Its logical target name remains the
catalog name; its physical backing table is `_col_<target>`.

Each batch transaction:

1. verifies the exact collection binding, ordinal key, converted value tags,
   limits, and batch digest;
2. validates `_doc` as one already-canonical strict UTF-8 JSON value;
3. inserts the key and exact canonical bytes through the Engine's direct
   collection payload;
4. refuses an existing key instead of updating it; and
5. commits the documents and migration receipt together.

Normal collection serialization is unchanged. In particular, migration does
not pass a `JsonElement` through `System.Text.Json` a second time, so literal
Unicode, HTML-sensitive characters, property order, negative zero, exponent
spelling, and trailing-zero number lexemes remain exact.

Cancellation or a fault before commit rolls back both documents and receipt.
After an indeterminate commit acknowledgement, reopen verifies the receipt and
skips or replays the batch exactly. Collection creation itself is also
transactional; a rolled-back new collection is removed from Engine caches
before retry.

## Validation

Normalized schema evidence exposes one logical `Collection` with `_key` and
`_doc`; the physical `_col_` table is not reported as an unexpected table.
Counts address the physical backing table. Row validation reads `_key` and
`_doc` through the collection-aware SQL snapshot path, then applies the
existing duplicate-preserving, order-independent checksum contract.

Activation remains gated on a published passing schema/count/checksum report.
After activation, the documents are available through
`GetCollectionAsync<JsonElement>`, exact-key lookup, scanning, and
collection-aware SQL.

## Qualification

The SDK gate covers:

- root-array and NDJSON framing, all JSON kinds, nested documents, empty input,
  Unicode and control escapes;
- exact `-0`, exponent, large-integer, and trailing-zero number lexemes;
- duplicate decoded-name rejection, projection and cursor isolation, privacy,
  row/value/batch bounds, and catalog substitution;
- exact conditional-capability admission and fail-closed lookalikes;
- direct-payload byte preservation, duplicate-key refusal, transaction
  requirements, commit/rollback, and collection recreation after rollback;
- every staged batch fault cutoff, fresh reopen/resume, receipt skipping,
  schema/count/checksum validation, activation, and collection API reads.

## Deferred route

An optional future slice may add a distinct
`csharpdb-json-collection-snapshot-package/v1` and an explicitly selected CLI
projection. It must reconstruct this SDK contract from retained raw bytes and
must not change table package v1, typed package v2, their cursors, or their
default CLI behavior. This enhancement is not part of the closed Phase 4
file-migration exit gate.

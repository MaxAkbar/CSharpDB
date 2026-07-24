# Migration JSON Object-Table Schema

## Status And Scope

This document freezes the second Track 4B contract: an immutable JSON source
binding and deterministic relational schema discovery for top-level object
rows.

The format reader remains generic. This table policy is an explicit adapter
layer over the ordered logical values defined by
[`migration-json-reader-foundation.md`](migration-json-reader-foundation.md).
It does not flatten paths, create child tables, infer typed intent from JSON
strings, or define the later typed sidecar, data-source batching, export,
resume, and CLI contracts.

## Immutable Source Binding

Inspection is always tied to a private byte-for-byte snapshot. The source
fingerprint binds:

- the complete content digest and byte length;
- root-array or whitespace-separated multiple-value framing;
- strict UTF-8, duplicate-name, property-order, and number-lexeme contracts;
  and
- every configured reader resource ceiling.

Changing either the bytes or any bound reader setting produces a different
source fingerprint. A caller-supplied logical identity is hashed before it is
placed in source metadata; source paths and arbitrary caller text are not
published.

Every inspection and later replay opens a fresh reader over the same snapshot.
The snapshot is integrity checked at binding and rehashed before each bound
reader is exposed, so a catalog cannot retain a stale source identity after
private-byte tampering. It remains caller owned.

## Structural Discovery And Type Profiling

JSON has no header. Inspection must therefore stream the complete snapshot to
discover structure, even when type profiling is disabled or sampled.

Structural coverage is full and establishes:

- the total top-level record count;
- eligible object rows and ineligible non-object rows;
- every distinct decoded top-level property name;
- deterministic first-encounter column order; and
- full presence, missing, and explicit-null counts for each column.

Type-profile coverage is separate. Only a bounded prefix of eligible object
rows and canonical value bytes contributes type evidence. A sampled type
decision always requires full-stream validation during apply.

No record or property value is retained after its counters and candidate flags
are updated.

## Table Rows And Columns

Only top-level objects are eligible relational rows. A valid top-level scalar,
array, or null remains valid JSON but is incompatible with this table adapter.
Inspection reports it with `MIG-JSON-SCHEMA-ROW-001`.

An empty input, no eligible object row, or eligible objects with no properties
does not provide an inferred table shape. Inspection reports
`MIG-JSON-SCHEMA-EMPTY-001`; a future typed sidecar may supply an explicit
schema for that case.

Columns are the union of decoded top-level property names over the complete
snapshot:

1. properties from the first object are added in encounter order;
2. every later property not already present is appended at first encounter;
3. identity uses ordinal decoded-name comparison; and
4. case-distinct and Unicode-distinct names remain distinct.

Stable catalog IDs are ordinal (`json:column:0`, `json:column:1`, and so on).
An exact blank or whitespace-only property name is retained as a facet while a
nonblank ordinal fallback is used as its catalog source name. Target naming is
a later deterministic mapping concern.

Late properties are never ignored. An unknown property encountered while
replaying the same bound snapshot is fatal schema drift.

## Missing And Explicit Null

A present JSON `null` and an absent property are distinct facts.

- Present null contributes an explicit-null count and later becomes
  `MigrationSourceValueKind.Null`.
- Absence contributes a missing count and never becomes target `NULL`
  implicitly.
- The default missing policy is `Reject`.
- `MIG-JSON-SCHEMA-MISSING-001` reports any strict column with observed
  absence.
- A per-column `AsNull` override is allowed only for a nullable column and is
  recorded as an explicit semantic rewrite.

Inferred nullability comes from the full structural scan. A column is nullable
when explicit null occurs or `AsNull` is selected. Strict missing rows may be
rejected while accepted rows retain a non-nullable target column.

## Logical Types And Representations

Ordinary JSON carries only native JSON type intent:

| Evidence | Inferred source type | Table representation |
| --- | --- | --- |
| Strings only | `Text` | Exact decoded string |
| Booleans only | `Boolean` | Lowercase `true` or `false` |
| Exact integral numbers in `Int64` | `SignedInteger` | Canonical base-10 integer |
| Exact nonnegative integral numbers in `UInt64` | `UnsignedInteger` | Canonical base-10 integer |
| Exact canonical fixed-point numbers | `Decimal` | Canonical invariant decimal |
| Nested, mixed, null-only, or lexically significant values | `Json` | `csharpdb-json-ordered-value/v1` text |

The catalog maps the `Json` table representation to target logical `Text` and
retains JSON representation facets. This avoids changing the versioned shared
type-mapping policy. Later data-source projection emits canonical JSON text as
a text source value.

Mixed non-null kinds widen to `Json`. Consequently, `"1"`, `1`, `true`,
objects, and arrays remain distinguishable after canonicalization. Objects and
arrays are never flattened by default. Collection-oriented adapters may retain
the ordered logical document directly.

JSON strings are not inspected for GUID, date/time, numeric, BLOB, or other
typed intent. Those declarations belong to the typed-sidecar slice.

## Number Policy

The inference policy never routes a number through `double`.

Narrower numeric candidates are activated only when conversion preserves the
number spelling accepted by the table contract:

- `SignedInteger` requires an integral lexeme within `Int64` and excludes
  negative zero.
- `UnsignedInteger` requires a nonnegative integral lexeme within `UInt64`.
  Values also representable by `Int64` prefer `SignedInteger`.
- `Decimal` requires fixed-point notation without an exponent or negative
  zero and must already equal canonical invariant decimal text. Its inference
  shape is arbitrary precision; target mapping may retain an oversized value
  as exact decimal text.
- Exponents, negative zero, redundant fractional trailing zeroes, and any
  other lexically significant spelling use `Json`.

Candidate precedence is signed integer, unsigned integer, decimal, then JSON.
The JSON fallback retains the exact validated number lexeme.

## Overrides

Overrides are ordinal declarations guarded by the exact decoded property name:

- column index;
- expected property name;
- logical type;
- optional nullability; and
- `Reject` or `AsNull` missing policy.

Overrides refine discovered columns and cannot invent an absent column.
Duplicate, negative, out-of-range, name-mismatched, or internally
inconsistent declarations fail deterministically.

`Text`, `Boolean`, and numeric overrides accept only their corresponding JSON
token kinds. `Json` accepts any non-null JSON kind and canonicalizes it.
Numeric overrides must satisfy the same exact lexical policy as inference;
they do not authorize spelling normalization or string-to-number coercion.
Every override is revalidated over the full stream during apply.

## Resource Limits

The table layer adds limits beyond the per-value reader ceilings:

| Limit | Default | Absolute ceiling |
| --- | ---: | ---: |
| Type-profile rows | 1,000 | 1,000,000 |
| Canonical type-profile bytes | 64 MiB | 64 MiB |
| Distinct columns | 4,096 | 16,384 |
| Cumulative decoded UTF-8 bytes in distinct names | 4 MiB | 64 MiB |
| Table name characters | 1,024 | 1,024 |

The reader continues to EOF after type evidence reaches either profile limit.
All structural, ordinal, byte, and value counters use checked arithmetic.
Column and aggregate-name limit failures are fatal.

## Diagnostics And Failure Semantics

Stable schema rules include:

- `MIG-JSON-SCHEMA-ROW-001` for a non-object top-level record;
- `MIG-JSON-SCHEMA-EMPTY-001` when no inferred table shape exists;
- `MIG-JSON-SCHEMA-MISSING-001` for strict missing properties;
- `MIG-JSON-SCHEMA-JSON-001` when evidence widens safely to canonical JSON;
- `MIG-JSON-SCHEMA-SAMPLE-001` for sampled type decisions;
- `MIG-JSON-SCHEMA-OVERRIDE-001` for incompatible overrides;
- `MIG-JSON-SCHEMA-LIMIT-COLUMNS-001` for distinct-column overflow; and
- `MIG-JSON-SCHEMA-LIMIT-NAMES-001` for aggregate decoded-name overflow.

Diagnostic IDs bind the stable rule, source fingerprint, column ordinal where
applicable, and a value-free reason. Messages never include property names or
source values.

Encoding, syntax, framing, duplicate-name, reader-limit, cancellation, I/O,
snapshot-integrity, schema-drift, and aggregate-limit failures remain fatal.
Later deterministic reject mode may handle only parsed row-local eligibility,
missing, nullability, and type mismatches under an explicit plan-bound rule
set.

## Required Qualification

Tests must cover:

- late first appearance and first-encounter order across differently ordered
  objects;
- case, Unicode, escaped-name equivalence, blank names, and fallback
  collisions;
- full null, presence, and missing counts with strict and explicit `AsNull`
  behavior;
- empty input, empty objects, and mixed object/non-object streams;
- signed and unsigned boundaries, values above `UInt64`, arbitrary precision
  fixed decimals, exponents, negative zero, and trailing-zero preservation;
- nested values and heterogeneous kinds widening to ordered canonical JSON;
- none/sample/full type coverage, exact row and byte cutoffs, and incompatible
  unseen tails;
- invalid and incompatible overrides;
- exact and one-over column/name limits;
- identical results across chunk sizes and both framing modes; and
- large-stream bounded memory, cancellation, integrity, and replay
  determinism.

Track 4B remains open after schema inspection. Later JSON migration slices now
implement the catalog-bound data source, deterministic row-local rejects,
cursor and batch accounting, raw and typed retained packages, source-bound
typed intent, JSON/NDJSON export with durable resume, and fail-fast retained-v1
CLI inspect, plan, apply/resume, and validation. Retained-v1 deterministic
reject policy, canonical artifact publication/reuse, resume, and validation are
also routed for root-array JSON and NDJSON. Collection projection, typed
export-intent generation, and typed-v2 deterministic-reject CLI routing remain
open. Explicitly sidecar-selected typed package v2 is routed through the
fail-fast CLI workflow.

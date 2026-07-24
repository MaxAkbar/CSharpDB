# Migration JSON Object-Table Data Source

## Status And Scope

This document freezes the third Track 4B contract: repeatable row projection
from an immutable JSON snapshot into provider-neutral migration batches.

It builds on
[`migration-json-reader-foundation.md`](migration-json-reader-foundation.md)
and
[`migration-json-table-schema.md`](migration-json-table-schema.md).
It does not define typed sidecars, collection targets, retained-source
packaging, export, or CLI behavior.

## Binding

`JsonMigrationDataSource` is bound to:

- one `JsonTableSchemaInferenceResult`;
- the exact caller-owned `JsonSourceSnapshot`;
- the byte-for-byte catalog produced by that inference result; and
- the catalog digest used by planning and apply.

Creation rejects snapshot, source, target-version, or catalog-policy drift and
verifies the retained bytes. Every read opens through the bound source reader,
which rehashes the snapshot before exposing values.

The source never owns the snapshot. Disposing it prevents new reads and does
not dispose the caller's retained bytes.

## Attempted Rows And Ordinals

Every top-level logical JSON value is one attempted source row. The durable
zero-based source row ordinal is `recordOrdinal - 1`; object and non-object
values share the same contiguous sequence.

Only objects can become relational rows. A non-object value is the first
row-level outcome and does not enter column projection. An object is indexed
by exact decoded property name. A property absent from the fully discovered
schema is fatal schema drift.

Requested columns are canonical `json:column:<ordinal>` identifiers. A
projection must be nonempty, duplicate-free, and within the bound catalog.
Output value order exactly matches request order.

## Value Projection

Present JSON `null` emits `MigrationSourceValueKind.Null` only when the column
is nullable. A missing property emits null only under an explicit `AsNull`
schema override.

Non-null values use the following invariant source representations:

| Table logical type | Required JSON evidence | Source kind | Canonical text |
| --- | --- | --- | --- |
| `Text` | string | `Text` | Exact decoded string |
| `Boolean` | boolean | `Boolean` | `true` or `false` |
| `SignedInteger` | exact compatible number | `SignedInteger` | Exact number lexeme |
| `UnsignedInteger` | exact compatible number | `UnsignedInteger` | Exact number lexeme |
| `Decimal` | exact compatible number | `Decimal` | Exact canonical fixed-point lexeme |
| `Json` | any non-null JSON value | `Text` | `csharpdb-json-ordered-value/v1` |

`Json` deliberately emits source kind `Text` because the inspected catalog
maps the versioned canonical JSON representation to exact target Text without
changing the shared versioned type mapper. A string in a mixed `Json` column
therefore includes JSON quotes and escaping; a string in a native `Text`
column does not.

Every streamed value is checked against the frozen scalar policy, including
values beyond sampled inference. Projection never parses through `double`.

## Row-Local Outcomes

Stable JSON data rules are:

- `MIG-JSON-DATA-ROW-001` — top-level value is not an object;
- `MIG-JSON-DATA-MISSING-001` — a strict projected property is absent;
- `MIG-JSON-DATA-NULL-001` — explicit null contradicts non-nullability;
- `MIG-JSON-DATA-TYPE-001` — a present value contradicts its logical type;
- `MIG-JSON-DATA-VALUE-SIZE-001` — accepted source value exceeds the read
  bound; and
- `MIG-JSON-DATA-ROW-SIZE-001` — accepted projected row cannot fit one batch.

Fail-fast is the default. The first failure is selected in source-row and
requested-column order. Its exception contains only stable object, batch, row,
column, and rule identifiers.

The deterministic-reject registry contains only row, missing, null, and type
rules. Value-size, row-size, encoding, syntax, framing, duplicate-name,
reader-limit, integrity, schema-drift, cursor, I/O, cancellation, and reject
budget failures are always fatal.

## Deterministic Reject Evidence

Reject evidence is value-free except for the explicitly bounded `rawValue`
entry required by the retained reject artifact. For JSON this entry is the
ordered canonical representation of the offending logical value, not a slice
of caller-controlled source bytes. Missing properties use a null entry.

Column outcomes use these ordinally sorted evidence names:

1. `columnIndex`;
2. `jsonValueKind`;
3. `propertyOrdinal`;
4. `rawValue`;
5. `recordByteLength`;
6. `recordOrdinal`;
7. `startByteOffset`;
8. `startBytePositionInLine`; and
9. `startLineNumber`.

Missing properties use null `propertyOrdinal` and `rawValue` entries.
Row-level non-object outcomes omit `columnIndex` and `propertyOrdinal`;
tolerant outcomes use a null column object ID. The fail-fast exception API
requires an object scope, so a non-object failure uses the table object ID in
that field. Rule and object identifiers, ordinals, offsets, lengths, and kind
names are stable; exception text never includes the raw value. The plan-bound
per-value, per-batch, per-run, and artifact byte limits are enforced before an
oversized canonical value is retained.

## Batches And Cursors

A batch contains a contiguous sequence of accepted and rejected outcomes.
`BatchSize` counts both. All-reject batches remain visible, and the terminal
batch always has a null next cursor.

The adapter applies lower safety ceilings of 65,536 buffered outcomes and
64 MiB of canonical batch payload. A batch splits before exceeding row,
canonical-byte, reject-count, reject-raw-byte, or artifact bounds. One outcome
that cannot fit remains fatal.

`csharpdb-json-cursor/v1` binds:

- source fingerprint, snapshot identity, and catalog digest;
- schema, scalar, canonical JSON, and source-binding policy versions;
- complete ordered discovered schema and requested projection;
- caller batch/value limits;
- the complete normalized reject policy; and
- next source-row and batch ordinals.

Resume requires the exact snapshot token and cursor scope. Replay starts from
the immutable snapshot, recomputes every prefix outcome and policy counter,
and yields only the exact suffix beginning at a real batch boundary. Changed
projection, limits, schema, catalog, reject rules, or reject budgets invalidate
the cursor.

## Required Qualification

Tests must cover:

- every logical type, explicit null, strict missing, and `AsNull`;
- nested, mixed, and lexically significant canonical JSON text;
- sampled incompatible tails and explicit override revalidation;
- arbitrary reordered projections and full property-name identity;
- non-object, missing, null, and type fail-fast and reject outcomes;
- value/row safety failures remaining fatal in reject mode;
- safe bounded evidence and every reject budget;
- exact row-count and byte-count batch splits, all-reject batches, and EOF;
- golden cursor shape, full cursor-scope drift, and every-boundary resume;
- repeated reads, snapshot/catalog mismatch, tamper, cancellation, and
  disposal; and
- provider-neutral apply/replay plus validation against a ledger-capable
  staged target.

The portable retained boundary for this source is specified in
[`migration-json-retained-package.md`](migration-json-retained-package.md).

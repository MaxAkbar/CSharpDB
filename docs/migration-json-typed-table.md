# Migration JSON Typed Object-Table Integration

## Status And Scope

This document freezes the sixth Track 4B contract: opt-in typed object-table
inference, catalogs, row projection, deterministic rejects, batching, and
cursor replay from a verified
[`csharpdb-json-table-intent/v1`](migration-json-typed-intent.md) sidecar.

Typed integration uses distinct schema and cursor contracts. Ordinary
`JsonTableSchemaInferer` results, catalogs, source values, and v1 cursors remain
unchanged. Typed results cannot be passed to
`JsonSnapshotPackage.WriteAsync`, so the existing retained package v1 cannot
discard or silently reinterpret intent. The distinct
[`JsonTypedSnapshotPackage`](migration-json-typed-retained-package.md) v2 API
now embeds the exact sidecar and replays this typed contract without changing
package v1.

## Public Contract

`JsonTypedTableSchemaInferer.InferAsync` and `DiscoverAsync` require an
immutable `JsonSourceBinding`, its exact `JsonSourceSnapshot`, and an already
verified `JsonTypedIntentManifest`. The manifest is reparsed from its defensive
canonical bytes against the supplied binding and its own exact-byte digest
before source discovery begins.

The result is a distinct `JsonTypedTableSchemaInferenceResult`, not an optional
mode on the v1 result. It exposes:

- source, snapshot, table, coverage, count, and diagnostic facts;
- the immutable intent manifest and its digest;
- the v1 representation schema used to discover each column;
- the optional typed declaration for each column;
- the effective `MigrationSourceValueKind`; and
- whether every streamed value requires exact codec validation.

`JsonMigrationDataSource.CreateAsync` has a typed-result overload. Catalog
validation, source ownership, read requests, deterministic reject policy,
batch limits, projection order, and disposal otherwise retain the established
JSON data-source contract.

## Versioned Boundary

Typed integration freezes these identifiers:

- schema: `csharpdb-json-typed-table-schema-v1`;
- scalar policy: `csharpdb-json-typed-table-scalar-v1`;
- cursor: `csharpdb-json-cursor/v2`; and
- cursor token: `csharpdb-json-cursor-token-v2`.

The ordinary identifiers remain:

- schema: `csharpdb-json-table-schema-v1`;
- scalar policy: `csharpdb-json-table-scalar-v1`;
- cursor: `csharpdb-json-cursor/v1`; and
- cursor token: `csharpdb-json-cursor-token-v1`.

Typed and ordinary cursors are mutually invalid. No filename, sibling sidecar,
string pattern, or value sample automatically selects typed behavior.

## Representation And Effective Types

The sidecar declaration is the authority for semantic type. A separate
representation schema still verifies the required native JSON token kind:

| Codec | Required JSON token | Effective source kind | Catalog logical type | Native type |
| --- | --- | --- | --- | --- |
| `BinaryBase64` | String | `Binary` | `binary` | `JSON_BASE64_STRING` |
| `DecimalString` | String | `Decimal` | `decimal` | `JSON_DECIMAL_STRING` |
| `DecimalNumber` | Number | `Decimal` | `decimal` | `JSON_DECIMAL_NUMBER` |
| `GuidD` | String | `Guid` | `guid` | `JSON_GUID_D_STRING` |
| `DateCSharpDbText` | String | `Date` | `date` | `JSON_DATE_CSHARPDB_TEXT` |
| `TimeCSharpDbText` | String | `Time` | `time` | `JSON_TIME_CSHARPDB_TEXT` |
| `DateTimeCSharpDbText` | String | `DateTime` | `dateTime` | `JSON_DATETIME_CSHARPDB_TEXT` |
| `DateTimeOffsetCSharpDbText` | String | `DateTimeOffset` | `dateTimeOffset` | `JSON_DATETIMEOFFSET_CSHARPDB_TEXT` |
| `Int64String` | String | `SignedInteger` | `signedInteger` | `JSON_INT64_STRING` |
| `UInt64String` | String | `UnsignedInteger` | `unsignedInteger` | `JSON_UINT64_STRING` |

Undeclared columns retain ordinary v1 inference, native types, and source-value
projection inside the typed catalog.

Intent declarations and caller-supplied representation overrides must address
disjoint ordinals. Typed inference synthesizes the required representation
override, including the exact decoded-name guard, nullability, and
missing-property policy. Every declared ordinal must exist in the completely
discovered shape and its decoded name must match using Unicode ordinal
comparison. A sidecar cannot invent a never-observed column or retarget a
declaration after discovery order changes.

## Exact Scalar Decoding

Every present, non-null declared value is validated during data-source replay.
Successful decoding emits:

- exact decoded bytes and no text for `Binary`;
- the original canonical fixed-point text for `Decimal`;
- versioned canonical CSharpDB text for GUID and temporal kinds; and
- exact invariant base-10 text for signed and unsigned integers.

Base64 must use the standard padded RFC 4648 alphabet, contain no whitespace,
decode within the retained bound, and re-encode to the exact original string.
Empty base64 represents an empty BLOB.

Decimal string and number forms use fixed-point canonical text: no leading
plus, leading zero, negative zero, exponent, trailing decimal point, or
trailing fractional zero. Declared precision and scale are maxima. Digit
processing is bounded before detailed lexical validation.

GUID, date, time, date/time, and date/time-with-offset strings parse through
the shared CSharpDB codec and must format back to the exact original text.
Equivalent alternate spellings are rejected. In particular, GUID text is
lowercase `D`, date/time uses a space rather than `T`, offsets are numeric
rather than `Z`, and time fractions follow the shared codec exactly.

Signed and unsigned integer strings must equal the invariant formatting of a
successfully parsed `Int64` or `UInt64`. This rejects a plus sign, leading
zeros, negative zero, whitespace, and overflow.

## Catalog And Planning

The typed catalog deterministically transforms the representation catalog:

- active schema and scalar identifiers become the typed identifiers;
- table facets bind the sidecar format, exact manifest digest, typed-value and
  text-codec contracts, and binary/decimal safety ceilings;
- declared-column facets bind codec, JSON representation, manifest digest,
  declared decimal facets, and full-stream validation; and
- effective logical and native types drive the existing provider-neutral
  mapping policy.

The catalog digest therefore changes for any codec, precision, scale,
nullability, missing policy, safety ceiling, source binding, or exact sidecar
digest change. Migration plans inherit that binding through the catalog.

Existing mappings remain authoritative: binary maps exactly to BLOB; signed
integers to INTEGER; unsigned integers to invariant TEXT; supported decimals
to scaled INTEGER and larger decimals to invariant TEXT; and GUID and temporal
kinds to their versioned canonical TEXT conversions. Typed decimals that
cannot use CSharpDB's scaled-`Int64` representation use the lossless
`json-typed-decimal-text` conversion. That typed-only conversion validates the
declared precision and scale directly, including `precision == scale`, without
changing the ordinary v1 decimal conversion.

## Row Outcomes And Resource Limits

Malformed token kinds, noncanonical lexical forms, and declared decimal facet
overflow are row-local
`MIG-JSON-DATA-TYPED-001` outcomes. They may be retained only when the caller
selects that rule in the deterministic reject policy. Without that explicit
policy, the first such row fails fast.

The representation overrides synthesized from typed declarations do not carry
v1 override-mismatch diagnostics into the typed catalog. An incompatible
profiled token remains visible in the representation evidence, but the exact
streamed row is governed by the typed fail-fast or deterministic-reject policy
rather than becoming a catalog-level apply blocker.

Existing rules remain authoritative for non-object rows, missing properties,
forbidden nulls, schema drift, value size, and row size. Missing-as-null emits
`MigrationSourceValueKind.Null` only when explicitly declared. Explicit null
also requires the effective column to be nullable.

Retained decoded-binary and decimal-digit ceilings, reader ceilings, request
value limits, row limits, and batch limits are fatal. A retained safety
ceiling wins before detailed lexical validation and cannot be converted into
a row reject. Binary batch accounting uses decoded bytes, not base64 text.

Reject evidence retains only the established bounded canonical source JSON
value and structural location. It never substitutes decoded BLOB bytes or
free-form parser messages.

## Cursor And Replay Binding

Typed cursor scope binds:

- the v2 cursor and token identifiers;
- exact source, snapshot, reader options, and catalog digest;
- typed schema/scalar and sidecar contract identifiers;
- exact intent manifest digest, limits, and ordered declarations;
- the full representation/effective column schema;
- selected projection and batch/value limits; and
- deterministic reject contract and budgets.

Every emitted batch boundary can therefore reproduce the exact remaining
accepted rows, rejects, byte accounting, ordinals, and terminal state.
Changing any bound field invalidates the old cursor generically. Concurrent
readers and independently reparsed equivalent manifests remain deterministic.

## Deferred Work

JSON export, CLI routing, collection projection, signatures, encryption, and
automatic sidecar discovery remain deferred. Portable typed migration can now
retain the raw immutable snapshot and exact source-bound sidecar together in
the explicitly selected
[`csharpdb-json-snapshot-package/v2`](migration-json-typed-retained-package.md)
format.

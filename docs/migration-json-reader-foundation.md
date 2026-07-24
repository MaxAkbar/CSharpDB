# Migration JSON Reader Foundation

## Status And Scope

This document freezes the first Track 4B contract: strict, forward-only,
bounded parsing of UTF-8 JSON root arrays and whitespace-separated top-level
JSON values, including ordinary NDJSON.

This slice is a format reader and logical-value foundation. It does not yet
claim schema inference, table projection, collection import, typed sidecars,
snapshot packaging, resumable target application, JSON export, or CLI
integration.

## Input Framing

The reader exposes two explicit framing modes:

- `RootArray` requires exactly one top-level JSON array. Each array element is
  yielded as one logical value. Leading and trailing JSON whitespace are
  allowed, an empty array is valid, and any token after the array is an error.
- `MultipleValues` yields whitespace-separated top-level JSON values. This
  accepts conventional NDJSON with one value per line and also permits other
  JSON whitespace between values. At least one JSON whitespace byte is
  required between adjacent values. Blank input, blank lines, and a missing
  final newline are valid.

Top-level values may be any JSON kind. A later relational-table adapter will
require object rows; keeping the format reader generic preserves a valid path
for collection documents and prevents table policy from leaking into framing.

Both modes reject comments, trailing commas, non-JSON numeric tokens, and
incomplete or extra syntax.

## Encoding

Input is strict UTF-8. A single leading UTF-8 byte-order mark is accepted by
default and is reported as source metadata, but it is not part of a logical
value. UTF-16 and UTF-32 byte-order marks, malformed UTF-8, invalid escape
sequences, and invalid Unicode scalar sequences fail deterministically.

The reader never performs replacement-character decoding.

## Logical Fidelity

Each yielded record contains its one-based source ordinal, zero-based source
byte offset, one-based line number, zero-based byte position in that line, raw
UTF-8 byte length, and one immutable logical value.

Absolute offsets and byte positions count a leading UTF-8 BOM when present.
Physical lines recognize CR, LF, and CRLF, with CRLF counted as one line
transition.

Logical values preserve:

- `Null`, `Boolean`, `String`, `Number`, `Object`, and `Array` as distinct
  kinds;
- explicit null as a present `Null` value, while a missing property remains
  absent;
- decoded property names exactly, including case and Unicode;
- object properties and array elements in encounter order;
- the exact validated UTF-8 lexeme of every number, including negative zero,
  exponent spelling, and insignificant decimal zeroes; and
- nested objects and arrays without flattening.

Property-name identity uses ordinal comparison after JSON escape decoding.
Consequently, `"a"` and `"\u0061"` are duplicates while `"a"` and `"A"` are
distinct.

Duplicate properties are fatal in this strict reader at every object depth.
The first duplicate reached within the configured resource limits is reported
with a stable rule and bounded location metadata. Resource or syntax failures
encountered earlier retain precedence. No last-wins or first-wins map is
constructed. A later tolerant migration adapter may convert a record-local
format failure into a reject, but it must not silently choose a duplicate
value.

Numbers are never routed through `double`. Mapping code may later select an
integer, decimal, floating-point, or text representation only when that choice
is lossless under the migration type policy.

## Ordered Canonical JSON

The companion logical-value serializer is
`csharpdb-json-ordered-value/v1`. It emits:

- UTF-8 without a byte-order mark or insignificant whitespace;
- object properties and array elements in encounter order;
- exact preserved number lexemes;
- lowercase `true`, `false`, and `null`;
- short JSON escapes for quote, reverse solidus, backspace, tab, newline,
  form feed, and carriage return;
- lowercase `\u00xx` escapes for the remaining control characters; and
- all other valid Unicode scalar values directly as UTF-8.

This is deterministic ordered canonicalization, not property sorting or
numeric normalization. It is suitable for retaining nested table values as
canonical JSON text without destroying source number spelling or object order.

## Resource Limits

The implementation owns one fixed 64 KiB source buffer and at most one bounded
raw logical value at a time. Public options are validated before source
enumeration.

| Limit | Default | Absolute ceiling |
| --- | ---: | ---: |
| Raw bytes per logical value | 16 MiB | 64 MiB |
| JSON depth | 64 | 128 |
| Properties per object | 4,096 | 16,384 |
| Elements per array | 65,536 | 65,536 |
| Total logical nodes per value | 65,536 | 65,536 |
| UTF-8 bytes in a decoded property name | 64 KiB | 1 MiB |
| UTF-8 bytes in a decoded string | 16 MiB | 16 MiB |
| UTF-8 bytes in a number token | 1 MiB | 16 MiB |

The raw-value ceiling is an additional outer bound. Configuring a smaller
value ceiling therefore also limits every contained token even when its
token-specific ceiling is larger.

Each option is independently configurable within its absolute ceiling.
Property and array limits may likewise exceed the node ceiling; whichever
applicable limit is reached first determines the failure.

The reader does not retain previously yielded values and never seeks. Total
allocation may grow with the number of records, but reader-owned live memory
is governed by the fixed source buffer and configured single-value limits,
not total source length.

## Diagnostics And Failure Semantics

Diagnostics use stable `MIG-JSON-*` rule identifiers for:

- framing and malformed JSON;
- invalid encoding;
- duplicate properties; and
- value, depth, property-count, array-count, node-count, property-name,
  string, and number limits.

Messages never include source values or property names. Diagnostics may include
the current logical-value ordinal and bounded byte/line location evidence.

The reader does not yield a partially parsed current value. Values yielded
before a later malformed value remain valid observations. Cancellation is
observed while filling the source buffer and while scanning large values.

The reader is single-use. Unless `LeaveOpen` is selected, disposing it closes
the source stream even when enumeration is abandoned.

## Required Qualification

The foundation tests must cover:

- root arrays, multiple values, empty inputs, blank lines, optional final
  newline, byte-order-mark handling, and non-seekable one-byte chunks;
- exact names, null versus absence, encounter order, nested values, escaped
  duplicate equivalence, and case-distinct properties;
- integer boundaries and overflow, high-precision decimals, large exponents,
  negative zero, and exact exponent/trailing-zero spelling;
- invalid number grammar, truncation, bad escapes, comments, trailing commas,
  invalid UTF-8 and Unicode scalars, framing violations, and every limit;
- cancellation, single-use enumeration, and source ownership; and
- large generated root-array and multiple-value streams whose first value is
  yielded before the unread suffix is made available.

Track 4B remains open after this foundation. The table-source, schema,
catalog, data-source, and retained-package layers are specified in the later
JSON migration notes. Typed sidecars, streaming JSON/NDJSON export,
resumability, and CLI integration remain open.

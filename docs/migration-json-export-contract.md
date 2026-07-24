# Migration JSON And NDJSON Export Contract

## Status And Scope

This document freezes the eighth Track 4B contract: deterministic,
restart-only streaming export from one typed CSharpDB table to either a
compact JSON root array or newline-delimited JSON.

The first slice includes:

- a strict canonical export manifest;
- exact physical and logical content evidence;
- bounded, forward-only table-row serialization; and
- a caller-owned empty output stream.

Retained-snapshot publication, CLI routing, durable checkpoints, mid-stream
resume, collection/document export, and typed-intent generation remain later
slices. The disposable Windows VM qualification is not required for this
restart-only writer because it makes no filesystem-durability claim.

## Public Contract

`JsonStreamingExporter.WriteAsync` accepts a
`JsonStreamingExportRequest` containing:

- the lossless v1 profile and selected root-array or NDJSON framing;
- safe source-snapshot evidence;
- one ordered CSharpDB `TableSchema`;
- rows in strictly increasing physical row-ID order;
- an exact total output-byte ceiling; and
- the per-value decoded BLOB ceiling.

Each `JsonExportRow` carries its physical row ID and values in schema order.
The row ID establishes order and is never emitted or hashed as a value.

The destination must be writable, seekable, empty, and positioned at byte
zero. It remains caller-owned. The writer flushes successful output but does
not close the stream or claim durable storage. A failed restart-only export
returns no result; the caller must discard or truncate any partial output and
restart from row zero.

Success returns:

- the validated `JsonExportManifest`;
- its exact canonical UTF-8 bytes; and
- the independently retainable canonical-manifest digest.

## Fixed Lossless V1 Codec

Both framings use:

- strict UTF-8 without a byte-order mark;
- compact JSON with no insignificant spaces;
- invariant scalar formatting;
- exact schema-order object properties;
- ordinal, case-sensitive property names;
- every property present in every row;
- JSON `null` for a nullable database NULL;
- minimal deterministic JSON string escaping; and
- one LF byte as the only line terminator.

Root-array output is exactly:

```text
[object,object]\n
```

An empty root array is exactly `[]\n`.

NDJSON output is exactly one compact object followed by LF per row. Empty
NDJSON output contains zero bytes. No blank lines, array delimiters, comments,
or byte-order mark are emitted.

The ordered column manifest distinguishes the four physical CSharpDB storage
types:

| CSharpDB type | JSON representation | Value encoding |
| --- | --- | --- |
| `Integer` | JSON number | signed 64-bit invariant decimal |
| `Real` | JSON number | finite binary64 round-trip text |
| `Text` | JSON string | strict Unicode JSON string |
| `Blob` | JSON string | padded RFC 4648 base64 |

Integers include the full signed 64-bit range. REAL values use invariant
round-trip formatting and preserve the emitted negative-zero lexeme; NaN and
positive or negative infinity are rejected because JSON has no lossless
representation for them. Text is always emitted as a JSON string, even when
its contents happen to be valid JSON. BLOBs are copied into bounded
operation-owned storage while rendered and are cleared after the row.

Property names and text must contain valid Unicode scalar sequences. Quotes,
backslashes, and control characters use fixed JSON escapes; other valid
Unicode is encoded as strict UTF-8. Duplicate rendered property names are not
allowed.

## Resource And Row Atomicity Rules

The writer binds and enforces the JSON reader's absolute compatibility
ceilings:

- no more than 16,384 properties per object;
- at most 1 MiB of decoded UTF-8 per property name;
- at most 16 MiB of decoded UTF-8 per text or base64 string;
- at most 64 MiB of encoded JSON per row object; and
- a positive caller-selected total output-byte ceiling.

The maximum decoded BLOB setting must fit its padded base64 representation
inside the string ceiling. Every row is validated completely before its first
byte is written. Width, nullability, runtime storage type, Unicode, scalar,
BLOB, row-size, and remaining total-byte checks therefore fail at the prior
complete row boundary rather than leaving a partial object.

After validation, the writer renders exactly one object into an
operation-owned buffer bounded by the 64 MiB logical-value ceiling. It
strict-parses those exact bytes, reconstructs the exported typed values, and
requires their independently computed canonical row hash to match the source
row hash. Only that verified buffer is written to the destination, in bounded
chunks, and it is cleared before the next row. This detects escape, numeric,
or base64 rendering drift without buffering the table or requiring a readable
destination.

Root-array accounting always reserves the final `]\n` before accepting a row.
NDJSON accounting includes each row's LF terminator. Exact limits succeed and
one byte below the required output size fails.

## Canonical Manifest

The canonical envelope format is
`csharpdb-json-export-manifest/v1`. Its payload binds:

- the lossless profile;
- source product version, snapshot byte length, and SHA-256;
- table name and signed-row-ID ascending order;
- the ordered column schema, physical types, nullability, encodings, and BLOB
  ceilings;
- UTF-8, framing, newline, property-order, null, escape, and resource rules;
- row count, exact data byte length, and exact data SHA-256; and
- source and exported logical content digests.

The ordered schema has contract `csharpdb-json-export-schema/v1` and its own
SHA-256. The manifest excludes paths, timestamps, host identity, random
identifiers, and row values.

Canonical JSON is strict UTF-8 without a BOM. The serializer uses fixed
property order and camel-case enum text, rejects comments, trailing commas,
integer enums, unknown, duplicate, mis-cased, missing, and null members, and
requires exact canonical bytes on reopen. The envelope, schema, source,
physical data, and logical hashes use lowercase SHA-256. The manifest is
bounded to 16 MiB, depth 64, and one million aggregate text characters.

The manifest digest protects consistency and detects replacement only when
retained independently. It is not a signature.

## Physical And Logical Evidence

The physical content section records the exact output byte length and SHA-256,
including delimiters and final LF bytes.

`csharpdb-json-export-ordered-content/v1` hashes:

1. its unique ASCII domain;
2. the `csharpdb-canon-v1` contract hash;
3. each canonical typed row hash in emission order; and
4. the final unsigned 64-bit big-endian row count.

This preserves order and duplicates without retaining the table in memory.
The lossless profile computes independent source and exported logical digests
and requires them to match.

## Failure And Privacy Rules

Failures identify the rule, row ID, and column ordinal or name needed to act
on the problem, but never include cell contents, BLOB bytes, source paths, or
raw manifest values. Cancellation is observed before output and throughout
row enumeration and writing. Temporary byte buffers containing encoded values
or digests are cleared before release where practical.

## Qualification

The merge gate covers:

- exact zero-, one-, and many-row bytes for both framings;
- deterministic manifests, data hashes, and logical hashes;
- schema-order names, hostile valid Unicode, supplementary characters,
  quotes, backslashes, controls, null, and empty text;
- signed 64-bit endpoints, finite binary64 edge cases, empty and maximum BLOBs;
- rejection of non-finite REALs, invalid Unicode, row-order, width, type, and
  nullability mismatches;
- exact and one-under row, BLOB, and total-byte limits;
- no partial current row on validation failure;
- cancellation, leave-open ownership, and deterministic replay;
- strict canonical-manifest tamper and resource-bound tests;
- parsing emitted bytes through the strict streaming JSON reader; and
- a 50,000-row bounded-memory fixture.

## Deferred Work

This slice does not publish files, route the CLI, retain checkpoints, resume a
partial export, emit nested collection documents, or create a typed-intent
sidecar.

In particular, `csharpdb-json-table-intent/v1` has no binary64 codec. A table
containing CSharpDB `Real` values therefore cannot yet be advertised as a
complete typed export-to-import round trip. Typed export must first define a
new versioned floating-point intent contract; the frozen v1 contract will not
be changed implicitly.

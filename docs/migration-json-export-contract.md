# Migration JSON And NDJSON Export Contract

## Status And Scope

This document freezes deterministic export from one typed retained CSharpDB
table to either a compact JSON root array or newline-delimited JSON. The
restart-only publisher is implemented. The durable checkpoint artifact,
prefix geometry, and generation transitions are also frozen here before the
prepared-output lease activates them.

The completed restart-only slice includes:

- a strict canonical export manifest;
- exact physical and logical content evidence;
- bounded, forward-only table-row serialization;
- a caller-owned empty output stream;
- private local-filesystem staging and fail-closed manifest-last publication;
- retained-snapshot source binding; and
- `migrate export --format json|ndjson` CLI routing.

Durable checkpoint publication, prepared-output leasing, mid-stream replay and
resume, killed-process staging reclamation, collection/document export, and
typed export intent remain later slices. The disposable Windows VM
qualification is also deferred. This restart-only slice flushes completed
staging files to durable storage as supported by the host, but makes no
directory-fsync or abrupt-power-loss guarantee.

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

## Frozen Checkpoint Artifact

`csharpdb-json-export-checkpoint/v1` is the canonical, bounded checkpoint
envelope. Its lowercase SHA-256 covers the format, digest algorithm, and
payload. The parser requires strict UTF-8 without a BOM, rejects comments,
trailing commas, duplicate or unknown properties, unsupported enum spellings,
noncanonical property order or whitespace, and input larger than 16 MiB.
Diagnostics do not repeat attacker-controlled names or values.

Every checkpoint contains:

- a nonnegative generation and `Writing` or `DataComplete` phase;
- an immutable `csharpdb-json-export-checkpoint-binding/v1` binding;
- the canonical binding digest;
- evidence for the last complete durable row boundary; and
- final logical and manifest evidence only in `DataComplete`.

The binding contains the lossless profile, exact retained source evidence and
independently pinned snapshot identity, ordered table manifest, framing, codec,
and resource ceilings. Progress contains the completed row count, signed last
row ID when at least one row is complete, exact physical prefix length and
digest, `csharpdb-json-export-ordered-content-prefix/v1`, and matching source
and exported logical-prefix digests. Prefix digests are replay evidence, not
serialized incremental-hash state.

`DataComplete` additionally contains matching final source/exported logical
digests and the digest of the reconstructed canonical export manifest. The
checkpoint serializer reconstructs that manifest and verifies the supplied
digest before accepting the checkpoint.

### Durable Prefix Geometry

For root-array framing, a zero-row `Writing` checkpoint describes exactly `[`.
A nonempty writing prefix starts with `[` and ends with the final complete
object's `}`; it has neither a trailing comma nor `]\n`. `DataComplete`
describes the full file: exactly `[]\n` for zero rows, or a prefix starting
with `[` and ending with `}]\n`.

For NDJSON, zero rows describe an empty file in both phases. Every nonempty
prefix starts with `{` and ends with the LF following a complete object.
`Writing` and `DataComplete` may therefore describe identical physical bytes;
source EOF evidence, not an extra delimiter, distinguishes the terminal
phase.

Length validation derives the minimum possible object size from the bound
schema and JSON string escaping, uses the strict reader's maximum value size
as the per-object upper bound, performs checked arithmetic, and reserves the
two-byte root-array closing tail while the phase is `Writing`.

### Generation Transitions

The first persisted generation is zero. Repeating a generation is allowed
only for exactly idempotent checkpoint content. Otherwise generation advances
by exactly one; binding, aggregation, and completed evidence cannot regress.
`DataComplete` is terminal.

A `Writing` generation advances rows, signed last row ID, bytes, and both
logical-prefix digests. The transition to `DataComplete` may include rows
written since the last periodic checkpoint or may only finalize existing
rows. With no new rows, root-array completion adds exactly `]\n` and changes
only physical evidence, while NDJSON completion changes only phase,
generation, and final EOF evidence.

This checkpoint contract does not itself create prepared files, replace an
active checkpoint, truncate a torn tail, or resume enumeration. Those actions
remain behind the deterministic prepared-output lease and replay coordinator
in the next slices.

## Retained-Snapshot Publication

`JsonExportPublisher.PublishAsync` accepts distinct, normalized, absolute
sibling data and manifest paths plus one `JsonStreamingExportRequest`. It
creates private sibling staging files, completes and verifies the streaming
export there, durably flushes each file as supported, and then commits the
data path before the canonical manifest path. It never overwrites a final
path.

The publisher supports a caller-controlled local Windows directory. UNC and
mapped-network locations fail closed. Existing directory components and final
files cannot be links, junctions, reparse points, devices, or other special
files. Final data and manifest files must each have one link and distinct
stable file identities. Private staging files receive an owner-only protected
ACL. Existing final files are reusable only while they retain that same
current-owner-only protected ACL. Non-Windows publication is not part of this
slice.

A rerun always regenerates the complete export before it examines final
content for reuse. The following states are accepted:

- neither final exists: publish data, then manifest;
- the exact data exists alone: reuse it and publish the exact manifest; or
- the exact data/manifest pair exists: reuse both.

A manifest without data, different existing content, aliases, unsafe path
components, or a data/manifest mismatch fails closed. Cancellation is observed
through staging and immediately before the data namespace commit. Once exact
final data is committed, the publisher completes the deterministic manifest
decision without observing cancellation so it does not deliberately strand a
recoverable data-only state.

`CSharpDbJsonExportAdapter` opens one retained snapshot only after path
preflight, verifies the independently supplied canonical snapshot identity,
captures and rechecks the physical table schema, and enumerates typed rows in
strictly increasing signed row-ID order. The session stays pinned through
export and publication. An exact-pair rerun therefore still requalifies the
retained source through EOF before reporting reuse. This v1 adapter uses the
built-in/default Engine reader composition. Snapshots requiring custom
storage, catalog, checksum, index, or serializer providers remain unsupported
until their provider provenance can be represented and bound into the export
manifest.

The CLI surface is:

```text
csharpdb migrate export <retained-snapshot.db>
  --format json|ndjson
  --table <physical-table>
  --out <table.json|table.ndjson>
  --manifest <table.manifest.json>
  --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<digest>>
  [--profile lossless-v1]
  [--max-data-bytes <positive-int64>]
  [--max-decoded-blob-bytes <positive-int32>]
  [--json]
```

`--format json` selects root-array framing; `--format ndjson` selects
LF-terminated NDJSON. The final `--json` flag selects structured command
status and is unambiguous with either data format. CSV checkpoint intervals
and the spreadsheet-safe lossy profile are rejected for JSON/NDJSON rather
than silently ignored.

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
- a 50,000-row bounded-memory fixture;
- exact new, data-only, and pair-reuse publication states;
- mismatch, alias, link, special-file, cancellation-cutoff, and injected-fault
  publication states;
- retained-snapshot identity, schema, row-order, and source-requalification
  behavior; and
- JSON/NDJSON CLI routing, result modes, empty framing, retries, and CSV-option
  isolation.

## Deferred Work

This slice freezes and validates checkpoint artifacts but does not yet persist
an active checkpoint, resume a partial export, emit nested collection
documents, or create a typed export-intent sidecar. Until the prepared-output
lease and replay coordinator are activated, an exact CLI rerun still starts
from row zero.

An uncatchable process termination can leave an unreferenced private
`.csharpdb-json-export-*.stage` sibling. A full rerun uses new private staging
and can still recover or reuse the exact finals, but deterministic leased
staging and safe orphan reclamation are deferred to the checkpoint and
process-crash slice. Directory-entry durability and abrupt-power-loss
qualification remain deferred with the disposable Windows VM work.

In particular, `csharpdb-json-table-intent/v1` has no binary64 codec. A table
containing CSharpDB `Real` values therefore cannot yet be advertised as a
complete typed export-to-import round trip. Typed export must first define a
new versioned floating-point intent contract; the frozen v1 contract will not
be changed implicitly.

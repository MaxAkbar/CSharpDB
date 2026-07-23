# Migration CSV Export Contract

This note records the typed manifest, canonical checkpoint contract, and
restart-only streaming-writer portions of the Phase 4A CSV export work. It
freezes the compatibility boundary that a later retained-source adapter,
reopener, publisher, and CLI must satisfy. It does not claim that the export
command, fail-closed manifest-last publication, filesystem checkpoint journal,
or durable resume are implemented.

## Contract Boundary

`csharpdb-csv-export-manifest/v1` is a canonical JSON sidecar for one CSharpDB
table export. It identifies:

- the source product version and immutable database snapshot by byte length
  and SHA-256;
- the table, deterministic row order, and ordered column schema;
- every fixed CSV formatting and scalar-encoding choice;
- the exported row count, exact CSV byte length, and exact CSV-byte SHA-256;
- the canonical logical source and exported-content digests; and
- the selected lossless or explicitly lossy export profile.

The sidecar deliberately excludes filesystem paths, timestamps, host identity,
random identifiers, and cell values. It can therefore be reproduced for the
same source snapshot, table, profile, and output bytes. The manifest binds one
data file by content rather than by a relocatable or attacker-controlled path.

The source and content hashes detect corruption and mismatched generations.
They are not signatures: a party able to replace both artifacts can calculate
new unkeyed hashes. A caller still needs a trusted location or an independently
retained manifest digest.

## Fixed CSV V1 Codec

The lossless profile has one fixed RFC 4180-compatible representation:

- strict UTF-8 without a byte-order mark;
- comma delimiter, double-quote quote character, and RFC 4180 quote doubling;
- CRLF record endings, one header record, and a final record ending;
- invariant-culture scalar formatting;
- an unquoted `\N` null token; a text value equal to `\N` must be quoted;
- contiguous fields in the manifest's column order; and
- deterministic CSharpDB table row-ID ascending order.

The ordered schema records each zero-based ordinal, exact source name, rendered
header, CSharpDB storage type, nullability, and scalar encoding. V1 supports:

| CSharpDB type | Manifest encoding |
| --- | --- |
| `Integer` | signed 64-bit invariant decimal |
| `Real` | finite binary64 round-trip text |
| `Text` | strict UTF-8 text with RFC 4180 escaping |
| `Blob` | padded RFC 4648 base64 plus an explicit decoded-byte ceiling |

The ordered column list has its own schema contract and SHA-256. Lossless
headers equal source column names exactly. The writer enforces the declared
field count and never uses omission to represent null or missing values. Each
BLOB column carries a positive per-value decoded-byte bound no larger than
12 MiB, which keeps its base64 text inside the strict reader's absolute
16 Mi-character field ceiling. Non-BLOB columns cannot claim that binary
bound.

## Content Proofs

The content section separates physical and logical evidence:

- `dataByteLength` and `dataDigest` bind the exact CSV bytes, including the
  header and final CRLF;
- `rowCount` is a 64-bit data-row count;
- `sourceLogicalDigest` binds the ordered typed rows read from the database
  snapshot;
- `exportedLogicalDigest` binds the ordered typed values represented by the
  exported CSV; and
- `csharpdb-canon-v1`, its contract digest, and
  `csharpdb-csv-export-ordered-content/v1` make the logical checksum rules
  explicit.

The ordered logical digest hashes the ASCII domain `CSDBCSV1`, the 32-byte
`csharpdb-canon-v1` contract hash, each 32-byte canonical row hash in emitted
order, and the final unsigned 64-bit big-endian row count. It therefore
preserves duplicate rows and row order without retaining the rows in memory.

For `LosslessV1`, the source and exported logical digests must be identical.
This is stronger than checking the CSV file alone: it prevents a syntactically
valid export from silently changing a typed value.

The contract serializer validates relationships within the sidecar. The
restart-only writer produces the physical and logical proofs while it streams
the CSV. Independent requalification of a published data/manifest pair remains
publication work.

## Explicit Spreadsheet-Safe Loss

`LosslessV1` is the default compatibility profile.
`SpreadsheetSafeLossyV1` is separately named because quoting a cell does not
prevent every spreadsheet from interpreting it as a formula, while changing
the cell does not preserve the source value.

The v1 spreadsheet policy prefixes an apostrophe when a text cell or header
starts with `=`, `+`, `-`, `@`, space, tab, CR, or LF. The policy is a
versioned, best-effort formula-injection mitigation, not a guarantee about
every spreadsheet application or import setting.

V1 rejects BLOB columns in the spreadsheet profile. Padded base64 can begin
with `+`, while prefixing that value would make it invalid base64 and break the
declared typed encoding. Lossless exports continue to support bounded BLOBs.

The lossy manifest must name rule `MIG-CSV-EXPORT-FORMULA-001` and algorithm
`spreadsheet-formula-prefix-apostrophe/v1`. It records only aggregate
transformed-header, transformed-row, and transformed-cell counts; it never
copies the affected values into the sidecar. When any cell changes, the
exported logical digest cannot claim equality with the source logical digest.
When no cell changes, those logical digests must remain equal. Cell counts are
bounded by transformed rows and the number of eligible `Text` columns, so the
sidecar cannot report an impossible aggregate.
The lossless profile rejects transformed headers, loss evidence, or unequal
logical digests.

## Canonical Manifest Rules

The sidecar is canonical strict UTF-8 JSON without a BOM. Its envelope carries
the format identifier, `sha256` algorithm, payload digest, and typed payload.
The digest covers the format, algorithm, and payload, but not its own value.
The serializer:

- uses fixed property order and camel-case string enums;
- rejects comments, trailing commas, duplicate or unknown members, integer
  enum values, invalid UTF-8/UTF-16, NUL characters, and noncanonical bytes;
- recomputes the envelope and ordered-schema hashes, and requires every source,
  data, and logical hash field to use lowercase SHA-256 text;
- caps the manifest at 16 MiB and aggregate manifest text at 1 Mi characters;
  and
- limits columns to the file adapter's bounded-record ceiling.

Reopening requires the exact canonical bytes, not merely JSON with equivalent
values.

## Canonical Checkpoint Contract

`csharpdb-csv-export-checkpoint/v1` now defines canonical, strict UTF-8 JSON
evidence for one complete CSV record boundary. Its immutable binding covers the
selected profile, source snapshot evidence and canonical retained-snapshot
identity, table and ordered schema, fixed CSV codec, maximum data bytes, and
the per-BLOB decoded-byte ceiling. A separate binding digest prevents a writer
from reopening prepared bytes under a changed source, schema, profile, codec,
or resource policy.

Each generation has either `Writing` or `DataComplete` phase and records:

- the completed data-row count and nullable last physical row ID;
- the exact physical prefix byte length and SHA-256;
- source and exported logical row-hash prefix digests under
  `csharpdb-csv-export-ordered-content-prefix/v1`; and
- aggregate spreadsheet-safe transformed-row and transformed-cell counts.

Physical row IDs are signed 64-bit values. Negative IDs are valid; the last ID
is absent exactly when the completed row count is zero. A zero-row checkpoint
is not an empty file marker: it must bind the exact rendered header bytes,
including quoting and the final CRLF, their exact byte length and SHA-256, and
the frozen empty logical-prefix digests.

`DataComplete` adds final source/export logical digests and the final manifest
digest. The serializer reconstructs the canonical export manifest from that
evidence and rejects a mismatched digest. `Writing` cannot carry completion
evidence, while `DataComplete` must carry it. Lossless checkpoints require
equal source/export logical evidence and zero transform counts; spreadsheet
checkpoints retain the same aggregate consistency rules as the final manifest.

The logical and physical prefix digests are verification evidence only. They
are not serialized or resumable SHA-256 internal state. A future recovery
implementation must rehash the prepared physical prefix through the recorded
byte boundary and replay retained source rows through the recorded signed row
ID to reconstruct physical and logical hash state before continuing.

This slice is a canonical serializer and validator, not a durable checkpoint
store. No prepared-output lease, disk-flush/fsync contract, atomic checkpoint
journal, cross-process recovery coordinator, or resume API exists yet.

## Restart-Only Streaming Writer

`CsvStreamingExporter` writes the fixed codec to a caller-owned, writable,
seekable stream that is empty and positioned at byte zero. It validates the
fixed source/profile/schema relationship before writing the header, requires
strictly increasing signed physical row IDs, validates a complete typed row
before writing any byte of that row, and leaves the destination open. UTF-8
text and padded base64 BLOBs are emitted in bounded chunks. Successful
completion flushes the stream and returns the canonical manifest bytes and
digest, but does not claim that the flush is durable.

The low-level request receives source evidence and rows separately. Its result
proves the emitted bytes and supplied typed row sequence; it does not by itself
prove that those rows came from the named retained snapshot. The future
retained-source adapter must construct both from one verified snapshot session.

This is deliberately a restart-only boundary. If enumeration, validation,
I/O, or cancellation fails, no manifest result is returned and the
caller-owned output must be discarded or truncated to zero before retrying.
The writer exposes no checkpoint callback or append contract and makes no claim
that a partial record is reusable. The separate canonical checkpoint models do
not change this restart-only writer API.

The slice does not provide an export CLI surface, filesystem checkpoint
journal, retained snapshot adapter, prepared-output lease or durable
flush/fsync, prepared output publication, or cross-process resume. It also does
not make a two-file CSV/manifest pair atomic.

The offline `RetainedDatabaseSnapshot` API now provides the required durable
source view and deterministic row-source seam: it materializes recovery only
on a private copy, publishes one clean database file, requires an independently
retained byte length and SHA-256 on reopen, and scans local physical tables in
strictly ascending row-ID order with an exclusive resume boundary. It rejects
views, system/internal tables, and external archives whose bytes are outside
the retained identity. A process-local reader transaction remains insufficient
after a crash. See
[`migration-csharpdb-retained-snapshot.md`](migration-csharpdb-retained-snapshot.md).

The canonical checkpoint contract now binds the retained snapshot, table and
ordered schema, export profile and codec, completed row boundary, byte
boundary, and expected output prefix. Until a retained-source adapter,
exclusive prepared-output lease, durable data flush, atomic checkpoint journal,
physical-prefix verifier, and source-prefix replay path enforce those
identities and boundaries, a failed export must restart from the beginning
rather than claim durable resume. The source, restart-only codec, and canonical
checkpoint prerequisites are now available; filesystem durability and resume
integration remain outstanding.

Phase 4A remains open until the retained-source adapter and CLI use this
contract, manifest-last data and sidecar publication fails closed, and large
interrupted exports pass bounded-memory and resume qualification.

# Migration CSV Export Contract

This note records the typed-manifest portion of the Phase 4A CSV export work.
It freezes the compatibility boundary that a later streaming exporter,
reopener, and CLI must satisfy. It does not claim that the CSV data writer,
export command, fail-closed manifest-last publication, or durable resume are
implemented.

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
headers equal source column names exactly. A later writer must also enforce the
declared field count and must not use omission to represent null or missing
values. Each BLOB column carries a positive per-value decoded-byte bound no
larger than 12 MiB, which keeps its base64 text inside the strict reader's
absolute 16 Mi-character field ceiling. Non-BLOB columns cannot claim that
binary bound.

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

The contract serializer validates relationships within the sidecar. It does
not yet stream the CSV or independently re-read the data file, so producing and
requalifying these proofs remains exporter work.

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

## Streaming And Resume Remain Deferred

This contract slice does not provide a streaming RFC 4180 writer, an export
CLI surface, checkpoint files, prepared output publication, or cross-process
resume. It also does not make a two-file CSV/manifest pair atomic.

The offline `RetainedDatabaseSnapshot` API now provides the required durable
source view and deterministic row-source seam: it materializes recovery only
on a private copy, publishes one clean database file, requires an independently
retained byte length and SHA-256 on reopen, and scans local physical tables in
strictly ascending row-ID order with an exclusive resume boundary. It rejects
views, system/internal tables, and external archives whose bytes are outside
the retained identity. A process-local reader transaction remains insufficient
after a crash. See
[`migration-csharpdb-retained-snapshot.md`](migration-csharpdb-retained-snapshot.md).

The future resume contract must bind its checkpoint to that retained snapshot,
table and ordered schema, export profile and codec, completed row boundary,
byte boundary, and verified output prefix. Until the exporter consumes the
retained snapshot and row-ID cursor through that checkpoint, a failed export
must restart from the beginning rather than claim durable resume. The source
prerequisites are now available; writer/checkpoint integration remains
outstanding.

Phase 4A remains open until the streaming writer and CLI use this contract,
manifest-last data and sidecar publication fails closed, lossless round trips
reproduce the typed logical digest, the spreadsheet profile reports its loss
exactly, and large interrupted exports pass bounded-memory and resume
qualification.

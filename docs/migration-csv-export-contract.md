# Migration CSV Export Contract

This note records the typed manifest, canonical checkpoint contract,
restart-only streaming writer, generic stateful resumable prepared-output
coordinator, and Windows-qualified prepared-output lease/journal portions of
the Phase 4A CSV export work. It freezes the compatibility boundary that a
later retained-source adapter, publisher, and CLI must satisfy. It does not
claim that a caller-supplied row factory proves source origin, that
manifest-last publication or power-loss qualification of checkpoint namespace
replacement is complete, that the export/resume command exists, or that the
prepared-output substrate supports non-Windows systems.

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
restart-only and resumable writers produce the physical and logical proofs
while streaming the CSV. Independent requalification of a published
data/manifest pair remains publication work.

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
are not serialized or resumable SHA-256 internal state. The stateful
coordinator therefore replays source rows from the beginning through the
recorded signed row ID to rebuild and verify both logical hash states and the
transform counters. It separately rehashes the actual prepared physical prefix
through the recorded byte boundary before appending. These checks depend on
the trusted row factory returning the same immutable source on every open; the
generic coordinator does not itself prove that source origin.

Checkpoint transitions are fail-closed. The first durable generation is zero.
The same generation is idempotent only when its canonical bytes are identical;
different content at the same generation is rejected, and later generations
must advance by exactly one. Row and byte progress advance together, all
counters are monotonic, signed row IDs strictly increase when row progress is
made, and physical or logical evidence cannot change without that progress.
The only no-row-progress transition is from `Writing` to `DataComplete` with
unchanged prefix evidence. `DataComplete` is terminal.

## Windows Prepared-Output Lease And Journal

`CsvExportPreparedOutputLease` implements the local Windows filesystem boundary
for the stateful prepared-output coordinator. A fully qualified, normalized
destination path deterministically selects three private siblings in that
destination's parent: prepared data, the active checkpoint, and a pending
checkpoint. Their names depend only on the destination path. The final
destination is never opened by the lease, and opening fails closed if that
final path already exists.

The prepared-data file is opened or created with a protected current-owner-only
ACL, no-follow regular-file and single-link checks, write-through semantics,
and an exclusive handle that acts as the cross-process lease. The parent
directory identity is pinned and rechecked around filesystem operations.
Existing active and pending siblings must also be private regular single-link
files. The active checkpoint read is capped at the canonical checkpoint byte
ceiling. A stale pending sibling is validated but ignored during recovery; it
is never checkpoint authority.

If no active checkpoint exists, an empty prepared file opens as `New`. A
nonempty prepared file opens as `UncheckpointedData`, with stream access
blocked until the caller explicitly invokes `ResetUncheckpointedAsync`.
Reset truncates only the private prepared file to zero and flushes that change
to disk. Disposal releases the handles but deliberately preserves the prepared
data and both checkpoint siblings.

When an active checkpoint exists, reopen first validates its canonical bytes
and immutable binding. It then requires the prepared file to contain at least
the recorded prefix, rehashes exactly that prefix, compares the SHA-256 in
constant time, and verifies that the boundary ends in CRLF. Only after all
three checks pass may an uncheckpointed tail be truncated and that truncation
flushed durably. A short, mismatched, or incomplete-record prefix is rejected
without truncation.

Checkpoint persistence is data-first. The prepared stream must end exactly at
the checkpoint byte boundary, the checkpoint carries that exact prefix length
and SHA-256 evidence, and the prepared bytes are flushed to disk before any
checkpoint authority changes. The canonical checkpoint bytes then replace the
entire pending sibling and are flushed to disk. After that durable-pending
point, cancellation is no longer observed; a handle-based atomic rename
creates or replaces the active checkpoint. Failure leaves the old active
checkpoint authoritative, while any stale pending file remains non-authority.

This substrate is intentionally limited to local Windows filesystems. UNC and
mapped network volumes fail closed, and non-Windows platforms receive
`PlatformNotSupportedException`. Abrupt-power-loss qualification of namespace
rename durability, final data/manifest publication, the retained-source export
adapter and its source-origin proof, and export/resume CLI wiring remain
pending.

## Stateful Resumable Prepared-Output Coordinator

`CsvStreamingExporter.WriteResumableAsync` now coordinates the fixed renderer,
canonical checkpoints, and prepared-output lease without publishing the final
CSV or manifest. A new output renders the exact header first and persists it as
durable generation zero with zero completed rows. It then streams rows in
strictly increasing signed row-ID order and persists a `Writing` checkpoint
after each configured interval of newly completed rows. Source/export logical
prefixes, physical bytes, and transform counts are accumulated without
retaining the row set in memory.

If private prepared bytes exist without any active checkpoint, no byte is
authoritative. The coordinator explicitly invokes the lease reset operation,
durably truncates that private file to zero, and starts again at header
generation zero. It never resets an output that has active checkpoint
authority.

On recovery, the lease first validates the active checkpoint and physical
prefix and durably truncates any tail beyond the last complete authoritative
record. The coordinator independently opens `OpenRows(null, ...)`, replays the
source from the beginning through the checkpoint's completed row count and
last signed row ID, and passes every row through the same preparation and CSV
rendering path into a hash-only sink. It compares the rebuilt byte length and
digest, both logical-prefix digests, row boundary, and transform counters with
the checkpoint. It then rereads and hashes the actual prepared prefix to seed
the live physical digest before opening a fresh
`OpenRows(lastCompletedRowId, ...)` sequence and appending.

At source EOF, the coordinator persists a terminal `DataComplete` generation
containing the final logical digests and exact manifest digest. Reopening a
`DataComplete` output still replays through the recorded boundary, proves EOF
on that same source enumeration, verifies the final logical digests, and
reconstructs the canonical manifest without appending data or creating another
checkpoint generation. This closes the generic stateful writer subpart; it
does not publish the prepared data or manifest.

`OpenRows` is intentionally a trusted seam, not source-origin evidence. Every
call must enumerate the same immutable source named by `Source` and
`SourceSnapshotIdentity`; a null boundary means from the beginning, and a
non-null boundary must be exclusive. A retained-snapshot adapter must still
construct the source evidence, canonical snapshot identity, table schema, and
all row sequences from one independently verified retained snapshot session.
That adapter and its source-origin proof remain pending.

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

The slice still does not provide an export CLI surface, retained-snapshot
export adapter and source-origin proof, or prepared-output publication. It also
does not make a two-file CSV/manifest pair atomic. The separate resumable
method and prepared-output lease provide the generic stateful coordinator and
Windows-qualified durable physical journal described above; they do not
change this restart-only writer API.

The offline `RetainedDatabaseSnapshot` API now provides the required durable
source view and deterministic row-source seam: it materializes recovery only
on a private copy, publishes one clean database file, requires an independently
retained byte length and SHA-256 on reopen, and scans local physical tables in
strictly ascending row-ID order with an exclusive resume boundary. It rejects
views, system/internal tables, and external archives whose bytes are outside
the retained identity. A process-local reader transaction remains insufficient
after a crash. See
[`migration-csharpdb-retained-snapshot.md`](migration-csharpdb-retained-snapshot.md).

The canonical checkpoint contract binds the retained snapshot, table and
ordered schema, export profile and codec, completed row boundary, byte
boundary, and expected output prefix. The Windows lease enforces exclusive
prepared-file ownership, durable data-before-checkpoint ordering, atomic active
checkpoint replacement, physical-prefix verification, and tail truncation on
reopen. The generic stateful coordinator now replays source rows and rebuilds
the logical prefix through that boundary. End-to-end retained-source resume
still requires the pending adapter to prove that every `OpenRows` sequence
came from the bound immutable snapshot.

Phase 4A remains open until the retained-source adapter and CLI use this
contract with source-origin proof, manifest-last data and sidecar publication
fails closed, namespace replacement passes abrupt-power-loss qualification,
and large interrupted exports pass bounded-memory and resume qualification.
The prepared-output support scope remains explicitly limited to local Windows
filesystems.

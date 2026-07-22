# Migration CSV Inspection And Source Binding

This note records the second Phase 4A implementation slice. It builds on the
strict logical-record reader by freezing source bytes before inspection,
detecting a delimiter without silent tie-breaking, and binding the exact bytes
and normalized CSV semantics into migration source identity.

## Immutable Source Snapshot

`CsvSourceSnapshot` streams the complete source into an exclusively reserved,
ownership-marked private workspace while computing SHA-256 over every raw byte.
The digest includes the BOM, original line endings, and all bytes beyond the
bounded inspection window. A configurable `MaxSourceBytes` bound limits disk
consumption; copy memory remains fixed-size and pooled source buffers are
cleared before reuse.

Inspection and subsequent readers open only the private snapshot. They never
combine an inspected prefix with a later version of a live path or stream.
Open read leases keep the snapshot alive, and disposal removes the workspace
after the final lease closes. Unix workspaces and files are created with
user-only permissions. The default operating-system temporary directory is the
recommended location; a custom Windows workspace must be a trusted directory
whose inherited ACL is appropriate for migration data.

The snapshot is process-lifetime infrastructure for this slice. Durable resume
across process restarts will require an explicitly persisted snapshot or a new
snapshot whose full identity is verified before target resume.

## Bounded Format Inspection

`CsvFormatInspector` reads a bounded prefix from the immutable snapshot. The
defaults are 1 MiB of raw bytes, 1 MiB of decoded characters, and 100 complete
logical records. Delimiter candidates are capped at 16 and validated before a
snapshot read.

Each candidate is parsed with the same strict RFC 4180 reader configuration as
the full import path. Candidate delimiters inside quoted fields are ignored,
escaped quotes are honored, and quoted physical newlines remain part of one
logical record. Evidence contains only counts and stable rule IDs, never source
values.

Resolution is deterministic:

- a unique compatible multi-column candidate with at least two consistent
  records may resolve with medium or high confidence;
- equally plausible candidates return `Ambiguous` rather than using candidate
  order, current culture, or a default comma as a tie-breaker;
- one-column, header-only, or otherwise weak evidence returns
  `InsufficientData` with at most a low-confidence suggestion;
- one configured candidate is treated as an explicit delimiter choice;
- an artificial byte boundary inside a quoted record or encoded scalar is
  reported as truncated evidence, not malformed end-of-input.

Encoding detection is intentionally conservative. Canonical UTF-8, UTF-16, and
UTF-32 are the accepted source encodings. Their BOMs are recognized, with
UTF-32 checked before overlapping UTF-16 preambles. Without a BOM, the
configured encoding is normalized to its canonical strict decoder and
validated. The adapter does not statistically guess legacy or BOMless
UTF-16/UTF-32 encodings. The full reader continues strict decoding after the
inspection prefix.

## Source Binding

`CsvSourceBinding` verifies the full private snapshot and produces two distinct
identities:

- `SnapshotIdentity` covers raw content digest and byte length only.
- `MigrationSourceIdentity.Fingerprint` covers raw content plus the normalized
  semantic CSV format.

The semantic format includes delimiter, quote, header policy, resolved encoding
and BOM-consumption behavior, newline policy, null policy, expected width, and
an exact digest of the configured culture's numeric and date/time conversion
policy. Parser safety limits and stream ownership are operational controls and
do not change semantic identity.

The source identity never persists the original path. With no logical name it
uses a content identity; a supplied logical name is SHA-256 hashed before it is
stored. A binding can open a reader only against a snapshot with the exact same
content identity.

## Follow-on Work

Confidence-bearing schema inference, explicit ordinal overrides, and migration
catalog inspection are now implemented in the next slice; see
[`migration-csv-schema-inference.md`](migration-csv-schema-inference.md).
`IMigrationDataSource`, deterministic row cursors, tolerant reject files,
prepared target writes, manifests, and export remain deferred. The migration
data source must consume this same binding so planning, apply, resume, and
validation cannot drift to different file bytes or parsing semantics.

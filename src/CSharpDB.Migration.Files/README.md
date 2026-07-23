# CSharpDB.Migration.Files

Streaming file-format adapters for `CSharpDB.Migration`.

The current Phase 4A slices provide a forward-only CSV logical-record reader,
an immutable raw-byte source snapshot, bounded delimiter/BOM inspection, a
deterministic content-plus-format source binding, and confidence-bearing schema
inference with ordinal overrides, migration-catalog adaptation, and a repeatable
`IMigrationDataSource` whose strict behavior remains the default. The reader
handles quoted multiline fields
and escaped quotes without loading the source file into memory, preserves exact
decoded text, and keeps null, empty, missing,
and trailing-empty fields distinct. Strict decoding and absolute field,
record, column, inspection, inference, manifest, and snapshot limits keep
malformed or hostile inputs bounded.

This project owns its file-parser dependencies. It does not expose CsvHelper
types through its public API, and the provider-neutral migration project does
not reference CsvHelper.

Schema inference retains only per-column counters and candidate flags. It
defaults ambiguous, sparse, mixed, empty, or lexically significant evidence to
`Text`; sampled profiles report an unknown total and require later full-stream
validation. `CsvMigrationSourceInspector` emits a validated shared migration
catalog without claiming that a sampled prefix proves the unseen tail. See
[`migration-csv-schema-inference.md`](../../docs/migration-csv-schema-inference.md).

`CsvMigrationDataSource` validates every projected scalar, preserves request
column order, splits before row or canonical-byte limits, and emits
snapshot/policy-bound cursors. Each pass opens a fresh reader over the same
caller-owned snapshot, which supports receipt replay and validation rereads.
Opt-in deterministic replay records only `MissingField`, `NullNotAllowed`, and
`TypeMismatch`; its frozen evidence preserves record/line coordinates, field
kind, quote state, and decoded pre-normalization text. Accepted and rejected
outcomes share one contiguous cursor interval, including all-reject batches.
Parser, encoding, integrity, target-conversion, and resource-limit failures stay
fatal.
See [`migration-csv-data-source.md`](../../docs/migration-csv-data-source.md).

`CsvSnapshotPackage` atomically retains the raw snapshot and exact reader,
inference, source, and catalog policy for digest-pinned cross-process reopen.
The CLI uses that boundary for inspect, apply, resume, and validation. A
50,000-row CI fixture and isolated 100K/1M measurements qualify fixed live
batches, exact resume/checksum behavior, bounded memory, and temporary cleanup;
see [`migration-csv-performance.md`](../../docs/migration-csv-performance.md).

Reject-artifact publication, reject-aware validation comparison, typed CSV
export manifests, and export remain later Phase 4A slices. The CLI therefore
continues to expose fail-fast apply only.

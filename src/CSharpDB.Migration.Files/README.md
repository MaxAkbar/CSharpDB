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

The capability-qualified SDK validation path now replays these same CSV
accepted/rejected outcomes and compares them with the target snapshot's
authoritative receipts and ledger before report publication. The
provider-neutral SDK can now materialize the canonical reject artifact from
that target-owned evidence. The CSV CLI now exposes deterministic tolerant
planning, apply, resume, and validation only through explicit reject policy,
limit, opt-in, and retained-artifact arguments; strict fail-fast remains the
default.

The typed `csharpdb-csv-export-manifest/v1` sidecar contract now binds a
CSharpDB snapshot, ordered typed schema, fixed RFC 4180 codec, physical data
digest, source/export logical digests, and a BLOB decoded-size ceiling.
`LosslessV1` preserves source
values; the separately named `SpreadsheetSafeLossyV1` profile records
aggregate formula-mitigation changes without putting cell values in the
manifest and rejects BLOB columns whose base64 could resemble a formula. The
streaming CSV writer, export CLI, fail-closed manifest-last publication, and
durable resume are not implemented. The offline retained read-only CSharpDB
snapshot prerequisite can now be reopened and verified across processes, but
the CSV writer and checkpoint journal do not yet consume it. See
[`migration-csv-export-contract.md`](../../docs/migration-csv-export-contract.md)
and
[`migration-csharpdb-retained-snapshot.md`](../../docs/migration-csharpdb-retained-snapshot.md).

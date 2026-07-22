# CSharpDB.Migration.Files

Streaming file-format adapters for `CSharpDB.Migration`.

The current Phase 4A slices provide a forward-only CSV logical-record reader,
an immutable raw-byte source snapshot, bounded delimiter/BOM inspection, a
deterministic content-plus-format source binding, and confidence-bearing schema
inference with ordinal overrides, migration-catalog adaptation, and a strict
repeatable `IMigrationDataSource`. The reader handles quoted multiline fields
and escaped quotes without loading the source file into memory, preserves exact
decoded text, and keeps null, empty, missing,
and trailing-empty fields distinct. Strict decoding and explicit field, record,
column, inspection, and snapshot limits keep malformed or hostile inputs
bounded.

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
See [`migration-csv-data-source.md`](../../docs/migration-csv-data-source.md).

Tolerant reject files, CLI orchestration, manifests, and export are later
Phase 4 slices.

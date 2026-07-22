# CSharpDB.Migration.Files

Streaming file-format adapters for `CSharpDB.Migration`.

The current Phase 4A slices provide a forward-only CSV logical-record reader,
an immutable raw-byte source snapshot, bounded delimiter/BOM inspection, and a
deterministic content-plus-format source binding. The reader handles quoted
multiline fields and escaped quotes without loading the source file into memory,
preserves exact decoded text, and keeps null, empty, missing, and trailing-empty
fields distinct. Strict decoding and explicit field, record, column, inspection,
and snapshot limits keep malformed or hostile inputs bounded.

This project owns its file-parser dependencies. It does not expose CsvHelper
types through its public API, and the provider-neutral migration project does
not reference CsvHelper.

Schema inference, tolerant rejects, migration data-source adaptation, prepared
target writes, durable resume, manifests, and export are later Phase 4 slices.

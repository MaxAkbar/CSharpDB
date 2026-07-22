# CSharpDB.Migration.Files

Streaming file-format adapters for `CSharpDB.Migration`.

The initial Phase 4A slice provides a forward-only CSV logical-record reader.
It handles quoted multiline fields and escaped quotes without loading the
source file into memory, preserves exact decoded text, and keeps null, empty,
missing, and trailing-empty fields distinct. Strict decoding and explicit
field, record, and column limits keep malformed or hostile inputs bounded.

This project owns its file-parser dependencies. It does not expose CsvHelper
types through its public API, and the provider-neutral migration project does
not reference CsvHelper.

Schema inference, tolerant rejects, prepared target writes, resume binding,
manifests, and export are later Phase 4 slices.

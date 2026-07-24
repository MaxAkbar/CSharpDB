# CSharpDB.Migration.LiteDb

This project is the read-only LiteDB 5.0.21 inspection and canonical BSON
encoding checkpoint for CSharpDB migration tooling.

The adapter opens a caller-supplied LiteDB file with `ReadOnly=true` and
`Upgrade=false`. It inventories every collection and index and, when profiling
is enabled, performs a full document scan that reports only field-presence and
BSON-type counts. Source values are never included in catalog diagnostics or
profile metadata.

LiteDB resolves collection names case-insensitively: requesting `People` and
`people` addresses one collection, so a file cannot expose two ordinally
distinct collection names in that collision group through the supported API.
The catalog records this name-comparison rule, and the inspector still rejects
any such collision if a future provider version exposes one.

Documents are represented by a versioned tagged JSON encoding. The encoding
preserves every LiteDB BSON type, including the distinction between `Int32` and
`Int64`, exact floating-point bits, exact decimal bits, binary data, object
identifiers, GUIDs, date/time ticks and kind, and minimum/maximum sentinels.
Document properties are ordered using ordinal comparison. The source `_id`
remains in the encoded document and is also encoded as a collision-proof typed
stable key.

This checkpoint intentionally does not provide backup packaging, streaming
apply/resume, validation, or CLI integration. Those release surfaces require a
retained immutable source package and are implemented in later Phase 6 work.

## Safety and bounds

Inspection and encoding enforce fixed limits for collection and index counts,
aggregate documents, field paths, nesting depth, fields per document, property
and string bytes, binary bytes, typed keys, path bytes, tagged JSON nodes and
container depth, and canonical output bytes. Exceeding a limit fails inspection
rather than returning partial metadata.

## Dependency

LiteDB 5.0.21 is used under the MIT License. See
`THIRD-PARTY-NOTICES.md`.

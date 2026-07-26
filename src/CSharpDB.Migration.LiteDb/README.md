# CSharpDB.Migration.LiteDb

This project provides the supported LiteDB 5 database-to-CSharpDB migration
route. It captures an offline/quiesced source into a retained `.csdblitedb`
snapshot, inventories collections and indexes, streams documents into CSharpDB
document collections, resumes from transactional target receipts, and
participates in schema, count, and checksum validation.

## Support boundary

- The source must be an unencrypted LiteDB 5 database. There is no password
  option and encrypted databases are rejected.
- Close every LiteDB writer before inspection and keep the source quiesced
  until the snapshot command completes. LiteDB has no SQLite-style coherent
  online backup API; capture holds a read handle that denies writers and
  deletion while it copies the file.
- Every LiteDB collection becomes a CSharpDB document collection with exactly
  two bridge values: `_key`, a collision-proof typed encoding of the LiteDB
  `_id`, and `_doc`, the complete tagged canonical BSON document. `_id` remains
  present inside `_doc`.
- Replay is deterministic in ascending built-in `_id` index order and supports
  fail-fast plans only. Deterministic rejects and reject artifacts are not
  supported for LiteDB.
- The built-in `_id` index is subsumed by the typed collection key. Other
  LiteDB indexes are inventoried but excluded from automatic target creation.
  Recreate required secondary or unique indexes manually after migration using
  reviewed CSharpDB semantics; BSON-path expressions, uniqueness, and LiteDB
  collation are not translated automatically.

LiteDB resolves collection names case-insensitively: requesting `People` and
`people` addresses one collection, so a file cannot expose two ordinally
distinct collection names in that collision group through the supported API.
The catalog records this comparison rule and fails closed if a future provider
exposes an ambiguous collision.

## Retained snapshot and workspace

`migrate inspect` writes a no-overwrite raw snapshot with the `.csdblitedb`
extension and a separately serialized catalog. The printed
`manifestDigest=sha256:...` is the snapshot content digest. Record that value in
trusted change control; apply, resume, and validation require it through
`--expected-manifest-digest`.

Apply and validation never stream from the original source or directly from
the retained package. Each command:

1. verifies the package against the trusted digest and `--max-source-bytes`;
2. copies it into a unique owner-private child of `--workspace`;
3. reconstructs the inspection catalog and requires its digest to match the
   supplied catalog;
4. reads only the verified private copy; and
5. removes that private copy when the command closes.

`--workspace` is optional and otherwise defaults to the system temporary
directory. For controlled operation, pass an existing caller-controlled local
directory that is not a link, junction, reparse point, or device. The workspace
is temporary isolation, not checkpoint authority; keep it stable during the
command, but do not rely on it for resume.

## CLI workflow

Create the workspace and checksum spill directories before invoking commands:

```powershell
New-Item -ItemType Directory -Force .\migration-work, .\migration-spill

csharpdb migrate inspect --source litedb --input .\source.db --package .\source.csdblitedb --out .\catalog.json --max-source-bytes 1073741824
# Record the printed manifestDigest in trusted change control.

csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json --ddl
csharpdb migrate preview .\plan.json --catalog .\catalog.json --scratch

csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\run.json

# Use this form after an interrupted or failed apply that left the staged
# target in place.
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\run.json --resume

csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\validation.json --level checksum --spill-dir .\migration-spill
```

Review the plan and preview before accepting exclusions. The example uses
`--accept-exclusions all` because namespaces and LiteDB index inventory are not
emitted as target objects; use an explicit comma-separated object-id list when
change control requires narrower approval.

Apply creates a new staged target and stops at `awaitingValidation`. It never
overwrites or activates an existing target. Rows and target receipts commit
together. If apply is interrupted, repeat the exact source package, digest,
catalog, plan, target, bounds, and workspace policy with `--resume`; only
batches with matching identities and digests are skipped. A changed package,
catalog, plan, snapshot identity, cursor policy, or target receipt fails closed.

Checksum validation reopens the same retained snapshot, replays the source, and
compares normalized schema, 64-bit counts, and partitioned canonical SHA-256
evidence. A passing report activates the staged target; failed, skipped, or
inconclusive validation withholds activation.

Inspection publishes the package before the catalog. If catalog publication
then fails, the `.csdblitedb` package is deliberately preserved for diagnosis;
the command does not overwrite or silently repair either artifact. A process
crash can also leave an owner-private temporary workspace child. After
confirming that no migration process is using it, inspect and remove that
orphan manually. The staged target and its transactional receipts—not a
workspace copy or run report—remain the resume authority.

## Canonical BSON and safety bounds

The tagged JSON encoding preserves every LiteDB BSON type, including the
distinction between `Int32` and `Int64`, exact floating-point and decimal bits,
binary data, object identifiers, GUIDs, date/time ticks and kind, and
minimum/maximum sentinels. Document properties use ordinal ordering. Catalog
profiles contain field-presence and BSON-type counts only; source scalar values
are never included in profile metadata or diagnostics.

Inspection, encoding, and replay enforce fixed limits for source bytes,
collection and index counts, aggregate documents, field paths, nesting depth,
fields per document, property and string bytes, binary bytes, typed keys,
tagged JSON nodes and depth, canonical values, buffered rows, and batch bytes.
Crossing a limit fails the operation rather than returning partial metadata or
partially accepting a batch.

## Dependency

LiteDB 5.0.21 is used under the MIT License. See
`THIRD-PARTY-NOTICES.md`.

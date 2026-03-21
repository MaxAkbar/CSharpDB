# Storage Inspector

The Storage Inspector is a read-only diagnostics toolkit for understanding the physical state of a CSharpDB database (`.db`) and WAL (`.db.wal`) file.

It is CLI-first and deterministic:

- no data mutation
- machine-readable JSON output (`schemaVersion: "1.0"`)
- fixed exit codes for automation

## Scope (v1)

Diagnostics cover:

- database file header and page map
- B+tree page and cell structure checks
- WAL header/frame/checksum checks
- catalog and index-root reachability checks

Out of scope:

- repair/auto-fix
- compaction/vacuum
- on-disk format changes

## Commands

Run commands through the CLI command mode:

```bash
csharpdb inspect <dbfile> [--json] [--out <file>] [--include-pages]
csharpdb inspect-page <dbfile> <pageId> [--json] [--hex]
csharpdb check-wal <dbfile> [--json]
csharpdb check-indexes <dbfile> [--index <name>] [--sample <n>] [--json]
```

`inspect` supports `--out` to write JSON to a file.

## Client/API/Admin

Client SDK methods (`CSharpDB.Client`):

- `InspectStorageAsync(path?, includePages?)`
- `CheckWalAsync(path?)`
- `InspectPageAsync(pageId, includeHex?, path?)`
- `CheckIndexesAsync(path?, indexName?, sampleSize?)`

REST endpoints (`CSharpDB.Api`):

- `GET /api/inspect`
- `GET /api/inspect/wal`
- `GET /api/inspect/page/{id}`
- `GET /api/inspect/indexes`

Admin UI (`CSharpDB.Admin`):

- Sidebar `Storage` action opens the **Storage** tab
- Displays header summary, page histogram, index checks, issue list, and page drill-down
- Also exposes maintenance actions plus backup/restore, with paths resolved on the connected host

## Exit Codes

- `0`: no warnings/errors
- `1`: warnings present, no errors
- `2`: one or more errors present
- `64`: invalid CLI usage/arguments

## JSON Contract

All inspector JSON responses include:

- `schemaVersion` (current: `"1.0"`)

The top-level report object depends on the command:

- `inspect` -> `DatabaseInspectReport`
- `inspect-page` -> `PageInspectReport`
- `check-wal` -> `WalInspectReport`
- `check-indexes` -> `IndexInspectReport`

`issues` entries use:

- `code` (stable rule identifier)
- `severity` (`Info`, `Warning`, `Error`)
- `message`
- optional `pageId`
- optional `offset`

## Integrity Rule IDs (v1)

Database/header/page checks:

- `DB_HEADER_SHORT`
- `DB_HEADER_BAD_MAGIC`
- `DB_HEADER_BAD_VERSION`
- `DB_HEADER_BAD_PAGE_SIZE`
- `DB_PAGE_COUNT_MISMATCH`
- `DB_FILE_TRAILING_BYTES`
- `DB_PAGE_SHORT_READ`
- `PAGE_HEADER_OUT_OF_RANGE`
- `PAGE_CELL_COUNT_OVERFLOW`
- `PAGE_CELL_CONTENT_START_OOB`
- `PAGE_CELL_CONTENT_OVERLAP`
- `PAGE_DUPLICATE_CELL_POINTER`
- `PAGE_CELL_POINTER_OOB`
- `CELL_VARINT_INVALID`
- `CELL_HEADER_INVALID`
- `CELL_TOTAL_SIZE_OOB`
- `LEAF_CELL_PAYLOAD_TOO_SMALL`
- `LEAF_CELL_KEY_OOB`
- `LEAF_CELL_PAYLOAD_OOB`
- `INTERIOR_CELL_PAYLOAD_TOO_SMALL`
- `INTERIOR_CELL_BYTES_OOB`
- `BTREE_LEAF_KEY_ORDER`
- `PAGE_TYPE_UNKNOWN`

B+tree reachability/cross checks:

- `SCHEMA_ROOT_MISSING`
- `BTREE_CHILD_OUT_OF_RANGE`
- `BTREE_PAGE_MISSING`
- `BTREE_PAGE_TYPE_INVALID`
- `BTREE_NULL_CHILD_REFERENCE`
- `CATALOG_ROOT_OUT_OF_RANGE`
- `CATALOG_ROOT_MISSING`
- `CATALOG_ROOT_BAD_PAGE_TYPE`
- `CATALOG_ENTRY_PAYLOAD_SHORT`
- `CATALOG_TABLE_SCHEMA_DECODE_FAILED`
- `CATALOG_INDEX_ENTRY_PAYLOAD_SHORT`
- `CATALOG_INDEX_SCHEMA_DECODE_FAILED`
- `CATALOG_VIEW_SCHEMA_DECODE_FAILED`
- `CATALOG_TRIGGER_SCHEMA_DECODE_FAILED`
- `INDEX_NOT_FOUND`
- `INDEX_ROOT_OUT_OF_RANGE`
- `INDEX_ROOT_MISSING`
- `INDEX_ROOT_BAD_PAGE_TYPE`
- `INDEX_TABLE_MISSING`
- `INDEX_COLUMN_MISSING`

WAL checks:

- `WAL_HEADER_SHORT`
- `WAL_HEADER_BAD_MAGIC`
- `WAL_HEADER_BAD_VERSION`
- `WAL_HEADER_BAD_PAGE_SIZE`
- `WAL_TRAILING_PARTIAL_FRAME`
- `WAL_FRAME_HEADER_SHORT`
- `WAL_FRAME_PAGE_SHORT`
- `WAL_FRAME_SALT_MISMATCH`
- `WAL_FRAME_HEADER_CHECKSUM_MISMATCH`
- `WAL_FRAME_DATA_CHECKSUM_MISMATCH`
- `WAL_NO_COMMIT_MARKER`

## Examples

Human-readable summary:

```bash
csharpdb inspect mydata.db
```

Machine-readable JSON:

```bash
csharpdb inspect mydata.db --json
```

Write JSON report to file:

```bash
csharpdb inspect mydata.db --json --out inspect-report.json
```

Inspect one page including hex dump:

```bash
csharpdb inspect-page mydata.db 12 --hex
```

Check WAL state:

```bash
csharpdb check-wal mydata.db
```

Check all indexes:

```bash
csharpdb check-indexes mydata.db
```

Check one index:

```bash
csharpdb check-indexes mydata.db --index idx_users_name --json
```

## Troubleshooting

- If `check-wal` reports no WAL file, that is usually normal after clean close/checkpoint.
- If DB checks report page-count mismatch with no errors, run `check-wal` to determine whether recent committed data still resides in WAL.
- If checks run while a writer is active, warnings can appear due to concurrent file changes; rerun diagnostics after write activity stops.

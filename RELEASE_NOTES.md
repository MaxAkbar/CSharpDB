# What's New

## v1.4.0 (2026-03-06)

### SQL Completeness and Schema Metadata
- Added first-class `IDENTITY` and `AUTOINCREMENT` column modifiers for `INTEGER` primary key columns.
- Identity metadata is now persisted and surfaced across SQL/system metadata and tooling.
- `sys.columns` now includes `is_identity` (0/1).
- REST API, CLI, and MCP schema/table introspection now expose identity flags.
- Backward compatibility preserved: legacy schemas without identity flags still surface `INTEGER PRIMARY KEY` columns as identity.

### Query and Insert Path Improvements
- Expanded integer ordered-index fast path for range predicates (`<`, `<=`, `>`, `>=`) and `BETWEEN`.
- `next rowid` high-water mark is now stored in table schema metadata and reused across sessions.
- Insert rowid allocation still accepts explicit values and advances the high-water mark when explicit values are higher.

### Storage Engine
- Implemented B+tree delete rebalancing with sibling borrow/merge for underflowed leaf pages.
- Added interior-child collapse handling after delete rebalancing to keep tree structure valid.

### Saved Queries and Admin UX
- Added internal saved query catalog table (`__saved_queries`) with unique query names and UTC audit timestamps.
- Added system virtual catalog projection for saved queries via `sys.saved_queries` (and `sys_saved_queries` alias).
- Added Admin Object Explorer grouping for `System (Virtual)` items including `sys.saved_queries`.
- Added Admin Query tab `Save`/`Load` UX for named SQL snippets with refreshable saved-query list.
- Saved query catalog remains hidden from regular user table listings and table endpoints.

### Validation and Test Coverage
- Added parser/tokenizer coverage for `IDENTITY`/`AUTOINCREMENT` syntax and validation rules.
- Added integration tests for identity insert behavior and persisted next-rowid continuity.
- Added schema serializer compatibility tests for legacy metadata payloads.
- Added B+tree delete rebalancing tests to validate merge/borrow behavior.
- Added service/system/API tests for saved-query catalog behavior and virtual catalog projection.

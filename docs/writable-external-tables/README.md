# Writable External Tables Plan

This document captures the planned design for making CSharpDB external tables
writable. The current external table feature is intentionally read-only and is
based on native `.csdbtable` table archives. Writable external tables should be
a separate opt-in capability backed by a mutable B+tree external-table file.

## Summary And Goals

The goal is to let users keep a table outside the main database file while
still using normal SQL `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements
against it.

The first implementation should:

- Keep existing `.csdbtable` archives read-only.
- Add a mutable `.csdbx` external-table file format.
- Require explicit writable registration.
- Support DML only: `INSERT`, `UPDATE`, and `DELETE`.
- Reuse existing CSharpDB storage primitives where practical.
- Keep query behavior consistent with current external table scans and joins.

## Current Read-Only Behavior

Today, external tables are registrations over native `.csdbtable` archives:

```sql
CREATE EXTERNAL TABLE archived_customers
FROM 'exports/customers.csdbtable';
```

The archive is a table snapshot. Query planning can resolve it as a table source
for normal `SELECT` statements, including filters, projections, joins, ordering,
and `COUNT(*)` metadata fast paths. Eligible integer primary-key point lookups
can use the embedded archive index.

The engine currently rejects mutating statements against external tables:

- `INSERT`
- `UPDATE`
- `DELETE`
- `ALTER TABLE`
- `CREATE INDEX`
- Trigger target usage

That behavior should remain the default for `.csdbtable` registrations.

## Target SQL Contract

Writable external tables must be opt-in at registration time:

```sql
CREATE EXTERNAL TABLE customers_archive
FROM 'exports/customers.csdbx'
WITH (WRITABLE = TRUE);
```

Existing syntax remains read-only:

```sql
CREATE EXTERNAL TABLE customers_archive
FROM 'exports/customers.csdbtable';
```

Registration rules:

- `WITH (WRITABLE = TRUE)` requires a mutable `.csdbx` file.
- `.csdbtable` archives cannot be made writable in place.
- If a user wants to write to an archive, Admin or a service API must convert it
  to `.csdbx` first.
- `sys.external_tables` should expose whether the registration is writable and
  which storage format backs it.

Recommended metadata additions:

| Column | Meaning |
| --- | --- |
| `is_writable` | `1` for writable external table registrations, otherwise `0`. |
| `storage_format` | `archive-native-v3` for `.csdbtable`, `mutable-btree-v1` for `.csdbx`. |
| `row_count` | Current row count for writable stores and manifest row count for archives. |

## Mutable `.csdbx` Storage Model

The `.csdbx` file should be a single-table mutable external store. It should not
be a full attached database in v1.

Use existing storage primitives:

- `Pager` for page management and WAL-backed durability.
- `SchemaCatalog` for table schema, row count, and `NextRowId`.
- `BTree` for the external table row store.
- Existing record serialization for row payloads.

The external file should contain:

- A file header identifying `CSDBEXT1`.
- One table schema.
- One table row B+tree.
- Persisted row count.
- Persisted `NextRowId`.
- Enough metadata to reopen the file without consulting the main database.

The main database stores only the external table registration metadata. The
external file owns its table rows and table metadata.

## DML Behavior

Writable external tables should participate in existing SQL DML paths as closely
as possible while targeting the external store instead of the main database
catalog.

### INSERT

`INSERT` should:

- Resolve column lists and values using the external table schema.
- Enforce nullability and integer primary-key identity behavior.
- Allocate `NextRowId` from the external file metadata.
- Insert into the external table B+tree.
- Update the external row count.

### UPDATE

`UPDATE` should:

- Scan or seek the external table rows using existing predicate evaluation where
  practical.
- Materialize rows to update before mutating the B+tree.
- Preserve primary-key uniqueness when the key changes.
- Update matching rows in the external B+tree.
- Keep external row count unchanged.

### DELETE

`DELETE` should:

- Scan or seek matching external rows.
- Delete matching row IDs from the external B+tree.
- Decrement the external row count.

## Non-Goals For V1

The first writable external table implementation should not include:

- Making `.csdbtable` archives writable.
- `ALTER TABLE` support for writable external tables.
- `CREATE INDEX` or `DROP INDEX` on writable external tables.
- Trigger targets on writable external tables.
- Foreign-key enforcement across the main database and external files.
- Cross-store explicit transactions.
- Multi-table external files.
- Treating `.csdbx` as a full attached database.

These restrictions keep the first version focused on the core writable table
contract and avoid introducing a distributed transaction problem.

## Admin Workflow

Admin should expose writable external tables through the existing Import / Export
surface.

Recommended workflow:

1. User opens Import / Export.
2. User selects Register External Table.
3. User chooses an archive or mutable external file path.
4. User enables `Writable external table`.
5. If the path is `.csdbtable`, Admin asks for a `.csdbx` conversion path.
6. Admin converts the archive to `.csdbx` using progress and cancellation.
7. Admin registers the `.csdbx` file with `WITH (WRITABLE = TRUE)`.

Object Explorer should show writable external tables distinctly from read-only
archives. Data-grid editing should be enabled only for writable registrations.

## Phased Implementation Plan

### Phase 1 - Storage Foundation

- Add the `.csdbx` file header and open/create APIs.
- Create a single-table external store wrapper over `Pager`, `SchemaCatalog`,
  `BTree`, and existing row serialization.
- Implement archive-to-`.csdbx` conversion.
- Add tests for create, reopen, row count, schema, nulls, blobs, identity state,
  and primary-key uniqueness.

### Phase 2 - SQL Registration And Metadata

- Extend parser support for `WITH (WRITABLE = TRUE)`.
- Extend external table metadata with `is_writable` and `storage_format`.
- Validate that writable registrations point at `.csdbx` files.
- Keep existing `.csdbtable` registrations read-only by default.
- Update `sys.external_tables`.

### Phase 3 - Query Integration

- Add a B+tree-backed external table scan operator.
- Resolve writable external tables to the new operator.
- Preserve existing filters, joins, projections, ordering, and aggregates.
- Keep current archive scan and primary-key lookup operators unchanged for
  `.csdbtable`.

### Phase 4 - DML Integration

- Route writable external table `INSERT`, `UPDATE`, and `DELETE` to an external
  mutation executor.
- Reuse existing row resolution and expression evaluation rules.
- Update external row count and `NextRowId` inside the `.csdbx` file.
- Reject unsupported DDL and explicit cross-store transaction usage with clear
  errors.

### Phase 5 - Admin Integration

- Add writable registration controls.
- Add archive-to-`.csdbx` conversion progress and cancellation.
- Show writable/read-only state in registered external table lists.
- Enable data-grid editing only for writable external tables.

## Test Plan

Parser tests:

- Parse writable external table registration.
- Reject malformed `WITH` options.
- Confirm current read-only syntax still parses.

Storage tests:

- Create and reopen `.csdbx`.
- Convert `.csdbtable` to `.csdbx`.
- Preserve schema, null values, integers, reals, text escaping, blobs, row count,
  and identity state.
- Reject duplicate primary keys.

Engine tests:

- Register writable external table and select rows.
- Insert into writable external table.
- Update writable external table rows.
- Delete writable external table rows.
- Join physical tables to writable external tables after mutations.
- Reject writes to read-only `.csdbtable` external tables.
- Reject `ALTER`, `CREATE INDEX`, and trigger target usage against writable
  external tables.
- Reject writable external table DML inside explicit cross-store transactions.
- Verify `sys.external_tables` exposes writable state and storage format.

Admin tests:

- Register read-only archive unchanged.
- Convert archive to writable external file.
- Register writable external table.
- Show progress and support cancellation during conversion.
- Show writable state in Object Explorer and Import / Export registration lists.

## Acceptance Criteria

- Existing read-only external table behavior is unchanged.
- Writable external tables require explicit opt-in.
- `.csdbtable` files are never mutated in place.
- `.csdbx` files can be reopened and queried after process restart.
- `INSERT`, `UPDATE`, and `DELETE` persist to the external file.
- Unsupported DDL fails with clear errors.
- Admin can convert and register writable external tables without blocking the UI.

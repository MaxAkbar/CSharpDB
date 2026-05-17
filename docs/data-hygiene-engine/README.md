# Data Hygiene Engine Plan

This document captures the planned design for a SQL-first Data Hygiene Engine in
CSharpDB. The goal is to make cleanup, validation, and relationship auditing
first-class database workflows instead of one-off scripts around the database.

## Summary And Goals

The Data Hygiene Engine should make CSharpDB feel like a smart database for
real-world data. It focuses on common operational problems that show up in
customer lists, booking systems, imports, and long-lived business databases:

- Detect duplicate records before changing data.
- Deduplicate tables transactionally with deterministic survivor selection.
- Merge duplicate records conservatively.
- Store database-owned validation rules as SQL expressions.
- Find orphaned child rows from declared or explicit relationships.

V1 is SQL-first. Admin UI, richer reporting, and pipeline integration should
build on the same engine behavior after the core commands are stable.

## Current Related Capabilities

CSharpDB already has pieces that this plan should build around:

- `CSharpDB.Pipelines` includes a package-level `Deduplicate` transform for ETL
  flows.
- SQL foreign keys and `sys.foreign_keys` metadata provide declared
  parent/child relationships.
- Trusted validation callbacks exist for Admin Forms and host-owned business
  logic.

The Data Hygiene Engine is different because it is database-level SQL. Its
rules and commands should be portable with the database metadata, queryable
through system catalog surfaces, and usable without host-owned C# callbacks.

## V1 SQL Contract

Duplicate detection is a read-only preview:

```sql
FIND DUPLICATES IN Customers ON Email;
```

Deduplication removes duplicate rows transactionally:

```sql
DEDUP Customers ON Email KEEP FIRST;
```

Duplicate merge keeps one winner row, fills null fields from duplicate rows
where possible, and then removes the duplicate rows:

```sql
MERGE DUPLICATES Customers ON Email;
```

Database-owned validation rules are SQL expressions:

```sql
CREATE VALIDATION RULE ValidEmail
ON Customers.Email
AS Email LIKE '%@%'
MESSAGE 'Email must contain @';
```

Rules are audit-only in v1. They are evaluated explicitly with `VALIDATE TABLE`
and do not block `INSERT` or `UPDATE` yet:

```sql
VALIDATE TABLE Customers;
```

Orphan detection uses declared foreign keys by default:

```sql
FIND ORPHANS IN Bookings;
```

When no foreign key exists, callers can provide an explicit relationship:

```sql
FIND ORPHANS IN Bookings.BookId REFERENCES Books.Id;
```

Default semantics:

- `KEEP FIRST` keeps the row with the lowest primary key when the table has one,
  otherwise the lowest rowid.
- `KEEP LAST` keeps the row with the highest primary key when the table has one,
  otherwise the highest rowid.
- `FIND` commands are read-only previews.
- `DEDUP` and `MERGE DUPLICATES` run through normal write transaction behavior
  and return affected-row and affected-group summaries.
- `MERGE DUPLICATES` does not overwrite non-null winner values in v1.
- Validation rules are evaluated through `VALIDATE TABLE`; write enforcement is
  future work.
- Orphan detection uses declared foreign keys first and explicit references when
  supplied.

## Metadata Model

Validation rules should be stored in an internal metadata table rather than in a
new storage catalog sentinel. The metadata should be sufficient to rebuild and
evaluate the rule expression after reopening the database.

Recommended metadata:

| Column | Meaning |
| --- | --- |
| `rule_name` | Case-insensitive validation rule identifier. |
| `table_name` | Target table. |
| `column_name` | Optional target column; null for row-level rules. |
| `expression_sql` | SQL expression that must evaluate truthy for a valid row. |
| `message` | User-facing validation failure message. |
| `created_at` | Creation timestamp. |
| `is_enabled` | Whether `VALIDATE TABLE` should evaluate the rule. |

Expose rules through `sys.validation_rules`. Hygiene commands should return
normal query results, so Admin, CLI, HTTP, gRPC, and ADO.NET surfaces can consume
the same output shape without a feature-specific transport.

## Execution Model

Duplicate detection groups rows by the requested key expressions. The result
should report enough information for users to preview the cleanup: duplicate
key values, duplicate group size, winner primary key or rowid, and duplicate
primary keys or rowids that would be removed.

Deduplication uses the same grouping and winner selection as duplicate
detection, then deletes non-winner rows in one transaction. The command should
return a summary with duplicate groups found, rows deleted, and rows kept.

Merge uses the same winner selection, then fills null winner columns from
duplicate rows when a non-null duplicate value is available. If multiple
duplicates provide different non-null values for the same empty winner column,
v1 should preserve the winner null and report the conflict rather than guessing.
After applying safe fill-null updates, it deletes the duplicate rows in the same
transaction.

Validation rules reuse the SQL expression evaluator against each row scope. A
rule passes when the expression evaluates truthy. `FALSE` or `NULL` should be
reported as a validation violation with the configured message.

Orphan detection builds anti-join style checks. With declared foreign keys, it
uses the FK metadata for the child and parent columns. With explicit references,
it validates the named child table/column and parent table/column before running
the check.

## Non-Goals

- No fuzzy matching in v1.
- No automatic write enforcement for validation rules in v1.
- No cross-table merge policies in v1.
- No Admin visual workspace in v1.
- No trusted C# validation callbacks as the primary implementation.
- No automatic repair of orphaned rows in v1.

## Phased Implementation Plan

Phase 1: read-only hygiene inspection.

- Add parser and AST support for `FIND DUPLICATES`, `FIND ORPHANS`, and
  `VALIDATE TABLE`.
- Add execution paths that return normal `QueryResult` objects.
- Add validation-rule metadata storage and `sys.validation_rules`.

Phase 2: transactional deduplication.

- Add parser and execution support for `DEDUP ... KEEP FIRST|LAST`.
- Reuse the duplicate detection grouping path for deterministic winner
  selection.
- Delete duplicate rows in one write transaction and return a cleanup summary.

Phase 3: conservative duplicate merge.

- Add `MERGE DUPLICATES` execution over the duplicate grouping path.
- Fill null winner columns from duplicate rows where there is exactly one safe
  non-null candidate value.
- Report unresolved merge conflicts without overwriting existing winner values.

Phase 4: product surfaces.

- Add an Admin hygiene workspace that previews duplicates, validation
  violations, and orphans before applying mutations.
- Add pipeline integration that can call the same engine behaviors from ETL
  packages.
- Add docs and examples for common import-cleanup workflows.

## Future Test Plan

- Parser tests for each new command and invalid syntax.
- Duplicate detection with nulls, case/collation behavior, composite keys, and
  empty tables.
- Dedup winner selection by primary key and rowid.
- Merge fill-null behavior and conflict preservation.
- Validation rule creation, listing through `sys.validation_rules`, and
  `VALIDATE TABLE` violations.
- Orphan detection from foreign-key metadata and explicit references.
- Transaction rollback for failed dedup and merge operations.

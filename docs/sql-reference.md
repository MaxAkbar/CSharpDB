# SQL Reference

Complete reference for the SQL dialect supported by CSharpDB.

---

## Data Types

| Type | Aliases | Description |
|------|---------|-------------|
| `INTEGER` | `INT` | 64-bit signed integer |
| `REAL` | `FLOAT`, `DOUBLE` | 64-bit IEEE 754 floating point |
| `TEXT` | `VARCHAR` | UTF-8 Unicode string |
| `BLOB` | — | Raw binary data |
| `NULL` | — | Explicit NULL value (any column unless constrained `NOT NULL`) |

CSharpDB uses a flexible type system. Arithmetic operators perform implicit coercion
between numeric types where needed.

---

## Statements

### CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name type [PRIMARY KEY] [IDENTITY | AUTOINCREMENT] [NOT NULL]
                     [COLLATE collation_name]
                     [REFERENCES other_table(column) [ON DELETE CASCADE | RESTRICT]],
    ...
);
```

**Constraints:**

| Constraint | Scope | Description |
|------------|-------|-------------|
| `PRIMARY KEY` | Column | Designates the row identity column (integer) |
| `IDENTITY` / `AUTOINCREMENT` | Column | Auto-incrementing integer primary key |
| `NOT NULL` | Column | Rejects NULL values on insert/update |
| `COLLATE` | Column | Sets collation for TEXT comparisons (see [Collations](#collations)) |
| `REFERENCES` | Column | Foreign key referencing another table's column |
| `ON DELETE CASCADE` | Foreign key | Deletes child rows when parent is deleted |
| `ON DELETE RESTRICT` | Foreign key | Prevents deletion of parent row while children exist |

### Temporary Tables

```sql
CREATE TEMP TABLE [IF NOT EXISTS] temp_name (
    column_name type [PRIMARY KEY] [IDENTITY | AUTOINCREMENT] [NOT NULL]
                     [COLLATE collation_name],
    ...
);

CREATE TEMPORARY TABLE [IF NOT EXISTS] temp_name (...);
DROP TEMP TABLE [IF EXISTS] temp_name;
PERSIST TEMP TABLE temp_name AS durable_name;
```

Temporary tables are session-scoped and backed by in-memory storage. Unqualified
table names resolve to temporary tables first, then durable tables/views/external
tables, so a temporary table can shadow a durable table for the current session.
`DROP TABLE name` drops the temporary table first when such a shadow exists.

`SELECT`, `INSERT`, `UPDATE`, `DELETE`, and joins work against temporary tables
through the normal SQL execution path. V1 supports columns, nullability,
collation, integer primary key/identity behavior, and rowid fallback. V1 rejects
temporary foreign keys, triggers, secondary indexes, external tables, validation
rules, full-text indexes, `ALTER TABLE`, `ANALYZE`, and data hygiene operations.

Temporary tables do not appear in `sys.tables`, `sys.objects`, backups,
checkpoints, or `SaveToFileAsync`. Current-session metadata is exposed through
`sys.temp_tables` / `sys_temp_tables` and `sys.temp_columns` /
`sys_temp_columns`.

`PERSIST TEMP TABLE temp_name AS durable_name` explicitly creates a new durable
table using the temporary table schema and copies current rows through the normal
durable mutation path. The durable target must not already exist. The command
returns `temp_table`, `target_table`, and `rows_persisted`.

For embedded and ADO.NET connections, temporary tables live for the connection or
database handle lifetime and are cleared when the session is disposed. Stateless
HTTP/gRPC `ExecuteSqlAsync` rejects temporary table commands; use
`BeginTransaction` plus `ExecuteInTransaction` for remote temporary workflows.

### ALTER TABLE

```sql
ALTER TABLE table_name ADD COLUMN column_name type [constraints];
ALTER TABLE table_name DROP COLUMN column_name;
ALTER TABLE table_name DROP CONSTRAINT constraint_name;
ALTER TABLE table_name RENAME TO new_name;
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;
```

### DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name;
```

### CREATE INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name
ON table_name (column1 [, column2, ...]);
```

### DROP INDEX

```sql
DROP INDEX [IF EXISTS] index_name;
```

### CREATE VIEW

```sql
CREATE VIEW [IF NOT EXISTS] view_name AS select_statement;
```

### DROP VIEW

```sql
DROP VIEW [IF EXISTS] view_name;
```

### CREATE TRIGGER

```sql
CREATE TRIGGER [IF NOT EXISTS] trigger_name
{BEFORE | AFTER} {INSERT | UPDATE | DELETE}
ON table_name
[FOR EACH ROW]
[WHEN condition]
BEGIN
    statement1;
    [statement2;]
    ...
END;
```

Triggers can reference `NEW` and `OLD` row aliases in their body and `WHEN` condition:

- `INSERT` triggers: `NEW` is available
- `DELETE` triggers: `OLD` is available
- `UPDATE` triggers: both `NEW` and `OLD` are available

### DROP TRIGGER

```sql
DROP TRIGGER [IF EXISTS] trigger_name;
```

### ANALYZE

```sql
ANALYZE table_name;
```

Collects per-column statistics (distinct count, min/max, frequency histograms, quantile
buckets) and index prefix statistics used by the query planner for cardinality estimation
and operator selection. See [Query Execution Pipeline](query-execution-pipeline.md) for
details on how statistics influence planning.

---

## Data Manipulation

### INSERT

```sql
INSERT INTO table_name [(column1, column2, ...)]
VALUES (value1, value2, ...);
```

Column list is optional when providing values for all columns in declaration order.

### UPDATE

```sql
UPDATE table_name
SET column1 = expression1 [, column2 = expression2, ...]
[WHERE condition];
```

### DELETE

```sql
DELETE FROM table_name
[WHERE condition];
```

### SELECT

```sql
SELECT [DISTINCT] column_list
FROM table_reference
[JOIN ...]
[WHERE condition]
[GROUP BY column1 [, column2, ...]]
[HAVING condition]
[ORDER BY column1 [ASC | DESC] [, ...]]
[LIMIT count]
[OFFSET skip];
```

#### Column List

```sql
SELECT *                              -- all columns
SELECT column_name                    -- single column
SELECT column_name AS alias           -- aliased column
SELECT table.column_name              -- qualified column
SELECT expression                     -- computed value
SELECT aggregate_function(...)        -- aggregate
```

#### FROM and JOIN

```sql
FROM table_name [AS alias]

-- Join types
INNER JOIN table_name ON condition
LEFT  JOIN table_name ON condition
RIGHT JOIN table_name ON condition
CROSS JOIN table_name
```

All join types except `CROSS JOIN` require an `ON` condition.

#### Subqueries

```sql
-- Scalar subquery (must return a single value)
SELECT (SELECT MAX(age) FROM users) AS max_age;

-- IN subquery
WHERE column IN (SELECT id FROM other_table)
WHERE column NOT IN (SELECT id FROM other_table)

-- EXISTS subquery
WHERE EXISTS (SELECT 1 FROM other_table WHERE condition)
```

---

## Data Hygiene

CSharpDB includes SQL-first data hygiene commands for duplicate cleanup,
audit-only validation rules, and relationship auditing. These commands return
normal query-shaped results through `ExecuteSqlAsync`, ADO.NET, Admin query tabs,
HTTP, gRPC, and the CLI.

### FIND DUPLICATES

```sql
FIND DUPLICATES IN table_name ON expression [, expression ...];
```

Scans the target table, evaluates the `ON` expressions for each row, and returns
one row per duplicate group. Text keys use existing column or expression
collation behavior, so `COLLATE NOCASE` can be used directly in the key list.

Result columns:

| Column | Description |
|--------|-------------|
| `key_values` | Display text for the evaluated duplicate key values |
| `group_size` | Number of rows in the duplicate group |
| `winner_rowid` | Deterministic survivor rowid using `KEEP FIRST` semantics |
| `winner_primary_key` | Survivor primary-key value, or `NULL` when no primary key exists |
| `duplicate_rowids` | Comma-separated rowids that are not the survivor |
| `duplicate_primary_keys` | Comma-separated duplicate primary-key values, or `NULL` when no primary key exists |

```sql
FIND DUPLICATES IN Customers ON Email COLLATE NOCASE;
FIND DUPLICATES IN Contacts ON FirstName, LastName, Phone;
```

### DEDUP

```sql
DEDUP table_name ON expression [, expression ...] KEEP FIRST | LAST;
```

Deletes non-winner rows for each duplicate group. `KEEP FIRST` keeps the lowest
primary-key value when the table has a primary key, otherwise the lowest rowid.
`KEEP LAST` keeps the highest primary-key value, otherwise the highest rowid.
Deletes run through the normal table mutation path, including indexes, foreign
keys, triggers, WAL, and transaction rollback.

Result columns: `table_name`, `duplicate_group_count`, `rows_deleted`,
`rows_kept`.

```sql
DEDUP Customers ON Email COLLATE NOCASE KEEP FIRST;
```

### MERGE DUPLICATES

```sql
MERGE DUPLICATES table_name ON expression [, expression ...];
```

Selects the same deterministic winner as `KEEP FIRST`, fills only `NULL` winner
columns when exactly one non-null duplicate value is available, reports merge
conflicts when multiple different values are found, and then deletes the
duplicate rows through the normal mutation path.

Result columns: `table_name`, `duplicate_group_count`, `rows_updated`,
`rows_deleted`, `merge_conflict_count`, `merge_conflicts`.

```sql
MERGE DUPLICATES Customers ON Email COLLATE NOCASE;
```

### CREATE VALIDATION RULE

```sql
CREATE VALIDATION RULE rule_name
ON table_name[.column_name]
AS expression
MESSAGE 'message text';
```

Validation rules are stored as database metadata and evaluated only when
`VALIDATE TABLE` is executed. V1 rules are audit-only; they do not block
`INSERT` or `UPDATE`.

Rules are stored in the hidden internal table `__validation_rules`. That table
is hidden from normal table/object listings and exposed through
`sys.validation_rules` and `sys_validation_rules`.

```sql
CREATE VALIDATION RULE ValidEmail
ON Customers.Email
AS Email LIKE '%@%'
MESSAGE 'Email must contain @';

SELECT rule_name, table_name, column_name, expression_sql, message
FROM sys.validation_rules;
```

### VALIDATE TABLE

```sql
VALIDATE TABLE table_name;
```

Evaluates enabled validation rules for the table and returns one row per
violation. A rule fails when the expression returns false or `NULL`.

Result columns: `rule_name`, `table_name`, `column_name`, `rowid`,
`primary_key`, `message`.

### FIND ORPHANS

```sql
FIND ORPHANS IN child_table;
FIND ORPHANS IN child_table.child_column REFERENCES parent_table.parent_column;
```

Without an explicit `REFERENCES` clause, CSharpDB uses declared foreign-key
metadata for the child table. With explicit references, both tables and columns
are validated before running the check. `NULL` child values are ignored.

Result columns: `constraint_name`, `child_table`, `child_column`, `child_rowid`,
`child_value`, `parent_table`, `parent_column`.

```sql
FIND ORPHANS IN Bookings;
FIND ORPHANS IN Bookings.BookId REFERENCES Books.Id;
```

Performance is proportional to the requested hygiene work: duplicate detection
scans the target table and groups keys in memory; validation is table rows times
enabled rules; orphan detection uses parent index lookups when available or a
parent value set built from one scan.

---

## Common Table Expressions (CTEs)

```sql
WITH cte_name [(column1, column2, ...)] AS (
    select_statement
)
[, another_cte AS (...)]
SELECT ... FROM cte_name ...;
```

Multiple CTEs can be chained with commas. Optional column name lists rename the CTE's
output columns.

> **Note:** The `RECURSIVE` keyword is parsed but recursive CTE execution is not yet
> implemented.

---

## Set Operations

```sql
select_statement UNION     select_statement
select_statement INTERSECT select_statement
select_statement EXCEPT    select_statement
```

Compound queries support trailing `ORDER BY`, `LIMIT`, and `OFFSET` applied to the
combined result.

---

## Expressions and Operators

### Arithmetic

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction (also unary negation) |
| `*` | Multiplication |
| `/` | Division (error on division by zero) |

### Comparison

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `<>` or `!=` | Not equal |
| `<` | Less than |
| `>` | Greater than |
| `<=` | Less than or equal |
| `>=` | Greater than or equal |

### Logical

| Operator | Description |
|----------|-------------|
| `AND` | Logical conjunction |
| `OR` | Logical disjunction |
| `NOT` | Logical negation |

### Special Expressions

| Expression | Example |
|------------|---------|
| `BETWEEN ... AND ...` | `WHERE age BETWEEN 18 AND 65` |
| `IN (...)` | `WHERE status IN ('active', 'pending')` |
| `NOT IN (...)` | `WHERE id NOT IN (1, 2, 3)` |
| `LIKE` | `WHERE name LIKE 'J%'` |
| `LIKE ... ESCAPE` | `WHERE code LIKE '100\%%' ESCAPE '\'` |
| `IS NULL` | `WHERE email IS NULL` |
| `IS NOT NULL` | `WHERE email IS NOT NULL` |

**LIKE wildcards:**

| Wildcard | Matches |
|----------|---------|
| `%` | Zero or more characters |
| `_` | Exactly one character |

---

## Functions

### Aggregate Functions

Used with or without `GROUP BY`. All except `COUNT(*)` ignore NULL values.

| Function | Description | Supports DISTINCT |
|----------|-------------|:-:|
| `COUNT(*)` | Number of rows | — |
| `COUNT(expr)` | Number of non-NULL values | Yes |
| `SUM(expr)` | Sum of numeric values | Yes |
| `AVG(expr)` | Average of numeric values | Yes |
| `MIN(expr)` | Minimum value | — |
| `MAX(expr)` | Maximum value | — |

```sql
SELECT COUNT(DISTINCT status), AVG(age) FROM users;
```

### Scalar Functions

| Function | Arguments | Returns | Description |
|----------|-----------|---------|-------------|
| `TEXT(expr)` | 1 | TEXT | Converts any value to its text representation |

Host applications can also register trusted in-process C# scalar functions and call
them from SQL expression positions such as `SELECT`, `WHERE`, `ORDER BY`,
`INSERT`, `UPDATE`, trigger bodies, and SQL procedure bodies. See
[Trusted C# Scalar Functions](trusted-csharp-functions/README.md).

---

## Parameters

Named parameters are supported in value positions using the `@` prefix:

```sql
SELECT * FROM users WHERE name = @name AND age > @minAge;
INSERT INTO users (name, age) VALUES (@name, @age);
UPDATE users SET name = @name WHERE id = @id;
DELETE FROM users WHERE id = @id;
```

Parameters cannot be used in identifier positions (table names, column names).

---

## Collations

Collations control how TEXT values are compared and sorted. They can be specified at the
column level in `CREATE TABLE` or at the expression level using the `COLLATE` operator.

| Collation | Description |
|-----------|-------------|
| `BINARY` | Byte-for-byte comparison (default) |
| `NOCASE` | Case-insensitive comparison |
| `NOCASE_AI` | Case-insensitive and accent-insensitive comparison |
| `ICU:<locale>` | Unicode ICU-based comparison with locale support |

```sql
-- Column-level collation
CREATE TABLE products (
    name TEXT COLLATE NOCASE
);

-- Expression-level collation
SELECT * FROM products ORDER BY name COLLATE NOCASE_AI;
```

---

## Limitations

The following SQL features are **not currently supported**:

- `CASE` / `WHEN` expressions
- `CAST` expressions (implicit coercion only)
- `DEFAULT` column values
- `CHECK` constraints
- `RETURNING` clause on INSERT/UPDATE/DELETE
- `UPSERT` / `ON CONFLICT` / `INSERT OR REPLACE`
- Recursive CTE execution (`WITH RECURSIVE` is parsed but not evaluated)
- String functions (`UPPER`, `LOWER`, `LENGTH`, `SUBSTR`, `TRIM`)
- Date/time functions (`DATE`, `TIME`, `DATETIME`, `STRFTIME`)
- Math functions (`ABS`, `ROUND`, `CEIL`, `FLOOR`)
- Window functions (`OVER`, `PARTITION BY`, `ROW_NUMBER`, etc.)
- Stored procedures
- Composite primary keys / composite foreign keys

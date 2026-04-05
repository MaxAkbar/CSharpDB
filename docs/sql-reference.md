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

namespace CSharpDB.Mcp.Helpers;

/// <summary>
/// Compact SQL syntax reference for CSharpDB, designed for AI model consumption.
/// Called by the GetSqlReference MCP tool so models can self-correct syntax errors.
/// </summary>
internal static class SqlReference
{
    public const string Text = """
        ═══ CSharpDB SQL Reference ═══

        ── DATA TYPES ──
        INTEGER  (aliases: INT)           — 64-bit signed integer
        REAL     (aliases: FLOAT, DOUBLE) — 64-bit IEEE 754 float
        TEXT     (aliases: VARCHAR)        — UTF-8 string (quote with single quotes, escape: '')
        BLOB                              — binary byte array

        ── CONSTRAINTS (column-level only) ──
        PRIMARY KEY   — one per table, auto-generates rowid if omitted
        IDENTITY      — INTEGER PRIMARY KEY identity marker (explicit inserts still allowed)
        AUTOINCREMENT — synonym for IDENTITY
        NOT NULL      — disallow NULL values

        ── CREATE TABLE ──
        CREATE TABLE [IF NOT EXISTS] name (
          col1 TYPE [PRIMARY KEY] [IDENTITY|AUTOINCREMENT] [NOT NULL],
          col2 TYPE [NOT NULL],
          ...
        )

        ── INSERT ──
        INSERT INTO table [(col1, col2, ...)] VALUES
          (val1, val2, ...),
          (val1, val2, ...)

        ── SELECT ──
        [WITH cte AS (SELECT ...) [, cte2 AS (SELECT ...)]]
        SELECT [col | expr [AS alias] | * ] , ...
        FROM table [alias]
          [INNER JOIN | LEFT [OUTER] JOIN | RIGHT [OUTER] JOIN | CROSS JOIN] table2 ON cond
        [WHERE expr]
        [GROUP BY expr, ...]
        [HAVING expr]
        [ORDER BY expr [ASC|DESC], ...]
        [LIMIT n]
        [OFFSET n]

        ── UPDATE ──
        UPDATE table SET col1 = expr, col2 = expr, ... [WHERE expr]

        ── DELETE ──
        DELETE FROM table [WHERE expr]

        ── DROP TABLE ──
        DROP TABLE [IF EXISTS] name

        ── ALTER TABLE ──
        ALTER TABLE name ADD [COLUMN] col TYPE [NOT NULL]
        ALTER TABLE name DROP [COLUMN] col
        ALTER TABLE name RENAME TO new_name
        ALTER TABLE name RENAME [COLUMN] old TO new

        ── CREATE INDEX ──
        CREATE [UNIQUE] INDEX [IF NOT EXISTS] idx ON table (col [, col, ...])

        ── DROP INDEX ──
        DROP INDEX [IF EXISTS] idx

        ── CREATE VIEW ──
        CREATE VIEW [IF NOT EXISTS] name AS SELECT ...

        ── DROP VIEW ──
        DROP VIEW [IF EXISTS] name

        ── CREATE TRIGGER ──
        CREATE TRIGGER [IF NOT EXISTS] name
          BEFORE|AFTER INSERT|UPDATE|DELETE ON table
          [FOR EACH ROW] [WHEN (condition)]
        BEGIN
          statement; ...
        END

        Use NEW.col for inserted/updated values, OLD.col for previous/deleted values.

        ── DROP TRIGGER ──
        DROP TRIGGER [IF EXISTS] name

        ── TRANSACTIONS ──
        BEGIN
        COMMIT
        ROLLBACK

        ── PLAN INSPECTION ──
        EXPLAIN [FOR] SELECT|INSERT|UPDATE|DELETE ...
          — returns the selected physical operator tree without opening it
        EXPLAIN ANALYZE [FOR] SELECT|INSERT|UPDATE|DELETE ...
          — executes exactly once and adds actual rows, loops, and elapsed time
          — profiled DML mutates under normal transaction semantics
        EXPLAIN ESTIMATE FOR SELECT ...
          — legacy bounded cardinality-estimate diagnostic
        estimated_rows is nullable when no planner estimate is available.
        estimated_cost uses stable relative row-work units, not elapsed time.
        Physical output is a stable rowset bounded to 500 rows and a 256 KiB
        inline content budget; that budget is not exact serialized transport size.
        Predicate literal and parameter values are redacted. Plain EXPLAIN rejects
        WITH, subquery, view, and duplicate-eliminating compound shapes whose current
        planning paths would perform eager work; use ANALYZE only when execution is intended.
        Cancellation and execution errors remain failures. Where safe, ANALYZE attaches
        a bounded, redacted partial-profile summary to the failure diagnostics.

        ── OPERATORS ──
        Comparison:  =  <>  <  >  <=  >=
        Logical:     AND  OR  NOT
        Arithmetic:  +  -  *  /
        Pattern:     LIKE (% = any chars, _ = one char) [ESCAPE 'c']
                     NOT LIKE
        Membership:  IN (val, val, ...)    NOT IN (val, val, ...)
        Range:       BETWEEN low AND high  NOT BETWEEN low AND high
        Null check:  IS NULL               IS NOT NULL
        Parameters:  @param_name

        ── AGGREGATE FUNCTIONS ──
        COUNT(*)              — count all rows
        COUNT(col)            — count non-null values
        COUNT(DISTINCT col)   — count distinct non-null values
        SUM(col)              — sum (supports DISTINCT)
        AVG(col)              — average (supports DISTINCT)
        MIN(col)              — minimum value
        MAX(col)              — maximum value

        ── WINDOW FUNCTIONS ──
        ROW_NUMBER(), RANK(), DENSE_RANK()
        COUNT, SUM, AVG, MIN, MAX with OVER (...)
        LAG(value[, offset[, default]]), LEAD(value[, offset[, default]])
        FIRST_VALUE(value), LAST_VALUE(value)
        Explicit ROWS frames support nonnegative integer-literal offsets.
        Named WINDOW name AS (...) definitions are reused with OVER name.
        Windows with identical PARTITION BY / ORDER BY can use different frames.
        Ordered defaults are peer-aware; without ORDER BY the whole partition is used.
        ASC places NULL first; DESC places NULL last.
        Default limits: 65,536 rows/partition and 262,144 buffered rows/stage.
        Exceeding either limit returns ResourceLimitExceeded.
        Execution is bounded in memory. RANGE/GROUPS/EXCLUDE, DISTINCT windows,
        incompatible specifications, mixed grouped/subquery windows, NULL-treatment
        syntax, and disk spill are unsupported.

        ── JOIN TYPES ──
        [INNER] JOIN ... ON condition
        LEFT [OUTER] JOIN ... ON condition
        RIGHT [OUTER] JOIN ... ON condition
        CROSS JOIN ...  (no ON clause, cartesian product)

        ── NOT SUPPORTED ──
        • SELECT DISTINCT
        • Subqueries (nested SELECT in WHERE / FROM)
        • UNION / INTERSECT / EXCEPT
        • Window forms beyond the bounded slice listed above
        • EXISTS / NOT EXISTS
        • FULL OUTER JOIN / NATURAL JOIN
        • DEFAULT, CHECK, FOREIGN KEY, UNIQUE column constraints
        • String functions (SUBSTR, UPPER, LOWER, TRIM, CONCAT, etc.)
        • Math functions (ABS, ROUND, CEIL, FLOOR, MOD, etc.)
        • Date functions (DATE, DATETIME, STRFTIME, etc.)
        • CAST, COALESCE, NULLIF, IIF, CASE WHEN
        • Savepoints
        • RETURNING clause
        • INSERT ... ON CONFLICT / UPSERT
        • Multiple statements in one call (send one at a time)
        """;
}

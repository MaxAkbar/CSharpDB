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
        BOOLEAN (BOOL, bare BIT)           — logical Boolean; zero=false, finite nonzero=true
        TINYINT                            — unsigned 8-bit integer (0..255)
        SMALLINT                           — signed 16-bit integer
        INTEGER (INT)                      — signed 32-bit integer
        BIGINT                             — signed 64-bit integer
        REAL                               — binary64 floating point; distinct logical name from DOUBLE
        DOUBLE PRECISION (DOUBLE, FLOAT)   — binary64 floating point
        DECIMAL[(p[,s])] (NUMERIC)         — exact; p=1..18, s=0..p; defaults 18,2 / p,0
        CHAR[(n)] (CHARACTER, NCHAR)       — fixed Unicode text; positive n pads with spaces
        VARCHAR[(n)] (CHARACTER VARYING,
          NVARCHAR)                        — variable Unicode text; positive n is a maximum
        TEXT (CLOB)                        — unbounded Unicode text (quote with ', escape as '')
        BINARY[(n)]                        — fixed bytes; positive n pads with zero bytes
        VARBINARY[(n)]                     — variable bytes; positive n is a maximum
        BLOB                               — binary byte array
        UUID (GUID, UNIQUEIDENTIFIER)      — 16-byte identifier
        DATE                               — calendar date
        TIME[(p)]                          — time of day; p=0..7 fractional digits
        DATETIME2[(p)] (DATETIME, no p)    — wall-clock date/time; p=0..7
        DATETIMEOFFSET[(p)]
          (TIMESTAMP[(p)] WITH TIME ZONE)  — offset date/time normalized to UTC; p=0..7
        INTERVAL YEAR TO MONTH             — year/month interval
        INTERVAL DAY TO SECOND[(p)]        — day/time interval; p=0..7
        JSON                               — validated canonical JSON text
        XML                                — validated compact XML text
        BIT(n)                             — fixed bit string; positive n is required
        BIT VARYING[(n)] (VARBIT[(n)])     — variable bit string; positive n is a maximum
        ROWVERSION (bare TIMESTAMP)        — special generated/read-only 8-byte token; not a CAST target
        NULL is a value marker, not a declared SQL type.

        ── CONSTRAINTS ──
        PRIMARY KEY
          — column or table syntax; table-level logical keys use INTEGER/TEXT
          — ordered composite INTEGER/TEXT logical keys are supported
          — a single INTEGER or BIGINT key retains generated row-identity behavior
        UNIQUE (col [, ...])
          — table constraint; ordered scalar/composite INTEGER/TEXT candidate key
        IDENTITY | AUTOINCREMENT
          — INTEGER/BIGINT PRIMARY KEY identity marker (explicit inserts remain allowed)
        NOT NULL
          — column constraint
        DEFAULT literal
          — typed INTEGER/REAL/TEXT/BLOB literal or NULL; not an expression/function
        [CONSTRAINT name] CHECK (expression)
          — column or table constraint; deterministic row-local expression
          — no parameters, functions, subqueries, or qualified references
        [CONSTRAINT name] FOREIGN KEY (child_col [, ...])
          REFERENCES parent (parent_col [, ...]) [MATCH SIMPLE]
          [ON DELETE action] [ON UPDATE action]
          — immediate scalar/composite INTEGER/TEXT relationship
          — MATCH SIMPLE is the default
          — action: RESTRICT | NO ACTION | CASCADE | SET NULL | SET DEFAULT

        ── CREATE TABLE ──
        CREATE TABLE [IF NOT EXISTS] name (
          col TYPE [PRIMARY KEY] [IDENTITY|AUTOINCREMENT] [NOT NULL]
                   [COLLATE collation] [DEFAULT literal]
                   [[CONSTRAINT name] CHECK (expression)]
                   [REFERENCES parent(parent_col) [MATCH SIMPLE]
                     [ON DELETE action] [ON UPDATE action]],
          ...,
          [[CONSTRAINT name] CHECK (expression)],
          [[CONSTRAINT name] PRIMARY KEY (col [, ...])],
          [[CONSTRAINT name] UNIQUE (col [, ...])],
          [[CONSTRAINT name] FOREIGN KEY (col [, ...])
             REFERENCES parent (parent_col [, ...]) [MATCH SIMPLE]
             [ON DELETE action] [ON UPDATE action]]
        )

        CREATE TEMP|TEMPORARY TABLE [IF NOT EXISTS] name (...)
          — session-scoped; use a remote transaction session for HTTP/gRPC
        PERSIST TEMP|TEMPORARY TABLE temp_name AS durable_name
        DROP TEMP|TEMPORARY TABLE [IF EXISTS] name

        ── INSERT ──
        INSERT INTO table [(col1, col2, ...)] VALUES
          (value_or_DEFAULT, ...), ...
        INSERT INTO table DEFAULT VALUES

        ── SELECT ──
        [WITH cte AS (SELECT ...) [, cte2 AS (SELECT ...)]]
        SELECT [DISTINCT] [col | expr [AS alias] | *], ...
        [FROM table [alias]
          [[INNER JOIN | LEFT [OUTER] JOIN | RIGHT [OUTER] JOIN] table2 ON cond
           | CROSS JOIN table2]]
        [WHERE expr]
        [GROUP BY expr, ...]
        [HAVING expr]
        [ORDER BY expr [ASC|DESC], ...]
        [LIMIT n]
        [OFFSET n]

        Scalar, IN/NOT IN, and EXISTS/NOT EXISTS subqueries are supported.
        Correlated subqueries are supported in WHERE, non-aggregate projection,
        and UPDATE/DELETE expressions, but not JOIN ON, GROUP BY, HAVING,
        ORDER BY, or aggregate projections.
        Set operations: UNION, UNION ALL, INTERSECT, EXCEPT.
        Trailing ORDER BY/LIMIT/OFFSET applies to the compound result.
        WITH supports non-recursive CTEs and optional output-column lists.

        ── UPDATE ──
        UPDATE table SET col1 = expr, col2 = expr, ... [WHERE expr]

        ── DELETE ──
        DELETE FROM table [WHERE expr]

        ── DROP TABLE ──
        DROP TABLE [IF EXISTS] name

        ── ALTER TABLE ──
        ALTER TABLE name ADD [COLUMN] col TYPE [constraints]
        ALTER TABLE name ADD CONSTRAINT constraint
        ALTER TABLE name DROP [COLUMN] col
        ALTER TABLE name DROP CONSTRAINT constraint_name
        ALTER TABLE name DROP PRIMARY KEY
        ALTER TABLE name ALTER COLUMN col TYPE INTEGER|REAL|TEXT|BLOB
        ALTER TABLE name ALTER COLUMN col SET DEFAULT literal
        ALTER TABLE name ALTER COLUMN col DROP DEFAULT
        ALTER TABLE name ALTER COLUMN col SET NOT NULL | DROP NOT NULL
        ALTER TABLE name ALTER COLUMN col SET COLLATION collation | DROP COLLATION
        ALTER TABLE name RENAME TO new_name
        ALTER TABLE name RENAME [COLUMN] old TO new
        ALTER TABLE name RENAME INDEX old TO new

        ALTER support is dependency-checked and deliberately bounded. In particular,
        physical rekey, type, and collation rewrites reject unsupported dependency
        shapes before mutation; consult the public SQL reference for those limits.

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
          [FOR EACH ROW]
        BEGIN
          statement; ...
        END

        Use NEW.col for inserted/updated values, OLD.col for previous/deleted values.
        Trigger WHEN conditions are not supported and are rejected before persistence.

        ── DROP TRIGGER ──
        DROP TRIGGER [IF EXISTS] name

        ── OTHER STATEMENTS ──
        ANALYZE [table]
        FIND DUPLICATES IN table ON expr [, ...]
        DEDUP table ON expr [, ...] KEEP FIRST|LAST
        MERGE DUPLICATES table ON expr [, ...]
        CREATE VALIDATION RULE name ON table[.column]
          AS expression MESSAGE 'message'
        VALIDATE TABLE table
        FIND ORPHANS IN child[.column] [REFERENCES parent.column]

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
        Comparison:  =  <>  !=  <  >  <=  >=
        Logical:     AND  OR  NOT
        Arithmetic:  +  -  *  /
        Pattern:     LIKE (% = any chars, _ = one char) [ESCAPE 'c']
                     NOT LIKE
        Membership:  IN (val, val, ...)    NOT IN (val, val, ...)
        Range:       BETWEEN low AND high  NOT BETWEEN low AND high
        Null check:  IS NULL               IS NOT NULL
        Collation:   expr COLLATE BINARY|NOCASE|NOCASE_AI|ICU:<locale>
        Parameters:  @param_name

        ── SCALAR FUNCTIONS ──
        Null/choice:
          TEXT, NZ, ISNULL, ISEMPTY, IIF, SWITCH, CHOOSE,
          COALESCE, IFNULL, NULLIF
        Text:
          LEN/LENGTH, LEFT, RIGHT, MID/SUBSTR/SUBSTRING,
          TRIM, LTRIM, RTRIM, UPPER/UCASE, LOWER/LCASE,
          INSTR, ORDINAL_STARTS_WITH, ORDINAL_ENDS_WITH,
          ORDINAL_CONTAINS, REPLACE, STRCOMP, VAL
        XML (XPath 1.0):
          XML_EXISTS/XMLEXISTS(xml, xpath[, namespace_json])
          XML_VALUE(xml, xpath[, namespace_json])
        Date/time:
          DATE, TIME, NOW/DATETIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND,
          DATEADD, DATEDIFF, DATEPART, DATESERIAL, TIMESERIAL, WEEKDAY, MONTHNAME
        Numeric/conversion:
          ABS, ROUND, INT/FLOOR, FIX, SGN,
          CSTR, CINT/CLNG, CDBL, CBOOL, CDATE, FORMAT
        Query sys.functions (or sys_functions) for canonical signatures,
        aliases, null behavior, and volatility.

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

        ── INTENTIONALLY NOT SUPPORTED ──
        • CASE/WHEN and CAST expressions (use IIF/SWITCH and conversion functions)
        • RETURNING on INSERT/UPDATE/DELETE
        • UPSERT, REPLACE, INSERT OR REPLACE, and INSERT ... ON CONFLICT
        • INTERSECT ALL and EXCEPT ALL
        • WITH RECURSIVE
        • Unregistered vendor functions such as STRFTIME, CEIL, and POWER
        • Window forms beyond the bounded slice listed above
        • FULL OUTER JOIN / NATURAL JOIN
        • MATCH FULL, MATCH PARTIAL, and DEFERRABLE foreign keys
        • Trigger WHEN conditions
        • SQL CREATE PROCEDURE and CALL (use the client procedure API)
        • SQL transaction/savepoint statements (use client transaction sessions)
        • Multiple statements in one call (send one at a time)
        """;
}

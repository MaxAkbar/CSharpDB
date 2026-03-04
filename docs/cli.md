# CSharpDB CLI Reference

The CSharpDB CLI (`CSharpDB.Cli`) is an interactive REPL for working with CSharpDB databases from the terminal. It supports SQL execution, meta-commands for introspection, transaction management, and batch file execution.

## Running the CLI

```bash
# Open or create a database
dotnet run --project src/CSharpDB.Cli -- mydata.db

# Without arguments, uses csharpdb.db in the current directory
dotnet run --project src/CSharpDB.Cli
```

The CLI also supports command mode (non-REPL) for storage diagnostics:

```bash
dotnet run --project src/CSharpDB.Cli -- inspect mydata.db
dotnet run --project src/CSharpDB.Cli -- inspect-page mydata.db 5 --hex
dotnet run --project src/CSharpDB.Cli -- check-wal mydata.db
dotnet run --project src/CSharpDB.Cli -- check-indexes mydata.db --json
```

If the first argument is one of `inspect`, `inspect-page`, `check-wal`, or `check-indexes`, command mode runs. Otherwise, the CLI opens REPL mode using the first argument as the database path.

## Interactive Usage

The REPL displays a `csdb>` prompt. Enter SQL statements followed by a semicolon, or use dot-prefixed meta-commands:

```
csdb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
OK (0 ms)

csdb> INSERT INTO users VALUES (1, 'Alice', 30);
1 row(s) affected (1 ms)

csdb> SELECT * FROM users;
┌────┬───────┬─────┐
│ id │ name  │ age │
├────┼───────┼─────┤
│  1 │ Alice │  30 │
└────┴───────┴─────┘
1 row(s) returned (0 ms)

csdb> .quit
```

Multi-line SQL is supported — the REPL waits for a semicolon before executing. Trigger bodies (`BEGIN ... END;`) are handled correctly.

---

## Meta-Commands

All meta-commands start with a dot (`.`). They are case-insensitive.

### Database Introspection

| Command | Description |
|---------|-------------|
| `.info` | Show database status: table, index, view, trigger, and collection counts |
| `.tables [PATTERN\|--all]` | List tables. By default, collection backing tables are hidden. Use a pattern to filter or `--all` to include internal tables. |
| `.schema [TABLE\|--all]` | Show the CREATE TABLE schema for one or all tables |
| `.indexes [TABLE]` | List indexes, optionally filtered by table name |
| `.views` | List all views |
| `.view <NAME>` | Show the CREATE VIEW SQL for a specific view |
| `.triggers [TABLE]` | List triggers, optionally filtered by table name |
| `.trigger <NAME>` | Show the CREATE TRIGGER SQL for a specific trigger |
| `.collections` | List document collections (NoSQL API) |

### Transaction Management

| Command | Description |
|---------|-------------|
| `.begin` | Begin an explicit transaction |
| `.commit` | Commit the current transaction |
| `.rollback` | Rollback the current transaction |
| `.checkpoint` | Flush WAL pages to the main database file |

### Mode Toggles

| Command | Description |
|---------|-------------|
| `.timing [on\|off\|status]` | Toggle query timing output (shows execution time in milliseconds) |
| `.snapshot [on\|off\|status]` | Toggle snapshot (read-only) mode for SELECT queries. When enabled, queries use a frozen point-in-time view that is unaffected by concurrent writes. |
| `.syncpoint [on\|off\|status]` | Toggle the sync fast path for primary key lookups. When enabled, cached PK lookups bypass the async pipeline for lower latency. |

### File Execution

| Command | Description |
|---------|-------------|
| `.read <FILE>` | Execute all SQL statements from a file. Statements are separated by semicolons. Results and errors are printed as they execute. |

### General

| Command | Description |
|---------|-------------|
| `.help` | Show a list of all available commands |
| `.quit` / `.exit` | Exit the REPL |

## SQL Introspection (`sys.*`)

You can query metadata with SQL in addition to dot-commands:

```sql
SELECT * FROM sys.tables ORDER BY table_name;
SELECT * FROM sys.columns WHERE table_name = 'users' ORDER BY ordinal_position;
SELECT * FROM sys.indexes WHERE table_name = 'users';
SELECT * FROM sys.views;
SELECT * FROM sys.triggers;
SELECT * FROM sys.objects ORDER BY object_type, object_name;
```

Underscored aliases are supported: `sys_tables`, `sys_columns`, `sys_indexes`, `sys_views`, `sys_triggers`, `sys_objects`.

---

## Storage Inspector Commands

These commands are read-only and return deterministic exit codes for automation:

- `0`: no warnings/errors
- `1`: warnings present, no errors
- `2`: errors present
- `64`: invalid CLI usage

| Command | Description |
|---------|-------------|
| `inspect <dbfile> [--json] [--out <file>] [--include-pages]` | Full DB file/header/page integrity scan |
| `inspect-page <dbfile> <pageId> [--json] [--hex]` | Inspect one page and optionally include hex dump |
| `check-wal <dbfile> [--json]` | Validate WAL header/frames/salts/checksums |
| `check-indexes <dbfile> [--index <name>] [--sample <n>] [--json]` | Validate index catalog/root/table/column consistency |

See [Storage Inspector](storage-inspector.md) for full rule IDs and JSON contract details.

---

## Examples

### Introspection

```
csdb> .tables
  users
  orders
  products

csdb> .schema users
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  age INTEGER
)

csdb> .indexes users
  idx_users_name  ON users(name)  UNIQUE

csdb> .info
  Tables:      3
  Indexes:     2
  Views:       1
  Triggers:    1
  Collections: 0
  Mode: WAL enabled, timing off, snapshot off
```

### Transactions

```
csdb> .begin
Transaction started.

csdb> INSERT INTO users VALUES (10, 'Test', 99);
1 row(s) affected

csdb> .rollback
Transaction rolled back.

csdb> SELECT * FROM users WHERE id = 10;
0 row(s) returned
```

### File Execution

```
csdb> .read samples/ecommerce-store.sql
Executing 84 statements...
84 succeeded, 0 failed.
```

### Snapshot Mode

```
csdb> .snapshot on
Snapshot mode enabled.

csdb> SELECT COUNT(*) FROM users;
┌──────────┐
│ COUNT(*) │
├──────────┤
│       42 │
└──────────┘

csdb> .snapshot off
Snapshot mode disabled.
```

---

## See Also

- [Getting Started Tutorial](getting-started.md) — C# API walkthrough with code examples
- [REST API Reference](rest-api.md) — HTTP endpoint documentation
- [FAQ](faq.md) — Common setup and troubleshooting answers
- [Sample Datasets](../samples/README.md) — SQL scripts to load test data

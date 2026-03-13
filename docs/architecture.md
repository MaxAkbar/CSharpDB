# CSharpDB Architecture

CSharpDB is a layered embedded database engine inspired by SQLite's architecture.
The core engine layers have clear responsibilities and mostly communicate with
adjacent layers. Above the engine, CSharpDB now exposes multiple consumer-facing
entry points, with `CSharpDB.Client` as the authoritative database API.

## Layer Overview

```
┌────────────────────────────────────────────────────────────────────┐
│ Hosts / Applications                                               │
│ CSharpDB.Api   CSharpDB.Admin   CSharpDB.Cli   CSharpDB.Mcp        │
├────────────────────────────────────────────────────────────────────┤
│ Consumer Access Layer                                              │
│ CSharpDB.Client                     CSharpDB.Data                  │
│ ICSharpDbClient                     ADO.NET Provider               │
├────────────────────────────────────────────────────────────────────┤
│ CSharpDB.Engine                                                    │
│ Database.OpenAsync / ExecuteAsync / Transactions / ReaderSession   │
├────────────────────────────────────────────────────────────────────┤
│ CSharpDB.Execution                                                 │
│ QueryPlanner, Operators, ExpressionEvaluator                       │
├───────────────────────────────┬────────────────────────────────────┤
│ CSharpDB.Sql                  │ CSharpDB.Storage                   │
│ Tokenizer, Parser, AST        │ Pager, B+Tree, WAL, RecordCodec    │
├───────────────────────────────┴────────────────────────────────────┤
│ CSharpDB.Core                                                      │
│ DbValue, DbType, Schema, ErrorCodes                                │
└────────────────────────────────────────────────────────────────────┘
```

**Dependency graph:**

```
Api     → Client
Admin   → Client
Cli     → Client
Cli     → Engine              (local-only helpers)
Cli     → Sql
Cli     → Storage.Diagnostics
Mcp     → Service → Client
Service → Sql                 (compatibility change notifications)
Data    → Engine
Client  → Engine
Client  → Sql
Client  → Storage.Diagnostics
Engine  → Execution → Sql
                    → Storage → Core
          Execution → Core
Engine  → Storage
Engine  → Sql
Engine  → Core
```

---

## Layer 1: Core (`CSharpDB.Core`)

Shared types used by every other layer. No dependencies.

| File | Purpose |
|------|---------|
| `DbType.cs` | Enum: `Null`, `Integer`, `Real`, `Text`, `Blob` |
| `DbValue.cs` | Discriminated union value type with comparison, equality, truthiness |
| `Schema.cs` | `ColumnDefinition`, `TableSchema`, `IndexSchema`, `TriggerSchema`, and related metadata types |
| `CSharpDbException.cs` | Exception with `ErrorCode` enum (IoError, TableNotFound, SyntaxError, DuplicateKey, WalError, Busy, etc.) |

### DbValue

`DbValue` is a `readonly struct` that can hold any of the five database types. It uses a compact internal layout — a `long` for integers, a `double` for reals, and an `object?` reference for strings and byte arrays. The `Type` property indicates which field is active.

Key behaviors:
- **Comparison**: NULLs sort first. Integer and Real are cross-comparable via promotion to double. Text uses ordinal string comparison. Blob uses byte-by-byte comparison.
- **Truthiness**: NULL and zero are falsy. Non-zero numbers, all strings, and all blobs are truthy. Used by WHERE clause evaluation.

---

## Layer 2: Storage (`CSharpDB.Storage`)

The storage layer manages all on-disk data structures. It handles file I/O, page caching, crash-safe transactions via WAL, B+tree operations, secondary indexes, and record encoding.

### File I/O

| File | Purpose |
|------|---------|
| `IStorageDevice.cs` | Abstract async interface: `ReadAsync`, `WriteAsync`, `FlushAsync`, `SetLengthAsync` |
| `FileStorageDevice.cs` | Implementation using `System.IO.RandomAccess` with `FileOptions.Asynchronous` |

The storage device abstraction means the engine could be backed by any byte-addressable store (memory, network, encrypted file).

### Page System

| File | Purpose |
|------|---------|
| `PageConstants.cs` | Page size (4096 bytes), file header layout, page types, WAL format constants |
| `SlottedPage.cs` | Structured access to slotted page layout (cells, pointers, free space) |
| `Pager.cs` | Page I/O, buffer pool, dirty tracking, page allocation/freelist, transaction lifecycle, WAL integration, snapshot readers |

#### Database File Format

The database is a sequence of 4096-byte pages. Page 0 contains the file header:

```
Offset  Size  Field
──────  ────  ─────
0       4     Magic bytes: "CSDB"
4       4     Format version (1)
8       4     Page size (4096)
12      4     Total page count
16      4     Schema catalog B+tree root page ID
20      4     Freelist head page ID (0 = empty)
24      4     Change counter
28      72    Reserved (zeroed)
100     ...   Page 0 content area (usable for B+tree data)
```

#### Slotted Page Layout

Each B+tree page uses a slotted page format:

```
┌───────────────────────────────────────────────────────────┐
│ Page Header (9 bytes)                                     │
│  [PageType:1] [CellCount:2] [ContentStart:2] [RightPtr:4] │
├───────────────────────────────────────────────────────────┤
│ Cell Pointer Array (2 bytes each, grows forward →)        │
│  [ptr0] [ptr1] [ptr2] ...                                 │
├───────────────────────────────────────────────────────────┤
│                    Free Space                             │
├───────────────────────────────────────────────────────────┤
│ Cell Content Area (grows ← backward from page end)        │
│  ... [cell2] [cell1] [cell0]                              │
└───────────────────────────────────────────────────────────┘
```

The cell pointer array and cell content area grow toward each other. When they meet, the page is full and must be split.

#### Pager

The `Pager` is the central coordinator for page-level operations:

- **Page cache**: In-memory `Dictionary<uint, byte[]>` of loaded pages
- **Dirty tracking**: `HashSet<uint>` of modified pages that need flushing
- **Allocation**: Pages are allocated from a freelist (linked list of free page IDs) or by extending the page count
- **Transactions**: Begin/Commit/Rollback lifecycle with WAL integration
- **Writer lock**: `SemaphoreSlim(1,1)` ensures single-writer access
- **Snapshot readers**: `CreateSnapshotReader(snapshot)` creates read-only pagers that see a frozen point-in-time view of the database

### Write-Ahead Log (WAL)

| File | Purpose |
|------|---------|
| `WriteAheadLog.cs` | WAL file I/O — frame-based append, commit, rollback, checkpoint, crash recovery |
| `WalIndex.cs` | In-memory index mapping `pageId → WAL file offset`, plus immutable snapshots |

CSharpDB uses a Write-Ahead Log for crash recovery and concurrent reader support. Modified pages are appended to a `.wal` file during commit, while the main `.db` file retains old data until checkpoint.

#### WAL File Format

```
┌──────────────────────────────────────────────────────┐
│ WAL Header (32 bytes)                                │
│  [magic:"CWAL"] [version:4] [pageSize:4]             │
│  [dbPageCount:4] [salt1:4] [salt2:4]                 │
│  [checksumSeed:4] [reserved:4]                       │
├──────────────────────────────────────────────────────┤
│ Frame 0 (4120 bytes)                                 │
│  [pageId:4] [dbPageCount:4] [salt1:4] [salt2:4]      │
│  [headerChecksum:4] [dataChecksum:4]                 │
│  [page data: 4096 bytes]                             │
├──────────────────────────────────────────────────────┤
│ Frame 1 ...                                          │
├──────────────────────────────────────────────────────┤
│ Frame N (commit frame: dbPageCount > 0)              │
└──────────────────────────────────────────────────────┘
```

#### Transaction Lifecycle (WAL Mode)

```
1. BEGIN TRANSACTION
   └── Acquire writer lock (SemaphoreSlim)
   └── Record WAL position

2. MODIFY PAGES
   └── Track dirty pages in memory
   └── Pages are modified in the page cache

3a. COMMIT
    └── Append all dirty pages as WAL frames
    └── Mark last frame as commit (dbPageCount > 0)
    └── Flush WAL to disk (commit point)
    └── Update in-memory WAL index
    └── Release writer lock
    └── Auto-checkpoint if WAL exceeds threshold (default: 1000 frames)

3b. ROLLBACK (or CRASH)
    └── Truncate WAL back to pre-transaction position
    └── Clear page cache
    └── Release writer lock
```

#### Crash Recovery

On database open, if a `.wal` file exists, the WAL is scanned frame-by-frame. Committed transactions (those with a valid commit frame) are replayed into the WAL index, and a checkpoint copies all committed pages to the DB file.

#### Concurrent Readers

Readers acquire a **snapshot** — a frozen copy of the WAL index at a point in time. Each snapshot reader gets its own `Pager` instance that routes page reads through the snapshot. This means:

- Readers see a consistent point-in-time view
- Writers do not block readers
- Multiple readers can be active simultaneously
- Checkpoint is skipped while readers are active (their snapshots reference WAL data)

### B+Tree

| File | Purpose |
|------|---------|
| `BTree.cs` | B+tree keyed by `long` rowid — insert, delete, find, split |
| `BTreeCursor.cs` | Forward-only cursor for sequential scans and seeks |

Each table's data is stored in a B+tree where the key is an auto-generated rowid and the value is an encoded row. Secondary indexes also use B+trees.

**Leaf page cell format:**
```
[totalSize:varint] [key:8 bytes] [payload bytes...]
```

**Interior page cell format:**
```
[totalSize:varint] [leftChild:4 bytes] [key:8 bytes]
```

Interior pages also store a "rightmost child" pointer in the page header. Leaf pages are linked via a "next leaf" pointer for efficient sequential scans.

**Operations:**
- **Insert**: Descend to the correct leaf, insert the cell. If the leaf overflows, split it and propagate the split key upward. If the root splits, create a new root.
- **Delete**: Descend to the leaf, remove the cell. (Simplified — no rebalancing/merging of underflowed pages.)
- **Find**: Descend from root to leaf following routing keys in interior pages.
- **Scan**: The `BTreeCursor` starts at the leftmost leaf and follows next-leaf pointers.

### Record Encoding

| File | Purpose |
|------|---------|
| `RecordEncoder.cs` | Serialize/deserialize `DbValue[]` rows to compact binary format |
| `Varint.cs` | LEB128 variable-length integer encoding |
| `SchemaSerializer.cs` | Serialize/deserialize `TableSchema` for the schema catalog |

Row encoding format:
```
[columnCount:varint] [type1:1 byte] [type2:1 byte] ... [data1] [data2] ...
```

Where each data field is:
- **Null**: nothing (0 bytes)
- **Integer**: varint-encoded `long`
- **Real**: 8 bytes (IEEE 754 double)
- **Text**: [length:varint] [UTF-8 bytes]
- **Blob**: [length:varint] [raw bytes]

### Schema Catalog

| File | Purpose |
|------|---------|
| `SchemaCatalog.cs` | In-memory cache of table/index/view/trigger schemas, backed by dedicated B+trees |

The schema catalog stores all database metadata in B+trees:

- **Table schemas**: table name, column definitions, root page ID
- **Index schemas**: index name, table name, columns, uniqueness, root page ID
- **View definitions**: view name → SQL text
- **Trigger definitions**: trigger name, table, timing, event, body SQL

On database open, all schemas are loaded into in-memory dictionaries for fast lookups. When objects are created or dropped, both the in-memory cache and the on-disk B+trees are updated.

---

## Layer 3: SQL Frontend (`CSharpDB.Sql`)

| File | Purpose |
|------|---------|
| `TokenType.cs` | Enum of all token types (keywords, operators, literals, punctuation) |
| `Token.cs` | Token struct: `Type`, `Value` (string), `Position` (int) |
| `Tokenizer.cs` | Hand-rolled lexical scanner with keyword lookup table |
| `Ast.cs` | AST node classes for all statement and expression types |
| `Parser.cs` | Recursive descent parser with precedence climbing for expressions |

### Supported Statements

| Category | Statements |
|----------|-----------|
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` (ADD/DROP COLUMN, RENAME TABLE/COLUMN) |
| **DML** | `INSERT INTO`, `SELECT`, `UPDATE`, `DELETE` |
| **Indexes** | `CREATE INDEX`, `DROP INDEX` (with `UNIQUE`, `IF NOT EXISTS`/`IF EXISTS`) |
| **Views** | `CREATE VIEW`, `DROP VIEW` |
| **Triggers** | `CREATE TRIGGER`, `DROP TRIGGER` (BEFORE/AFTER, INSERT/UPDATE/DELETE) |
| **CTEs** | `WITH ... AS (...) SELECT ...` |

### Parsing Pipeline

```
SQL string → Tokenizer → Token[] → Parser → AST (Statement tree)
```

The **tokenizer** scans the input character by character, recognizing keywords (case-insensitive), identifiers, numeric literals (integer and real), string literals (single-quoted with `''` escaping), operators, and punctuation.

The **parser** is a recursive descent parser. Each SQL statement type has its own parsing method. Expression parsing uses precedence climbing to correctly handle operator precedence:

```
Precedence (low to high):
  OR
  AND
  NOT (unary)
  =, <>, <, >, <=, >=, LIKE, IN, BETWEEN, IS NULL
  +, -
  *, /
  - (unary)
```

---

## Layer 4: Execution (`CSharpDB.Execution`)

| File | Purpose |
|------|---------|
| `IOperator.cs` | Iterator interface: `OpenAsync`, `MoveNextAsync`, `Current` |
| `Operators.cs` | Physical operators: TableScan, IndexScan, Filter, Projection, Sort, Limit, Aggregate, Join, etc. |
| `ExpressionEvaluator.cs` | Evaluates expression AST against a row (including LIKE, IN, BETWEEN, IS NULL, aggregates) |
| `QueryPlanner.cs` | Converts AST statements into executable operator trees or DML/DDL actions |

### Iterator Model

Query execution follows the Volcano/iterator model. Each operator implements `IOperator`:

```csharp
public interface IOperator : IAsyncDisposable
{
    ColumnDefinition[] OutputSchema { get; }
    ValueTask OpenAsync(CancellationToken ct = default);
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
    DbValue[] Current { get; }
}
```

Operators form a tree. The root operator pulls rows upward by calling `MoveNextAsync` on its child, which in turn calls its child, and so on down to the leaf scan operator.

### Operator Catalog

| Operator | Purpose |
|----------|---------|
| `TableScanOperator` | Full table scan via `BTreeCursor` — reads all rows sequentially |
| `IndexScanOperator` | Index-based lookup — seeks in index B+tree, then fetches rows from table B+tree |
| `FilterOperator` | Applies a WHERE predicate, skipping non-matching rows |
| `ProjectionOperator` | Selects/reorders columns, evaluates computed expressions and aliases |
| `SortOperator` | Materializes all input, sorts by ORDER BY clauses |
| `LimitOperator` | Caps output at N rows with optional OFFSET |
| `AggregateOperator` | GROUP BY grouping with aggregate functions (COUNT, SUM, AVG, MIN, MAX) |
| `NestedLoopJoinOperator` | Implements INNER, LEFT, RIGHT, and CROSS JOINs |

### Query Planning

For **SELECT**, the planner builds an operator tree:
```
TableScan/IndexScan → [Filter] → [Join] → [Aggregate] → [Having] → [Sort] → [Projection] → [Limit]
```

The planner includes basic **index selection**: when a WHERE clause contains an equality check (`col = value`) on an indexed column, the planner substitutes `IndexScanOperator` for `TableScanOperator`.

For **DML** (INSERT, UPDATE, DELETE) and **DDL** (CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP TRIGGER), the planner executes the operation directly against the B+tree and schema catalog, returning a row-count result.

**Triggers** are fired automatically during INSERT, UPDATE, and DELETE operations. The planner checks for BEFORE and AFTER triggers on the affected table and executes their body SQL statements. A recursion guard prevents infinite trigger chains (max depth: 16).

**Views** are expanded inline during query planning — a reference to a view in a FROM clause is replaced with the view's SQL definition, parsed and planned recursively.

**CTEs** (`WITH` clause) are materialized eagerly — the CTE query is executed first and its results are stored in memory, then referenced by the main query.

### Expression Evaluator

The `ExpressionEvaluator` is a static class that recursively evaluates an `Expression` AST node against a current row. It handles:
- **Column references** — look up by column name (or qualified `table.column`) in the schema
- **Literals** — integer, real, text, null
- **Binary operators** — arithmetic (+, -, *, /), comparison (=, <>, <, >, <=, >=), logical (AND, OR)
- **Unary operators** — NOT, negation
- **LIKE** — pattern matching with `%` and `_` wildcards, optional ESCAPE character
- **IN** — membership test against a list of values
- **BETWEEN** — range check (inclusive)
- **IS NULL / IS NOT NULL** — null testing
- **Aggregate functions** — COUNT, SUM, AVG, MIN, MAX (with DISTINCT support)

---

## Layer 5: Engine (`CSharpDB.Engine`)

| File | Purpose |
|------|---------|
| `Database.cs` | Top-level API: open/create database, execute SQL, manage transactions, checkpoint, reader sessions |

The `Database` class ties all layers together:

1. **Open**: Creates the storage device, WAL, WAL index, and pager. Runs crash recovery if a WAL file exists. Loads the schema catalog.
2. **ExecuteAsync**: Parses SQL → dispatches to QueryPlanner → returns `QueryResult`
3. **Auto-commit**: Non-SELECT statements automatically begin and commit a transaction if none is active
4. **Explicit transactions**: `BeginTransactionAsync` / `CommitAsync` / `RollbackAsync` for multi-statement atomicity
5. **CheckpointAsync**: Manually triggers a WAL checkpoint (copies committed WAL pages to DB file)
6. **CreateReaderSession**: Returns an independent `ReaderSession` that sees a snapshot of the database at the current point in time. Multiple reader sessions can coexist with an active writer.
7. **Dispose**: Rolls back any uncommitted transaction, checkpoints, deletes WAL file, and closes the pager

---

## Layer 6: Unified Client (`CSharpDB.Client`)

`CSharpDB.Client` is the authoritative database API for CSharpDB consumers.

It owns the public client contract and transport selection boundary used by the
CLI, Web API, Admin dashboard, and future external consumers. Transport details
stay behind this layer.

Key pieces:

- `ICSharpDbClient` — transport-agnostic database contract
- `CSharpDbClientOptions` — endpoint / data source / connection string options
- `CSharpDbTransport` — public transport selector
- `AddCSharpDbClient(...)` — DI registration helper

Current direction:

- **Direct transport is implemented today** and is backed by `CSharpDB.Engine`
- **HTTP transport is implemented** and targets `CSharpDB.Api`
- **gRPC transport is implemented** and targets `CSharpDB.Daemon`
- **Named Pipes is part of the public transport model** but is not implemented yet
- **The client does not depend on `CSharpDB.Data`**
- **New database-facing functionality should be added here first**

Current surface includes:

- database info and metadata
- table schemas, browse, CRUD, and table/column DDL
- indexes, views, and triggers
- saved queries
- procedures and procedure execution
- SQL execution
- client-managed transactions
- document collections
- checkpoint and storage diagnostics

Implementation dependencies:

- `CSharpDB.Engine`
- `CSharpDB.Sql`
- `CSharpDB.Storage.Diagnostics`

This means the current direct client is a high-level engine-backed API, not an
ADO.NET wrapper.

---

## Layer 7: ADO.NET Provider (`CSharpDB.Data`)

| File | Purpose |
|------|---------|
| `CSharpDbConnection.cs` | `DbConnection` implementation — open, close, connection string parsing |
| `CSharpDbCommand.cs` | `DbCommand` implementation — parameterized SQL execution |
| `CSharpDbDataReader.cs` | `DbDataReader` implementation — forward-only row iteration with typed accessors |
| `CSharpDbParameter.cs` | `DbParameter` / `DbParameterCollection` for parameterized queries |
| `CSharpDbTransaction.cs` | `DbTransaction` for explicit transaction control |
| `CSharpDbFactory.cs` | `DbProviderFactory` for ADO.NET provider registration |
| `SqlParameterBinder.cs` | Binds `@param` placeholders in SQL to parameter values |
| `TypeMapper.cs` | Maps between CSharpDB types and .NET CLR types |

The ADO.NET provider allows CSharpDB to be used with the standard `System.Data.Common` APIs, making it compatible with ORMs and existing .NET data access code:

```csharp
await using var conn = new CSharpDbConnection("Data Source=myapp.db");
await conn.OpenAsync();

using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM users WHERE age > @age";
cmd.Parameters.AddWithValue("@age", 25);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(1));
}
```

---

## Layer 8: REST API (`CSharpDB.Api`)

The REST API exposes the full database feature set over HTTP using ASP.NET Core Minimal APIs. It enables cross-language interoperability — any language with an HTTP client can work with CSharpDB.

Components:
- **Endpoints** — organized by resource (tables, rows, indexes, views, triggers, procedures, SQL, info, inspection)
- **DTOs** — Request/response records for type-safe serialization
- **JSON helpers** — Coerce `System.Text.Json` `JsonElement` values to CLR primitives for the client
- **Exception middleware** — Maps `CSharpDbException` error codes to HTTP status codes (404, 409, 422, etc.)
- **OpenAPI + Scalar** — Auto-generated API spec with interactive documentation at `/scalar`

The API now injects `ICSharpDbClient` directly. It does not depend on
`CSharpDB.Data` or engine internals directly.

See the [REST API Reference](../docs/rest-api.md) for the complete endpoint documentation.

---

## Layer 9: Admin Dashboard (`CSharpDB.Admin`)

A Blazor Server application that provides a web-based UI for database administration. Features:
- Tab-based interface for browsing tables, views, indexes, and triggers
- Paginated data grid with column headers
- SQL execution panel
- Procedure editing and execution
- Storage inspection
- Schema introspection (columns, types, constraints)

The Admin dashboard now injects `ICSharpDbClient` directly. It uses an
admin-local change notification service to refresh UI state after mutations.

---

## Layer 10: CLI And MCP Hosts

Two additional host applications sit above the consumer access layer:

- **`CSharpDB.Cli`** — the interactive shell and local tooling entrypoint. It
  now routes normal database access through `CSharpDB.Client`, while still
  keeping a few local-only direct helpers for engine- and diagnostics-specific
  features.
- **`CSharpDB.Mcp`** — the MCP server host. It resolves `ICSharpDbClient`
  directly and shares the same client configuration model as the other hosts.

---

## End-to-End: Life of a Query

Here's what happens when you call `db.ExecuteAsync("SELECT name FROM users WHERE age > 25 ORDER BY name")`:

```
1. Parser.Parse(sql)
   ├── Tokenizer: "SELECT" "name" "FROM" "users" "WHERE" "age" ">" "25" "ORDER" "BY" "name"
   └── Parser: SelectStatement { Columns=[name], Table=users, Where=age>25, OrderBy=[name ASC] }

2. QueryPlanner.ExecuteSelect(stmt)
   ├── Resolve "users" → TableSchema (from SchemaCatalog)
   ├── Check for usable index on WHERE columns
   ├── Build: TableScanOperator(users_btree) or IndexScanOperator if applicable
   ├── Wrap:  FilterOperator(scan, "age > 25")
   ├── Wrap:  SortOperator(filter, [name ASC])
   └── Wrap:  ProjectionOperator(sort, [name])
   └── Return: QueryResult(projectionOp)

3. User calls result.GetRowsAsync()
   └── Opens operator chain top-down
       └── ProjectionOp.MoveNextAsync()
           └── SortOp.MoveNextAsync()  [materializes all matching rows, sorts]
               └── FilterOp.MoveNextAsync()  [skips rows where age <= 25]
                   └── TableScanOp.MoveNextAsync()
                       └── BTreeCursor.MoveNextAsync()
                           └── Pager.GetPageAsync(leafPageId)
                               ├── Check page cache
                               ├── Check WAL index for latest version
                               └── Fall through to FileStorageDevice.ReadAsync(offset)
```

Each row flows upward through the operator chain, transformed at each stage, until it reaches the caller.

---

## See Also

- [Getting Started Tutorial](getting-started.md) — Step-by-step walkthrough with code examples
- [Internals & Contributing](internals.md) — How to extend the engine, add SQL statements, create operators
- [REST API Reference](rest-api.md) — HTTP endpoint documentation
- [Roadmap](roadmap.md) — Planned features and project direction
- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md) — Performance data across all engine layers

# CSharpDB Internals & Contributing

This guide is for developers who want to understand, extend, or contribute to CSharpDB.

## Project Structure

```
CSharpDB.slnx
├── src/
│   ├── CSharpDB.Core/               Shared types (no dependencies)
│   │   ├── DbType.cs                  Data type enum
│   │   ├── DbValue.cs                 Discriminated union value type
│   │   ├── Schema.cs                  ColumnDefinition, TableSchema, IndexSchema, TriggerSchema
│   │   └── CSharpDbException.cs       Typed exception with ErrorCode
│   │
│   ├── CSharpDB.Storage/            On-disk storage (depends on Core)
│   │   ├── PageConstants.cs            Page size, header offsets, page types, WAL constants
│   │   ├── Varint.cs                   LEB128 variable-length integer codec
│   │   ├── IStorageDevice.cs           Abstract async file I/O interface
│   │   ├── FileStorageDevice.cs        Concrete implementation via RandomAccess
│   │   ├── Pager.cs                    Page cache, dirty tracking, allocation, transactions, WAL, snapshots
│   │   ├── WriteAheadLog.cs            WAL file I/O: frames, commit, rollback, checkpoint, recovery
│   │   ├── WalIndex.cs                 In-memory WAL index + immutable snapshots for concurrent reads
│   │   ├── SlottedPage.cs              Structured access to slotted page layout
│   │   ├── BTree.cs                    B+tree: insert, delete, find, split
│   │   ├── BTreeCursor.cs             Forward-only cursor for scans and seeks
│   │   ├── RecordEncoder.cs            Encode/decode DbValue[] ↔ byte[]
│   │   ├── SchemaSerializer.cs         Encode/decode TableSchema ↔ byte[]
│   │   └── SchemaCatalog.cs            In-memory schema cache (tables, indexes, views, triggers) backed by B+trees
│   │
│   ├── CSharpDB.Sql/                SQL frontend (depends on Core)
│   │   ├── TokenType.cs                Token type enum
│   │   ├── Token.cs                    Token struct
│   │   ├── Tokenizer.cs               Hand-rolled lexical scanner
│   │   ├── Ast.cs                      Statement and Expression AST nodes
│   │   ├── Parser.cs                   Recursive descent parser
│   │   ├── SqlScriptSplitter.cs        Multi-statement script splitting (tracks BEGIN/END depth for triggers)
│   │   └── SqlStatementClassifier.cs   Classifies statements as read-only or mutating
│   │
│   ├── CSharpDB.Execution/          Query execution (depends on Core, Sql, Storage)
│   │   ├── IOperator.cs                Iterator interface
│   │   ├── Operators.cs                TableScan, IndexScan, Filter, Projection, Sort, Limit, Aggregate, Join
│   │   ├── ExpressionEvaluator.cs      Evaluates Expression AST against a row
│   │   └── QueryPlanner.cs             AST → operator tree or direct DML/DDL execution
│   │
│   ├── CSharpDB.Engine/             Public API (depends on all above)
│   │   └── Database.cs                 Open, Execute, Transactions, Checkpoint, ReaderSession
│   │
│   ├── CSharpDB.Client/             Unified client SDK (depends on Engine, Sql, Storage.Diagnostics)
│   │   ├── ICSharpDbClient.cs          Public client contract (all database operations)
│   │   ├── CSharpDbClient.cs           Factory: Create() → transport-specific implementation
│   │   ├── CSharpDbClientOptions.cs    Configuration (DataSource, Endpoint, Transport, ConnectionString)
│   │   ├── CSharpDbTransport.cs        Transport enum (Direct, Http, Grpc, Tcp, NamedPipes)
│   │   ├── ServiceCollectionExtensions.cs  DI registration (AddCSharpDbClient)
│   │   ├── Internal/                   Direct-transport implementation (engine-backed client)
│   │   └── Models/                     Schema, data, procedure, transaction, and collection models
│   │
│   ├── CSharpDB.Data/               ADO.NET provider (depends on Engine)
│   │   ├── CSharpDbConnection.cs       DbConnection implementation
│   │   ├── CSharpDbCommand.cs          DbCommand with parameterized queries
│   │   ├── CSharpDbDataReader.cs       DbDataReader with typed accessors
│   │   ├── CSharpDbParameter.cs        DbParameter / DbParameterCollection
│   │   ├── CSharpDbTransaction.cs      DbTransaction implementation
│   │   ├── CSharpDbFactory.cs          DbProviderFactory registration
│   │   ├── SqlParameterBinder.cs       @param placeholder binding
│   │   └── TypeMapper.cs               CSharpDB ↔ CLR type mapping
│   │
│   ├── CSharpDB.Native/             NativeAOT C FFI library (depends on Engine, Execution, Core)
│   │   ├── NativeExports.cs            20 exported C functions (open, close, execute, result iteration, transactions, errors)
│   │   ├── HandleTable.cs              GCHandle-based opaque pointer management
│   │   ├── StringCache.cs              Unmanaged UTF-8 string lifetime management
│   │   ├── BlobCache.cs                Pinned byte[] lifetime management
│   │   ├── ErrorState.cs               Thread-local errno-style error reporting
│   │   └── csharpdb.h                  C header file for consumers
│   │
│   ├── CSharpDB.Cli/                Interactive REPL (depends on Client)
│   │   ├── Program.cs                  Entry point with CLI argument parsing
│   │   ├── CliShellOptions.cs          Parses --endpoint, --server, --transport flags
│   │   ├── Repl.cs                     Read-eval-print loop
│   │   ├── TableFormatter.cs           ASCII table output with alignment
│   │   ├── MetaCommands.cs             .tables, .schema, .quit, etc.
│   │   └── MetaCommandContext.cs       Session state (client, transactions, snapshots)
│   │
│   ├── CSharpDB.Service/            Compatibility facade over CSharpDB.Client (depends on Client)
│   │   ├── CSharpDbService.cs          Delegates to ICSharpDbClient
│   │   └── Models/                     TableBrowseResult, ViewBrowseResult, SqlExecutionResult, ViewDefinition
│   │
│   ├── CSharpDB.Admin/              Blazor Server admin dashboard (depends on Service)
│   │   ├── Program.cs                  Blazor Server entry point
│   │   ├── Services/                   Theme, toast, modal, tab manager, DatabaseChangeService
│   │   └── Components/                 Razor components for UI
│   │
│   ├── CSharpDB.Api/                REST API (depends on Service)
│   │   ├── Program.cs                  ASP.NET Core Minimal API entry point
│   │   ├── Endpoints/                  TableEndpoints, RowEndpoints, IndexEndpoints, ViewEndpoints, etc.
│   │   ├── Dtos/                       Request/response record types
│   │   ├── Helpers/                    JSON coercion helpers
│   │   └── Middleware/                 Exception handling middleware
│   │
│   └── CSharpDB.Mcp/                MCP server for AI assistants (depends on Service)
│       ├── Program.cs                  Generic Host with stdio transport
│       ├── Tools/                      SchemaTools, DataTools, MutationTools, SqlTools (15 tools)
│       └── Helpers/                    JSON serialization and value coercion
│
├── clients/
│   └── node/                         Node.js/TypeScript client (wraps CSharpDB.Native via koffi)
│       ├── src/index.ts                Database class, query/execute/transaction API
│       ├── src/native.ts               koffi FFI bindings to CSharpDB.Native
│       ├── examples/                   Basic usage example
│       └── tests/                      Integration tests
│
├── tests/
│   ├── CSharpDB.Tests/              Engine unit + integration tests
│   │   ├── VarintTests.cs              Varint round-trip encoding
│   │   ├── RecordEncoderTests.cs       Row encoding/decoding
│   │   ├── TokenizerTests.cs           SQL tokenization
│   │   ├── ParserTests.cs              SQL parsing to AST
│   │   ├── IntegrationTests.cs         Full SQL round-trips (end-to-end)
│   │   ├── WalTests.cs                 WAL mode: commit, rollback, crash recovery, snapshots
│   │   ├── ClientSqlExecutionTests.cs  Client SDK SQL execution tests
│   │   └── SqlScriptSplitterTests.cs   Script splitting edge cases
│   │
│   ├── CSharpDB.Data.Tests/         ADO.NET provider tests
│   │   ├── ConnectionTests.cs          Connection open/close/state
│   │   ├── CommandTests.cs             Parameterized queries, ExecuteScalar, ExecuteNonQuery
│   │   ├── DataReaderTests.cs          Typed getters, schema table, null handling
│   │   └── TransactionTests.cs         ADO.NET transaction commit/rollback
│   │
│   ├── CSharpDB.Cli.Tests/          CLI smoke + integration tests
│   │
│   └── CSharpDB.Benchmarks/         Performance benchmarks
│
├── docs/
│   ├── tutorials/native-ffi/         FFI tutorials (JavaScript via koffi, Python via ctypes)
│   └── service-daemon/               Service daemon design document
│
└── samples/                          Sample datasets + import helpers
    ├── ecommerce-store/
    │   ├── schema.sql                  Northwind Electronics
    │   └── procedures.json
    ├── medical-clinic/
    │   ├── schema.sql                  Riverside Health Center
    │   └── procedures.json
    ├── school-district/
    │   ├── schema.sql                  Maplewood School District
    │   └── procedures.json
    ├── feature-tour/
    │   ├── schema.sql                  Northstar Field Services
    │   ├── procedures.json
    │   └── queries.sql
    └── run-sample.csx
```

---

## How a SELECT Query Flows Through the System

Tracing `db.ExecuteAsync("SELECT name FROM users WHERE age > 25")`:

### Step 1: Database.ExecuteAsync (Engine)
- `Database.ExecuteAsync` calls `Parser.Parse(sql)` to get an AST
- Since it's a SELECT (not DML), no auto-transaction is started
- Calls `QueryPlanner.ExecuteAsync(stmt)`

### Step 2: Parser.Parse (Sql)
- `Tokenizer.Tokenize()` scans the string into tokens:
  `SELECT`, `name`, `FROM`, `users`, `WHERE`, `age`, `>`, `25`
- `Parser.ParseStatement()` recognizes SELECT and delegates to `ParseSelect()`
- Produces a `SelectStatement` with:
  - Columns: `[ColumnRefExpression("name")]`
  - TableName: `"users"`
  - Where: `BinaryExpression(GreaterThan, ColumnRef("age"), Literal(25))`

### Step 3: QueryPlanner.ExecuteSelect (Execution)
- Resolves `"users"` against `SchemaCatalog` to get `TableSchema` and root page
- Creates a `BTree` using the planner's own `Pager` (important for snapshot readers)
- Checks for usable indexes (equality predicates on indexed columns)
- Builds operator pipeline:
  1. `TableScanOperator(tree, schema)` — or `IndexScanOperator` if an index applies
  2. `FilterOperator(scan, whereExpr, schema)` — applies `age > 25`
  3. `ProjectionOperator(filter, [name], schema)` — extracts the `name` column
- Returns `QueryResult(projectionOp)`

### Step 4: User iterates rows (Execution → Storage)
- Calling `result.GetRowsAsync()` opens the operator chain
- `ProjectionOperator.MoveNextAsync()` calls `FilterOperator.MoveNextAsync()`
- `FilterOperator` calls `TableScanOperator.MoveNextAsync()` in a loop
- `TableScanOperator` uses `BTreeCursor.MoveNextAsync()` to walk leaf pages
- `BTreeCursor` calls `Pager.GetPageAsync()` which checks: cache → WAL index → disk
- For each row, `FilterOperator` evaluates `ExpressionEvaluator.Evaluate(whereExpr, row, schema)`
- If the expression returns truthy, the row passes through to `ProjectionOperator`
- `ProjectionOperator` extracts the `name` column and yields it

---

## How to Add a New SQL Statement

Example: adding `TRUNCATE TABLE name`.

### 1. Add AST node (`Ast.cs`)
```csharp
public sealed class TruncateTableStatement : Statement
{
    public required string TableName { get; init; }
}
```

### 2. Add parsing (`Parser.cs`)
Add `"TRUNCATE"` to the keyword list in `Tokenizer.cs`, then in `Parser.ParseStatement()`:
```csharp
TokenType.Truncate => ParseTruncate(),
```
Implement `ParseTruncate()` to consume `TRUNCATE TABLE <identifier>`.

### 3. Add execution (`QueryPlanner.cs`)
```csharp
TruncateTableStatement truncate => await ExecuteTruncateAsync(truncate, ct),
```
Implement `ExecuteTruncateAsync` — delete all rows from the B+tree, return `new QueryResult(deletedCount)`.

### 4. Add tests (`IntegrationTests.cs`)
Write a test that creates a table, inserts rows, truncates, and verifies the table is empty.

---

## How to Add a New Operator

Example: adding a `DistinctOperator` that deduplicates rows.

### 1. Create the operator class (`Operators.cs`)
```csharp
public sealed class DistinctOperator : IOperator
{
    private readonly IOperator _source;
    private readonly HashSet<string> _seen = new();

    public ColumnDefinition[] OutputSchema => _source.OutputSchema;
    public DbValue[] Current => _source.Current;

    public DistinctOperator(IOperator source) => _source = source;

    public ValueTask OpenAsync(CancellationToken ct) => _source.OpenAsync(ct);

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct)
    {
        while (await _source.MoveNextAsync(ct))
        {
            var key = string.Join("|", _source.Current.Select(v => v.ToString()));
            if (_seen.Add(key)) return true;
        }
        return false;
    }

    public ValueTask DisposeAsync() => _source.DisposeAsync();
}
```

### 2. Wire it into the planner (`QueryPlanner.cs`)
Insert `DistinctOperator` into the operator tree in `ExecuteSelect()` when the AST indicates DISTINCT.

### 3. Update the parser
Recognize `SELECT DISTINCT` in `ParseSelect()` and set a flag on `SelectStatement`.

---

## Testing Strategy

### Unit Tests (per layer)

| Test file | Layer | What it tests |
|-----------|-------|---------------|
| `VarintTests.cs` | Storage | Varint encoding round-trips for unsigned/signed values |
| `RecordEncoderTests.cs` | Storage | Row encoding/decoding for all DbValue types |
| `TokenizerTests.cs` | Sql | Keyword recognition, string escaping, operators, numbers, comments |
| `ParserTests.cs` | Sql | AST generation for each statement type, complex expressions |

### Integration Tests (end-to-end)

| Category | Examples |
|----------|----------|
| **Basic CRUD** | CREATE TABLE + INSERT + SELECT, UPDATE, DELETE, DROP TABLE |
| **Filtering** | WHERE with AND/OR/NOT, LIKE, IN, BETWEEN, IS NULL |
| **Aggregates** | COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING |
| **JOINs** | INNER JOIN, LEFT JOIN, RIGHT JOIN, CROSS JOIN, multi-table |
| **Schema** | ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE) |
| **Indexes** | CREATE INDEX, UNIQUE index, index-based lookups |
| **Views** | CREATE VIEW, SELECT from view, DROP VIEW |
| **CTEs** | WITH clause, multiple CTEs, CTE referencing CTE |
| **Triggers** | BEFORE/AFTER INSERT/UPDATE/DELETE, trigger with multiple statements |
| **Transactions** | BEGIN/COMMIT/ROLLBACK, persistence across reopen |
| **WAL** | Commit through WAL, rollback, crash recovery, concurrent readers, checkpointing |

### ADO.NET Tests

| Test file | What it tests |
|-----------|---------------|
| `ConnectionTests.cs` | Open/close, connection state, connection string parsing, GetTableNames/GetTableSchema |
| `CommandTests.cs` | ExecuteNonQuery, ExecuteScalar, ExecuteReader, parameterized queries |
| `DataReaderTests.cs` | Typed getters (GetInt64, GetString, GetDouble, GetBoolean), IsDBNull, GetSchemaTable, HasRows |
| `TransactionTests.cs` | ADO.NET transaction commit/rollback |

### Running Tests

```bash
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj -- -class "CSharpDB.Tests.IntegrationTests"  # Run integration tests
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj -- -class "CSharpDB.Tests.WalTests"          # Run WAL tests
dotnet run --project tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj --                      # Run ADO.NET tests
dotnet run --project tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --                        # Run CLI tests
```

---

## Concurrency Model

CSharpDB supports **single writer + concurrent readers** via WAL mode:

- **Writer**: A `SemaphoreSlim(1,1)` ensures only one write transaction is active at a time. The writer appends modified pages to the WAL file and commits by flushing the WAL.

- **Readers**: Each reader acquires a `WalSnapshot` — a frozen copy of the WAL index. The snapshot routes page reads through the WAL, so the reader sees a consistent point-in-time view even while the writer modifies data.

- **Checkpoint**: Periodically (after 1000+ WAL frames by default, or manually via `CheckpointAsync`), committed WAL pages are copied to the main DB file and the WAL is reset. Checkpoint is skipped if any readers are active.

- **Crash Recovery**: On open, the WAL is scanned for committed transactions. Valid committed frames are checkpointed to bring the DB file up to date.

```
Writer flow:      BeginTransaction → modify pages → CommitAsync (append to WAL) → release lock
Reader flow:      AcquireSnapshot → create snapshot pager → read pages (WAL or DB file) → release
Checkpoint flow:  Acquire mutex → copy WAL pages to DB → reset WAL → release mutex
```

---

## Current Limitations

These are known simplifications:

| Area | Limitation |
|------|-----------|
| **B+tree** | Delete doesn't rebalance/merge underflowed pages |
| **Storage** | No page-level compression |
| **Storage** | No mmap read path |
| **Query** | No subqueries |
| **Query** | No window functions |
| **Query** | No UNION / INTERSECT / EXCEPT |
| **RowId** | Initial next-rowid load still scans keys once per table (high-water mark not persisted in table metadata) |
| **Concurrency** | Single writer only (no multi-writer) |
| **Indexes** | Composite indexes are supported, but general range-scan pushdown is still limited |

---

## Roadmap

See [docs/roadmap.md](roadmap.md) for the full roadmap with near-term, mid-term, and long-term goals.

## See Also

- [Architecture Guide](architecture.md) — Layer-by-layer design deep dive
- [Getting Started Tutorial](getting-started.md) — Step-by-step walkthrough with code examples
- [Client SDK](../src/CSharpDB.Client/README.md) — Unified client API and transport model
- [Native Library Reference](../src/CSharpDB.Native/README.md) — C FFI API, build instructions, cross-language examples
- [Node.js Client](../clients/node/README.md) — TypeScript/JavaScript package
- [REST API Reference](rest-api.md) — All 33 API endpoints with examples
- [MCP Server Reference](mcp-server.md) — AI assistant integration via Model Context Protocol
- [CLI Reference](cli.md) — Interactive REPL commands and meta-commands
- [Service Daemon Design](service-daemon/README.md) — Background service architecture and roadmap
- [FFI Tutorials](tutorials/native-ffi/) — JavaScript and Python interop guides
- [Roadmap](roadmap.md) — Planned features and project direction
- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md) — Performance data and comparison

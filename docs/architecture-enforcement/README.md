# Architecture Enforcement: Single Authoritative API Access Layer

Enforce a strict architecture where **only CSharpDB.Api** communicates with the database engine. All other consumers (CLI, Admin, MCP, external clients) go through HTTP using a new `CSharpDB.Client` SDK.

---

## Motivation

CSharpDB is a 12-project .NET 10.0 solution. Today, multiple projects access the database engine through different paths:

- **CLI** references `CSharpDB.Engine` directly
- **Admin** references `CSharpDB.Service` (in-process)
- **MCP** references `CSharpDB.Service` (in-process)
- **API** references `CSharpDB.Service` (the intended gateway)

This creates uncontrolled access paths to the storage layer, making it impossible to enforce cross-cutting concerns (auth, rate limiting, audit logging, multi-tenancy) in a single place.

**Scope:** Core architectural enforcement — SDK creation, missing API endpoints, consumer refactoring. Auth, multi-tenancy, and advanced features are deferred to follow-up work.

---

## Before / After Architecture

```
BEFORE (multiple direct access paths)

┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│   CLI   │  │  Admin  │  │   MCP   │  │   API   │
└────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
     │            │            │            │
  Engine       Service      Service      Service
     │            │            │            │
     └────────────┴────────────┴────────────┘
                        │
                   CSharpDB Engine
```

```
AFTER (single gateway)

┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────────┐
│   CLI   │  │  Admin  │  │   MCP   │  │ External App │
└────┬────┘  └────┬────┘  └────┬────┘  └──────┬───────┘
     │            │            │               │
     └────────────┴─────┬──────┴───────────────┘
                        │  HTTP (CSharpDB.Client SDK)
                        ▼
                 ┌─────────────┐
                 │  CSharpDB   │
                 │    .Api     │  ← sole gateway
                 └──────┬──────┘
                        │
                    Service + Data (ADO.NET)
                        │
                   CSharpDB Engine
```

---

## Updated Project Reference Graph

```
Core (no deps)                              UNCHANGED
├── Storage → Core                          UNCHANGED
│   └── Storage.Diagnostics → Core,Storage  UNCHANGED
├── Sql → Core                              UNCHANGED
└── Execution → Core, Sql, Storage          UNCHANGED

Engine → Core, Storage, Sql, Execution      UNCHANGED
Data → Engine                               UNCHANGED (server-side ADO.NET only)
Service → Data, Storage.Diagnostics         MODIFIED  (add collection/checkpoint methods)
Api → Service                               MODIFIED  (new endpoints, versioning)

Client → (none — only System.Net.Http)      NEW PROJECT (HTTP SDK)

Cli → Client, Sql, Api                      MODIFIED  (remote client + `serve` launcher)
Admin → Client                              MODIFIED  (HTTP consumer)
Mcp → Client                                MODIFIED  (HTTP consumer)
```

---

## Implementation Plan

### Phase 1: New API Endpoints

The current API is missing features that consumers need. Add these before creating the client SDK so the SDK has a complete surface to wrap.

#### 1A. Transaction Endpoints

```
POST   /api/v1/transactions              → { transactionId, expiresAt }
POST   /api/v1/transactions/{id}/execute → body: { sql } → SqlResultResponse
POST   /api/v1/transactions/{id}/commit  → 204
POST   /api/v1/transactions/{id}/rollback → 204
```

**Design:** A `TransactionManager` service holds a `ConcurrentDictionary<string, TransactionSession>`. Each session owns a dedicated `CSharpDbConnection` + `DbTransaction` with a `SemaphoreSlim` for serialized access. A background timer auto-rolls-back sessions exceeding a configurable timeout (default: 60s). This is independent of `CSharpDbService` — no changes to the shared singleton.

**New files:**
- `src/CSharpDB.Api/Endpoints/TransactionEndpoints.cs`
- `src/CSharpDB.Api/Services/TransactionManager.cs`
- `src/CSharpDB.Api/Dtos/TransactionDtos.cs`

#### 1B. Collection Endpoints (Document API)

```
GET    /api/v1/collections                       → ["col1", "col2"]
GET    /api/v1/collections/{name}/count           → { count }
GET    /api/v1/collections/{name}/{key}           → { key, document }
PUT    /api/v1/collections/{name}/{key}           → body: {document} → 204
DELETE /api/v1/collections/{name}/{key}           → 204
GET    /api/v1/collections/{name}?page&pageSize   → paginated scan
```

**Service changes:** Add to `CSharpDbService`:
- `GetCollectionNamesAsync()` — delegates to engine's `GetCollectionNames()`
- `GetDocumentAsync(collection, key)` — uses `Collection<JsonElement>`
- `PutDocumentAsync(collection, key, JsonElement)`
- `DeleteDocumentAsync(collection, key)`
- `GetCollectionCountAsync(collection)`
- `BrowseCollectionAsync(collection, page, pageSize)`

**New files:**
- `src/CSharpDB.Api/Endpoints/CollectionEndpoints.cs`
- `src/CSharpDB.Api/Dtos/CollectionDtos.cs`

#### 1C. Admin Endpoints

```
POST   /api/v1/admin/checkpoint   → 204
GET    /api/v1/admin/info         → { dataSource, tables, indexes, views, triggers }
```

**Service changes:** Add `CheckpointAsync()` to `CSharpDbService`.

**New file:** `src/CSharpDB.Api/Endpoints/AdminEndpoints.cs`

#### 1D. Schema Change Events (SSE)

```
GET    /api/v1/events/schema   → Server-Sent Events stream
```

Converts existing `CSharpDbService.TablesChanged`/`SchemaChanged` events into an SSE stream using `Channel<T>`.

**New file:** `src/CSharpDB.Api/Endpoints/EventEndpoints.cs`

#### 1E. API Versioning

Change `app.MapGroup("/api")` → `app.MapGroup("/api/v1")` in `Program.cs`. All existing and new endpoints live under `/api/v1/`.

**Modified file:** `src/CSharpDB.Api/Program.cs`

---

### Phase 2: Create CSharpDB.Client (HTTP SDK)

A new project with zero dependency on any CSharpDB internal projects. Only depends on `System.Net.Http` and `System.Text.Json`.

#### Interface: `ICSharpDbClient`

```csharp
public interface ICSharpDbClient : IAsyncDisposable
{
    // Schema
    Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default);
    Task<TableSchemaDto?> GetTableSchemaAsync(string tableName, CancellationToken ct = default);
    Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default);
    Task<IReadOnlyList<IndexDto>> GetIndexesAsync(CancellationToken ct = default);
    Task<IReadOnlyList<ViewDto>> GetViewsAsync(CancellationToken ct = default);
    Task<ViewDto?> GetViewAsync(string viewName, CancellationToken ct = default);
    Task<IReadOnlyList<TriggerDto>> GetTriggersAsync(CancellationToken ct = default);
    Task<DatabaseInfoDto> GetInfoAsync(CancellationToken ct = default);

    // DDL - Tables
    Task DropTableAsync(string tableName, CancellationToken ct = default);
    Task RenameTableAsync(string tableName, string newName, CancellationToken ct = default);
    Task AddColumnAsync(string tableName, string columnName, string type, bool notNull, CancellationToken ct = default);
    Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default);
    Task RenameColumnAsync(string tableName, string columnName, string newName, CancellationToken ct = default);

    // DDL - Indexes, Views, Triggers
    Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default);
    Task DropIndexAsync(string indexName, CancellationToken ct = default);
    Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default);
    Task DropViewAsync(string viewName, CancellationToken ct = default);
    Task CreateTriggerAsync(string triggerName, string tableName, string timing, string triggerEvent, string bodySql, CancellationToken ct = default);
    Task DropTriggerAsync(string triggerName, CancellationToken ct = default);
    // Update variants follow the same pattern

    // Data
    Task<BrowseResultDto> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default);
    Task<BrowseResultDto> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default);
    Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, string pkValue, CancellationToken ct = default);
    Task<MutationResultDto> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default);
    Task<MutationResultDto> UpdateRowAsync(string tableName, string pkColumn, string pkValue, Dictionary<string, object?> values, CancellationToken ct = default);
    Task<MutationResultDto> DeleteRowAsync(string tableName, string pkColumn, string pkValue, CancellationToken ct = default);

    // SQL
    Task<SqlResultDto> ExecuteSqlAsync(string sql, CancellationToken ct = default);

    // Transactions
    Task<TransactionDto> BeginTransactionAsync(CancellationToken ct = default);
    Task<SqlResultDto> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default);
    Task CommitTransactionAsync(string transactionId, CancellationToken ct = default);
    Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default);

    // Collections (Document API)
    Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default);
    Task<JsonElement?> GetDocumentAsync(string collection, string key, CancellationToken ct = default);
    Task PutDocumentAsync(string collection, string key, JsonElement document, CancellationToken ct = default);
    Task DeleteDocumentAsync(string collection, string key, CancellationToken ct = default);

    // Admin
    Task CheckpointAsync(CancellationToken ct = default);

    // Diagnostics
    Task<JsonElement> InspectStorageAsync(bool includePages = false, CancellationToken ct = default);
    Task<JsonElement> InspectWalAsync(CancellationToken ct = default);
    Task<JsonElement> InspectPageAsync(uint pageId, bool includeHex = false, CancellationToken ct = default);
    Task<JsonElement> CheckIndexesAsync(string? indexName = null, int? sampleSize = null, CancellationToken ct = default);

    // Events
    IAsyncEnumerable<SchemaChangeEvent> SubscribeSchemaChangesAsync(CancellationToken ct = default);
}
```

#### Implementation: `CSharpDbClient`

- Constructor takes `HttpClient` (supports `IHttpClientFactory` and DI patterns)
- All methods: serialize request, HTTP call, deserialize response, throw `CSharpDbClientException` on non-success
- SSE subscription uses `HttpClient.GetStreamAsync` + line-by-line parsing

#### DTO Models (`CSharpDB.Client.Models/`)

These are the **only** types consumers interact with. No dependency on `CSharpDB.Core`:

- `TableSchemaDto`, `ColumnDto`
- `IndexDto`, `ViewDto`, `TriggerDto`
- `BrowseResultDto`, `SqlResultDto`, `MutationResultDto`
- `TransactionDto`, `DatabaseInfoDto`
- `SchemaChangeEvent`

#### SDK Usage Example

```csharp
// DI registration
builder.Services.AddHttpClient<ICSharpDbClient, CSharpDbClient>(client =>
    client.BaseAddress = new Uri("http://localhost:5000/api/v1"));

// Usage
public class MyService(ICSharpDbClient db)
{
    public async Task DoWork()
    {
        var tables = await db.GetTableNamesAsync();

        var result = await db.ExecuteSqlAsync("SELECT * FROM users WHERE age > 25");

        var txn = await db.BeginTransactionAsync();
        await db.ExecuteInTransactionAsync(txn.TransactionId, "INSERT INTO logs (msg) VALUES ('hello')");
        await db.CommitTransactionAsync(txn.TransactionId);
    }
}
```

#### New files:
- `src/CSharpDB.Client/CSharpDB.Client.csproj`
- `src/CSharpDB.Client/ICSharpDbClient.cs`
- `src/CSharpDB.Client/CSharpDbClient.cs`
- `src/CSharpDB.Client/CSharpDbClientException.cs`
- `src/CSharpDB.Client/Models/*.cs`

---

### Phase 3: Add `serve` Subcommand to CLI

The CLI gains a `csharpdb serve <dbfile>` subcommand that starts the API server:

```bash
# Start the API server
csharpdb serve mydata.db --port 5001

# Connect REPL to a running server
csharpdb --server http://localhost:5001

# Or via environment variable
CSHARPDB_SERVER=http://localhost:5001 csharpdb
```

When `args[0]` is `serve`:
1. Build a minimal `WebApplication` hosting the API endpoints
2. Bind to a configurable port (default: 5000)
3. Print the URL and block until Ctrl+C

The `serve` subcommand is the **only** path that touches the engine. The REPL itself is purely an HTTP client.

**Project references for CLI:**
- `CSharpDB.Client` (REPL mode)
- `CSharpDB.Api` (`serve` mode — hosts API in-process)
- `CSharpDB.Sql` (tokenizer for multi-line input detection)

**New file:** `src/CSharpDB.Cli/ServerCommand.cs`
**Modified:** `src/CSharpDB.Cli/Program.cs`

---

### Phase 4: Refactor CLI to Use HTTP Client

Replace all direct `Database` usage in the REPL with `ICSharpDbClient` calls.

#### MetaCommandContext changes:
- Remove `Database` property, replace with `ICSharpDbClient Client`
- Remove `ReaderSession` / snapshot support (not available over HTTP)
- Transaction methods delegate to `Client.BeginTransactionAsync()` etc.
- `ExecuteSqlAsync` delegates to `Client.ExecuteSqlAsync()`

#### Command-by-command migration:

| Command | Current | New |
|---------|---------|-----|
| SQL execution | `Database.ExecuteAsync(statement)` | `Client.ExecuteSqlAsync(sql)` |
| `.tables` | `Database.GetTableNames()` | `Client.GetTableNamesAsync()` |
| `.schema` | `Database.GetTableSchema(name)` | `Client.GetTableSchemaAsync(name)` |
| `.indexes` | `Database.GetIndexes()` | `Client.GetIndexesAsync()` |
| `.views` | `Database.GetViewNames()` | `Client.GetViewsAsync()` |
| `.view <n>` | `Database.GetViewSql(name)` | `Client.GetViewAsync(name)` |
| `.triggers` | `Database.GetTriggers()` | `Client.GetTriggersAsync()` |
| `.collections` | `Database.GetCollectionNames()` | `Client.GetCollectionNamesAsync()` |
| `.begin` | `Database.BeginTransactionAsync()` | `Client.BeginTransactionAsync()` (store txn ID) |
| `.commit` | `Database.CommitAsync()` | `Client.CommitTransactionAsync(txnId)` |
| `.rollback` | `Database.RollbackAsync()` | `Client.RollbackTransactionAsync(txnId)` |
| `.checkpoint` | `Database.CheckpointAsync()` | `Client.CheckpointAsync()` |
| `.snapshot` | `Database.CreateReaderSession()` | Print "Not available in HTTP mode" |
| `.syncpoint` | `Database.PreferSyncPointLookups` | Print "Not available in HTTP mode" |
| `.info` | Direct engine properties | `Client.GetInfoAsync()` |
| `.read <file>` | Parse + execute per statement | `Client.ExecuteSqlAsync()` per statement |
| Inspector cmds | `DatabaseInspector` directly | `Client.InspectStorageAsync()` etc. |

When in a transaction, SQL is sent via `Client.ExecuteInTransactionAsync(txnId, sql)`.

**Files to modify:**
- `src/CSharpDB.Cli/CSharpDB.Cli.csproj`
- `src/CSharpDB.Cli/Program.cs`
- `src/CSharpDB.Cli/MetaCommandContext.cs`
- `src/CSharpDB.Cli/Repl.cs`
- `src/CSharpDB.Cli/MetaCommands.cs`
- `src/CSharpDB.Cli/InspectorCommandRunner.cs`

---

### Phase 5: Refactor Admin to Use HTTP Client

Replace `CSharpDbService` injection with `ICSharpDbClient` in all Blazor components. Admin runs as a separate process connecting over HTTP.

#### Program.cs changes:
```csharp
// BEFORE
builder.Services.AddSingleton<CSharpDbService>();

// AFTER
builder.Services.AddHttpClient<ICSharpDbClient, CSharpDbClient>(client =>
    client.BaseAddress = new Uri(builder.Configuration["CSharpDbApi:BaseUrl"]
        ?? "http://localhost:5000"));
```

#### Schema change notifications:
A new `SchemaWatcherService` replaces direct event subscriptions:
1. Connects to `GET /api/v1/events/schema` SSE endpoint
2. Raises local `SchemaChanged`/`TablesChanged` events
3. Components subscribe to this service instead of `CSharpDbService`

#### Component migration:

| Component | Calls to migrate |
|-----------|-----------------|
| `NavMenu.razor` | `GetTableNamesAsync`, `GetViewNamesAsync`, `GetIndexesAsync`, `GetTriggersAsync`, event subscriptions |
| `QueryTab.razor` | `ExecuteSqlAsync` |
| `DataTab.razor` | `BrowseTableAsync`, `BrowseViewAsync`, `GetTableSchemaAsync`, row CRUD |
| `StorageTab.razor` | `InspectStorageAsync`, `CheckWalAsync`, `InspectPageAsync`, `CheckIndexesAsync` |

`CSharpDB.Core` types (`IndexSchema`, `TriggerSchema`) are replaced with client DTOs (`IndexDto`, `TriggerDto`).

**Project reference change:** Remove `CSharpDB.Service`, add `CSharpDB.Client`.

**New file:** `src/CSharpDB.Admin/Services/SchemaWatcherService.cs`

---

### Phase 6: Refactor MCP to Use HTTP Client

Replace `CSharpDbService` with `ICSharpDbClient` in all MCP tools.

```csharp
// BEFORE
builder.Services.AddSingleton<CSharpDbService>();

// AFTER  (priority: --server arg > CSHARPDB_SERVER env var > default)
string serverUrl = /* resolve from args/env */ "http://localhost:5000";
builder.Services.AddSingleton<ICSharpDbClient>(
    _ => new CSharpDbClient(new HttpClient { BaseAddress = new Uri(serverUrl) }));
```

Every MCP tool class changes its injected dependency from `CSharpDbService` to `ICSharpDbClient`. Method signatures are nearly identical — mostly a 1:1 rename with DTO type changes.

**Project reference change:** Remove `CSharpDB.Service`, add `CSharpDB.Client`.

---

### Phase 7: Cleanup and Verification

1. Grep all `.csproj` files to confirm CLI/Admin/MCP do **not** reference `CSharpDB.Engine`, `CSharpDB.Storage`, or `CSharpDB.Data` (except CLI referencing `CSharpDB.Api` for serve mode)
2. Update `CSharpDB.slnx` to include `CSharpDB.Client`
3. Update test projects (`CSharpDB.Cli.Tests`)
4. Consider a new `CSharpDB.Client.Tests` project for integration tests using `WebApplicationFactory<Program>`

---

## API Endpoint Examples

```bash
# List tables
curl http://localhost:5000/api/v1/tables

# Execute SQL
curl -X POST http://localhost:5000/api/v1/sql/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE age > 25"}'

# Start a transaction
curl -X POST http://localhost:5000/api/v1/transactions
# → {"transactionId": "abc-123", "expiresAt": "2025-01-01T00:01:00Z"}

# Execute within transaction
curl -X POST http://localhost:5000/api/v1/transactions/abc-123/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users (name, age) VALUES ('\''Alice'\'', 30)"}'

# Commit
curl -X POST http://localhost:5000/api/v1/transactions/abc-123/commit

# Put a document
curl -X PUT http://localhost:5000/api/v1/collections/sessions/user42 \
  -H "Content-Type: application/json" \
  -d '{"lastLogin": "2025-01-01", "preferences": {"theme": "dark"}}'

# Subscribe to schema changes (SSE)
curl -N http://localhost:5000/api/v1/events/schema

# WAL checkpoint
curl -X POST http://localhost:5000/api/v1/admin/checkpoint

# Storage inspection
curl http://localhost:5000/api/v1/inspect?includePages=false
```

---

## Updated Project Structure

```
src/
  CSharpDB.Core/                     UNCHANGED
  CSharpDB.Storage/                  UNCHANGED
  CSharpDB.Storage.Diagnostics/      UNCHANGED
  CSharpDB.Sql/                      UNCHANGED
  CSharpDB.Execution/                UNCHANGED
  CSharpDB.Engine/                   UNCHANGED
  CSharpDB.Data/                     UNCHANGED (server-side ADO.NET provider)
  CSharpDB.Service/                  MODIFIED  (+collections, +checkpoint)
  CSharpDB.Api/                      MODIFIED
    Endpoints/
      TableEndpoints.cs              existing
      RowEndpoints.cs                existing
      IndexEndpoints.cs              existing
      ViewEndpoints.cs               existing
      TriggerEndpoints.cs            existing
      SqlEndpoints.cs                existing
      SchemaEndpoints.cs             existing
      InspectEndpoints.cs            existing
      TransactionEndpoints.cs        NEW
      CollectionEndpoints.cs         NEW
      AdminEndpoints.cs              NEW
      EventEndpoints.cs              NEW
    Services/
      TransactionManager.cs          NEW
    Dtos/                            existing (extend)
    Middleware/                       existing
  CSharpDB.Client/                   NEW PROJECT
    ICSharpDbClient.cs
    CSharpDbClient.cs
    CSharpDbClientException.cs
    Models/
      TableSchemaDto.cs
      SqlResultDto.cs
      BrowseResultDto.cs
      TransactionDto.cs
      CollectionDtos.cs
      DiagnosticDtos.cs
      SchemaChangeEvent.cs
  CSharpDB.Cli/                      MODIFIED (HTTP client + serve subcommand)
  CSharpDB.Admin/                    MODIFIED (HTTP client consumer)
    Services/
      SchemaWatcherService.cs        NEW
  CSharpDB.Mcp/                      MODIFIED (HTTP client consumer)

tests/
  CSharpDB.Tests/                    existing
  CSharpDB.Data.Tests/               existing
  CSharpDB.Cli.Tests/                MODIFIED
  CSharpDB.Benchmarks/               existing
```

---

## Risks and Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| HTTP latency for CLI REPL | ~1-5ms per query vs ~0.1ms in-process | Acceptable for interactive use. `serve` keeps it local. |
| Transaction session leaks | Orphaned HTTP transactions hold locks | `TransactionManager` auto-rollback timer (60s default) |
| Result set buffering | Large SELECTs fully materialized in JSON | Add `maxRows` limit. Streaming can be added later. |
| SSE connection reliability | Admin loses real-time updates if SSE drops | Auto-reconnect with exponential backoff in `SchemaWatcherService` |
| CLI `serve` couples CLI to Api | CLI references both Client and Api | Acceptable. `serve` is convenience; REPL only uses Client. |
| Breaking change for CLI users | `csharpdb mydata.db` no longer works directly | Migration: `csharpdb serve mydata.db` + `csharpdb --server ...` |
| Admin loses `CSharpDB.Core` types | Components use `IndexSchema`, `TriggerSchema` | Client DTOs (`IndexDto`, `TriggerDto`) replace these 1:1 |
| JSON type coercion | JSON has no integer vs long, no blob | API already handles this via `JsonHelper.CoerceDictionary` |

---

## Verification Plan

1. `dotnet build CSharpDB.slnx` — all projects compile
2. `dotnet test` — all existing tests pass
3. Start API, hit all endpoints via curl/Scalar UI
4. `csharpdb serve test.db --port 5001`, then `csharpdb --server http://localhost:5001` — run `.tables`, SQL, `.begin`/`.commit`
5. Start Admin pointing at API URL — verify sidebar, query tab, storage tab
6. Run MCP server with `--server` — verify tools via MCP inspector
7. Grep `.csproj` files to confirm CLI/Admin/MCP don't reference Engine/Storage/Data/Service directly

---

## See Also

- [Architecture Guide](../architecture.md) — Current engine layer overview
- [Roadmap](../roadmap.md) — Project direction and priorities
- [REST API Reference](../rest-api.md) — Existing HTTP endpoint documentation

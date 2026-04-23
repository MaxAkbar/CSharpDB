# CSharpDB.Api

`CSharpDB.Api` is the REST/HTTP host for CSharpDB.

It is a thin ASP.NET Core layer over `CSharpDB.Client`. Requests are handled
through `ICSharpDbClient`, which currently uses the direct engine-backed client
under the hood.

gRPC is not hosted here. The dedicated gRPC host lives in
`CSharpDB.Daemon`.

## What This Project Is For

Use this project when you want to:

- expose a local CSharpDB database over HTTP
- test the database through a browser-based API UI
- integrate with tools that prefer REST over direct embedded access
- inspect database, WAL, and index state remotely

Use `CSharpDB.Client` directly when you are writing an in-process consumer and do
not need HTTP.

## Architecture

The API host is intentionally thin:

- ASP.NET Core provides routing, hosting, JSON serialization, and middleware
- `CSharpDB.Client` is the authoritative database API
- `ICSharpDbClient` is registered at startup from configuration
- the client is warmed up during startup with `GetInfoAsync()` so configuration
  and database initialization failures happen early

Current request flow:

1. HTTP request hits an endpoint under `/api`
2. the endpoint resolves `ICSharpDbClient` from DI
3. the client executes against the configured database
4. the endpoint maps client models to HTTP response DTOs
5. exceptions are translated to `application/problem+json`

## Configuration

The API reads the database connection string from `ConnectionStrings:CSharpDB`.

Default `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  }
}
```

If no connection string is configured, the API falls back to:

```text
Data Source=csharpdb.db
```

That means a local `csharpdb.db` file is used by default.

## Running Locally

Start the API:

```powershell
dotnet run --project src/CSharpDB.Api/CSharpDB.Api.csproj
```

With the default launch profile, the local URLs are:

- `https://localhost:61819`
- `http://localhost:61818`

The launch profile also sets:

- `ASPNETCORE_ENVIRONMENT=Development`

## Testing Through The Browser UI

In Development, the project exposes:

- Scalar UI at `/scalar`
- OpenAPI JSON at `/openapi/v1.json`

Open one of these after starting the API:

- `http://localhost:61818/scalar`
- `https://localhost:61819/scalar`

Raw OpenAPI document:

- `http://localhost:61818/openapi/v1.json`
- `https://localhost:61819/openapi/v1.json`

Notes:

- `launchBrowser` is disabled, so the browser does not open automatically
- Scalar is only mapped in Development
- if you run the built executable directly, make sure `ASPNETCORE_ENVIRONMENT`
  is set to `Development` if you want the UI

## JSON Conventions

The API uses:

- camelCase JSON property names
- camelCase enum serialization
- null values omitted when writing responses

Examples:

- `before`, `after` for trigger timing
- `insert`, `update`, `delete` for trigger event values in JSON

## CORS

The API currently allows all origins, methods, and headers.

This is useful for local testing but is not a hardened production policy.

## Authentication

There is currently no authentication or authorization in this project.

Anyone who can reach the API can execute database operations.

## Endpoint Overview

All routes are under `/api`.

### Database Info

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/info` | Returns top-level database counts and data source information. |

### Tables And Columns

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/tables` | List table names. |
| `GET` | `/api/tables/{name}/schema` | Get a table schema. |
| `GET` | `/api/tables/{name}/count` | Get row count for a table. |
| `DELETE` | `/api/tables/{name}` | Drop a table. |
| `PATCH` | `/api/tables/{name}/rename` | Rename a table. |
| `POST` | `/api/tables/{name}/columns` | Add a column. |
| `DELETE` | `/api/tables/{name}/columns/{col}` | Drop a column. |
| `PATCH` | `/api/tables/{name}/columns/{col}/rename` | Rename a column. |

### Rows

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/tables/{name}/rows?page=1&pageSize=50` | Browse rows in a table. |
| `GET` | `/api/tables/{name}/rows/{pkValue}?pkColumn=id` | Get a row by primary key value. |
| `POST` | `/api/tables/{name}/rows` | Insert a row. |
| `PUT` | `/api/tables/{name}/rows/{pkValue}?pkColumn=id` | Update a row by primary key. |
| `DELETE` | `/api/tables/{name}/rows/{pkValue}?pkColumn=id` | Delete a row by primary key. |

### Indexes

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/indexes` | List indexes. |
| `POST` | `/api/indexes` | Create an index. |
| `PUT` | `/api/indexes/{name}` | Update an index definition. |
| `DELETE` | `/api/indexes/{name}` | Drop an index. |

### Views

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/views` | List views. |
| `GET` | `/api/views/{name}` | Get a view definition. |
| `GET` | `/api/views/{name}/rows?page=1&pageSize=50` | Browse rows from a view. |
| `POST` | `/api/views` | Create a view. |
| `PUT` | `/api/views/{name}` | Update a view. |
| `DELETE` | `/api/views/{name}` | Drop a view. |

### Triggers

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/triggers` | List triggers. |
| `POST` | `/api/triggers` | Create a trigger. |
| `PUT` | `/api/triggers/{name}` | Update a trigger. |
| `DELETE` | `/api/triggers/{name}` | Drop a trigger. |

Accepted trigger values:

- `timing`: `before`, `after`
- `event`: `insert`, `update`, `delete`

### Procedures

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/procedures?includeDisabled=true` | List procedures. |
| `GET` | `/api/procedures/{name}` | Get a procedure definition. |
| `POST` | `/api/procedures` | Create a procedure. |
| `PUT` | `/api/procedures/{name}` | Update a procedure. |
| `DELETE` | `/api/procedures/{name}` | Delete a procedure. |
| `POST` | `/api/procedures/{name}/execute` | Execute a procedure. |

### Saved Queries

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/saved-queries` | List saved queries. |
| `GET` | `/api/saved-queries/{name}` | Get a saved query. |
| `PUT` | `/api/saved-queries/{name}` | Create or update a saved query. |
| `DELETE` | `/api/saved-queries/{name}` | Delete a saved query. |

### SQL

| Method | Route | Description |
| --- | --- | --- |
| `POST` | `/api/sql/execute` | Execute arbitrary SQL. |

This is the main way to create tables today.

### Transactions

| Method | Route | Description |
| --- | --- | --- |
| `POST` | `/api/transactions` | Begin a client-managed transaction session. |
| `POST` | `/api/transactions/{id}/execute` | Execute SQL inside a transaction. |
| `POST` | `/api/transactions/{id}/commit` | Commit a transaction. |
| `POST` | `/api/transactions/{id}/rollback` | Roll back a transaction. |

### Collections

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/collections` | List document collections. |
| `GET` | `/api/collections/{name}/count` | Get collection document count. |
| `GET` | `/api/collections/{name}?page=1&pageSize=50` | Browse collection documents. |
| `GET` | `/api/collections/{name}/document?key=...` | Get one document by key. |
| `PUT` | `/api/collections/{name}/document?key=...` | Put one document by key. |
| `DELETE` | `/api/collections/{name}/document?key=...` | Delete one document by key. |

### ETL Pipelines

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/etl/pipelines?limit=100` | List stored pipeline definitions. |
| `GET` | `/api/etl/pipelines/{name}` | Get a stored pipeline definition. |
| `GET` | `/api/etl/pipelines/{name}/revisions?limit=25` | List stored pipeline revisions. |
| `GET` | `/api/etl/pipelines/{name}/revisions/{revision}` | Get one stored pipeline revision. |
| `PUT` | `/api/etl/pipelines/{name}` | Save a pipeline definition. |
| `DELETE` | `/api/etl/pipelines/{name}` | Delete a stored pipeline. |
| `POST` | `/api/etl/pipelines/{name}/run?mode=Run` | Run a stored pipeline. |
| `POST` | `/api/etl/validate` | Validate an inline pipeline package. |
| `POST` | `/api/etl/run` | Run, dry-run, validate, or resume an inline pipeline package. |
| `GET` | `/api/etl/runs?limit=50` | List pipeline runs. |
| `GET` | `/api/etl/runs/{runId}` | Get one pipeline run. |
| `GET` | `/api/etl/runs/{runId}/package` | Get the package captured for a run. |
| `GET` | `/api/etl/runs/{runId}/rejects` | List rejected rows for a run. |
| `POST` | `/api/etl/runs/{runId}/resume` | Resume a run from its checkpoint. |

### Storage Inspection

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/inspect?includePages=false&path=...` | Inspect database storage. |
| `GET` | `/api/inspect/wal?path=...` | Inspect the WAL. |
| `GET` | `/api/inspect/page/{id}?hex=false&path=...` | Inspect a page. |
| `GET` | `/api/inspect/indexes?index=...&sample=...&path=...` | Check indexes. |

### Maintenance

| Method | Route | Description |
| --- | --- | --- |
| `POST` | `/api/maintenance/checkpoint` | Checkpoint the WAL. |
| `POST` | `/api/maintenance/backup` | Write a committed snapshot backup. |
| `POST` | `/api/maintenance/restore` | Validate or restore a database snapshot. |
| `POST` | `/api/maintenance/migrate-foreign-keys` | Validate or retrofit foreign-key metadata. |
| `GET` | `/api/maintenance/report` | Get a maintenance and space-usage report. |
| `POST` | `/api/maintenance/reindex` | Rebuild indexes. |
| `POST` | `/api/maintenance/vacuum` | Rewrite the database to reclaim free pages. |

## Request Examples

### Create A Table

```http
POST /api/sql/execute
Content-Type: application/json
```

```json
{
  "sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);"
}
```

### Insert A Row

```http
POST /api/tables/users/rows
Content-Type: application/json
```

```json
{
  "values": {
    "id": 1,
    "name": "Max",
    "email": "max@example.com"
  }
}
```

### Browse Rows

```http
GET /api/tables/users/rows?page=1&pageSize=50
```

### Create A View

```http
POST /api/views
Content-Type: application/json
```

```json
{
  "viewName": "active_users",
  "selectSql": "SELECT id, name FROM users;"
}
```

### Create A Trigger

```http
POST /api/triggers
Content-Type: application/json
```

```json
{
  "triggerName": "users_before_insert",
  "tableName": "users",
  "timing": "before",
  "event": "insert",
  "bodySql": "SELECT 1;"
}
```

### Create A Procedure

```http
POST /api/procedures
Content-Type: application/json
```

```json
{
  "name": "get_user_by_id",
  "bodySql": "SELECT * FROM users WHERE id = @id;",
  "parameters": [
    {
      "name": "id",
      "type": "Integer",
      "required": true
    }
  ],
  "description": "Returns one user by id",
  "isEnabled": true
}
```

### Execute A Procedure

```http
POST /api/procedures/get_user_by_id/execute
Content-Type: application/json
```

```json
{
  "args": {
    "id": 1
  }
}
```

## Response Shapes

Some common response shapes:

- `BrowseResponse`: paged tabular data with column names
- `MutationResponse`: rows affected
- `SqlResultResponse`: query or non-query SQL execution result
- `ProcedureExecutionResponse`: multi-statement procedure execution details
- `CollectionBrowseResult`: paged collection documents
- `PipelineRunResult`: ETL execution state, metrics, rejects, and checkpoints
- `DatabaseInfoResponse`: top-level object counts

See `Dtos/Requests.cs`, `Dtos/Responses.cs`, `Dtos/ProcedureDtos.cs`, and
`Dtos/PipelineDtos.cs` for the current source-of-truth contract types.

## Error Handling

Errors are returned as `application/problem+json`.

Example:

```json
{
  "status": 404,
  "title": "NotFound",
  "detail": "Table 'users' not found."
}
```

Current status mapping:

- `400 BadRequest`
  - invalid request arguments
  - SQL syntax errors
  - type mismatch errors
  - client configuration errors
- `404 NotFound`
  - missing tables
  - missing columns
  - missing triggers
  - endpoint-specific missing resources
- `409 Conflict`
  - duplicate keys
  - existing tables
  - existing triggers
- `422 UnprocessableEntity`
  - constraint violations
- `503 ServiceUnavailable`
  - busy database
- `500 InternalServerError`
  - unexpected runtime failures

## Development Notes

- the API is currently mapped under `/api`, not `/api/v1`
- there is no dedicated endpoint yet for creating tables outside raw SQL
- the API uses the same authoritative client contract as other consumers
- the API host is suitable for local development and integration testing, but it
  is not yet production-hardened

## Useful Commands

Build:

```powershell
dotnet build src/CSharpDB.Api/CSharpDB.Api.csproj
```

Run:

```powershell
dotnet run --project src/CSharpDB.Api/CSharpDB.Api.csproj
```

Run API tests:

```powershell
dotnet test tests/CSharpDB.Api.Tests/CSharpDB.Api.Tests.csproj
```

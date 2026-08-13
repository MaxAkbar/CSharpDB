# CSharpDB.Api

`CSharpDB.Api` is the REST/HTTP host for CSharpDB.

It is a thin ASP.NET Core layer over `CSharpDB.Client`. Requests are handled
through `ICSharpDbClient`, which currently uses the direct engine-backed client
under the hood.

The standalone API host remains supported for REST-only deployments. For new
remote deployments that need both REST and gRPC, prefer
[`CSharpDB.Daemon`](../CSharpDB.Daemon/README.md), which now hosts the same
REST `/api` surface and gRPC from one warm database instance.

## What This Project Is For

Use this project when you want to:

- expose a local CSharpDB database over HTTP without running the daemon
- test the database through a browser-based API UI
- integrate with tools that prefer REST over direct embedded access
- inspect database, WAL, and index state remotely

Use `CSharpDB.Daemon` when REST and gRPC clients should share one long-running
remote database host.

Use `CSharpDB.Client` directly when you are writing an in-process consumer and do
not need HTTP.

## Architecture

The API host is intentionally thin:

- ASP.NET Core provides routing, hosting, JSON serialization, and middleware
- `CSharpDB.Client` is the authoritative database API
- `ICSharpDbClient` is registered at startup from configuration
- the HTTP listener starts first, then a bounded background `GetInfoAsync()`
  probe initializes the database and drives cached readiness state
- the route/middleware setup is shared with `CSharpDB.Daemon` so both hosts
  expose the same REST API surface

Current request flow:

1. HTTP request hits an endpoint under `/api`
2. the endpoint resolves `ICSharpDbClient` from DI
3. the client executes against the configured database
4. the endpoint maps client models to HTTP response DTOs
5. exceptions are translated to `application/problem+json`

## Configuration

The API reads the database connection string from `ConnectionStrings:CSharpDB`.
Optional API-key protection is configured from `CSharpDB:Api:Security`.

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

### Health and readiness

Health routing is enabled by default and is independent of the master
`CSharpDB:Observability:Enabled` switch:

```json
{
  "CSharpDB": {
    "Observability": {
      "Health": {
        "Enabled": true,
        "LivenessPath": "/health/live",
        "ReadinessPath": "/health/ready",
        "ReadinessTimeout": "00:00:02"
      }
    }
  }
}
```

`GET /health/live` and `GET /health/ready` are anonymous orchestration probes.
They read cached process state only, never resolve or call the database, and
return only `{"status":"healthy"}` or `{"status":"unhealthy"}` with `200`
or `503`. Liveness remains healthy when database initialization fails;
readiness stays unhealthy while the background initializer retries. The host
becomes not ready before listener shutdown and non-live after it stops.

`GET /api/diagnostics/health` returns the detailed typed
`HealthDiagnosticsSnapshot` and follows the same fail-closed diagnostics access
policy as other runtime diagnostics. It is available even while the database
is not ready and does not query the database.

A full restore is not ready through replacement and reopen verification.
Mutating foreign-key migration, reindex, and vacuum are not ready while their
exclusive work and bounded reopen verification run. Restore validation,
foreign-key validation, backup, and checkpoint do not change readiness. A
failed full restore remains not ready until a later background probe verifies
the active database; other failed exclusive work remains ready when an
immediate bounded probe proves the database is still available.

Health paths must be canonical, distinct exact paths. They cannot collide with
REST, Prometheus, gRPC, OpenAPI, Scalar, or another mapped endpoint; startup
rejects collisions regardless of mapping order. Disabling health leaves both
minimal paths unmapped.

### Observability and structured logging

The standalone host binds `CSharpDB:Observability`, validates it before database
warmup, gives the same options instance to the direct database, and owns a
`CSharpDbDiagnosticLoggerBridge` for the host lifetime. HTTP requests establish
an `Http` transport scope while preserving ASP.NET Core's inbound `Activity`.

Safe query logging can be enabled as follows:

```json
{
  "CSharpDB": {
    "Observability": {
      "Enabled": true,
      "DatabaseAlias": "primary",
      "Logging": {
        "Enabled": true,
        "Queries": true,
        "SlowQueries": true,
        "SlowQueryThreshold": "00:00:00.500",
        "SlowQueryThresholdOverrides": {
          "Query": "00:00:01"
        },
        "SqlText": "None"
      }
    }
  }
}
```

`CSharpDB.Operational` receives host/lifecycle/API events and `CSharpDB.Query`
receives query completion, slow-query, failure, and cancellation events. Stable
fields include opaque operation/parent ids, operation class/role/outcome,
configured database alias, transport, optional safe session/trace/fingerprint,
durations, row counts, and reviewed `error.code`/`error.type` values.

The default `SqlText` value is `None`. Built-in logs do not include SQL,
parameters, row values, credentials, connection strings, paths, raw exceptions,
or exception messages. `Normalized` capture is an explicit opt-in. `Raw` can
expose SQL literals and emits startup warning
`CSharpDB.Host.RawSqlCaptureEnabled` (event id `1003`) exactly once after the
logging bridge subscribes. Logging-provider failures do not prevent host
startup or change request results.

Safe API rejection/unhandled-error events retain the existing default logging
behavior even when general observability is disabled. Embedding applications
that map the REST surface without registering the typed bridge use the same
stable event id, reviewed template, and safe code/type/trace fields through a
no-throw compatibility logger.

### OpenTelemetry and Prometheus exporters

Hosted exporters are disabled by default and are registered only by the API or
daemon host. The core database, client, and observability packages do not take
an OpenTelemetry exporter dependency. A representative production
configuration is:

```json
{
  "CSharpDB": {
    "Observability": {
      "Enabled": true,
      "OpenTelemetry": {
        "Enabled": true,
        "SamplingRatio": 0.1,
        "Resource": {
          "ServiceName": "orders-api",
          "ServiceNamespace": "CSharpDB",
          "DeploymentEnvironment": "production"
        },
        "Otlp": {
          "Enabled": true
        },
        "Console": {
          "Enabled": false
        }
      },
      "Prometheus": {
        "Enabled": true,
        "Path": "/metrics",
        "AllowInsecureRemoteAccess": false
      }
    },
    "Api": {
      "Security": {
        "Mode": "ApiKey",
        "ApiKey": "replace-with-a-secret",
        "ApiKeyHeaderName": "X-CSharpDB-Api-Key"
      }
    }
  }
}
```

OpenTelemetry uses the `CSharpDB` activity source and meter. Sampling is
parent-based with the configured trace-id ratio, so a remote parent's sampling
decision is preserved. The standalone service-name default is `CSharpDB.Api`;
service version, process-lifetime instance id, and deployment environment are
filled from safe host metadata unless explicitly configured. Console and OTLP
export are separate opt-ins. Configure OTLP destinations and credentials with
the standard `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL`,
`OTEL_EXPORTER_OTLP_HEADERS`, and `OTEL_EXPORTER_OTLP_TIMEOUT` environment
variables rather than storing secrets in this configuration.

Register observability once, after any unkeyed
`CSharpDbObservabilityOptions` instance override; the first
`AddCSharpDbObservability` call fixes the hosted provider shape and later calls
are idempotent. Replacing or mutating the effective options afterward is
rejected before provider startup. A legacy factory/type options registration remains supported
for the pre-host logger/history bridge, but cannot be evaluated early enough to
wire hosted OpenTelemetry or Prometheus services; Prometheus mapping therefore
fails clearly if that legacy registration later enables scraping. Prefer normal
configuration binding or a pre-registered options instance for hosted export.

The canonical span names/attributes, complete metric name/kind/unit/tag schema,
privacy rules, and temporality/version policy are in the
[CSharpDB.Observability contract](../CSharpDB.Observability/README.md#phase-4-trace-and-metric-schema).
The host enablement matrix is:

| Observability | OpenTelemetry | Prometheus | Host behavior |
| --- | --- | --- | --- |
| disabled | disabled | disabled | No telemetry provider, exporter, background exporter service, or scrape route is registered. |
| enabled | disabled | disabled | Configured history/logging may run; no CSharpDB trace/metric provider is registered. |
| enabled | enabled | either | CSharpDB tracing and metrics providers are registered; console and OTLP export still require their own flags. |
| enabled | disabled | enabled | Metrics and the exact protected scrape route are registered without a CSharpDB tracing provider. |

Enabling OpenTelemetry or Prometheus while global observability is disabled is
invalid. Enabling OpenTelemetry without console or OTLP creates in-process
providers but no export destination or outbound exporter loop. The standalone
REST server activity is the parent of the logical CSharpDB query span; health
and metrics infrastructure paths are excluded from ASP.NET Core tracing.

Prometheus is independent: it can be enabled while
`OpenTelemetry:Enabled=false`. Its exact configured path is mapped on the
ordinary Kestrel listener selected by `ASPNETCORE_URLS`; there is no separate
management listener. When API-key mode is enabled, a missing or invalid key
returns `401`. With security mode `None`, only the actual loopback peer is
accepted; forwarded address headers do not grant access. Setting
`AllowInsecureRemoteAccess=true` explicitly permits unauthenticated remote
scrapes and emits a startup warning. A rejected remote peer receives `403`.
When Prometheus is disabled its path is not mapped and returns `404`.

Prometheus paths must be canonical exact paths and cannot collide with REST,
gRPC, OpenAPI, Scalar, or health routes. If a custom path is configured, the
default `/metrics` path remains unmapped. The Prometheus ASP.NET Core exporter
is currently supplied by OpenTelemetry's prerelease exporter package; validate
the scrape and publish gates when upgrading it.

Current support boundary: automatic physical checkpoints and startup WAL
recovery publish metrics but not physical spans. Ownerless path-only static
restore validation/restore, reindex, vacuum, and foreign-key migration calls
have no runtime telemetry identity; database/client-owned operations are the
observable path. The BCL libraries retain their existing trimming/NativeAOT
contract, but this ASP.NET Core host does not make a NativeAOT-hosting claim;
supported publish and package qualification remain release evidence, not an
assumption.

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

Authentication defaults to `None` for backward compatibility. Enable API-key
mode to require a shared key on every REST route under `/api`.

```json
{
  "CSharpDB": {
    "Api": {
      "Security": {
        "Mode": "ApiKey",
        "ApiKey": "replace-with-a-secret",
        "ApiKeyHeaderName": "X-CSharpDB-Api-Key",
        "AllowInsecureRemoteDiagnostics": false,
        "AllowSensitiveQueryDetailAccess": false
      }
    }
  }
}
```

Clients send the same key through `CSharpDbClientOptions`:

```csharp
await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Http,
    Endpoint = "https://db-host",
    ApiKey = "replace-with-a-secret",
});
```

Missing or wrong keys return `401 Unauthorized`. API-key mode is a simple
shared-secret guard; it is not JWT, RBAC, mTLS, or a replacement for TLS
termination and network access controls.

Runtime diagnostics have an additional fail-closed access policy. In
`ApiKey` mode they require the configured key. In `None` mode they are allowed
only when the request has a proven loopback address; null, wildcard, and
non-loopback addresses are denied unless
`AllowInsecureRemoteDiagnostics=true` is an explicit operator choice. Query
detail always also requires `AllowSensitiveQueryDetailAccess=true`, including
in API-key mode. Missing or wrong keys return `401`; policy denials return
`403`.

## Endpoint Overview

All routes are under `/api`.

### Database Info

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/info` | Returns top-level database counts and data source information. |

### Runtime Diagnostics

| Method | Route | Description |
| --- | --- | --- |
| `GET` | `/api/diagnostics/health` | Get cached detailed host liveness/readiness state. |
| `GET` | `/api/diagnostics/runtime` | Get the current runtime summary. |
| `GET` | `/api/diagnostics/queries/active?maximumRecords=100` | Get a capped active-query snapshot. |
| `GET` | `/api/diagnostics/queries/recent?maximumRecords=100` | Get a capped recent-query snapshot. |
| `GET` | `/api/diagnostics/queries/{operationId}/plan` | Get the retained bounded plan summary without replaying SQL. |
| `GET` | `/api/diagnostics/sessions?maximumRecords=100` | Get capped database and host-request session state. |
| `GET` | `/api/diagnostics/queries/{operationId}/detail` | Get separately captured and authorized query detail. |

Diagnostics requests are suppressed from their own query/session observation,
so polling the endpoints does not recursively fill the ledger. Session results
merge the host's in-flight HTTP requests with the underlying database sessions
when runtime diagnostics are enabled. Normal summaries and lists never contain
SQL text, values, connection strings, credentials, or file paths. A client
without the optional diagnostics capability returns `501 Not Implemented`.

`HttpContext.RequestAborted` is forwarded through diagnostics, maintenance,
and storage inspection calls. Cancellation is cooperative: parsing, planning,
and synchronous fast paths may complete before observing it. The API does not
provide a query/session kill endpoint.

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
- `401 Unauthorized`
  - missing or invalid API key when API-key mode is enabled
- `403 Forbidden`
  - diagnostics access denied by the loopback or sensitive-detail policy
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
- `501 NotImplemented`
  - the configured client does not support optional runtime diagnostics
- `503 ServiceUnavailable`
  - busy database
- `500 InternalServerError`
  - unexpected runtime failures

## Development Notes

- the API is currently mapped under `/api`, not `/api/v1`
- there is no dedicated endpoint yet for creating tables outside raw SQL
- the API uses the same authoritative client contract as other consumers
- the standalone API host is suitable for local development, REST-only hosting,
  and integration testing, but it is not a hardened multi-tenant server
- use `CSharpDB.Daemon` when REST should share a warm hosted database instance
  with gRPC

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

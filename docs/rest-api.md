# CSharpDB REST API Reference

The CSharpDB REST API exposes the full database feature set over HTTP, enabling cross-language interoperability. Built with ASP.NET Core Minimal APIs, it includes OpenAPI documentation and an interactive Scalar UI.

## Running the API

```bash
dotnet run --project src/CSharpDB.Api
```

The API starts on `http://localhost:61818` (HTTP) and `https://localhost:61819` (HTTPS).

**Interactive documentation:** Open `http://localhost:61818/scalar/v1` in a browser to explore and test endpoints with the Scalar API explorer.

## Configuration

The default database path is configured in `src/CSharpDB.Api/appsettings.json`:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  }
}
```

CORS is enabled for all origins by default (development convenience). JSON responses use camelCase naming and omit null values.

---

## Endpoints

All endpoints are prefixed with `/api`.

### Database Info

#### `GET /api/info`

Returns a summary of the database.

**Response:**
```json
{
  "dataSource": "csharpdb.db",
  "tableCount": 3,
  "indexCount": 2,
  "viewCount": 1,
  "triggerCount": 1
}
```

---

### Storage Inspection

Read-only physical diagnostics endpoints for `.db` and `.wal` inspection.

#### `GET /api/inspect`

Run a full database file inspection.

**Query parameters:**
- `includePages` (default: `false`) — include per-page decoded details in the response
- `path` (optional) — override database path for this request

#### `GET /api/inspect/wal`

Inspect WAL header/frame/checksum state.

**Query parameters:**
- `path` (optional) — override database path for this request

#### `GET /api/inspect/page/{id}`

Inspect a single page by page id.

**Query parameters:**
- `hex` (default: `false`) — include page hex dump
- `path` (optional) — override database path for this request

#### `GET /api/inspect/indexes`

Validate index metadata and root tree reachability.

**Query parameters:**
- `index` (optional) — check one index by name
- `sample` (optional) — sample size hint for future index validation passes
- `path` (optional) — override database path for this request

Responses follow the diagnostics models documented in [Storage Inspector](storage-inspector.md).

---

### Tables

#### `GET /api/tables`

List all table names.

**Response:**
```json
["users", "orders", "products"]
```

#### `GET /api/tables/{name}/schema`

Get the full schema for a table.

**Response:**
```json
{
  "tableName": "users",
  "columns": [
    { "name": "id", "type": "INTEGER", "isPrimaryKey": true, "isNotNull": false },
    { "name": "name", "type": "TEXT", "isPrimaryKey": false, "isNotNull": true },
    { "name": "age", "type": "INTEGER", "isPrimaryKey": false, "isNotNull": false }
  ]
}
```

#### `GET /api/tables/{name}/count`

Get the row count for a table.

**Response:**
```json
{ "count": 42 }
```

#### `DELETE /api/tables/{name}`

Drop a table.

**Response:** `200 OK` with `{ "message": "Table 'users' dropped." }`

#### `PATCH /api/tables/{name}/rename`

Rename a table.

**Request:**
```json
{ "newName": "customers" }
```

**Response:** `200 OK` with `{ "message": "Table renamed from 'users' to 'customers'." }`

#### `POST /api/tables/{name}/columns`

Add a column to a table.

**Request:**
```json
{ "columnName": "email", "type": "TEXT", "notNull": false }
```

**Response:** `200 OK` with `{ "message": "Column 'email' added to 'users'." }`

#### `DELETE /api/tables/{name}/columns/{col}`

Drop a column.

**Response:** `200 OK` with `{ "message": "Column 'email' dropped from 'users'." }`

#### `PATCH /api/tables/{name}/columns/{col}/rename`

Rename a column.

**Request:**
```json
{ "newName": "full_name" }
```

**Response:** `200 OK` with `{ "message": "Column renamed from 'name' to 'full_name' in 'users'." }`

---

### Rows

#### `GET /api/tables/{name}/rows`

Browse table rows with pagination.

**Query parameters:**
- `page` (default: 1) — Page number
- `pageSize` (default: 50, max: 1000) — Rows per page

**Response:**
```json
{
  "tableName": "users",
  "page": 1,
  "pageSize": 50,
  "totalRows": 3,
  "columns": ["id", "name", "age"],
  "rows": [
    { "id": 1, "name": "Alice", "age": 30 },
    { "id": 2, "name": "Bob", "age": 25 }
  ]
}
```

#### `GET /api/tables/{name}/rows/{pkValue}`

Get a single row by primary key.

**Query parameters:**
- `pkColumn` (default: "id") — Name of the primary key column

**Response:**
```json
{ "id": 1, "name": "Alice", "age": 30 }
```

#### `POST /api/tables/{name}/rows`

Insert a new row.

**Request:**
```json
{ "values": { "id": 4, "name": "Diana", "age": 28 } }
```

**Response:** `201 Created` with `{ "message": "Row inserted into 'users'.", "rowsAffected": 1 }`

#### `PUT /api/tables/{name}/rows/{pkValue}`

Update a row by primary key.

**Query parameters:**
- `pkColumn` (default: "id") — Name of the primary key column

**Request:**
```json
{ "values": { "name": "Diana Updated", "age": 29 } }
```

**Response:** `200 OK` with `{ "message": "Row updated in 'users'.", "rowsAffected": 1 }`

#### `DELETE /api/tables/{name}/rows/{pkValue}`

Delete a row by primary key.

**Query parameters:**
- `pkColumn` (default: "id") — Name of the primary key column

**Response:** `200 OK` with `{ "message": "Row deleted from 'users'.", "rowsAffected": 1 }`

---

### Indexes

#### `GET /api/indexes`

List all indexes.

**Response:**
```json
[
  { "name": "idx_category", "tableName": "products", "columnName": "category", "isUnique": false }
]
```

#### `POST /api/indexes`

Create an index.

**Request:**
```json
{ "indexName": "idx_email", "tableName": "users", "columnName": "email", "isUnique": true }
```

**Response:** `201 Created` with `{ "message": "Index 'idx_email' created." }`

#### `PUT /api/indexes/{name}`

Update (drop and recreate) an index.

**Request:**
```json
{ "newIndexName": "idx_user_email", "tableName": "users", "columnName": "email", "isUnique": true }
```

**Response:** `200 OK`

#### `DELETE /api/indexes/{name}`

Drop an index.

**Response:** `200 OK` with `{ "message": "Index 'idx_email' dropped." }`

---

### Views

#### `GET /api/views`

List all views.

**Response:**
```json
["order_summary", "product_catalog"]
```

#### `GET /api/views/{name}`

Get a view definition.

**Response:**
```json
{
  "viewName": "order_summary",
  "selectSql": "SELECT o.id, c.name, o.total FROM orders o INNER JOIN customers c ON o.customer_id = c.id"
}
```

#### `GET /api/views/{name}/rows`

Browse view results with pagination.

**Query parameters:**
- `page` (default: 1)
- `pageSize` (default: 50, max: 1000)

**Response:** Same shape as table browse (columns + rows).

#### `POST /api/views`

Create a view.

**Request:**
```json
{ "viewName": "expensive_products", "selectSql": "SELECT name, price FROM products WHERE price > 50" }
```

**Response:** `201 Created`

#### `PUT /api/views/{name}`

Update a view (drop and recreate).

**Request:**
```json
{ "newViewName": "expensive_products", "selectSql": "SELECT name, price FROM products WHERE price > 100" }
```

**Response:** `200 OK`

#### `DELETE /api/views/{name}`

Drop a view.

**Response:** `200 OK`

---

### Triggers

#### `GET /api/triggers`

List all triggers.

**Response:**
```json
[
  {
    "name": "trg_update_stock",
    "tableName": "order_items",
    "timing": "After",
    "event": "Insert",
    "bodySql": "UPDATE products SET stock = stock - NEW.quantity WHERE id = NEW.product_id"
  }
]
```

#### `POST /api/triggers`

Create a trigger.

**Request:**
```json
{
  "triggerName": "trg_audit_insert",
  "tableName": "users",
  "timing": "After",
  "event": "Insert",
  "bodySql": "INSERT INTO audit_log VALUES ('INSERT', NEW.name)"
}
```

Timing values: `"Before"`, `"After"`
Event values: `"Insert"`, `"Update"`, `"Delete"`

**Response:** `201 Created`

#### `PUT /api/triggers/{name}`

Update a trigger (drop and recreate).

**Response:** `200 OK`

#### `DELETE /api/triggers/{name}`

Drop a trigger.

**Response:** `200 OK`

---

### SQL Execution

#### `POST /api/sql/execute`

Execute an arbitrary SQL statement.

**Request:**
```json
{ "sql": "SELECT name, price FROM products WHERE price > 10 ORDER BY price DESC" }
```

**Response (query):**
```json
{
  "isQuery": true,
  "columnNames": ["name", "price"],
  "rows": [
    { "name": "Widget", "price": 29.99 },
    { "name": "Gadget", "price": 14.99 }
  ],
  "rowsAffected": 0,
  "elapsed": 1.23
}
```

**Response (mutation):**
```json
{
  "isQuery": false,
  "columnNames": null,
  "rows": null,
  "rowsAffected": 3,
  "elapsed": 0.87
}
```

---

## Error Handling

The API uses standard HTTP status codes and returns structured error responses:

| HTTP Status | CSharpDB Error Code | Meaning |
|-------------|---------------------|---------|
| 400 | `SyntaxError`, `TypeMismatch` | Bad request — invalid SQL or type mismatch |
| 404 | `TableNotFound`, `ColumnNotFound` | Resource not found |
| 409 | `DuplicateKey`, `TableAlreadyExists` | Conflict — duplicate resource |
| 422 | `ConstraintViolation` | Constraint violated (NOT NULL, UNIQUE) |
| 503 | `Busy` | Database is busy (another writer is active) |
| 500 | (other) | Unexpected server error |

**Error response format:**
```json
{
  "error": "Table 'nonexistent' not found.",
  "code": "TableNotFound"
}
```

In development mode, a `detail` field with a stack trace is included.

---

## See Also

- [Getting Started Tutorial](getting-started.md) — Engine API walkthrough
- [Architecture Guide](architecture.md) — How the engine works internally
- [CLI Reference](cli.md) — Interactive REPL commands
- [Sample Datasets](../samples/README.md) — Ready-to-run SQL scripts

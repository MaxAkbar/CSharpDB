# Stored Procedures (Table-Backed)

This feature adds a managed, table-backed procedure catalog to CSharpDB. Procedures are stored in `__procedures`, executed in-process, and exposed through `CSharpDB.Client`, REST API, and Admin Web.

Native SQL procedure syntax (`CREATE PROCEDURE` / `CALL`) is not part of v1. Instead, procedures are metadata plus parameterized SQL body text.

## Schema

```sql
CREATE TABLE __procedures (
    name TEXT PRIMARY KEY,
    body_sql TEXT NOT NULL,
    params_json TEXT NOT NULL,
    description TEXT,
    is_enabled INTEGER NOT NULL,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);

CREATE INDEX idx___procedures_is_enabled ON __procedures (is_enabled);
```

The client layer auto-creates this catalog on first access.

## Parameter Metadata Format

`params_json` is a JSON array:

```json
[
  {
    "name": "id",
    "type": "INTEGER",
    "required": true,
    "default": null,
    "description": "User ID"
  }
]
```

Supported types:

- `INTEGER` -> `long`
- `REAL` -> `double`
- `TEXT` -> `string`
- `BLOB` -> `byte[]` (API/Admin pass base64 strings)

## API

Endpoints:

- `GET /api/procedures`
- `GET /api/procedures/{name}`
- `POST /api/procedures`
- `PUT /api/procedures/{name}`
- `DELETE /api/procedures/{name}`
- `POST /api/procedures/{name}/execute`

Create request:

```json
{
  "name": "GetUserById",
  "bodySql": "SELECT * FROM users WHERE id = @id;",
  "parameters": [
    { "name": "id", "type": "INTEGER", "required": true, "default": null, "description": "User ID" }
  ],
  "description": "Lookup by ID",
  "isEnabled": true
}
```

Execute request:

```json
{
  "args": {
    "id": 42
  }
}
```

Execute response (success):

```json
{
  "procedureName": "GetUserById",
  "succeeded": true,
  "statements": [
    {
      "statementIndex": 0,
      "statementText": "SELECT * FROM users WHERE id = @id;",
      "isQuery": true,
      "columnNames": ["id", "name"],
      "rows": [{ "id": 42, "name": "Alice" }],
      "rowsAffected": 1,
      "elapsedMs": 0.41
    }
  ],
  "error": null,
  "failedStatementIndex": null,
  "elapsedMs": 0.56
}
```

Execute response (validation/runtime failure) returns `400` with the same shape and `succeeded=false`.

## Admin Web Usage

Admin includes a **Procedures** section in Object Explorer and a dedicated Procedure tab:

- Create/edit/delete procedure definitions
- Manage parameter metadata (name/type/required/default/description)
- Toggle enabled state
- Execute with JSON args
- Inspect per-statement results and errors

## Execution Semantics

- Procedure body is split into SQL statements using the same tokenizer-based statement splitter used by SQL console execution.
- All statements run inside one transaction.
- On any statement failure:
  - transaction is rolled back
  - prior statement effects are reverted
  - response includes completed statement results plus error metadata
- On success:
  - transaction commits
  - response includes per-statement timing and result payloads
- Schema-change statements are visible to subsequent client, API, and Admin reads. Hosts should handle refresh notifications at the host layer.

## Validation Rules

- Procedure names and parameter names follow identifier rules: start with letter/underscore, continue with letter/digit/underscore.
- Parameter names are case-insensitive and must be unique.
- Body SQL parameter tokens (`@param`) must all exist in metadata.
- Unknown execution args are rejected.
- Required params must be provided as non-null unless a default is available.
- Type coercion is strict:
  - `INTEGER`: integral values or parseable integer strings
  - `REAL`: numeric values or parseable real strings
  - `TEXT`: string
  - `BLOB`: byte array or base64 string
- Empty procedure bodies are rejected.
- Disabled procedures cannot execute.

## Internal Table Visibility

`__procedures` is hidden from generic table listing/browse/mutate flows:

- not returned by `GetTableNamesAsync`
- rejected by standard table endpoints
- not shown in Admin **Tables** section

Power users can still query it directly from SQL Query tab / SQL endpoint.

## End-to-End Example

1. Create procedure:

```http
POST /api/procedures
```

```json
{
  "name": "UpsertCounter",
  "bodySql": "CREATE TABLE IF NOT EXISTS counters (id INTEGER PRIMARY KEY, n INTEGER NOT NULL); INSERT INTO counters VALUES (@id, @n); SELECT id, n FROM counters WHERE id = @id;",
  "parameters": [
    { "name": "id", "type": "INTEGER", "required": true },
    { "name": "n", "type": "INTEGER", "required": true }
  ],
  "isEnabled": true
}
```

2. Execute:

```http
POST /api/procedures/UpsertCounter/execute
```

```json
{ "args": { "id": 1, "n": 5 } }
```

3. Read per-statement response:

- statement 0: DDL rows affected
- statement 1: insert rows affected
- statement 2: query rows payload

## Limitations and Future Work

- No native SQL `CREATE PROCEDURE` / `CALL` syntax in v1
- No file/script import format in v1 (JSON payloads only)
- No CLI/MCP-specific procedure commands in v1

# Procedure Catalog v1 Plan (`__procedures`) Across Service, API, and Admin

## Summary
Implement a table-backed stored-procedure system using a managed internal catalog table (`__procedures`), with strict typed parameter metadata, named REST execution endpoint, and full Admin CRUD + run UI.

This plan is decision-complete and aligned to your choices:
- Table-only source of truth.
- Full CRUD + Run in Admin.
- `POST /api/procedures/{name}/execute`.
- Strict typed parameter validation.
- Per-statement execution results.
- Auto-create catalog table at startup.
- Keep route prefix style at `/api/*`.
- Hide `__procedures` from normal table browsing.

## Locked Decisions
| Area | Decision |
|---|---|
| Procedure source | Table-only (`__procedures`) |
| Input format | JSON payloads only |
| API route style | `/api/*` (no `/api/v1` in this feature) |
| Execute endpoint | `POST /api/procedures/{name}/execute` |
| Admin scope | Full CRUD + run |
| Result shape | Per-statement results |
| SQL allowance | All SQL (SELECT/DML/DDL) |
| Param validation | Strict typed validation |
| Bootstrap | Auto-ensure catalog on startup |
| Internal table visibility | Hidden from normal table/object lists |

## Scope
1. Service layer procedure catalog + executor.
2. REST API endpoints + DTOs for procedure CRUD and run.
3. Admin Web Procedures section with full CRUD and execute UX.
4. Internal table hiding from generic table UI/API browse.
5. Tests for service behavior and API contracts.
6. Docs updates including new README at `docs/procedures/README.md`.

## Non-Goals (v1)
1. Native SQL syntax (`CREATE PROCEDURE`, `CALL`).
2. File/script importer.
3. CLI/MCP procedure commands.
4. Role-based authorization model.

## Data Model (`__procedures`)
Create and manage one internal table:

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
```

Optional index (valid because integer column):
```sql
CREATE INDEX idx___procedures_is_enabled ON __procedures (is_enabled);
```

### `params_json` format
`params_json` is a JSON array of parameter definitions:

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

`type` allowed values: `INTEGER`, `REAL`, `TEXT`, `BLOB`.

BLOB input in API/Admin JSON is Base64 string; service decodes to `byte[]`.

## Public API / Interface Additions

### Service (`CSharpDB.Service`)
Add new models:
1. `ProcedureParameterDefinition`.
2. `ProcedureDefinition`.
3. `ProcedureStatementExecutionResult`.
4. `ProcedureExecutionResult`.

Add new service methods on `CSharpDbService`:
1. `Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true)`.
2. `Task<ProcedureDefinition?> GetProcedureAsync(string name)`.
3. `Task CreateProcedureAsync(ProcedureDefinition definition)`.
4. `Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition)`.
5. `Task DeleteProcedureAsync(string name)`.
6. `Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args)`.

Add new event:
1. `event Action? ProceduresChanged`.

### REST API (`CSharpDB.Api`)
Add DTOs in new file `src/CSharpDB.Api/Dtos/ProcedureDtos.cs`.

Requests:
1. `CreateProcedureRequest`.
2. `UpdateProcedureRequest`.
3. `ExecuteProcedureRequest`.
4. `ProcedureParameterRequest`.

Responses:
1. `ProcedureSummaryResponse`.
2. `ProcedureDetailResponse`.
3. `ProcedureParameterResponse`.
4. `ProcedureStatementResultResponse`.
5. `ProcedureExecutionResponse`.

Add endpoint file `src/CSharpDB.Api/Endpoints/ProcedureEndpoints.cs` with:
1. `GET /api/procedures`.
2. `GET /api/procedures/{name}`.
3. `POST /api/procedures`.
4. `PUT /api/procedures/{name}`.
5. `DELETE /api/procedures/{name}`.
6. `POST /api/procedures/{name}/execute`.

Map endpoints in `src/CSharpDB.Api/Program.cs`.

### Admin (`CSharpDB.Admin`)
Add new tab type and UI:
1. Add `TabKind.Procedure`.
2. Add `OpenProcedureTab(name)` and `OpenNewProcedureTab()` to `TabManagerService`.
3. Add `ProcedureTab.razor` for full CRUD + run.
4. Add Procedures tree group in `NavMenu.razor`.
5. Add Procedure count in `StatusBar.razor`.
6. Add MainLayout tab switch case for Procedure tab.

## Execution Semantics (Service)
1. Startup calls `EnsureProcedureCatalogAsync()` inside `InitializeAsync()` after opening connection.
2. Procedure CRUD writes to `__procedures`, serializes/deserializes `params_json`.
3. `ExecuteProcedureAsync` loads definition, verifies `is_enabled = 1`, validates args strictly.
4. Body SQL split into statements using tokenizer-based splitter (reuse existing splitter behavior).
5. Execute inside one transaction.
6. Collect per-statement result objects:
   - `statementIndex`
   - `isQuery`
   - `columnNames`
   - `rows`
   - `rowsAffected`
   - `elapsedMs`
7. On failure:
   - rollback transaction
   - return accumulated statement results
   - set `error` and `failedStatementIndex`
8. On success:
   - commit transaction
   - include all statement results and total elapsed.
9. If any statement is schema mutation, trigger existing schema notifications plus `ProceduresChanged` where relevant.

## Validation Rules
1. Procedure name must match identifier rule used elsewhere (letter/underscore start, alnum/underscore rest).
2. Parameter names must be unique case-insensitively.
3. Parameter names in metadata exclude `@`; SQL uses `@name`.
4. Every `@param` token found in `body_sql` must exist in metadata.
5. Unknown execution args are rejected.
6. Required params must be present and non-null unless default exists.
7. Type coercion:
   - `INTEGER`: `long` only (string numeric allowed if parse succeeds).
   - `REAL`: `double` (or integer convertible).
   - `TEXT`: string.
   - `BLOB`: `byte[]` or Base64 string.
8. Empty or whitespace `body_sql` is rejected.
9. Disabled procedures cannot execute.

## Internal Table Hiding Behavior
1. `GetTableNamesAsync` filters out `__procedures`.
2. Generic table browse/mutate endpoints reject `__procedures`.
3. Admin Tables tree excludes `__procedures`.
4. Query tab can still access `__procedures` explicitly via SQL (power-user path).

## File-Level Implementation Plan

### Phase 1: Service and Models
1. Update `src/CSharpDB.Service/CSharpDbService.cs`:
   - add catalog ensure on startup
   - add procedure CRUD + execute methods
   - add strict arg validation/coercion helpers
   - add per-statement executor helper
   - add `ProceduresChanged` event
   - add internal-table filtering logic.
2. Add new model files under `src/CSharpDB.Service/Models/`:
   - `ProcedureDefinition.cs`
   - `ProcedureParameterDefinition.cs`
   - `ProcedureExecutionResult.cs`
   - `ProcedureStatementExecutionResult.cs`
3. Update `src/CSharpDB.Service/README.md` with new API examples and model table.

### Phase 2: API
1. Add `src/CSharpDB.Api/Dtos/ProcedureDtos.cs`.
2. Add `src/CSharpDB.Api/Endpoints/ProcedureEndpoints.cs`.
3. Update `src/CSharpDB.Api/Program.cs` to map procedure endpoints.
4. Update `src/CSharpDB.Api/Endpoints/SchemaEndpoints.cs` and `Dtos/Responses.cs`:
   - add `procedureCount` to info response.
5. Keep existing error middleware behavior; endpoint returns `BadRequest` for execution validation/runtime errors with structured execution payload.

### Phase 3: Admin Web
1. Update `src/CSharpDB.Admin/Models/TabDescriptor.cs` with `TabKind.Procedure`.
2. Update `src/CSharpDB.Admin/Services/TabManagerService.cs`:
   - add open existing/new procedure tab helpers.
3. Update `src/CSharpDB.Admin/Components/Layout/MainLayout.razor`:
   - render `ProcedureTab`.
4. Update `src/CSharpDB.Admin/Components/Layout/NavMenu.razor`:
   - add Procedures group, load from service, open tabs.
5. Add `src/CSharpDB.Admin/Components/Tabs/ProcedureTab.razor`:
   - fields: name, description, enabled, parameter editor grid, SQL body editor
   - actions: create, update, delete, run
   - run args JSON input and per-statement result viewer.
6. Update `src/CSharpDB.Admin/Components/Layout/StatusBar.razor`:
   - add procedures count.
7. Update `src/CSharpDB.Admin/wwwroot/css/app.css` with procedure tab styles.

### Phase 4: Tests
1. Add service tests in `tests/CSharpDB.Tests/ServiceProcedureTests.cs`.
2. Add API tests in new project `tests/CSharpDB.Api.Tests` (WebApplicationFactory-based) or equivalent endpoint integration harness.
3. Keep existing tests unchanged and ensure full suite passes.

## Test Cases and Scenarios

### Service Tests
1. Auto-bootstrap creates `__procedures` on fresh DB.
2. `GetTableNamesAsync` excludes `__procedures`.
3. Create/get/list/update/delete procedure lifecycle.
4. Duplicate create returns conflict/throws expected exception.
5. Execute with valid required and default args.
6. Execute with unknown arg fails validation.
7. Execute with type mismatch fails validation.
8. Execute with multi-statement body returns per-statement results.
9. Failure in statement N rolls back prior statement effects.
10. Disabled procedure execution is rejected.

### API Tests
1. CRUD endpoint happy paths.
2. Execute endpoint success with per-statement payload.
3. Execute endpoint validation errors (`400`).
4. Missing procedure (`404`) for get/update/delete/execute.
5. `/api/tables` excludes `__procedures`.

### Admin Acceptance Scenarios
1. Procedures group appears in sidebar with count.
2. Create procedure from UI, reload, still present.
3. Edit params/body and save updates.
4. Execute with args and view per-statement results.
5. Delete with confirmation removes from list.
6. `__procedures` not shown in Tables group.

## README to Create

Create file: `docs/procedures/README.md`

### README content plan (verbatim structure)
1. Title: `# Stored Procedures (Table-Backed)`.
2. Overview explaining table-backed design and why no native SQL `CALL`.
3. Schema section with `CREATE TABLE __procedures` DDL.
4. Parameter metadata JSON format and type rules.
5. API section with all procedure endpoints and sample requests/responses.
6. Admin usage section (create, edit, run, delete workflows).
7. Execution semantics section:
   - transaction behavior
   - per-statement results
   - rollback on failure.
8. Validation rules section.
9. Internal table visibility behavior (`__procedures` hidden from normal table list).
10. End-to-end example:
    - create procedure
    - execute with args
    - sample per-statement response.
11. Limitations and future work:
    - no native `CREATE PROCEDURE/CALL`
    - no file importer in v1
    - no CLI/MCP commands in v1.

## Rollout and Compatibility
1. Backward compatible for existing API consumers; new endpoints are additive.
2. Minor behavior change: internal table hidden from generic table listings.
3. No migration prerequisite for users; catalog table auto-created on startup.
4. Release notes should include new procedure catalog feature and endpoint list.

## Assumptions and Defaults
1. Existing SQL endpoint behavior remains unchanged.
2. Procedure execution returns per-statement full row payloads (no row cap in v1).
3. Default values are stored as JSON-native values in `params_json`.
4. UTC timestamps stored as ISO-8601 text.
5. No auth changes are introduced in this feature.
6. Plan mode constraint acknowledged: README and code edits are to be implemented in execution mode, with the README path and content structure fixed above.

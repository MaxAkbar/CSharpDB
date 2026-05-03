# Database Code Modules With VS Code Sync Plan

## Summary

Add database-owned C# code modules that can be saved with a CSharpDB database,
compiled by CSharpDB, and invoked by Admin Forms events. Editing and debugging
should happen in VS Code, not in the Admin browser UI.

This is the Access-style "code travels with the database" model, but with C#
and normal developer tooling. Admin remains the app-builder and runtime surface.
VS Code remains the serious code editor and debugger.

The core implementation must be a public .NET API, not a CLI-first design.

## Product Position

CSharpDB will have three separate automation lanes:

| Lane | Who Owns Code | Where It Lives | Use For |
| --- | --- | --- | --- |
| Declarative macros/actions | Database metadata | Form JSON/database metadata | No-code app behavior, navigation, field updates, command orchestration |
| Host callbacks | Host application | C# host project startup registration | External services, compiled integrations, application-owned code |
| Database code modules | Database | Database module metadata, synced to files for editing | Access-like form/business logic that travels with the database |

Database code modules are not a replacement for host callbacks. Host callbacks
remain the right path for application services, email, queues, filesystem,
network calls, or privileged host operations.

## Goals

- Store C# source modules in the database.
- Let Admin create form/control event stubs.
- Let VS Code edit synced `.cs` files with normal C# tooling.
- Sync source between the database and a local workspace folder.
- Compile modules with Roslyn and return diagnostics.
- Surface diagnostics in Admin and VS Code.
- Execute trusted compiled modules from Admin Forms runtime events.
- Keep the core reusable through a public API based on `ICSharpDbClient`.
- Avoid Monaco/CodeMirror bloat in Admin.
- Keep CLI optional; it is not the primary architecture.

## Non-Goals For V1

- No full in-browser IDE.
- No Monaco dependency for Admin code editing.
- No arbitrary hidden execution from untrusted databases.
- No remote dynamic assembly loading from form JSON.
- No debugger implemented inside Admin.
- No replacement for host callback integrations.
- No direct unmanaged/native code from modules.
- No unrestricted file/network/process APIs from database-owned code.

## High-Level Architecture

```text
CSharpDB.CodeModules
  public .NET API
  module metadata model
  import/export/sync services
  Roslyn build/diagnostic services
  trusted runtime contracts
  uses ICSharpDbClient

CSharpDB.Admin
  lists modules
  creates event stubs
  shows trust/build/diagnostic state
  invokes compiled trusted module procedures at runtime
  uses CSharpDB.CodeModules directly

VS Code Extension
  module tree
  open/sync files
  Problems diagnostics
  build command
  debug harness command
  talks to .NET sidecar/language server

.NET Sidecar / Language Server
  references CSharpDB.CodeModules
  exposes JSON-RPC to VS Code extension
  owns local file watching and DB sync
```

The CLI is optional future work. If added, it should be a thin wrapper over
`CSharpDB.CodeModules`, not the source of truth.

## Project Layout

```text
src/
  CSharpDB.CodeModules/
    CSharpDB.CodeModules.csproj
    Models/
    Storage/
    Sync/
    Compilation/
    Runtime/
    Diagnostics/

  CSharpDB.Admin/
    uses CSharpDB.CodeModules

vscode-extension/
  src/
    extension.ts
    moduleTree.ts
    diagnostics.ts
    sidecarClient.ts

  sidecar/
    CSharpDB.CodeModules.LanguageServer.csproj
```

The sidecar can live under `vscode-extension/sidecar` or `src/` depending on
packaging. It should not duplicate code-module logic.

## Public API

The core API should be usable by Admin, tests, VS Code sidecar, and host apps.

```csharp
public sealed class CSharpDbCodeModuleClient
{
    public CSharpDbCodeModuleClient(ICSharpDbClient client);

    public Task<IReadOnlyList<CodeModuleSummary>> ListAsync(
        CodeModuleListRequest? request = null,
        CancellationToken ct = default);

    public Task<CodeModuleDefinition?> GetAsync(
        string moduleId,
        CancellationToken ct = default);

    public Task<CodeModuleDefinition> UpsertAsync(
        CodeModuleDefinition module,
        CodeModuleWriteOptions? options = null,
        CancellationToken ct = default);

    public Task DeleteAsync(
        string moduleId,
        CodeModuleDeleteOptions? options = null,
        CancellationToken ct = default);

    public Task<CodeModuleExportResult> ExportAsync(
        CodeModuleExportRequest request,
        CancellationToken ct = default);

    public Task<CodeModuleImportResult> ImportAsync(
        CodeModuleImportRequest request,
        CancellationToken ct = default);

    public Task<CodeModuleBuildResult> BuildAsync(
        CodeModuleBuildRequest request,
        CancellationToken ct = default);

    public Task<CodeModuleTrustResult> TrustAsync(
        CodeModuleTrustRequest request,
        CancellationToken ct = default);
}
```

### Key Models

```csharp
public sealed record CodeModuleDefinition(
    string ModuleId,
    string Name,
    CodeModuleKind Kind,
    string Language,
    string Source,
    CodeModuleScope Scope,
    IReadOnlyDictionary<string, string> Metadata,
    string? SourceHash = null,
    string? LastBuiltHash = null,
    CodeModuleTrustState TrustState = CodeModuleTrustState.Untrusted,
    IReadOnlyList<CodeModuleEventBinding>? EventBindings = null);

public enum CodeModuleKind
{
    Standard = 0,
    Form = 1,
    Report = 2,
    Shared = 3,
}

public sealed record CodeModuleEventBinding(
    string OwnerKind,
    string OwnerId,
    string EventName,
    string ProcedureName,
    string ModuleId);
```

`Language` should be `"csharp"` in V1. The field exists so the wire shape can
evolve later.

## Database Storage

V1 can use system tables instead of embedding code directly inside form JSON.
Forms can reference module event procedures by id/name.

Suggested tables:

```sql
CREATE TABLE IF NOT EXISTS _admin_code_modules (
    module_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    language TEXT NOT NULL,
    source TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    last_built_hash TEXT,
    trust_state TEXT NOT NULL,
    metadata_json TEXT,
    created_utc TEXT NOT NULL,
    updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS _admin_code_module_bindings (
    binding_id TEXT PRIMARY KEY,
    module_id TEXT NOT NULL,
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    procedure_name TEXT NOT NULL,
    metadata_json TEXT,
    FOREIGN KEY (module_id) REFERENCES _admin_code_modules(module_id)
);

CREATE TABLE IF NOT EXISTS _admin_code_module_builds (
    build_id TEXT PRIMARY KEY,
    module_set_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    diagnostics_json TEXT,
    artifact_hash TEXT,
    built_utc TEXT NOT NULL
);
```

Open question: whether compiled assemblies should be persisted in the database.
For V1, prefer not storing binaries until the runtime model is settled. Store
source, diagnostics, and hashes first. Compile on host startup or on first use.

## Exported Workspace Shape

VS Code should edit normal files generated from database modules.

```text
.csharpdb-code/
  csharpdb.codeproj.json
  forms/
    Customers/
      CustomersForm.cs
  modules/
    InventoryRules.cs
    SharedValidation.cs
  obj/
    generated/
      CSharpDbCodeModuleRuntime.g.cs
      FormContracts.g.cs
  .vscode/
    tasks.json
    launch.json
```

Manifest example:

```json
{
  "database": "C:\\data\\app.db",
  "formatVersion": 1,
  "modules": [
    {
      "moduleId": "forms.customers",
      "path": "forms/Customers/CustomersForm.cs",
      "sourceHash": "sha256:...",
      "kind": "Form",
      "ownerKind": "Form",
      "ownerId": "customers-form"
    }
  ]
}
```

The manifest is required for conflict detection and stable module ids.

## C# Module Shape

Generated form module example:

```csharp
using CSharpDB.CodeModules.Runtime;

namespace CSharpDB.DatabaseCode.Forms;

public sealed class CustomersFormModule : FormCodeModule
{
    public void Form_BeforeUpdate(FormBeforeUpdateContext context)
    {
        if (Me.Status == "Closed" && Me.ClosedDate is null)
        {
            context.Cancel = true;
            context.Message = "Closed date is required.";
        }
    }

    public void btnShip_Click(FormControlEventContext context)
    {
        Me.Status = "Shipped";
        DoCmd.SaveRecord();
    }
}
```

This is C#, but with generated helper APIs that feel like Access:

- `Me`
- `Controls`
- strongly typed field properties when schema is known
- `DoCmd`
- event context objects
- validation/cancel semantics

## Runtime Contracts

`CSharpDB.CodeModules.Runtime` should define small, controlled contracts:

```csharp
public abstract class FormCodeModule
{
    protected dynamic Me { get; }
    protected IFormCommandApi DoCmd { get; }
    protected IFormControlApi Controls { get; }
}

public sealed class FormBeforeUpdateContext
{
    public bool Cancel { get; set; }
    public string? Message { get; set; }
}

public interface IFormCommandApi
{
    void SaveRecord();
    void NewRecord();
    void Refresh();
    void OpenForm(string formName, object? filter = null);
    void RunAction(string actionName);
    ValueTask RunCommandAsync(string commandName, object? args = null);
}
```

The runtime API should expose form-safe operations only. Host integrations still
go through trusted host callbacks.

## Compilation

Use Roslyn in `CSharpDB.CodeModules.Compilation`.

Build inputs:

- module source files
- generated runtime contracts
- generated form schema wrappers
- allowed framework references
- CSharpDB runtime contract assembly references

Build outputs:

- success/failure
- diagnostics with file/module/line/column
- module set hash
- optional in-memory assembly
- optional debug harness project

Diagnostics model:

```csharp
public sealed record CodeModuleDiagnostic(
    string ModuleId,
    string? FilePath,
    CodeModuleDiagnosticSeverity Severity,
    string Code,
    string Message,
    int StartLine,
    int StartColumn,
    int EndLine,
    int EndColumn);
```

VS Code maps these to the Problems panel. Admin shows them in the module detail
view.

## Trust And Security

Database-owned C# is powerful. V1 must fail closed.

Trust states:

| State | Meaning |
| --- | --- |
| `Untrusted` | Source exists but cannot execute. |
| `TrustedForThisMachine` | User explicitly trusted this DB/module set locally. |
| `TrustedBySignature` | Future signed module/package trust. |
| `Revoked` | Explicitly blocked. |

Execution requirements:

- Database must be trusted.
- Module source hash must match the trusted hash.
- Build diagnostics must have no errors.
- Runtime policy must allow module execution.
- Host must opt into code module execution.

Admin should show clear trust prompts. It should never silently execute code
from a database file just because it contains modules.

## VS Code Extension

The VS Code extension should be a UI and sync shell, not the implementation
owner.

Commands:

- `CSharpDB: Connect Database`
- `CSharpDB: Export Code Modules`
- `CSharpDB: Import Code Modules`
- `CSharpDB: Sync Current File To Database`
- `CSharpDB: Watch Code Modules`
- `CSharpDB: Build Code Modules`
- `CSharpDB: Create Form Event Stub`
- `CSharpDB: Open Module From Database`
- `CSharpDB: Generate Debug Harness`

Views:

- Code Modules tree
- Forms and event procedures
- Build diagnostics
- Trust state

The extension talks to a .NET sidecar/language server over JSON-RPC. The
sidecar references `CSharpDB.CodeModules` and uses the public API.

## Sidecar Protocol

The protocol should be simple JSON-RPC:

```text
codeModules/connect
codeModules/list
codeModules/export
codeModules/import
codeModules/watch
codeModules/build
codeModules/upsert
codeModules/createEventStub
codeModules/trust
```

The sidecar owns:

- loading CSharpDB assemblies
- talking to `ICSharpDbClient`
- file watching
- conflict detection
- Roslyn builds
- diagnostics translation

## Admin UX

Admin should not become the main editor. It should provide:

- Code Modules tab.
- module list and details.
- source read-only or simple textarea fallback.
- build/trust status.
- diagnostics history.
- create event stub button from form designer.
- "Open in VS Code" command when extension/deep link support exists.

Form designer integration:

- select form/control event.
- choose "Create Code Module Handler".
- Admin generates `Form_BeforeUpdate`, `btnSave_Click`, etc.
- event binding points to module id and procedure name.
- module appears in Code Modules tab.

## Import/Export And Conflict Handling

Every sync uses hashes:

- database source hash
- exported file hash
- manifest hash

Import should detect:

- file changed, DB unchanged: import cleanly.
- DB changed, file unchanged: update file on export.
- both changed: conflict result, no overwrite by default.

Conflict result includes:

- module id
- db hash
- file hash
- paths
- suggested resolution

## Debugging Strategy

V1 should not promise live Admin breakpoints. Instead:

1. Generate a debug harness project.
2. Reference `CSharpDB.CodeModules.Runtime`.
3. Generate sample event contexts from form metadata.
4. Let VS Code run/debug the harness normally.

Later live debugging can attach to Admin or a dedicated module host, but that
requires a runtime/debug adapter design.

## Runtime Execution

Admin Forms runtime flow:

1. Event fires.
2. Runtime resolves event binding to module id/procedure.
3. Trust policy is checked.
4. Module set is compiled or loaded from cache.
5. Procedure is invoked with a typed context.
6. Result maps back to existing form event behavior:
   - cancel save
   - set error/message
   - mutate current record
   - dispatch allowed macro/actions

Failures:

- untrusted: block execution with clear message
- missing module/procedure: block event with diagnostics
- compile error: block event with diagnostics
- exception: fail event and record diagnostics

## Relationship To Host Callbacks

Database modules should call host services only through explicit host callbacks:

```csharp
await DoCmd.RunCommandAsync("SendOpsDigest", new { OrderId = Me.Id });
```

This keeps external side effects behind host registration and policy.

## Testing Plan

Core API tests:

- create/list/get/update/delete modules
- module metadata round-trip
- event binding round-trip
- import/export manifest generation
- import conflict detection
- hash stability
- build success/failure diagnostics
- trust enforcement

Admin tests:

- create event stub from form/control event
- module appears in catalog
- untrusted module blocks execution
- trusted module executes
- compile errors shown in diagnostics
- form save cancellation from code module

VS Code sidecar tests:

- JSON-RPC list/export/import/build
- file watcher sync
- diagnostics mapping
- conflict reporting

Runtime tests:

- form before update cancel
- button click mutation
- missing procedure
- compile failure
- thrown exception
- host callback bridge through `DoCmd.RunCommandAsync`

## Phased Implementation

### Phase 1: Core Storage And API

- Add `CSharpDB.CodeModules`.
- Add module models.
- Add system-table storage through `ICSharpDbClient`.
- Add list/get/upsert/delete.
- Add tests.

### Phase 2: Import/Export Sync

- Add file layout and manifest.
- Add export/import APIs.
- Add conflict detection.
- Add tests with temp directories.

### Phase 3: Roslyn Build Diagnostics

- Add C# compile service.
- Add generated runtime references.
- Return diagnostics.
- Store last build status/hash.

### Phase 4: Admin Catalog And Stub Generation

- Add Code Modules tab.
- Add module detail/status.
- Add form/control event stub generation.
- Add binding metadata.

### Phase 5: VS Code Sidecar And Extension

- Add sidecar JSON-RPC process.
- Add VS Code module tree.
- Add export/import/watch/build commands.
- Map diagnostics to Problems panel.

### Phase 6: Trusted Runtime Execution

- Add trust state and prompts.
- Add form event execution path.
- Add runtime contexts.
- Add diagnostics.

### Phase 7: Debug Harness

- Generate harness project.
- Add VS Code launch/task config.
- Add sample event contexts.

## Open Questions

- Store compiled assemblies in DB or cache only on disk?
- Exact trust UX and where local trust is stored.
- Whether form modules should be partial classes generated around user code.
- How strongly typed `Me` should be in V1.
- Whether remote databases can execute modules or only edit/sync them.
- How module runtime policy composes with existing callback policy.
- Whether code modules can reference other modules in V1.
- How to version module runtime contracts.

## Recommendation

Proceed with `CSharpDB.CodeModules` as the core. Do not build an Admin Monaco
editor for V1. Do not make the CLI a required layer.

Use Admin for module discovery, trust, event stub creation, build status, and
runtime execution. Use VS Code plus a .NET sidecar for editing, sync,
diagnostics, and debug harness workflows.

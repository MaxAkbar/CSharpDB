# CSharpDB.CodeModules

Database-owned C# code modules, workspace sync, local trust, and Admin Forms
runtime contracts for CSharpDB.

This project is consumed by `CSharpDB.Admin.Forms` and `CSharpDB.Admin`. It is
not a standalone web host or an in-browser code editor.

## What This Project Provides

- database-backed module storage in `__code_modules`
- build history and diagnostics storage in `__code_module_builds`
- module CRUD through `CSharpDbCodeModuleClient`
- stable source hashes and module-set hashes
- export/import workspace sync for normal editor workflows
- Roslyn-based in-memory builds with structured diagnostics
- local trust grants keyed by normalized database path and module-set hash
- Admin Forms runtime contracts for Access-like form code modules
- trusted in-process form event dispatch with explicit host opt-in

## Module Kinds

| Kind | Purpose |
| --- | --- |
| `Form` | Event-handler module associated with one Admin Form. |
| `Standard` | Shared helper methods used by form modules. |
| `Class` | Shared helper classes used by form modules. |

Form modules normally derive from `FormCodeModule`. Standard and class modules
compile into the same database-owned assembly and can be referenced by form
handlers.

## Service Registration

Register the code-module services in a host that already provides
`ICSharpDbClient`:

```csharp
using CSharpDB.CodeModules;

builder.Services.AddCSharpDbCodeModules();
```

Hosts that want to execute trusted code modules in-process must opt in
explicitly:

```csharp
builder.Services.AddCSharpDbCodeModules(options =>
{
    options.EnableInProcessExecution = true;
});
```

The extension registers:

- `CSharpDbCodeModuleClient`
- `ICodeModuleTrustStore`
- `ICodeModuleFormEventDispatcher`

The default trust store writes local trust state to
`%LOCALAPPDATA%\CSharpDB\code-module-trust.json`. Trust is intentionally not
stored in the database, so moving or editing a database does not silently grant
execution permission on another machine or for another module-set hash.

## Core Contracts

| Contract | Purpose |
| --- | --- |
| `CSharpDbCodeModuleClient` | List, get, upsert, delete, export, import, build, trust, and inspect module trust state. |
| `CodeModuleDefinition` | Persisted module source, kind, owner, type name, metadata, source hash, and timestamps. |
| `CodeModuleBuildResult` | Build status, module-set hash, diagnostics, build timestamp, and optional in-memory assembly bytes. |
| `CodeModuleHandler` | Event binding reference to a module id, type name, and method name. |
| `FormCodeModule` | Base type for Admin Forms code modules. Exposes `Me`, `DoCmd`, and `CurrentEvent`. |
| `FormEventContext` | Runtime form event data, arguments, metadata, message, and cancellation state. |
| `FormBeforeEventContext` | Cancelable before-event context for insert, update, and delete workflows. |
| `FormControlEventContext` | Control event context with selected control id and control type. |
| `IFormCommandApi` | Safe form actions such as set field, show message, run action sequence, run host command, save, new, refresh, open/close form, and apply/clear filter. |

## Execution Model

Code module execution is gated by three checks:

1. The host must enable in-process execution.
2. The current module set must build successfully.
3. The current module-set hash must be trusted locally for the current database
   path.

Untrusted modules, missing handlers, compile failures, and thrown handler
exceptions fail the event with diagnostics. Before-event handlers can cancel an
operation by calling `Cancel` on the event context. Form handlers can mutate the
current record through `Me`; writes must target fields that exist on the current
form record.

```csharp
using System;
using System.Threading.Tasks;
using CSharpDB.CodeModules.Runtime;

namespace CSharpDB.UserCode.Forms;

public sealed class CustomersModule : FormCodeModule
{
    public void BeforeUpdate(FormBeforeEventContext context)
    {
        if (string.Equals((string?)Me.Status, "Closed", StringComparison.OrdinalIgnoreCase))
            context.Cancel("Closed customers cannot be edited.");
    }

    public async Task OnClick(FormControlEventContext context)
    {
        await DoCmd.ShowMessageAsync($"Clicked {context.ControlId}");
    }
}
```

Handlers may return `void`, `Task`, or `ValueTask`. They may accept no
parameters or one parameter assignable from the runtime event context.

## Workspace Sync

`ExportAsync` writes a normal file workspace:

```text
.csharpdb-code/
  csharpdb.codeproj.json
  forms/
  modules/
  classes/
```

Users can edit the exported `.cs` files in their preferred editor. `ImportAsync`
uses the manifest source hash and current database source hash to detect
conflicts. If both the database module and exported file changed since export,
the import reports a conflict and does not overwrite either side.

## Runtime Boundaries

This package does not provide sandboxing. Safety comes from explicit host opt-in,
restricted runtime contracts, successful build validation, and local trust for a
specific module-set hash.

The current scope does not include an in-browser IDE, VS Code sidecar commands,
file watching, debugger integration, report modules, aggregate/table-valued UDFs,
native plugin extensions, remote delegate serialization, or database-owned code
execution through daemon transports.

## Build

```powershell
dotnet build src/CSharpDB.CodeModules/CSharpDB.CodeModules.csproj
dotnet test tests/CSharpDB.CodeModules.Tests/CSharpDB.CodeModules.Tests.csproj
```

Related Admin Forms coverage:

```powershell
dotnet test tests/CSharpDB.Admin.Forms.Tests/CSharpDB.Admin.Forms.Tests.csproj --filter "CodeModule|FormEvent|ControlEvent|JsonRoundtrip"
```

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.Primitives`
- `Microsoft.CodeAnalysis.CSharp`
- `Microsoft.Extensions.DependencyInjection.Abstractions`

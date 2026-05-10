# User-Defined Functions And Commands Plan

This page tracks the shipped and remaining work for CSharpDB function,
command, and automation extensibility. The implementation separates portable
database metadata from local execution trust: databases can reference functions,
commands, validation rules, declarative action sequences, and C# code modules,
while executable code runs only in trusted host contexts.

## Shipped Capabilities

- Trusted in-process C# scalar functions through `DbFunctionRegistry`, available
  to SQL, triggers/procedures, direct clients, Admin Forms/Reports, and
  pipelines.
- Common built-in scalar functions for SQL and Admin formulas, including text,
  date/time, numeric, conversion, null, and conditional helpers.
- Admin Forms domain helper functions such as `DLookup`, `DCount`, `DSum`,
  `DAvg`, `DMin`, and `DMax`, resolved by the rendered Forms runtime with
  bounded row loading.
- Trusted command callbacks through `DbCommandRegistry`, used by Admin Forms,
  Admin Reports, and pipeline lifecycle hooks.
- Admin Forms action sequences for host commands, reusable sequences, field
  updates, messages, stop, rendered record navigation/save/delete/refresh/go-to,
  open/close form, filter, SQL/procedure, control-property, conditional, and
  rule workflows.
- Database-owned C# code modules for the local Admin Forms MVP: module source is
  stored in `__code_modules`, build diagnostics are stored in
  `__code_module_builds`, VS Code-friendly file sync exports/imports
  `.csharpdb-code/csharpdb.codeproj.json` plus `.cs` files, and form/control
  events can bind to trusted in-process handlers.
- Automation metadata and validation surfaces that let exported forms, reports,
  and pipeline packages describe required host callbacks.

## Current Boundaries

- Scalar delegates are host-owned and in-process; delegates are not serialized
  over HTTP, gRPC, or package files.
- Trusted callbacks are policy-mediated but not sandboxed. They run with the
  permissions of the host process that registered them.
- Database-owned C# source execution requires host opt-in, successful Roslyn
  build, and explicit local trust for the current module-set hash. Trust is not
  stored in the database and source changes invalidate it.
- The current code-module runtime is limited to local Admin Forms form/control
  handlers; reports, procedures, daemon/remote execution, in-browser editing,
  and debugging integration remain outside this slice.
- Custom scalar functions are intentionally not used for optimizer pushdown,
  expression indexes, generated columns, or constant folding.

## Future Work

- Aggregate and table-valued UDFs.
- Native/plugin extension loading with an explicit trust and packaging model.
- Report modules and broader database-owned code module surfaces beyond the
  local Admin Forms MVP.
- Sandboxed UDF execution, including the WebAssembly/Wasmtime research track.
- Remote delegate or extension registration for daemon-hosted deployments.
- Additional Access-style form/control events, macro loops, on-error handling,
  temp/session variables, and broader report/query/import/export actions.

## Related Docs

- [Trusted C# Callbacks](../trusted-csharp-functions/README.md)
- [Trusted Validation Rules](../trusted-csharp-functions/validation-rules.md)
- [Access-Style Macro Actions](../trusted-csharp-functions/access-style-macro-actions.md)
- [Access-Style Functions and Macros](../admin-forms-access-parity/access-style-functions-and-macros.md)
- [Database Code Modules With VS Code Sync Plan](../trusted-csharp-functions/database-code-modules-vscode-plan.md)
- [WebAssembly sandboxed UDFs](../roadmap.md#long-term)

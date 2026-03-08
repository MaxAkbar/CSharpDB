# What's New

## v1.6.0 (2026-03-08)

### Unified Client and Host Migration
- Added `CSharpDB.Client` as the authoritative public database API with transport-selecting `CSharpDbClientOptions`, `ICSharpDbClient`, and DI registration helpers.
- Added direct engine-backed client execution as the default behavior, with explicit transport selection for `Direct`, `Http`, `Grpc`, `Tcp`, and named pipes; non-direct network transports are recognized for future expansion.
- Migrated the CLI, Web API, Admin UI, MCP host, and umbrella `CSharpDB` package to use the new client surface.
- Reduced `CSharpDB.Service` to a compatibility facade so hosts no longer need a separate authoritative service API.

### SQL Ownership and Tooling Consistency
- Moved SQL script splitting, statement completeness checks, and read/write classification into `CSharpDB.Sql`.
- Updated the CLI REPL and direct client execution paths to share the same trigger-aware SQL parsing behavior.
- Added transport-neutral client result shaping for schema, browsing, saved queries, procedures, diagnostics, and metadata flows used by host applications.

### Documentation and Packaging
- Updated architecture, API, MCP, service, and package documentation to reflect the client-first design.
- Added a dedicated `CSharpDB.Client` README and included the client in the `CSharpDB` metapackage.
- Documented the API Scalar UI and MCP client-target configuration for local testing and integration.
- Added `CSharpDB.Native`, a Node client package scaffold, and native FFI tutorials for JavaScript and Python consumers.

### Validation and Coverage
- Added CLI integration coverage for startup, `.info`, positional database paths, and `.read` script execution.
- Added SQL-layer tests for script splitting, trigger-aware statement handling, and statement classification.
- Added direct-client regression coverage for multi-statement execution and CLI/client file-handle reuse.

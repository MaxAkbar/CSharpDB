# What's New

## v1.9.0 (2026-03-12)

### NativeAOT VS Code Extension
- Added the first NativeAOT-backed local VS Code extension for CSharpDB, using `CSharpDB.Native` through the embedded C API instead of a REST API or daemon for local IDE workflows.
- Added workspace `.db` auto-connect, schema explorer support for tables, columns, views, indexes, triggers, and procedures, and `.csql` language support with syntax highlighting, completion, and hover help.
- Added integrated query results, data browser CRUD for tables with read-only view browsing, table designer flows, and storage diagnostics inside the VS Code extension.

### In-Memory Storage Stability
- Fixed in-memory storage growth near the managed array limit so checkpoints can grow directly to the required size instead of failing when a doubling step would overflow the supported buffer ceiling.
- Prevented intermittent `Collection put (in-memory)` benchmark and checkpoint failures caused by the memory device rejecting a valid small growth request near the high-water mark.
- Expanded in-memory WAL checkpoint error context to include committed page count, target page count, required length, and device length for faster failure analysis.

### NativeAOT and Trimming Clarity
- Moved `Collection<T>` trim/AOT warnings to the public API boundary so NativeAOT and trimmed-app limitations are explicit where the typed collection API is consumed.
- Added linker-friendly annotations for reflected collection member access and removed internal analyzer noise around collection index binding resolution.
- Clarified the runtime requirements of typed collection JSON serialization and deserialization while preserving the existing SQL-first path for NativeAOT-sensitive scenarios.

### Tooling and Repo Baseline
- Updated the VS Code extension TypeScript configuration from the deprecated Node 10 resolver to the Node 16 module and resolution settings.
- Bumped the repo SDK baseline to `.NET 10.0.200` for the current branch and release prep workflow.


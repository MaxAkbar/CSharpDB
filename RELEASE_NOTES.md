# What's New

## v2.0.0 (Unreleased)

### Client Transport Completion
- Added the REST-backed `Http` transport implementation for `CSharpDB.Client`, so the unified client now has working Direct, HTTP, and gRPC paths.
- Completed the API coverage needed by the HTTP client for collections, saved queries, procedures, transaction sessions, checkpointing, maintenance, and storage inspection flows.
- Updated client/CLI endpoint resolution so `http://` and `https://` endpoints map cleanly to the dedicated `CSharpDB.Api` host, while `CSharpDB.Daemon` remains the gRPC host.

### Breaking Transport Cleanup
- Removed the unsupported `Tcp` transport placeholder from the public client transport model, CLI parsing, and related docs so the contract matches the transports that actually exist.
- Left `NamedPipes` as the only planned additional client transport for future same-machine daemon scenarios.

### Compatibility Surface Cleanup
- Removed the legacy `CSharpDB.Service` and `CSharpDB.Core` compatibility projects from the `v2.0` repo and solution.
- Removed `CSharpDB.Service` from the all-in-one `CSharpDB` package so the `v2.0` entry package only pulls in the primary client, engine, ADO.NET, and diagnostics surface.
- Updated the active docs to point legacy consumers directly to `CSharpDB.Client` and `CSharpDB.Primitives`.


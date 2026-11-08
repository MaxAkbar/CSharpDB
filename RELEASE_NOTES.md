# What's New

## CSharpDB 4.7.1

CSharpDB 4.7.1 updates dependencies while preserving compatible OpenAPI,
CSV import, and EF Core tooling behavior. These notes cover changes since v4.7.0.

### Dependency and tooling updates

- Align the EF Core provider, adoption samples, tests, and repository-local
  `dotnet-ef` tool on 10.0.14 through the shared version property. Update the
  generated migration SQL corpus to the matching EF product version.
- Update ASP.NET Core and Microsoft extension packages to 10.0.14, retain the
  supported Microsoft.OpenApi 2.x dependency at 2.13.0, and keep CsvHelper at
  33.1.0 until its replacement parser API is separately adopted.
- Update OpenTelemetry packages to 1.20.1 and align the Prometheus exporter at
  1.20.1-beta.1. Update Scalar, gRPC code generation, desktop WebView2, and test
  tooling dependencies.
- Update the optional Microsoft Access worker's dependencies to 10.0.14 and
  the SQL Server adapter's PKCS and ScriptDom dependencies. Keep both workers'
  exact dependency inventories and third-party notices aligned with their bundles.

### Upgrade and scope

Database and WAL formats are unchanged. The custom TCP transport remains a
separate prototype; this patch does not claim a network performance improvement.

## CSharpDB 4.7.0

CSharpDB 4.7.0 establishes shared SDK and ADO.NET gRPC connection/session handling
and fixes archive-backed SQL in the compiled native library.
These notes cover changes since v4.6.6.

### Remote client foundation

- Share physical gRPC clients between SDK rentals and pooled ADO.NET connections
  with matching options. Bound pool capacity, honor cancellation while waiting,
  and isolate endpoints, credentials, routes, and caller-owned HTTP clients.
- Add explicit native HTTP/2 gRPC selection and experimental persistent command
  sessions for SQL and transactions. Existing Auto/unary defaults remain in
  place; streaming requires an explicit opt-in.
- Preserve transaction ownership, private temporary-table state, result schemas,
  and logical column types across rentals and sessions. Drain active operations
  before returning a client, reject stale rentals, and clean up unfinished
  transactions without automatically replaying commands after disconnect.
- Prevent a queued transaction begin from blocking a schema owner's commit or
  rollback. Cancel and drain pending begins during client disposal, and clean up
  transactions that cannot be registered after cancellation or shutdown.
- Preserve the selected gRPC call mode when Admin clones routed client options.

### Archive and dependency fixes

- Fix external archive table registration in the NativeAOT library by generating
  schema and manifest JSON metadata. Keep existing archive JSON bytes, enum
  validation, legacy omitted-field defaults, and explicit-null rejection.
- Add a published NativeAOT archive SQL smoke covering scans, primary-key lookup,
  declared types, and close/reopen persistence through the C API. Fail native
  publication on trimming or AOT warnings.
- Support private migration reject artifacts on Linux tmpfs, including RAM-backed
  temporary directories, while retaining owner, permission, link and ACL checks.
- Update the optional SQL Server migration adapter's PKCS dependency to 9.0.22
  and keep its exact bundle inventories and third-party notice aligned.
- Include all 27 solution test modules in the ordinary pull-request CI matrix.

### Scope and support

Persistent streaming remains experimental and opt-in. This release does not
include the custom TCP prototype or remote prepared-statement handles, and no
verified network performance improvement is claimed. The native archive smoke
checks the C API; it does not establish remote gRPC support under NativeAOT.

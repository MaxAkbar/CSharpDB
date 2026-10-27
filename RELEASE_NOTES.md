# What's New

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
- Preserve the selected gRPC call mode when Admin clones routed client options.

### Archive and dependency fixes

- Fix external archive table registration in the NativeAOT library by generating
  schema and manifest JSON metadata. Keep existing archive JSON bytes, enum
  validation, legacy omitted-field defaults, and explicit-null rejection.
- Add a published NativeAOT archive SQL smoke covering scans, primary-key lookup,
  declared types, and close/reopen persistence through the C API. Fail native
  publication on trimming or AOT warnings.
- Update the optional SQL Server migration adapter's PKCS dependency to 9.0.22
  and keep its exact bundle inventories and third-party notice aligned.
- Include all 27 solution test modules in the ordinary pull-request CI matrix.

### Scope and support

Persistent streaming remains experimental and opt-in. This release does not
include the custom TCP prototype or remote prepared-statement handles, and no
verified network performance improvement is claimed. The native archive smoke
checks the C API; it does not establish remote gRPC support under NativeAOT.

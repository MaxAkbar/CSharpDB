# CSharpDB.Daemon

`CSharpDB.Daemon` is the preferred combined remote host for CSharpDB.

It exposes the `CSharpDB.Client` contract over gRPC and the existing REST/HTTP
surface under `/api` from one long-running process. Both transports use the same
warm daemon-hosted `ICSharpDbClient` instance.

## What This Process Is Designed For

`CSharpDB.Daemon` is designed as a long-running service process for systems that
want to keep one CSharpDB database available to multiple clients through a
stable remote endpoint.

Current design assumptions:

- one daemon process manages one database file
- clients talk to it through `CSharpDB.Client` with `Transport = Grpc` or
  `Transport = Http`
- the daemon is best suited for trusted internal networks, local development, or
  service-to-service communication
- the daemon is a thin transport host over `ICSharpDbClient`, not a separate
  database engine

This makes it a good fit for:

- backend services that should not open the database file directly
- local tools that need a reusable database process
- REST tooling and gRPC clients that should share one warm database instance
- test environments that want a stable remote-style endpoint
- future service-daemon work without adding a second database host process

It is not yet designed as:

- a multi-tenant database server
- a public internet-facing database endpoint
- a multi-database host in one process
- a hardened public production service with built-in auth, metrics, or health
  endpoints

## Current Runtime Model

Today the daemon does the following:

1. starts an ASP.NET Core host
2. registers a direct `ICSharpDbClient` from configuration
3. opens and validates the configured database during startup by calling
   `GetInfoAsync()`
4. exposes REST routes under `/api`, including `/api/info`
5. exposes explicit generated gRPC methods under
   `/csharpdb.rpc.CSharpDbRpc/*` such as
   `/csharpdb.rpc.CSharpDbRpc/GetInfo` and
   `/csharpdb.rpc.CSharpDbRpc/ExecuteSql`

By default the daemon now opens the configured database in lazy-resident hybrid
incremental-durable mode, enables concurrent auto-commit `INSERT` execution on
the host, and applies the write-optimized storage preset. The backing file is
still the single source of durable state; hybrid mode just keeps touched pages
resident in memory inside the daemon process.

The transport contract is implemented in:

- [`src/CSharpDB.Client/Protos/csharpdb_rpc.proto`](../CSharpDB.Client/Protos/csharpdb_rpc.proto)
- [`src/CSharpDB.Client/Internal/GrpcTransportClient.cs`](../CSharpDB.Client/Internal/GrpcTransportClient.cs)
- [`src/CSharpDB.Daemon/Grpc/CSharpDbRpcService.cs`](./Grpc/CSharpDbRpcService.cs)

The contract is now method-based and strongly typed:

- each `ICSharpDbClient` operation maps to an explicit RPC
- complex models use protobuf messages
- dynamic row/document/argument values use a recursive protobuf value shape instead of JSON payload strings
- maintenance operations such as backup/restore, reindex, vacuum, and foreign-key retrofit migration are first-class RPCs rather than a generic tunnel

## Protocol Model

`CSharpDB.Daemon` now hosts both remote protocols by default:

- REST/HTTP routes are available under `/api`
- gRPC routes are available under `/csharpdb.rpc.CSharpDbRpc/*`
- both transports share the same daemon-hosted database client and cache

The standalone [`CSharpDB.Api`](../CSharpDB.Api/README.md) host remains
supported for existing REST-only deployments. Use the daemon when REST and gRPC
clients should share one warm database instance.

## Transport Guidance

Use transports as follows:

- `Direct`: fastest overall when the caller can open the database locally in the same process space; this bypasses the daemon entirely
- `Grpc`: recommended remote transport and the fastest supported network transport in the current codebase
- `Http`: use the daemon for REST/JSON routes that should share the daemon database instance, or use [`CSharpDB.Api`](../CSharpDB.Api/README.md) for standalone REST-only hosting
- `NamedPipes`: reserved in transport enums and parsers, but not implemented end to end today

When `Transport = Grpc` connects to a plain `http://` daemon endpoint,
`CSharpDB.Client` uses gRPC-Web compatibility so gRPC and REST can share the
default HTTP service URL. HTTPS endpoints and custom gRPC test clients can still
use native gRPC.

For remote access to a daemon on another machine, use `Grpc` or `Http` with a
normal base address such as:

```text
http://db-host:5820
https://db-host:5821
```

This is standard TCP networking through ASP.NET Core/Kestrel. It is the
supported cross-server path. Named pipes are not the current answer for
cross-server deployment here.

For best remote performance:

- keep `ICSharpDbClient` instances alive instead of reconnecting per operation
- reuse the same gRPC channel/client for many requests
- prefer `Direct` only when you do not need process or machine isolation

## Requirements

- .NET SDK 10.0 or newer to build from source
- no .NET SDK requirement when using the self-contained release archives
- a filesystem location where the daemon can create and update:
  - `*.db`
  - `*.wal`
- an HTTP/2-capable path between client and daemon when using native gRPC

For local development, plain `http://localhost:...` is enough.

For remote deployment, prefer TLS termination and network controls in front of
the daemon because the daemon currently has no built-in authentication layer.

## Configuration

The daemon reads the database path plus a small daemon-only host database
section:

- `ConnectionStrings:CSharpDB`
- `CSharpDB:Daemon:EnableRestApi`
- `CSharpDB:HostDatabase:OpenMode`
- `CSharpDB:HostDatabase:ImplicitInsertExecutionMode`
- `CSharpDB:HostDatabase:UseWriteOptimizedPreset`
- `CSharpDB:HostDatabase:HotTableNames`
- `CSharpDB:HostDatabase:HotCollectionNames`
- standard ASP.NET Core host settings such as `ASPNETCORE_URLS`
- standard ASP.NET Core environment selection such as
  `ASPNETCORE_ENVIRONMENT`

Default [`appsettings.json`](./appsettings.json):

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  },
  "CSharpDB": {
    "Daemon": {
      "EnableRestApi": true
    },
    "HostDatabase": {
      "OpenMode": "HybridIncrementalDurable",
      "ImplicitInsertExecutionMode": "ConcurrentWriteTransactions",
      "UseWriteOptimizedPreset": true,
      "HotTableNames": [],
      "HotCollectionNames": []
    }
  }
}
```

If no connection string is provided, the daemon falls back to:

```text
Data Source=csharpdb.db
```

Current daemon defaults:

- `EnableRestApi = true`
- `OpenMode = HybridIncrementalDurable`
- `ImplicitInsertExecutionMode = ConcurrentWriteTransactions`
- `UseWriteOptimizedPreset = true`
- `HotTableNames = []`
- `HotCollectionNames = []`

Useful overrides:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\\data\\app.db"
$env:CSharpDB__Daemon__EnableRestApi = "false"
$env:CSharpDB__HostDatabase__OpenMode = "Direct"
$env:CSharpDB__HostDatabase__ImplicitInsertExecutionMode = "Serialized"
$env:CSharpDB__HostDatabase__HotTableNames__0 = "users"
$env:CSharpDB__HostDatabase__HotCollectionNames__0 = "session_cache"
$env:ASPNETCORE_URLS = "http://0.0.0.0:5820"
```

Linux/macOS shell:

```bash
export ConnectionStrings__CSharpDB="Data Source=/var/lib/csharpdb/app.db"
export CSharpDB__Daemon__EnableRestApi="false"
export CSharpDB__HostDatabase__OpenMode="Direct"
export CSharpDB__HostDatabase__ImplicitInsertExecutionMode="Serialized"
export CSharpDB__HostDatabase__HotTableNames__0="users"
export CSharpDB__HostDatabase__HotCollectionNames__0="session_cache"
export ASPNETCORE_URLS="http://0.0.0.0:5820"
```

`OpenMode=HybridIncrementalDurable` is the default and recommended daemon
shape. Use `Direct` only when you want the host to open the backing file
without the lazy-resident hybrid cache. `HotTableNames` and
`HotCollectionNames` are optional hybrid-only preload hints. Set
`CSharpDB:Daemon:EnableRestApi=false` only when the daemon should expose gRPC
without the REST `/api` surface.

## Local Development

Run directly from source:

```powershell
dotnet run --project src/CSharpDB.Daemon/CSharpDB.Daemon.csproj
```

Run with an explicit database path and port:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\\data\\sample.db"
$env:ASPNETCORE_URLS = "http://localhost:5820"
dotnet run --project src/CSharpDB.Daemon/CSharpDB.Daemon.csproj
```

If startup succeeds, the daemon has already opened the database and validated
the configuration.

If you want to start the daemon together with the admin UI, use the helper
scripts documented in [`scripts/README.md`](../../scripts/README.md).

## Client Connection

The intended consumer is `CSharpDB.Client`.

gRPC example:

```csharp
using CSharpDB.Client;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Grpc,
    Endpoint = "http://localhost:5820",
});

var info = await client.GetInfoAsync();
var tables = await client.GetTableNamesAsync();
```

HTTP/REST example against the same daemon:

```csharp
using CSharpDB.Client;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Http,
    Endpoint = "http://localhost:5820",
});

var info = await client.GetInfoAsync();
```

Notes:

- use `Transport = Grpc` for the generated RPC contract
- use `Transport = Http` for REST/JSON routes under `/api`
- set `Endpoint` to the daemon base address, not the raw RPC path
- the client handles the generated gRPC and REST contracts internally
- for remote hosts, prefer `https://...` or private-network `http://...` endpoints with long-lived client reuse

## Deployment Patterns

### 1. Developer Machine

Best when:

- you want a reusable local database process
- you are testing the remote transport

Typical setup:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\\data\\dev.db"
$env:ASPNETCORE_URLS = "http://localhost:5820"
dotnet run --project src/CSharpDB.Daemon/CSharpDB.Daemon.csproj
```

### 2. Service Host Or VM

Best when:

- one app or a small group of internal services needs a shared database process
- the host is under your operational control

Typical pattern:

1. publish the daemon
2. place the binary and config under a dedicated service directory
3. store the database on persistent local disk
4. bind the daemon to a private interface or reverse proxy
5. run it under a dedicated service account

Publish example:

```powershell
dotnet publish src/CSharpDB.Daemon/CSharpDB.Daemon.csproj -c Release -o .\\artifacts\\daemon
```

Self-contained Windows publish:

```powershell
dotnet publish src/CSharpDB.Daemon/CSharpDB.Daemon.csproj -c Release -r win-x64 --self-contained true -o .\\artifacts\\daemon-win-x64
```

Self-contained Linux publish:

```powershell
dotnet publish src/CSharpDB.Daemon/CSharpDB.Daemon.csproj -c Release -r linux-x64 --self-contained true -o ./artifacts/daemon-linux-x64
```

Release archive publish:

```powershell
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime win-x64
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime linux-x64
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime osx-arm64
```

### 3. Sidecar-Style Deployment

Best when:

- a single application should own the database process
- you want remote transport semantics without exposing the database broadly

Typical pattern:

- run the daemon on the same machine or container network as the app
- bind to localhost or a private container port
- point only the colocated app at the daemon

## Release Archives

Tagged releases now attach self-contained daemon archives:

- `csharpdb-daemon-v{version}-win-x64.zip`
- `csharpdb-daemon-v{version}-linux-x64.tar.gz`
- `csharpdb-daemon-v{version}-osx-arm64.tar.gz`
- `SHA256SUMS.txt`

Each archive contains the daemon executable, production-ready default config,
this README, and service install assets under `service/`.

Verify an archive before installing:

```bash
sha256sum -c SHA256SUMS.txt
```

For a maintainer/operator walkthrough that explains which scripts are part of
the release cycle and which scripts are run after downloading an archive, see
[`scripts/README.md`](../../scripts/README.md).

## Example Service Setup

The release archives include service scripts and templates. The scripts copy the
extracted archive into an install directory, write `appsettings.Production.json`,
configure the database path and bind URL, and register the OS service.

### Windows Service

Run from an elevated PowerShell session inside the extracted Windows archive:

```powershell
.\service\windows\install-csharpdb-daemon.ps1 -Start
```

Defaults:

- service name: `CSharpDBDaemon`
- install directory: `C:\Program Files\CSharpDB\Daemon`
- data directory: `C:\ProgramData\CSharpDB`
- bind URL: `http://127.0.0.1:5820`

Override defaults:

```powershell
.\service\windows\install-csharpdb-daemon.ps1 `
  -ServiceName CSharpDBDaemon `
  -InstallDirectory "C:\Services\CSharpDB" `
  -DataDirectory "D:\Data\CSharpDB" `
  -Url "http://0.0.0.0:5820" `
  -Force `
  -Start
```

Uninstall:

```powershell
.\service\windows\uninstall-csharpdb-daemon.ps1
```

### systemd

Run from inside the extracted Linux archive:

```bash
sudo ./service/linux/install-csharpdb-daemon.sh --start
```

Defaults:

- service name: `csharpdb-daemon`
- install directory: `/opt/csharpdb-daemon`
- data directory: `/var/lib/csharpdb`
- service user/group: `csharpdb`
- bind URL: `http://127.0.0.1:5820`

Override defaults:

```bash
sudo ./service/linux/install-csharpdb-daemon.sh \
  --install-dir /srv/csharpdb-daemon \
  --data-dir /srv/csharpdb-data \
  --url http://0.0.0.0:5820 \
  --force \
  --start
```

Uninstall:

```bash
sudo ./service/linux/uninstall-csharpdb-daemon.sh
```

### launchd

Run from inside the extracted macOS archive:

```bash
sudo ./service/macos/install-csharpdb-daemon.sh --start
```

Defaults:

- service label: `com.csharpdb.daemon`
- install directory: `/usr/local/lib/csharpdb-daemon`
- data directory: `/usr/local/var/csharpdb`
- bind URL: `http://127.0.0.1:5820`

Override defaults:

```bash
sudo ./service/macos/install-csharpdb-daemon.sh \
  --install-dir /usr/local/lib/csharpdb-daemon \
  --data-dir /usr/local/var/csharpdb \
  --url http://0.0.0.0:5820 \
  --force \
  --start
```

Uninstall:

```bash
sudo ./service/macos/uninstall-csharpdb-daemon.sh
```

## Upgrade And Configuration

To upgrade, extract the newer archive and rerun the matching install script with
the same service name, data directory, and `--force` / `-Force`. The scripts
replace the installed daemon files but do not delete the database directory.

The generated production config keeps the current daemon defaults:

- `OpenMode = HybridIncrementalDurable`
- `ImplicitInsertExecutionMode = ConcurrentWriteTransactions`
- `UseWriteOptimizedPreset = true`

Service-level environment variables still override JSON configuration. The
supported keys remain the standard daemon settings:

- `ConnectionStrings__CSharpDB`
- `ASPNETCORE_URLS`
- `CSharpDB__Daemon__EnableRestApi`
- `CSharpDB__HostDatabase__OpenMode`
- `CSharpDB__HostDatabase__ImplicitInsertExecutionMode`
- `CSharpDB__HostDatabase__UseWriteOptimizedPreset`
- `CSharpDB__HostDatabase__HotTableNames__0`
- `CSharpDB__HostDatabase__HotCollectionNames__0`

## Storage And Filesystem Notes

Because the daemon uses the configured database file directly, treat the
database directory as durable application state.

Operational guidance:

- place the database on local persistent storage, not a transient temp folder
- back up both the `.db` and any active recovery artifacts according to your
  operational policy
- avoid having unrelated processes manipulate the same database files directly
  while the daemon is responsible for them

## Networking Notes

Native gRPC requires HTTP/2 semantics. For the default plaintext HTTP service
URL, `CSharpDB.Client` uses gRPC-Web compatibility for `Transport = Grpc` so
gRPC and REST can share the same base URL. REST uses ordinary HTTP semantics
under `/api`.

Practical guidance:

- local development can use `http://localhost:...`
- internal deployments should prefer private networking
- if you place the daemon behind a proxy or ingress, make sure that proxy
  correctly supports native gRPC or gRPC-Web, depending on the client path
- do not expose the daemon directly to untrusted clients until authentication,
  authorization, and stronger operational controls exist

## Observability And Operations

Current state:

- startup fails early if the database cannot be opened
- ASP.NET Core logging is available through the standard host logging pipeline
- `/api/info` is available when REST hosting is enabled

Not implemented yet in this host:

- `/health`
- `/metrics`
- authentication
- authorization
- TLS-specific configuration helpers
- admin endpoints beyond the existing database REST API

For broader future direction, see
[`docs/roadmap.md`](../../docs/roadmap.md).

## Troubleshooting

### The client cannot connect

Check:

- the daemon is running
- `Endpoint` points at the daemon base address
- `Transport = Grpc` or `Transport = Http` is set in `CSharpDbClientOptions`
- the bound URL in `ASPNETCORE_URLS` is reachable from the client

### REST returns 404 under `/api`

Check whether `CSharpDB:Daemon:EnableRestApi` or
`CSharpDB__Daemon__EnableRestApi` has been set to `false`.

### Startup fails immediately

Check:

- `ConnectionStrings:CSharpDB` is present and valid
- the database directory exists
- the daemon process has read/write access to that directory

### gRPC works locally but fails behind infrastructure

Check:

- HTTP/2 support through the full request path
- proxy or ingress gRPC support
- whether TLS termination changed protocol handling

## Source Layout

Important files:

- [`Program.cs`](./Program.cs) - daemon host startup and service wiring
- [`Grpc/CSharpDbRpcService.cs`](./Grpc/CSharpDbRpcService.cs) - gRPC server implementation
- [`../CSharpDB.Api/CSharpDbRestApiHostExtensions.cs`](../CSharpDB.Api/CSharpDbRestApiHostExtensions.cs) - shared REST host wiring
- [`appsettings.json`](./appsettings.json) - default configuration
- [`../CSharpDB.Client/Protos/csharpdb_rpc.proto`](../CSharpDB.Client/Protos/csharpdb_rpc.proto) - transport contract

## Status

This README documents the current daemon implementation, v3.4.0 service
packaging, and v3.4.0 REST/gRPC host consolidation. Auth, TLS helpers, and
marketplace distribution remain tracked in
[`docs/roadmap.md`](../../docs/roadmap.md).

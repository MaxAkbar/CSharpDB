# CSharpDB.Daemon

`CSharpDB.Daemon` is the dedicated gRPC host for CSharpDB.

It exposes the `CSharpDB.Client` contract over gRPC and keeps that transport
separate from the REST-only [`CSharpDB.Api`](../CSharpDB.Api/README.md) host.

## What This Process Is Designed For

`CSharpDB.Daemon` is designed as a long-running service process for systems that
want to keep one CSharpDB database available to multiple clients through a
stable gRPC endpoint.

Current design assumptions:

- one daemon process manages one database file
- clients talk to it through `CSharpDB.Client` with `Transport = Grpc`
- the daemon is best suited for trusted internal networks, local development, or
  service-to-service communication
- the daemon is a thin transport host over `ICSharpDbClient`, not a separate
  database engine

This makes it a good fit for:

- backend services that should not open the database file directly
- local tools that need a reusable database process
- test environments that want a stable remote-style endpoint
- future service-daemon work without coupling that work to the REST API host

It is not yet designed as:

- a multi-tenant database server
- a public internet-facing database endpoint
- a multi-database host in one process
- a hardened production service with built-in auth, metrics, health endpoints,
  or OS service installers

## Current Runtime Model

Today the daemon does the following:

1. starts an ASP.NET Core host
2. registers `ICSharpDbClient` from configuration
3. opens and validates the configured database during startup by calling
   `GetInfoAsync()`
4. exposes explicit generated gRPC methods under
   `/csharpdb.rpc.CSharpDbRpc/*` such as
   `/csharpdb.rpc.CSharpDbRpc/GetInfo` and
   `/csharpdb.rpc.CSharpDbRpc/ExecuteSql`

The transport contract is implemented in:

- [`src/CSharpDB.Client/Protos/csharpdb_rpc.proto`](../CSharpDB.Client/Protos/csharpdb_rpc.proto)
- [`src/CSharpDB.Client/Internal/GrpcTransportClient.cs`](../CSharpDB.Client/Internal/GrpcTransportClient.cs)
- [`src/CSharpDB.Daemon/Grpc/CSharpDbRpcService.cs`](./Grpc/CSharpDbRpcService.cs)

The contract is now method-based and strongly typed:

- each `ICSharpDbClient` operation maps to an explicit RPC
- complex models use protobuf messages
- dynamic row/document/argument values use a recursive protobuf value shape instead of JSON payload strings

## Protocol Boundary

The host split is intentional:

- `CSharpDB.Api` is the REST/HTTP host
- `CSharpDB.Daemon` is the gRPC host

If you want HTTP/JSON routes under `/api/...`, use `CSharpDB.Api`.

If you want the `CSharpDB.Client` remote transport over gRPC, use
`CSharpDB.Daemon`.

## Requirements

- .NET SDK 10.0 or newer to build from source
- a filesystem location where the daemon can create and update:
  - `*.db`
  - `*.wal`
- an HTTP/2-capable path between client and daemon

For local development, plain `http://localhost:...` is enough.

For remote deployment, prefer TLS termination and network controls in front of
the daemon because the daemon currently has no built-in authentication layer.

## Configuration

The daemon currently reads only a small amount of configuration:

- `ConnectionStrings:CSharpDB`
- standard ASP.NET Core host settings such as `ASPNETCORE_URLS`
- standard ASP.NET Core environment selection such as
  `ASPNETCORE_ENVIRONMENT`

Default [`appsettings.json`](./appsettings.json):

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=csharpdb.db"
  }
}
```

If no connection string is provided, the daemon falls back to:

```text
Data Source=csharpdb.db
```

Useful overrides:

```powershell
$env:ConnectionStrings__CSharpDB = "Data Source=C:\\data\\app.db"
$env:ASPNETCORE_URLS = "http://0.0.0.0:5820"
```

Linux/macOS shell:

```bash
export ConnectionStrings__CSharpDB="Data Source=/var/lib/csharpdb/app.db"
export ASPNETCORE_URLS="http://0.0.0.0:5820"
```

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

## Client Connection

The intended consumer is `CSharpDB.Client`.

Example:

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

Notes:

- use `Transport = Grpc`
- set `Endpoint` to the daemon base address, not the raw RPC path
- the client handles the generated gRPC contract internally

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

### 3. Sidecar-Style Deployment

Best when:

- a single application should own the database process
- you want remote transport semantics without exposing the database broadly

Typical pattern:

- run the daemon on the same machine or container network as the app
- bind to localhost or a private container port
- point only the colocated app at the daemon

## Example Service Setup

The repo does not yet ship production-ready install scripts for systemd,
Windows Service, or launchd. Current deployment is manual.

Use the following as starting points, not as final supported installers.

### Windows Service

Publish first, then register the executable:

```powershell
sc.exe create CSharpDBDaemon binPath= "C:\Services\CSharpDB\CSharpDB.Daemon.exe"
sc.exe start CSharpDBDaemon
```

Recommended:

- set `ConnectionStrings__CSharpDB` as a machine or service environment value
- run under a dedicated account with access only to the database directory
- bind to a private address where possible

### systemd

Example unit:

```ini
[Unit]
Description=CSharpDB gRPC Daemon
After=network.target

[Service]
WorkingDirectory=/opt/csharpdb
ExecStart=/opt/csharpdb/CSharpDB.Daemon
Environment=ConnectionStrings__CSharpDB=Data Source=/var/lib/csharpdb/app.db
Environment=ASPNETCORE_URLS=http://0.0.0.0:5820
Restart=on-failure
User=csharpdb
Group=csharpdb

[Install]
WantedBy=multi-user.target
```

### launchd

The same model applies on macOS:

- publish the daemon
- register it with `launchd`
- provide `ConnectionStrings__CSharpDB` and `ASPNETCORE_URLS`
- run it as a dedicated service user where appropriate

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

gRPC requires HTTP/2 semantics.

Practical guidance:

- local development can use `http://localhost:...`
- internal deployments should prefer private networking
- if you place the daemon behind a proxy or ingress, make sure that proxy
  correctly supports gRPC
- do not expose the daemon directly to untrusted clients until authentication,
  authorization, and stronger operational controls exist

## Observability And Operations

Current state:

- startup fails early if the database cannot be opened
- ASP.NET Core logging is available through the standard host logging pipeline

Not implemented yet in this host:

- `/health`
- `/metrics`
- authentication
- authorization
- TLS-specific configuration helpers
- admin endpoints
- service install scripts

For broader future direction, see
[`docs/service-daemon/README.md`](../../docs/service-daemon/README.md).

## Troubleshooting

### The client cannot connect

Check:

- the daemon is running
- `Endpoint` points at the daemon base address
- `Transport = Grpc` is set in `CSharpDbClientOptions`
- the bound URL in `ASPNETCORE_URLS` is reachable from the client

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
- [`appsettings.json`](./appsettings.json) - default configuration
- [`../CSharpDB.Client/Protos/csharpdb_rpc.proto`](../CSharpDB.Client/Protos/csharpdb_rpc.proto) - transport contract

## Status

This README documents the current daemon implementation, not the full planned
service-daemon feature set. The broader roadmap remains in
[`docs/service-daemon/README.md`](../../docs/service-daemon/README.md).

# Scripts

These scripts are developer, operator, and release helpers for local source
runs, repository maintenance, and CSharpDB daemon release packaging.

The local start scripts launch `dotnet run` processes from the repo and update
the admin app config so the web UI starts in the expected transport mode.
Daemon service install scripts live under [`deploy/daemon`](../deploy/daemon)
and are included in daemon release archives.

## How The Scripts Fit The Release Cycle

There are three different script groups:

- Release maintainers use `scripts/Publish-CSharpDbDaemonRelease.ps1` directly
  for local packaging checks. The GitHub Release workflow also uses this script
  when a `v*` tag is pushed.
- Operators use the service scripts after a release is published. These scripts
  are included inside each daemon archive under `service/`.
- Developers use `Start-CSharpDbAdminAndDaemon.ps1` and
  `Start-CSharpDbAdminDirect.ps1` for local source runs. Developers can also
  use `Start-CSharpDbAdminFormsWeb.ps1` when they only need the runtime form
  host. These are not release packaging scripts.

## Release Maintainer Walkthrough

Use this path before tagging or when validating release packaging locally.

1. Build and test the repo as usual.

```powershell
dotnet build CSharpDB.slnx -c Release
dotnet test tests\CSharpDB.Daemon.Tests\CSharpDB.Daemon.Tests.csproj -c Release
```

2. Publish one local archive for a fast packaging check.

```powershell
.\scripts\Publish-CSharpDbDaemonRelease.ps1 `
  -Version 3.4.0 `
  -Runtime win-x64 `
  -OutputRoot artifacts\daemon-release-local
```

3. Confirm the archive and checksum exist.

```powershell
Get-ChildItem artifacts\daemon-release-local\archives
Get-Content artifacts\daemon-release-local\archives\SHA256SUMS.txt
```

4. For the real release, push a `v*` tag. The GitHub Release workflow publishes
   the daemon archives for `win-x64`, `linux-x64`, and `osx-arm64`, smoke-starts
   each extracted binary, calls the daemon REST `/api/info` endpoint, verifies a
   gRPC `GetInfoAsync` client call, combines checksums, and attaches everything
   to the GitHub Release.

```powershell
git tag v3.4.0
git push origin v3.4.0
```

## Operator Walkthrough

Use this path after a GitHub Release is published.

1. Download the daemon archive for the target OS:
   `csharpdb-daemon-v{version}-win-x64.zip`,
   `csharpdb-daemon-v{version}-linux-x64.tar.gz`, or
   `csharpdb-daemon-v{version}-osx-arm64.tar.gz`.
2. Verify the archive with the published `SHA256SUMS.txt`.
3. Extract the archive on the target machine.
4. Run the matching service installer from inside the extracted archive.
5. Connect REST clients to `http://127.0.0.1:5820/api/...` or gRPC clients to
   the same base URL. The installed daemon exposes both transports by default.

Windows, from an elevated PowerShell session:

```powershell
.\service\windows\install-csharpdb-daemon.ps1 -Start
```

Linux:

```bash
sudo ./service/linux/install-csharpdb-daemon.sh --start
```

macOS:

```bash
sudo ./service/macos/install-csharpdb-daemon.sh --start
```

To upgrade, extract the newer archive and rerun the same installer with the
same data directory plus `-Force` on Windows or `--force` on Linux/macOS. The
installers replace daemon files but do not delete the database directory.

To disable the daemon REST surface while keeping gRPC enabled, set
`CSharpDB__Daemon__EnableRestApi=false` in the service environment or generated
`appsettings.Production.json`, then restart the service.

## Scripts

### `Clear-GitHubWorkflowRuns.ps1`

Use this when the GitHub Actions run history has become noisy and you want to
delete older completed runs.

What it does:

- resolves the GitHub repository from the local `origin` remote by default
- enumerates workflow runs through the GitHub CLI
- deletes only completed runs older than the configured cutoff
- supports `-WhatIf` and `-Confirm` for safe dry runs

Example:

```powershell
& .\scripts\Clear-GitHubWorkflowRuns.ps1 -OlderThanDays 7
& .\scripts\Clear-GitHubWorkflowRuns.ps1 -OlderThanDays 30 -WhatIf
```

### `Start-CSharpDbAdminAndDaemon.ps1`

Use this when the admin site should talk to the gRPC daemon.

What it does:

- reads the daemon launch URL from
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- reads the daemon database connection string from
  [`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json)
- updates [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json) to:
  - set `CSharpDB.Transport = "grpc"`
  - set `CSharpDB.Endpoint` to the daemon base URL
  - copy the daemon connection string into `ConnectionStrings:CSharpDB`
- starts `CSharpDB.Daemon`
- waits for the daemon port to come up
- starts `CSharpDB.Admin`

The daemon started by this script also serves the REST `/api` surface on the
same base URL unless `CSharpDB:Daemon:EnableRestApi` is disabled.

### `Start-CSharpDbAdminDirect.ps1`

Use this when the admin site should open a local database directly without the
daemon.

What it does:

- updates [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json) to:
  - set `CSharpDB.Transport = "direct"`
  - remove `CSharpDB.Endpoint`
  - keep or set `ConnectionStrings:CSharpDB`
- starts only `CSharpDB.Admin`

The default Admin direct configuration uses
`CSharpDB:HostDatabase:OpenMode = "HybridIncrementalDurable"`, so direct mode
opens through the hybrid incremental-durable local host path. The Admin host
warms one in-process database instance at startup and keeps it alive until the
Admin app shuts down or the user switches databases. Set
`CSharpDB:HostDatabase:OpenMode = "Direct"` if you need the older plain direct
open path for a local run.

### `Start-CSharpDbAdminFormsWeb.ps1`

Use this when you want to run stored forms through the forms-only runtime host
without opening the full Admin studio.

What it does:

- reads the default target database path from
  [`src/CSharpDB.Admin.Forms.Web/appsettings.json`](../src/CSharpDB.Admin.Forms.Web/appsettings.json)
  unless you pass `-DataSource`
- starts `src/CSharpDB.Admin.Forms.Web`
- passes the resolved `CSharpDB:DataSource` and `--urls` values through
  command-line configuration overrides
- waits for the forms host port to accept TCP connections
- optionally opens the root runtime page in the default browser

Use a sample database that already contains seeded forms, such as the
Fulfillment Hub sample database, when you want the runtime root page to list
real forms immediately.

### `Publish-CSharpDbDaemonRelease.ps1`

Use this when preparing self-contained daemon release archives.

What it does:

- publishes `src/CSharpDB.Daemon` for one or more runtime identifiers
- uses Release, self-contained, single-file, non-trimmed publish settings
- stages the daemon with service assets from `deploy/daemon`
- creates `csharpdb-daemon-v{version}-{rid}.zip` for Windows
- creates `csharpdb-daemon-v{version}-{rid}.tar.gz` for Linux/macOS
- writes `SHA256SUMS.txt`

Examples:

```powershell
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime win-x64
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime linux-x64,osx-arm64
```

Default runtimes:

- `win-x64`
- `linux-x64`
- `osx-arm64`

Outputs are written under `artifacts\daemon-release` unless `-OutputRoot` is
provided.

## Daemon Service Installers

Release archives include OS service assets under `service/`:

- Windows: `service/windows/install-csharpdb-daemon.ps1`
- Windows: `service/windows/uninstall-csharpdb-daemon.ps1`
- Linux: `service/linux/csharpdb-daemon.service`
- Linux: `service/linux/install-csharpdb-daemon.sh`
- Linux: `service/linux/uninstall-csharpdb-daemon.sh`
- macOS: `service/macos/com.csharpdb.daemon.plist`
- macOS: `service/macos/install-csharpdb-daemon.sh`
- macOS: `service/macos/uninstall-csharpdb-daemon.sh`

Default service settings:

| Platform | Service | Install directory | Data directory | URL |
|----------|---------|-------------------|----------------|-----|
| Windows | `CSharpDBDaemon` | `C:\Program Files\CSharpDB\Daemon` | `C:\ProgramData\CSharpDB` | `http://127.0.0.1:5820` |
| Linux | `csharpdb-daemon` | `/opt/csharpdb-daemon` | `/var/lib/csharpdb` | `http://127.0.0.1:5820` |
| macOS | `com.csharpdb.daemon` | `/usr/local/lib/csharpdb-daemon` | `/usr/local/var/csharpdb` | `http://127.0.0.1:5820` |

Installers accept service name, install directory, data directory, bind URL,
and force/overwrite options. Windows scripts support `-WhatIf`; Linux and macOS
scripts require `sudo` and fail early when not run as root.

The generated production config enables REST by default through
`CSharpDB:Daemon:EnableRestApi=true`. Service-level environment variables can
override this with `CSharpDB__Daemon__EnableRestApi=false`.

## Quick Start

From the repo root in PowerShell:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1
```

Start the admin in direct mode:

```powershell
& .\scripts\Start-CSharpDbAdminDirect.ps1
```

Start the forms-only runtime host against the Fulfillment Hub sample database:

```powershell
& .\scripts\Start-CSharpDbAdminFormsWeb.ps1 `
  -DataSource samples\fulfillment-hub\bin\Debug\net10.0\fulfillment-hub-demo.db `
  -OpenBrowser
```

Open the admin site automatically after startup:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -OpenAdmin
```

Use a specific direct-mode database:

```powershell
& .\scripts\Start-CSharpDbAdminDirect.ps1 -ConnectionString "Data Source=C:\data\demo.db"
```

## What Gets Changed

Both scripts rewrite
[`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json).

That means:

- the current admin transport mode persists after the script exits
- if you switch between gRPC mode and direct mode, the last script you ran wins
- if you have local edits in `src/CSharpDB.Admin/appsettings.json`, the script
  may overwrite those transport-related settings

The daemon script does not modify
[`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json).
It only reads from it.

## Start And Stop Workflow

### Recommended: capture the process IDs

Use `-PassThru` so PowerShell returns the host PIDs:

```powershell
$session = & .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -PassThru
$session
```

Example output object:

```text
DaemonEndpoint : https://localhost:49995
AdminUrl       : https://localhost:61816
DaemonPid      : 12345
AdminPid       : 23456
```

Stop both hosts:

```powershell
Stop-Process -Id $session.AdminPid, $session.DaemonPid
```

For direct mode:

```powershell
$session = & .\scripts\Start-CSharpDbAdminDirect.ps1 -PassThru
Stop-Process -Id $session.AdminPid
```

### If you already started them without `-PassThru`

Find the running host processes by command line:

```powershell
Get-CimInstance Win32_Process |
  Where-Object {
    $_.Name -eq 'dotnet.exe' -and
    $_.CommandLine -match 'CSharpDB.Admin|CSharpDB.Daemon'
  } |
  Select-Object ProcessId, CommandLine
```

Stop them:

```powershell
Get-CimInstance Win32_Process |
  Where-Object {
    $_.Name -eq 'dotnet.exe' -and
    $_.CommandLine -match 'CSharpDB.Admin|CSharpDB.Daemon'
  } |
  ForEach-Object { Stop-Process -Id $_.ProcessId }
```

## Common Options

### `-NoLaunch`

Update config only without starting any process:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -NoLaunch
& .\scripts\Start-CSharpDbAdminDirect.ps1 -NoLaunch
```

This is useful when you want to inspect the config change first.

### `-OpenAdmin`

Open the admin URL in the default browser after startup succeeds:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -OpenAdmin
```

### `-PassThru`

Return the resolved URLs and PIDs:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -PassThru
& .\scripts\Start-CSharpDbAdminDirect.ps1 -PassThru
```

### Startup timeout overrides

If your machine is slow or the first build is cold, increase the wait time:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1 `
  -DaemonStartupTimeoutSeconds 60 `
  -AdminStartupTimeoutSeconds 60
```

## Config Sources

The admin-and-daemon startup script resolves values from the repo files below:

- daemon URL:
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- daemon database connection string:
  [`src/CSharpDB.Daemon/appsettings.json`](../src/CSharpDB.Daemon/appsettings.json)
- admin URL:
  [`src/CSharpDB.Admin/Properties/launchSettings.json`](../src/CSharpDB.Admin/Properties/launchSettings.json)

The direct-mode script resolves values from:

- admin URL:
  [`src/CSharpDB.Admin/Properties/launchSettings.json`](../src/CSharpDB.Admin/Properties/launchSettings.json)
- admin database connection string:
  [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json), unless you pass `-ConnectionString`

The forms-runtime script resolves values from:

- forms host database path:
  [`src/CSharpDB.Admin.Forms.Web/appsettings.json`](../src/CSharpDB.Admin.Forms.Web/appsettings.json), unless you pass `-DataSource`
- forms host URL:
  the script `-Url` parameter, defaulting to `http://127.0.0.1:5095`

## Use `Get-Help`

Both scripts now include comment-based help:

```powershell
Get-Help .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -Detailed
Get-Help .\scripts\Start-CSharpDbAdminDirect.ps1 -Detailed
Get-Help .\scripts\Start-CSharpDbAdminFormsWeb.ps1 -Detailed
```

## Troubleshooting

### The admin starts in the wrong mode

Check the current values in
[`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json):

- `CSharpDB.Transport`
- `CSharpDB.Endpoint`
- `ConnectionStrings:CSharpDB`

Run the appropriate script again with `-NoLaunch` if you want to confirm the
config update without starting the hosts.

### The daemon script times out

Check:

- the daemon launch URL in
  [`src/CSharpDB.Daemon/Properties/launchSettings.json`](../src/CSharpDB.Daemon/Properties/launchSettings.json)
- whether another process is already using that port
- whether HTTPS development certificates or local firewall rules are blocking
  the host startup

### Closing the shell did not stop the hosts

That is expected. The scripts use `Start-Process`, so the launched `dotnet`
processes keep running until you stop them explicitly.

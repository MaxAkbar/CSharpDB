# Scripts

These scripts are developer and operator helpers for local source runs and
repository maintenance tasks.

They do not install Windows services, `systemd` units, or scheduled tasks. They
launch `dotnet run` processes from the repo and update the admin app config so
the web UI starts in the expected transport mode.

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

### `Start-CSharpDbAdminDirect.ps1`

Use this when the admin site should open the database file directly without the
daemon.

What it does:

- updates [`src/CSharpDB.Admin/appsettings.json`](../src/CSharpDB.Admin/appsettings.json) to:
  - set `CSharpDB.Transport = "direct"`
  - remove `CSharpDB.Endpoint`
  - keep or set `ConnectionStrings:CSharpDB`
- starts only `CSharpDB.Admin`

## Quick Start

From the repo root in PowerShell:

```powershell
& .\scripts\Start-CSharpDbAdminAndDaemon.ps1
```

Start the admin in direct mode:

```powershell
& .\scripts\Start-CSharpDbAdminDirect.ps1
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

The gRPC startup script resolves values from the repo files below:

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

## Use `Get-Help`

Both scripts now include comment-based help:

```powershell
Get-Help .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -Detailed
Get-Help .\scripts\Start-CSharpDbAdminDirect.ps1 -Detailed
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

<#
.SYNOPSIS
Starts the forms-only web host against a target CSharpDB database.

.DESCRIPTION
This script is intended for local development and operator-style runtime
testing. It starts `src/CSharpDB.Admin.Forms.Web`, points it at a direct local
database path, waits for the web host to accept TCP connections, and can open
the runtime root page in the default browser.

The script does not rewrite project configuration files. It passes the target
database path and bind URL as command-line configuration overrides to
`dotnet run`.

.PARAMETER DataSource
Overrides the target database path. When omitted, the script uses
`CSharpDB:DataSource` from `src/CSharpDB.Admin.Forms.Web/appsettings.json`,
falling back to `csharpdb.db`.

.PARAMETER Url
The URL passed to `--urls` for the forms host. Defaults to
`http://127.0.0.1:5095`.

.PARAMETER NoLaunch
Resolves values but does not start the forms host.

.PARAMETER OpenBrowser
Opens the forms runtime root page in the default browser after startup
succeeds.

.PARAMETER PassThru
Returns the resolved URL, database path, and process ID so the caller can stop
the process later with `Stop-Process`.

.PARAMETER StartupTimeoutSeconds
How long to wait for the forms host endpoint to start accepting TCP
connections.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\Start-CSharpDbAdminFormsWeb.ps1

Starts the forms runtime host using the default data source from
`src/CSharpDB.Admin.Forms.Web/appsettings.json`.

.EXAMPLE
.\scripts\Start-CSharpDbAdminFormsWeb.ps1 `
  -DataSource C:\Users\maxim\source\Code\CSharpDB\samples\fulfillment-hub\bin\Debug\net10.0\fulfillment-hub-demo.db `
  -OpenBrowser

Starts the forms runtime host against the Fulfillment Hub sample database and
opens the root page in the default browser.

.EXAMPLE
$session = & .\scripts\Start-CSharpDbAdminFormsWeb.ps1 -PassThru
Stop-Process -Id $session.FormsPid

Starts the forms runtime host and captures the process ID so it can be stopped
later.
#>
[CmdletBinding()]
param(
    [string]$DataSource,
    [string]$Url = 'http://127.0.0.1:5095',
    [switch]$NoLaunch,
    [switch]$OpenBrowser,
    [switch]$PassThru,
    [int]$StartupTimeoutSeconds = 30
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Read-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path -Path $Path)) {
        throw "Required file was not found: $Path"
    }

    return Get-Content -Path $Path -Raw | ConvertFrom-Json
}

function Wait-ForTcpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [Uri]$Uri,
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds,
        [System.Diagnostics.Process]$Process
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        if ($null -ne $Process) {
            $Process.Refresh()
            if ($Process.HasExited) {
                return $false
            }
        }

        $client = $null

        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $connectTask = $client.ConnectAsync($Uri.Host, $Uri.Port)

            if ($connectTask.Wait([TimeSpan]::FromSeconds(1)) -and $client.Connected) {
                return $true
            }
        }
        catch {
        }
        finally {
            if ($null -ne $client) {
                $client.Dispose()
            }
        }

        Start-Sleep -Milliseconds 500
    }

    return $false
}

function Test-TcpEndpointAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [Uri]$Uri
    )

    $client = $null

    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $connectTask = $client.ConnectAsync($Uri.Host, $Uri.Port)
        return -not ($connectTask.Wait([TimeSpan]::FromMilliseconds(400)) -and $client.Connected)
    }
    catch {
        return $true
    }
    finally {
        if ($null -ne $client) {
            $client.Dispose()
        }
    }
}

function Stop-ProcessIfRunning {
    param(
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process]$Process
    )

    $Process.Refresh()
    if (-not $Process.HasExited) {
        Stop-Process -Id $Process.Id -Force
    }
}

function Get-ConfiguredDataSource {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Config
    )

    if ($null -ne $Config.PSObject.Properties['CSharpDB'] -and
        $null -ne $Config.CSharpDB -and
        $null -ne $Config.CSharpDB.PSObject.Properties['DataSource'] -and
        -not [string]::IsNullOrWhiteSpace($Config.CSharpDB.DataSource)) {
        return $Config.CSharpDB.DataSource
    }

    if ($null -ne $Config.PSObject.Properties['ConnectionStrings'] -and
        $null -ne $Config.ConnectionStrings -and
        $null -ne $Config.ConnectionStrings.PSObject.Properties['CSharpDB'] -and
        -not [string]::IsNullOrWhiteSpace($Config.ConnectionStrings.CSharpDB)) {
        $match = [regex]::Match($Config.ConnectionStrings.CSharpDB, '(?i)\bData Source\s*=\s*(?<value>[^;]+)')
        if ($match.Success) {
            return $match.Groups['value'].Value.Trim()
        }
    }

    return 'csharpdb.db'
}

function Resolve-RepoRelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue,
        [Parameter(Mandatory = $true)]
        [string]$RepoRoot
    )

    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $PathValue
    }

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return $PathValue
    }

    return (Join-Path $RepoRoot $PathValue)
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..')).Path

$formsWebProjectPath = Join-Path $repoRoot 'src\CSharpDB.Admin.Forms.Web\CSharpDB.Admin.Forms.Web.csproj'
$formsWebAppSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Admin.Forms.Web\appsettings.json'

if (-not (Test-Path -Path $formsWebProjectPath)) {
    throw "Forms web project file was not found: $formsWebProjectPath"
}

$formsWebConfig = Read-JsonFile -Path $formsWebAppSettingsPath
$effectiveDataSource = if ([string]::IsNullOrWhiteSpace($DataSource)) {
    Get-ConfiguredDataSource -Config $formsWebConfig
}
else {
    $DataSource
}

$resolvedDataSource = Resolve-RepoRelativePath -PathValue $effectiveDataSource -RepoRoot $repoRoot
$urlUri = [Uri]$Url

if (-not (Test-TcpEndpointAvailable -Uri $urlUri)) {
    throw "The forms runtime URL is already accepting TCP connections: $Url"
}

if (-not (Test-Path -Path $resolvedDataSource)) {
    Write-Warning "The target database path does not exist yet: $resolvedDataSource"
}

$result = [pscustomobject]@{
    FormsUrl   = $Url
    DataSource = $resolvedDataSource
    FormsPid   = $null
}

if ($NoLaunch) {
    if ($PassThru) {
        return $result
    }

    Write-Host "Resolved forms host settings:"
    Write-Host "  Url: $Url"
    Write-Host "  DataSource: $resolvedDataSource"
    return
}

$dotnetArgs = @(
    'run',
    '--project', $formsWebProjectPath,
    '--',
    '--urls', $Url,
    "--CSharpDB:DataSource=$resolvedDataSource"
)

Write-Host "Starting CSharpDB.Admin.Forms.Web..."
Write-Host "  Url: $Url"
Write-Host "  DataSource: $resolvedDataSource"

$formsProcess = Start-Process -FilePath 'dotnet' -ArgumentList $dotnetArgs -WorkingDirectory $repoRoot -PassThru

try {
    if (-not (Wait-ForTcpEndpoint -Uri $urlUri -TimeoutSeconds $StartupTimeoutSeconds -Process $formsProcess)) {
        Stop-ProcessIfRunning -Process $formsProcess
        throw "The forms runtime did not start accepting TCP connections within $StartupTimeoutSeconds second(s): $Url"
    }
}
catch {
    Stop-ProcessIfRunning -Process $formsProcess
    throw
}

Write-Host "CSharpDB.Admin.Forms.Web is running."
Write-Host "  PID: $($formsProcess.Id)"
Write-Host "  Url: $Url"

if ($OpenBrowser) {
    Start-Process $Url | Out-Null
}

$result.FormsPid = $formsProcess.Id

if ($PassThru) {
    return $result
}

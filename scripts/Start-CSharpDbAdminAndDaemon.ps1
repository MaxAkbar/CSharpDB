<#
.SYNOPSIS
Configures the admin site for gRPC mode, then starts the daemon and admin hosts.

.DESCRIPTION
This script is intended for local development and manual operator workflows.
It reads the daemon launch profile and configuration, updates
`src/CSharpDB.Admin/appsettings.json` to point at the daemon over gRPC, starts
`CSharpDB.Daemon`, waits for the daemon port to accept TCP connections, and
then starts `CSharpDB.Admin`.

The script does not install Windows services or background tasks. It launches
two `dotnet run` processes. If you close the shell that launched this script,
those child processes continue running until you stop them explicitly.

.PARAMETER NoLaunch
Only updates the admin configuration. Does not start either host.

.PARAMETER OpenAdmin
Opens the admin URL in the default browser after startup succeeds.

.PARAMETER PassThru
Returns the resolved URLs and process IDs so the caller can stop the processes
later with `Stop-Process`.

.PARAMETER DaemonStartupTimeoutSeconds
How long to wait for the daemon endpoint to start accepting TCP connections.

.PARAMETER AdminStartupTimeoutSeconds
How long to wait for the admin endpoint to start accepting TCP connections.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\Start-CSharpDbAdminAndDaemon.ps1

Starts the daemon and the admin site using the launch profiles in the source
tree.

.EXAMPLE
$session = & .\scripts\Start-CSharpDbAdminAndDaemon.ps1 -PassThru
Stop-Process -Id $session.AdminPid, $session.DaemonPid

Starts both hosts and captures the process IDs so they can be stopped later.

.EXAMPLE
.\scripts\Start-CSharpDbAdminAndDaemon.ps1 -NoLaunch -PassThru

Updates the admin config for gRPC mode and returns the resolved URLs without
starting either host.
#>
[CmdletBinding()]
param(
    [switch]$NoLaunch,
    [switch]$OpenAdmin,
    [switch]$PassThru,
    [int]$DaemonStartupTimeoutSeconds = 30,
    [int]$AdminStartupTimeoutSeconds = 30
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

function Get-OrAddProperty {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [object]$DefaultValue
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $DefaultValue
        return $DefaultValue
    }

    return $property.Value
}

function Set-JsonProperty {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [AllowNull()]
        [object]$Value
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value
        return
    }

    $Object.$Name = $Value
}

function Get-LaunchProfile {
    param(
        [Parameter(Mandatory = $true)]
        [object]$LaunchSettings,
        [Parameter(Mandatory = $true)]
        [string]$PreferredProfileName
    )

    $profiles = $LaunchSettings.PSObject.Properties['profiles']
    if ($null -eq $profiles -or $null -eq $profiles.Value) {
        throw 'The launch settings file does not define any profiles.'
    }

    $preferred = $profiles.Value.PSObject.Properties[$PreferredProfileName]
    if ($null -ne $preferred) {
        return [pscustomobject]@{
            Name    = $preferred.Name
            Profile = $preferred.Value
        }
    }

    $firstProfile = @($profiles.Value.PSObject.Properties)[0]
    if ($null -eq $firstProfile) {
        throw 'The launch settings file does not define any profiles.'
    }

    return [pscustomobject]@{
        Name    = $firstProfile.Name
        Profile = $firstProfile.Value
    }
}

function Select-PreferredUrl {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ApplicationUrl,
        [string]$PreferredScheme = 'https'
    )

    $urls = @(
        $ApplicationUrl.Split(';', [System.StringSplitOptions]::RemoveEmptyEntries) |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ }
    )

    if ($urls.Count -eq 0) {
        return $null
    }

    $preferredPrefix = '{0}://' -f $PreferredScheme
    $preferred = $urls | Where-Object { $_.StartsWith($preferredPrefix, [System.StringComparison]::OrdinalIgnoreCase) } | Select-Object -First 1
    if ($preferred) {
        return $preferred
    }

    return $urls[0]
}

function Wait-ForTcpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [Uri]$Uri,
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
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

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..')).Path

$daemonProjectPath = Join-Path $repoRoot 'src\CSharpDB.Daemon\CSharpDB.Daemon.csproj'
$daemonAppSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Daemon\appsettings.json'
$daemonLaunchSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Daemon\Properties\launchSettings.json'

$adminProjectPath = Join-Path $repoRoot 'src\CSharpDB.Admin\CSharpDB.Admin.csproj'
$adminAppSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Admin\appsettings.json'
$adminLaunchSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Admin\Properties\launchSettings.json'

$daemonConfig = Read-JsonFile -Path $daemonAppSettingsPath
$daemonLaunchSettings = Read-JsonFile -Path $daemonLaunchSettingsPath
$adminConfig = Read-JsonFile -Path $adminAppSettingsPath
$adminLaunchSettings = Read-JsonFile -Path $adminLaunchSettingsPath
$originalAdminJson = $adminConfig | ConvertTo-Json -Depth 20

$daemonLaunchProfile = Get-LaunchProfile -LaunchSettings $daemonLaunchSettings -PreferredProfileName 'CSharpDB.Daemon'
$daemonEndpoint = Select-PreferredUrl -ApplicationUrl $daemonLaunchProfile.Profile.applicationUrl -PreferredScheme 'https'

if (-not $daemonEndpoint) {
    throw "Unable to determine the daemon endpoint from $daemonLaunchSettingsPath."
}

$adminLaunchProfile = Get-LaunchProfile -LaunchSettings $adminLaunchSettings -PreferredProfileName 'CSharpDB.Admin'
$adminUrl = Select-PreferredUrl -ApplicationUrl $adminLaunchProfile.Profile.applicationUrl -PreferredScheme 'https'

$csharpDbSection = Get-OrAddProperty -Object $adminConfig -Name 'CSharpDB' -DefaultValue ([pscustomobject]@{})
Set-JsonProperty -Object $csharpDbSection -Name 'Transport' -Value 'grpc'
Set-JsonProperty -Object $csharpDbSection -Name 'Endpoint' -Value $daemonEndpoint

$daemonConnectionString = $null
if ($null -ne $daemonConfig.PSObject.Properties['ConnectionStrings'] -and
    $null -ne $daemonConfig.ConnectionStrings -and
    $null -ne $daemonConfig.ConnectionStrings.PSObject.Properties['CSharpDB']) {
    $daemonConnectionString = $daemonConfig.ConnectionStrings.CSharpDB
}

if (-not [string]::IsNullOrWhiteSpace($daemonConnectionString)) {
    $connectionStringsSection = Get-OrAddProperty -Object $adminConfig -Name 'ConnectionStrings' -DefaultValue ([pscustomobject]@{})
    Set-JsonProperty -Object $connectionStringsSection -Name 'CSharpDB' -Value $daemonConnectionString
}

$updatedAdminJson = $adminConfig | ConvertTo-Json -Depth 20

if ($updatedAdminJson -ne $originalAdminJson) {
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($adminAppSettingsPath, "$updatedAdminJson`r`n", $utf8NoBom)
}

Write-Host "Updated admin config:"
Write-Host "  Transport: grpc"
Write-Host "  Endpoint : $daemonEndpoint"

if ($daemonConnectionString) {
    Write-Host "  Database : $daemonConnectionString"
}

if ($NoLaunch) {
    if ($PassThru) {
        [pscustomobject]@{
            DaemonEndpoint    = $daemonEndpoint
            AdminUrl          = $adminUrl
            DaemonProfileName = $daemonLaunchProfile.Name
            AdminProfileName  = $adminLaunchProfile.Name
        }
    }

    return
}

$daemonArgs = @(
    'run',
    '--project', $daemonProjectPath,
    '--launch-profile', $daemonLaunchProfile.Name
)

Write-Host "Starting daemon profile '$($daemonLaunchProfile.Name)'..."
$daemonProcess = Start-Process -FilePath 'dotnet' -ArgumentList $daemonArgs -WorkingDirectory $repoRoot -PassThru

if (-not (Wait-ForTcpEndpoint -Uri ([Uri]$daemonEndpoint) -TimeoutSeconds $DaemonStartupTimeoutSeconds)) {
    Stop-ProcessIfRunning -Process $daemonProcess
    throw "The daemon did not start listening on $daemonEndpoint within $DaemonStartupTimeoutSeconds seconds."
}

Write-Host "Daemon is listening on $daemonEndpoint (PID $($daemonProcess.Id))."

$adminArgs = @(
    'run',
    '--project', $adminProjectPath,
    '--launch-profile', $adminLaunchProfile.Name
)

Write-Host "Starting admin profile '$($adminLaunchProfile.Name)'..."
$adminProcess = Start-Process -FilePath 'dotnet' -ArgumentList $adminArgs -WorkingDirectory $repoRoot -PassThru

if ($adminUrl) {
    if (-not (Wait-ForTcpEndpoint -Uri ([Uri]$adminUrl) -TimeoutSeconds $AdminStartupTimeoutSeconds)) {
        Stop-ProcessIfRunning -Process $adminProcess
        Stop-ProcessIfRunning -Process $daemonProcess
        throw "The admin site did not start listening on $adminUrl within $AdminStartupTimeoutSeconds seconds."
    }

    Write-Host "Admin is listening on $adminUrl (PID $($adminProcess.Id))."

    if ($OpenAdmin) {
        Start-Process -FilePath $adminUrl | Out-Null
        Write-Host "Opened $adminUrl in the default browser."
    }
}

if ($PassThru) {
    [pscustomobject]@{
        DaemonEndpoint = $daemonEndpoint
        AdminUrl       = $adminUrl
        DaemonPid      = $daemonProcess.Id
        AdminPid       = $adminProcess.Id
    }
}

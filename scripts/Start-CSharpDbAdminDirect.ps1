<#
.SYNOPSIS
Configures the admin site for direct mode, then starts only the admin host.

.DESCRIPTION
This script is intended for local development and manual operator workflows.
It updates `src/CSharpDB.Admin/appsettings.json` to use
`CSharpDbTransport.Direct`, removes any daemon endpoint from the admin config,
ensures a connection string exists, builds `CSharpDB.Admin`, and then starts it.

The script does not install a Windows service or a background task. It builds
the admin project first, then launches one `dotnet run --no-build` process. If
you close the shell that launched this script, the child process continues
running until you stop it explicitly.

.PARAMETER NoLaunch
Only updates the admin configuration. Does not start the admin host.

.PARAMETER OpenAdmin
Opens the admin URL in the default browser after startup succeeds.

.PARAMETER PassThru
Returns the resolved URL and process ID so the caller can stop the process
later with `Stop-Process`.

.PARAMETER ConnectionString
Overrides the admin database connection string before launch.

.PARAMETER AdminStartupTimeoutSeconds
How long to wait for the admin endpoint to start accepting TCP connections
after the admin project has built and the app process has launched.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\Start-CSharpDbAdminDirect.ps1

Starts the admin site in direct mode using the connection string already stored
in `src/CSharpDB.Admin/appsettings.json`.

.EXAMPLE
.\scripts\Start-CSharpDbAdminDirect.ps1 -ConnectionString "Data Source=C:\data\demo.db" -OpenAdmin

Updates the admin config to point at a specific database, starts the admin
site, and opens the browser.

.EXAMPLE
$session = & .\scripts\Start-CSharpDbAdminDirect.ps1 -PassThru
Stop-Process -Id $session.AdminPid

Starts the admin site and captures the process ID so it can be stopped later.
#>
[CmdletBinding()]
param(
    [switch]$NoLaunch,
    [switch]$OpenAdmin,
    [switch]$PassThru,
    [string]$ConnectionString,
    [int]$AdminStartupTimeoutSeconds = 60
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

function Remove-JsonProperty {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Object,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $property = $Object.PSObject.Properties[$Name]
    if ($null -ne $property) {
        $Object.PSObject.Properties.Remove($Name)
    }
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

function Get-LaunchUris {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ApplicationUrl
    )

    return @(
        $ApplicationUrl.Split(';', [System.StringSplitOptions]::RemoveEmptyEntries) |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ } |
            ForEach-Object { [Uri]$_ }
    )
}

function Test-TcpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [Uri]$Uri
    )

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

    return $false
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
        if (Test-TcpEndpoint -Uri $Uri) {
            return $true
        }

        Start-Sleep -Milliseconds 500
    }

    return $false
}

function Wait-ForAnyTcpEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [Uri[]]$Uris,
        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds,
        [System.Diagnostics.Process]$Process
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        if ($null -ne $Process) {
            $Process.Refresh()
            if ($Process.HasExited) {
                return $null
            }
        }

        foreach ($uri in $Uris) {
            if (Test-TcpEndpoint -Uri $uri) {
                return $uri
            }
        }

        Start-Sleep -Milliseconds 500
    }

    return $null
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

$adminProjectPath = Join-Path $repoRoot 'src\CSharpDB.Admin\CSharpDB.Admin.csproj'
$adminAppSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Admin\appsettings.json'
$adminLaunchSettingsPath = Join-Path $repoRoot 'src\CSharpDB.Admin\Properties\launchSettings.json'

$adminConfig = Read-JsonFile -Path $adminAppSettingsPath
$adminLaunchSettings = Read-JsonFile -Path $adminLaunchSettingsPath
$originalAdminJson = $adminConfig | ConvertTo-Json -Depth 20

$adminLaunchProfile = Get-LaunchProfile -LaunchSettings $adminLaunchSettings -PreferredProfileName 'CSharpDB.Admin'
$adminUrl = Select-PreferredUrl -ApplicationUrl $adminLaunchProfile.Profile.applicationUrl -PreferredScheme 'https'
$adminUris = @(Get-LaunchUris -ApplicationUrl $adminLaunchProfile.Profile.applicationUrl)

$csharpDbSection = Get-OrAddProperty -Object $adminConfig -Name 'CSharpDB' -DefaultValue ([pscustomobject]@{})
Set-JsonProperty -Object $csharpDbSection -Name 'Transport' -Value 'direct'
Remove-JsonProperty -Object $csharpDbSection -Name 'Endpoint'

$connectionStringsSection = Get-OrAddProperty -Object $adminConfig -Name 'ConnectionStrings' -DefaultValue ([pscustomobject]@{})
$effectiveConnectionString = $ConnectionString

if ([string]::IsNullOrWhiteSpace($effectiveConnectionString)) {
    $existingConnectionString = $null
    if ($null -ne $connectionStringsSection.PSObject.Properties['CSharpDB']) {
        $existingConnectionString = $connectionStringsSection.CSharpDB
    }

    if ([string]::IsNullOrWhiteSpace($existingConnectionString)) {
        $effectiveConnectionString = 'Data Source=csharpdb.db'
    }
    else {
        $effectiveConnectionString = $existingConnectionString
    }
}

Set-JsonProperty -Object $connectionStringsSection -Name 'CSharpDB' -Value $effectiveConnectionString

$updatedAdminJson = $adminConfig | ConvertTo-Json -Depth 20

if ($updatedAdminJson -ne $originalAdminJson) {
    $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($adminAppSettingsPath, "$updatedAdminJson`r`n", $utf8NoBom)
}

Write-Host "Updated admin config:"
Write-Host "  Transport: direct"
Write-Host "  Database : $effectiveConnectionString"

if ($NoLaunch) {
    if ($PassThru) {
        [pscustomobject]@{
            AdminUrl         = $adminUrl
            AdminProfileName = $adminLaunchProfile.Name
            ConnectionString = $effectiveConnectionString
        }
    }

    return
}

$adminArgs = @(
    'run',
    '--no-build',
    '--project', $adminProjectPath,
    '--launch-profile', $adminLaunchProfile.Name
)

Write-Host "Building admin project..."
dotnet build $adminProjectPath | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "The admin project build failed with exit code $LASTEXITCODE."
}

Write-Host "Starting admin profile '$($adminLaunchProfile.Name)'..."
$adminProcess = Start-Process -FilePath 'dotnet' -ArgumentList $adminArgs -WorkingDirectory $repoRoot -PassThru

if ($adminUris.Count -gt 0) {
    $listeningUri = Wait-ForAnyTcpEndpoint -Uris $adminUris -TimeoutSeconds $AdminStartupTimeoutSeconds -Process $adminProcess
    if ($null -eq $listeningUri) {
        Stop-ProcessIfRunning -Process $adminProcess

        $configuredUrls = ($adminUris | ForEach-Object { $_.ToString().TrimEnd('/') }) -join ', '
        throw "The admin site did not start listening on any configured URL ($configuredUrls) within $AdminStartupTimeoutSeconds seconds."
    }

    $adminUrl = $listeningUri.ToString().TrimEnd('/')
    Write-Host "Admin is listening on $adminUrl (PID $($adminProcess.Id))."

    if ($OpenAdmin) {
        Start-Process -FilePath $adminUrl | Out-Null
        Write-Host "Opened $adminUrl in the default browser."
    }
}

if ($PassThru) {
    [pscustomobject]@{
        AdminUrl         = $adminUrl
        AdminPid         = $adminProcess.Id
        ConnectionString = $effectiveConnectionString
    }
}

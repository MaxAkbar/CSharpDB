<#
.SYNOPSIS
Installs CSharpDB.Daemon as a Windows Service.

.DESCRIPTION
Copies an extracted daemon release archive to an install directory, writes
appsettings.Production.json, creates a Windows Service, and configures service
environment variables for the database path and bind URL.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$ServiceName = 'CSharpDBDaemon',
    [string]$InstallDirectory = (Join-Path $env:ProgramFiles 'CSharpDB\Daemon'),
    [string]$DataDirectory = (Join-Path $env:ProgramData 'CSharpDB'),
    [string]$Url = 'http://127.0.0.1:5820',
    [string]$SourceDirectory,
    [switch]$Force,
    [switch]$Start
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Installing CSharpDB.Daemon as a Windows Service requires an elevated PowerShell session.'
    }
}

function Write-ProductionSettings {
    param(
        [string]$Path,
        [string]$DatabasePath
    )

    $settings = [ordered]@{
        ConnectionStrings = [ordered]@{
            CSharpDB = "Data Source=$DatabasePath"
        }
        CSharpDB = [ordered]@{
            Daemon = [ordered]@{
                EnableRestApi = $true
            }
            HostDatabase = [ordered]@{
                OpenMode = 'HybridIncrementalDurable'
                ImplicitInsertExecutionMode = 'ConcurrentWriteTransactions'
                UseWriteOptimizedPreset = $true
                HotTableNames = @()
                HotCollectionNames = @()
            }
        }
    }

    $settings | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding UTF8
}

Assert-Administrator

if ([string]::IsNullOrWhiteSpace($SourceDirectory)) {
    $SourceDirectory = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
}
elseif (-not [System.IO.Path]::IsPathRooted($SourceDirectory)) {
    $SourceDirectory = (Resolve-Path $SourceDirectory).Path
}

$existingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existingService -and -not $Force.IsPresent) {
    throw "Service '$ServiceName' already exists. Re-run with -Force to replace it."
}

if ($PSCmdlet.ShouldProcess($ServiceName, 'Install CSharpDB.Daemon Windows Service')) {
    if ($existingService) {
        if ($existingService.Status -ne 'Stopped') {
            Stop-Service -Name $ServiceName -Force -ErrorAction Stop
            $existingService.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
        }

        & sc.exe delete $ServiceName | Out-Null
        Start-Sleep -Seconds 1
    }

    New-Item -ItemType Directory -Path $InstallDirectory, $DataDirectory -Force | Out-Null
    Copy-Item -Path (Join-Path $SourceDirectory '*') -Destination $InstallDirectory -Recurse -Force

    $exePath = Join-Path $InstallDirectory 'CSharpDB.Daemon.exe'
    if (-not (Test-Path -Path $exePath)) {
        throw "Could not find daemon executable after copy: $exePath"
    }

    $databasePath = Join-Path $DataDirectory 'csharpdb.db'
    Write-ProductionSettings -Path (Join-Path $InstallDirectory 'appsettings.Production.json') -DatabasePath $databasePath

    New-Service `
        -Name $ServiceName `
        -BinaryPathName "`"$exePath`"" `
        -DisplayName 'CSharpDB Daemon' `
        -Description 'CSharpDB remote daemon service.' `
        -StartupType Automatic | Out-Null

    $serviceKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName"
    $environment = @(
        'DOTNET_ENVIRONMENT=Production',
        'ASPNETCORE_ENVIRONMENT=Production',
        "ASPNETCORE_URLS=$Url",
        "ConnectionStrings__CSharpDB=Data Source=$databasePath",
        'CSharpDB__Daemon__EnableRestApi=true'
    )
    New-ItemProperty -Path $serviceKey -Name Environment -PropertyType MultiString -Value $environment -Force | Out-Null

    & sc.exe failure $ServiceName reset= 60 actions= restart/5000/restart/10000/''/0 | Out-Null

    if ($Start.IsPresent) {
        Start-Service -Name $ServiceName
    }

    Write-Host "Installed service '$ServiceName'."
    Write-Host "  Install directory: $InstallDirectory"
    Write-Host "  Data directory: $DataDirectory"
    Write-Host "  URL: $Url"
}

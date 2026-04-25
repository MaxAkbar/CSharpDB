<#
.SYNOPSIS
Uninstalls the CSharpDB.Daemon Windows Service.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$ServiceName = 'CSharpDBDaemon',
    [string]$InstallDirectory = (Join-Path $env:ProgramFiles 'CSharpDB\Daemon'),
    [switch]$RemoveInstallDirectory
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Uninstalling CSharpDB.Daemon as a Windows Service requires an elevated PowerShell session.'
    }
}

Assert-Administrator

if ($PSCmdlet.ShouldProcess($ServiceName, 'Uninstall CSharpDB.Daemon Windows Service')) {
    $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($service) {
        if ($service.Status -ne 'Stopped') {
            Stop-Service -Name $ServiceName -Force -ErrorAction Stop
            $service.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(30))
        }

        & sc.exe delete $ServiceName | Out-Null
        Write-Host "Deleted service '$ServiceName'."
    }
    else {
        Write-Host "Service '$ServiceName' was not installed."
    }

    if ($RemoveInstallDirectory.IsPresent -and (Test-Path -Path $InstallDirectory)) {
        Remove-Item -LiteralPath $InstallDirectory -Recurse -Force
        Write-Host "Removed install directory: $InstallDirectory"
    }
}

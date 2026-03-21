<#
.SYNOPSIS
Publishes the CSharpDB Admin and Daemon projects.

.DESCRIPTION
This script publishes `src/CSharpDB.Admin` and `src/CSharpDB.Daemon` into
separate output folders under a common publish root. By default it publishes
both projects in Release configuration to `artifacts\publish`.

.PARAMETER Configuration
The build configuration passed to `dotnet publish`.

.PARAMETER Runtime
Optional runtime identifier such as `win-x64` or `linux-x64`.

.PARAMETER SelfContained
Publishes as self-contained when specified.

.PARAMETER OutputRoot
The root folder where publish outputs are written. Each project is published
to its own subfolder below this root.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\Publish-CSharpDbAdminAndDaemon.ps1

Publishes both projects in Release configuration under `artifacts\publish`.

.EXAMPLE
.\scripts\Publish-CSharpDbAdminAndDaemon.ps1 -Runtime win-x64 -SelfContained

Publishes both projects for Windows x64 as self-contained deployments.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$Configuration = 'Release',
    [string]$Runtime,
    [switch]$SelfContained,
    [string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..')).Path

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot 'artifacts\publish'
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}

$projects = @(
    [pscustomobject]@{
        Name       = 'CSharpDB.Admin'
        Project    = Join-Path $repoRoot 'src\CSharpDB.Admin\CSharpDB.Admin.csproj'
        OutputPath = Join-Path $OutputRoot 'CSharpDB.Admin'
    },
    [pscustomobject]@{
        Name       = 'CSharpDB.Daemon'
        Project    = Join-Path $repoRoot 'src\CSharpDB.Daemon\CSharpDB.Daemon.csproj'
        OutputPath = Join-Path $OutputRoot 'CSharpDB.Daemon'
    }
)

foreach ($project in $projects) {
    if (-not (Test-Path -Path $project.Project)) {
        throw "Project file was not found: $($project.Project)"
    }
}

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null

foreach ($project in $projects) {
    $arguments = @(
        'publish',
        $project.Project,
        '--configuration', $Configuration,
        '--output', $project.OutputPath
    )

    if (-not [string]::IsNullOrWhiteSpace($Runtime)) {
        $arguments += @('--runtime', $Runtime)
    }

    if ($SelfContained.IsPresent) {
        $arguments += @('--self-contained', 'true')
    }

    if ($PSCmdlet.ShouldProcess($project.Name, "dotnet $($arguments -join ' ')")) {
        Write-Host "Publishing $($project.Name) to $($project.OutputPath)..."
        & dotnet @arguments

        if ($LASTEXITCODE -ne 0) {
            throw "dotnet publish failed for $($project.Name) with exit code $LASTEXITCODE."
        }
    }
}

Write-Host "Publish complete."
Write-Host "  Output root: $OutputRoot"

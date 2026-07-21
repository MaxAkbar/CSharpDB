<#
.SYNOPSIS
Publishes the portable CSharpDB Admin desktop release archive.

.DESCRIPTION
Publishes the WPF/WebView2 desktop shell and the CSharpDB.Admin web host as
self-contained Windows applications. The Admin host is placed in the desktop
shell's private admin folder, and the complete runnable layout is packaged as
a ZIP for GitHub Releases.

.PARAMETER Version
The release version used in the archive name. When omitted, the script derives
it from a v-prefixed GITHUB_REF_NAME, then falls back to 0.0.0-local.

.PARAMETER Runtime
The Windows runtime identifier to publish. Defaults to win-x64.

.PARAMETER Configuration
The build configuration passed to dotnet publish. Defaults to Release.

.PARAMETER OutputRoot
The output root. Defaults to artifacts/admin-release.

.PARAMETER NoRestore
Passes --no-restore to dotnet publish.

.EXAMPLE
.\scripts\Publish-CSharpDbAdminRelease.ps1 -Version 4.3.0
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$Version,
    [ValidateSet('win-x64')]
    [string]$Runtime = 'win-x64',
    [string]$Configuration = 'Release',
    [string]$OutputRoot,
    [switch]$NoRestore
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Resolve-Version {
    param([string]$RequestedVersion)

    if (-not [string]::IsNullOrWhiteSpace($RequestedVersion)) {
        return $RequestedVersion.Trim().TrimStart('v')
    }

    if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_REF_NAME) -and $env:GITHUB_REF_NAME.StartsWith('v')) {
        return $env:GITHUB_REF_NAME.Substring(1)
    }

    return '0.0.0-local'
}

function Invoke-AdminPublish {
    param(
        [string]$ProjectPath,
        [string]$Destination,
        [string]$RuntimeIdentifier,
        [string]$BuildConfiguration,
        [switch]$SkipRestore
    )

    $arguments = @(
        'publish', $ProjectPath,
        '--configuration', $BuildConfiguration,
        '--runtime', $RuntimeIdentifier,
        '--self-contained', 'true',
        '--output', $Destination,
        '-p:PublishSingleFile=false'
    )

    if ($SkipRestore.IsPresent) {
        $arguments += '--no-restore'
    }

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed for $ProjectPath with exit code $LASTEXITCODE."
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$releaseVersion = Resolve-Version -RequestedVersion $Version

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot 'artifacts\admin-release'
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}

$adminProject = Join-Path $repoRoot 'src\CSharpDB.Admin\CSharpDB.Admin.csproj'
$desktopProject = Join-Path $repoRoot 'src\CSharpDB.Admin.Desktop\CSharpDB.Admin.Desktop.csproj'
$publishRoot = Join-Path $OutputRoot 'publish'
$adminPublish = Join-Path $publishRoot 'admin'
$desktopPublish = Join-Path $publishRoot 'desktop'
$archiveRoot = Join-Path $OutputRoot 'archives'
$archiveName = "csharpdb-admin-desktop-v$releaseVersion-$Runtime.zip"
$archivePath = Join-Path $archiveRoot $archiveName

foreach ($projectPath in @($adminProject, $desktopProject)) {
    if (-not (Test-Path -LiteralPath $projectPath -PathType Leaf)) {
        throw "Required project was not found: $projectPath"
    }
}

if ($PSCmdlet.ShouldProcess($Runtime, "Publish $archiveName")) {
    if (Test-Path -LiteralPath $OutputRoot) {
        Remove-Item -LiteralPath $OutputRoot -Recurse -Force
    }

    New-Item -ItemType Directory -Path $adminPublish, $desktopPublish, $archiveRoot -Force | Out-Null

    Write-Host 'Publishing CSharpDB.Admin...'
    Invoke-AdminPublish -ProjectPath $adminProject -Destination $adminPublish -RuntimeIdentifier $Runtime -BuildConfiguration $Configuration -SkipRestore:$NoRestore

    Write-Host 'Publishing CSharpDB.Admin.Desktop...'
    Invoke-AdminPublish -ProjectPath $desktopProject -Destination $desktopPublish -RuntimeIdentifier $Runtime -BuildConfiguration $Configuration -SkipRestore:$NoRestore

    if (-not (Test-Path -LiteralPath (Join-Path $adminPublish 'CSharpDB.Admin.exe') -PathType Leaf)) {
        throw 'The published Admin host executable was not created.'
    }

    if (-not (Test-Path -LiteralPath (Join-Path $adminPublish 'wwwroot\help\index.html') -PathType Leaf)) {
        throw 'The published Admin host does not contain its help files.'
    }

    if (-not (Test-Path -LiteralPath (Join-Path $desktopPublish 'CSharpDB.Admin.Desktop.exe') -PathType Leaf)) {
        throw 'The published desktop executable was not created.'
    }

    Copy-Item -LiteralPath $adminPublish -Destination (Join-Path $desktopPublish 'admin') -Recurse -Force
    Compress-Archive -Path (Join-Path $desktopPublish '*') -DestinationPath $archivePath -Force

    $hash = Get-FileHash -LiteralPath $archivePath -Algorithm SHA256
    $checksumPath = Join-Path $archiveRoot 'ADMIN-SHA256SUMS.txt'
    Set-Content -LiteralPath $checksumPath -Value "$($hash.Hash.ToLowerInvariant())  $archiveName" -Encoding ASCII

    Write-Host 'CSharpDB Admin desktop release archive complete.'
    Write-Host "  Archive:  $archivePath"
    Write-Host "  Checksum: $checksumPath"
}

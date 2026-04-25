<#
.SYNOPSIS
Publishes release archives for CSharpDB.Daemon.

.DESCRIPTION
Publishes CSharpDB.Daemon as self-contained, single-file, non-trimmed
artifacts for one or more runtime identifiers. Each publish output is staged
with the service installation assets under deploy/daemon and then archived for
GitHub Releases. A SHA256SUMS.txt file is generated for all archives in the
archive output folder.

.PARAMETER Version
The release version used in archive names. When omitted, the script tries to
derive it from GITHUB_REF_NAME when it is a v-prefixed tag, then falls back to
0.0.0-local.

.PARAMETER Runtime
Runtime identifiers to publish. Defaults to win-x64, linux-x64, and osx-arm64.

.PARAMETER Configuration
The build configuration passed to dotnet publish. Defaults to Release.

.PARAMETER OutputRoot
The root folder where publish, stage, and archive outputs are written. Defaults
to artifacts/daemon-release.

.PARAMETER NoRestore
Passes --no-restore to dotnet publish.

.EXAMPLE
.\scripts\Publish-CSharpDbDaemonRelease.ps1 -Version 3.4.0 -Runtime win-x64

Publishes and archives the Windows x64 daemon release artifact.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$Version,
    [string[]]$Runtime = @('win-x64', 'linux-x64', 'osx-arm64'),
    [string]$Configuration = 'Release',
    [string]$OutputRoot,
    [switch]$NoRestore
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Resolve-Version {
    param([string]$RequestedVersion)

    if (-not [string]::IsNullOrWhiteSpace($RequestedVersion)) {
        return $RequestedVersion.TrimStart('v')
    }

    if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_REF_NAME) -and $env:GITHUB_REF_NAME.StartsWith('v')) {
        return $env:GITHUB_REF_NAME.Substring(1)
    }

    return '0.0.0-local'
}

function Get-ArchiveName {
    param(
        [string]$ReleaseVersion,
        [string]$Rid
    )

    $extension = if ($Rid.StartsWith('win-', [StringComparison]::OrdinalIgnoreCase)) { 'zip' } else { 'tar.gz' }
    return "csharpdb-daemon-v$ReleaseVersion-$Rid.$extension"
}

function New-TarGzArchive {
    param(
        [string]$SourceDirectory,
        [string]$DestinationPath
    )

    Push-Location $SourceDirectory
    try {
        & tar -czf $DestinationPath .
        if ($LASTEXITCODE -ne 0) {
            throw "tar failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        Pop-Location
    }
}

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..')).Path
$releaseVersion = Resolve-Version $Version

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot 'artifacts\daemon-release'
}
elseif (-not [System.IO.Path]::IsPathRooted($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot $OutputRoot
}

$projectPath = Join-Path $repoRoot 'src\CSharpDB.Daemon\CSharpDB.Daemon.csproj'
$serviceAssetsRoot = Join-Path $repoRoot 'deploy\daemon'
$publishRoot = Join-Path $OutputRoot 'publish'
$stageRoot = Join-Path $OutputRoot 'stage'
$archiveRoot = Join-Path $OutputRoot 'archives'

if (-not (Test-Path -Path $projectPath)) {
    throw "Daemon project file was not found: $projectPath"
}

if (-not (Test-Path -Path $serviceAssetsRoot)) {
    throw "Service assets folder was not found: $serviceAssetsRoot"
}

New-Item -ItemType Directory -Path $publishRoot, $stageRoot, $archiveRoot -Force | Out-Null

$createdArchives = [System.Collections.Generic.List[string]]::new()

foreach ($rid in $Runtime) {
    if ([string]::IsNullOrWhiteSpace($rid)) {
        continue
    }

    $rid = $rid.Trim()
    $publishDir = Join-Path $publishRoot $rid
    $stageDir = Join-Path $stageRoot $rid
    $archiveName = Get-ArchiveName -ReleaseVersion $releaseVersion -Rid $rid
    $archivePath = Join-Path $archiveRoot $archiveName

    if (Test-Path -Path $publishDir) {
        Remove-Item -LiteralPath $publishDir -Recurse -Force
    }

    if (Test-Path -Path $stageDir) {
        Remove-Item -LiteralPath $stageDir -Recurse -Force
    }

    $publishArgs = @(
        'publish',
        $projectPath,
        '--configuration', $Configuration,
        '--runtime', $rid,
        '--self-contained', 'true',
        '--output', $publishDir,
        '-p:PublishSingleFile=true',
        '-p:PublishTrimmed=false',
        '-p:IncludeNativeLibrariesForSelfExtract=true'
    )

    if ($NoRestore.IsPresent) {
        $publishArgs += '--no-restore'
    }

    if ($PSCmdlet.ShouldProcess($rid, "dotnet $($publishArgs -join ' ')")) {
        Write-Host "Publishing CSharpDB.Daemon for $rid..."
        & dotnet @publishArgs

        if ($LASTEXITCODE -ne 0) {
            throw "dotnet publish failed for $rid with exit code $LASTEXITCODE."
        }

        New-Item -ItemType Directory -Path $stageDir -Force | Out-Null
        Copy-Item -Path (Join-Path $publishDir '*') -Destination $stageDir -Recurse -Force
        Copy-Item -Path $serviceAssetsRoot -Destination (Join-Path $stageDir 'service') -Recurse -Force
        Copy-Item -Path (Join-Path $repoRoot 'src\CSharpDB.Daemon\README.md') -Destination (Join-Path $stageDir 'README.md') -Force

        if (Test-Path -Path $archivePath) {
            Remove-Item -LiteralPath $archivePath -Force
        }

        Write-Host "Creating $archiveName..."
        if ($archiveName.EndsWith('.zip', [StringComparison]::OrdinalIgnoreCase)) {
            Compress-Archive -Path (Join-Path $stageDir '*') -DestinationPath $archivePath -Force
        }
        else {
            New-TarGzArchive -SourceDirectory $stageDir -DestinationPath $archivePath
        }

        $createdArchives.Add($archivePath) | Out-Null
    }
}

$archives = Get-ChildItem -Path $archiveRoot -File |
    Where-Object { $_.Name -like 'csharpdb-daemon-v*.zip' -or $_.Name -like 'csharpdb-daemon-v*.tar.gz' } |
    Sort-Object Name

$checksumsPath = Join-Path $archiveRoot 'SHA256SUMS.txt'
$checksumLines = foreach ($archive in $archives) {
    $hash = Get-FileHash -Path $archive.FullName -Algorithm SHA256
    "$($hash.Hash.ToLowerInvariant())  $($archive.Name)"
}

Set-Content -Path $checksumsPath -Value $checksumLines -Encoding ASCII

Write-Host "Daemon release archives complete."
Write-Host "  Version: $releaseVersion"
Write-Host "  Archive root: $archiveRoot"
foreach ($archive in $createdArchives) {
    Write-Host "  Created: $archive"
}
Write-Host "  Checksums: $checksumsPath"

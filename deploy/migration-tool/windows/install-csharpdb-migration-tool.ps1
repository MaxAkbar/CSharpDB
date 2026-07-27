#requires -Version 7.0

<#
.SYNOPSIS
Copies an extracted CSharpDB migration CLI release to a selected directory.

.DESCRIPTION
Installs the framework-dependent migration CLI without creating a service,
requesting elevation, or changing PATH. The destination must be absent or
empty unless -Force is supplied. -Force overwrites colliding files but does
not delete unrelated destination files.

The .NET 10 runtime must already be installed.

.PARAMETER InstallDirectory
Existing empty directory or one new leaf directory beneath an existing,
caller-controlled parent.

.PARAMETER SourceDirectory
Extracted archive root. By default this is the directory two levels above this
script inside the release archive.

.PARAMETER Force
Allows colliding files in a nonempty install directory to be overwritten.

.EXAMPLE
.\install\windows\install-csharpdb-migration-tool.ps1 `
  -InstallDirectory "$env:LOCALAPPDATA\CSharpDB\MigrationTool"
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string] $InstallDirectory,

    [string] $SourceDirectory,

    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Test-EquivalentPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Left,

        [Parameter(Mandatory = $true)]
        [string] $Right
    )

    return [string]::Equals(
        [IO.Path]::TrimEndingDirectorySeparator(
            [IO.Path]::GetFullPath($Left)),
        [IO.Path]::TrimEndingDirectorySeparator(
            [IO.Path]::GetFullPath($Right)),
        [StringComparison]::OrdinalIgnoreCase)
}

function Test-PathWithin {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Parent
    )

    $fullPath = [IO.Path]::TrimEndingDirectorySeparator(
        [IO.Path]::GetFullPath($Path))
    $fullParent = [IO.Path]::TrimEndingDirectorySeparator(
        [IO.Path]::GetFullPath($Parent))
    return $fullPath.StartsWith(
        $fullParent + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase)
}

function Assert-ExistingPathHasNoReparsePoints {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,

        [Parameter(Mandatory = $true)]
        [string] $Description
    )

    $current = [IO.Path]::GetFullPath($Path)
    while (-not [string]::IsNullOrWhiteSpace($current)) {
        $item = Get-Item `
            -LiteralPath $current `
            -Force `
            -ErrorAction SilentlyContinue
        if ($null -ne $item -and
            ($item.Attributes -band
                [IO.FileAttributes]::ReparsePoint) -ne 0)
        {
            throw "$Description cannot pass through a link or reparse point: $current"
        }

        $parent = [IO.Directory]::GetParent($current)
        if ($null -eq $parent -or
            (Test-EquivalentPath `
                -Left $current `
                -Right $parent.FullName))
        {
            break
        }

        $current = $parent.FullName
    }
}

function Get-ContainedChildPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Parent,

        [Parameter(Mandatory = $true)]
        [string] $RelativePath
    )

    if ([IO.Path]::IsPathRooted($RelativePath)) {
        throw "A release path unexpectedly became rooted: $RelativePath"
    }

    $destination = [IO.Path]::GetFullPath(
        (Join-Path $Parent $RelativePath))
    if (-not (Test-PathWithin -Path $destination -Parent $Parent)) {
        throw "A release path escapes the install directory: $RelativePath"
    }

    return $destination
}

if ([string]::IsNullOrWhiteSpace($SourceDirectory)) {
    $SourceDirectory = Join-Path $PSScriptRoot '../..'
}
$sourceCandidate = [IO.Path]::GetFullPath($SourceDirectory)
Assert-ExistingPathHasNoReparsePoints `
    -Path $sourceCandidate `
    -Description 'The extracted migration release path'
if (-not (Test-Path -LiteralPath $sourceCandidate -PathType Container)) {
    throw "The extracted migration release directory does not exist: $sourceCandidate"
}
$sourceRoot = (
    Resolve-Path -LiteralPath $sourceCandidate
).ProviderPath
Assert-ExistingPathHasNoReparsePoints `
    -Path $sourceRoot `
    -Description 'The extracted migration release path'
$sourceItem = Get-Item -LiteralPath $sourceRoot -Force
if (($sourceItem.Attributes -band
        [IO.FileAttributes]::ReparsePoint) -ne 0)
{
    throw 'The extracted migration release directory cannot be a link or reparse point.'
}
$sourceLinks = @(
    Get-ChildItem `
        -LiteralPath $sourceRoot `
        -Recurse `
        -Force |
        Where-Object {
            ($_.Attributes -band
                [IO.FileAttributes]::ReparsePoint) -ne 0
        })
if ($sourceLinks.Count -gt 0) {
    throw 'The extracted migration release cannot contain links or reparse points.'
}

$requiredFiles = @(
    (Join-Path $sourceRoot 'csharpdb.exe'),
    (Join-Path $sourceRoot 'LICENSE'),
    (Join-Path $sourceRoot 'README.md'),
    (Join-Path $sourceRoot 'adapters/sqlserver/csharpdb-migration-sqlserver-worker.exe'),
    (Join-Path $sourceRoot 'adapters/sqlserver/THIRD-PARTY-NOTICES.md'),
    (Join-Path $sourceRoot 'adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'),
    (Join-Path $sourceRoot 'adapters/mysql/csharpdb-migration-mysql-worker.exe'),
    (Join-Path $sourceRoot 'adapters/mysql/THIRD-PARTY-NOTICES.md'),
    (Join-Path $sourceRoot 'adapters/access/csharpdb-migration-access-worker.exe'),
    (Join-Path $sourceRoot 'adapters/access/CSharpDB.Migration.Access.dll'),
    (Join-Path $sourceRoot 'adapters/access/CSharpDB.Migration.Retained.dll'),
    (Join-Path $sourceRoot 'adapters/access/System.Data.OleDb.dll'),
    (Join-Path $sourceRoot 'adapters/access/THIRD-PARTY-NOTICES.md'),
    (Join-Path $sourceRoot 'adapters/access/csharpdb-migration-access-worker.deps.json')
)
$missing = @(
    $requiredFiles |
        Where-Object {
            -not (Test-Path -LiteralPath $_ -PathType Leaf)
        })
if ($missing.Count -gt 0) {
    throw "The extracted migration release is incomplete: $($missing -join ', ')"
}

$installCandidate = [IO.Path]::TrimEndingDirectorySeparator(
    [IO.Path]::GetFullPath($InstallDirectory))
Assert-ExistingPathHasNoReparsePoints `
    -Path $installCandidate `
    -Description 'The install path'
$installName = [IO.Path]::GetFileName($installCandidate)
$installParent = [IO.Path]::GetDirectoryName($installCandidate)
if ([string]::IsNullOrWhiteSpace($installParent) -or
    [string]::IsNullOrWhiteSpace($installName) -or
    -not (Test-Path -LiteralPath $installParent -PathType Container))
{
    throw 'The install directory parent must already exist and be caller-controlled.'
}
$installParent = (
    Resolve-Path -LiteralPath $installParent
).ProviderPath
Assert-ExistingPathHasNoReparsePoints `
    -Path $installParent `
    -Description 'The install directory parent'
$installRoot = Join-Path $installParent $installName
if (-not (Test-PathWithin -Path $installRoot -Parent $installParent)) {
    throw 'The derived install directory must remain inside its existing parent.'
}
$parentItem = Get-Item -LiteralPath $installParent -Force
if (($parentItem.Attributes -band
        [IO.FileAttributes]::ReparsePoint) -ne 0)
{
    throw 'The install directory parent cannot be a link or reparse point.'
}

$installItem = Get-Item `
    -LiteralPath $installRoot `
    -Force `
    -ErrorAction SilentlyContinue
$destinationExists = $null -ne $installItem
if ($destinationExists) {
    if (($installItem.Attributes -band
            [IO.FileAttributes]::ReparsePoint) -ne 0)
    {
        throw 'The install destination cannot be a link or reparse point.'
    }
    if (-not $installItem.PSIsContainer) {
        throw "The install destination is not a directory: $installRoot"
    }
    $installRoot = (
        Resolve-Path -LiteralPath $installRoot
    ).ProviderPath
    if (-not (Test-PathWithin -Path $installRoot -Parent $installParent)) {
        throw 'The resolved install directory must remain inside its existing parent.'
    }

    $destinationLinks = @(
        Get-ChildItem `
            -LiteralPath $installRoot `
            -Recurse `
            -Force |
            Where-Object {
                ($_.Attributes -band
                    [IO.FileAttributes]::ReparsePoint) -ne 0
            })
    if ($destinationLinks.Count -gt 0) {
        throw 'The install destination cannot contain links or reparse points, including with -Force.'
    }

    $destinationHasContent = $null -ne (
        Get-ChildItem -LiteralPath $installRoot -Force |
            Select-Object -First 1)
    if ($destinationHasContent -and -not $Force.IsPresent) {
        throw 'The install destination is not empty. Pass -Force to overwrite colliding files.'
    }
}

if ((Test-EquivalentPath -Left $installRoot -Right $sourceRoot) -or
    (Test-PathWithin -Path $installRoot -Parent $sourceRoot) -or
    (Test-PathWithin -Path $sourceRoot -Parent $installRoot))
{
    throw 'The install directory and extracted archive directory cannot contain one another.'
}

$dotnet = Get-Command `
    dotnet `
    -CommandType Application `
    -ErrorAction SilentlyContinue
if ($null -eq $dotnet) {
    throw 'The framework-dependent migration CLI requires the .NET 10 runtime, but dotnet was not found.'
}
$installedRuntimes = @(
    & $dotnet.Source --list-runtimes 2>$null)
if ($LASTEXITCODE -ne 0 -or
    -not (
        $installedRuntimes |
            Where-Object {
                $_ -match '^Microsoft\.NETCore\.App 10\.'
            }))
{
    throw 'Install the Microsoft .NET 10 runtime before installing the migration CLI.'
}

if (-not $PSCmdlet.ShouldProcess(
        $installRoot,
        "Copy the CSharpDB migration CLI from $sourceRoot"))
{
    return
}

Assert-ExistingPathHasNoReparsePoints `
    -Path $sourceRoot `
    -Description 'The extracted migration release path'
Assert-ExistingPathHasNoReparsePoints `
    -Path $installRoot `
    -Description 'The install path'
[IO.Directory]::CreateDirectory($installRoot) | Out-Null
Assert-ExistingPathHasNoReparsePoints `
    -Path $installRoot `
    -Description 'The install path'
foreach ($directory in (
        Get-ChildItem `
            -LiteralPath $sourceRoot `
            -Recurse `
            -Directory `
            -Force |
            Sort-Object FullName))
{
    $relative = [IO.Path]::GetRelativePath(
        $sourceRoot,
        $directory.FullName)
    $destination = Get-ContainedChildPath `
        -Parent $installRoot `
        -RelativePath $relative
    [IO.Directory]::CreateDirectory(
        $destination) |
        Out-Null
}
foreach ($file in Get-ChildItem `
        -LiteralPath $sourceRoot `
        -Recurse `
        -File `
        -Force)
{
    $relative = [IO.Path]::GetRelativePath(
        $sourceRoot,
        $file.FullName)
    $destination = Get-ContainedChildPath `
        -Parent $installRoot `
        -RelativePath $relative
    [IO.Directory]::CreateDirectory(
        [IO.Path]::GetDirectoryName($destination)) |
        Out-Null
    [IO.File]::Copy(
        $file.FullName,
        $destination,
        $Force.IsPresent)
}

Write-Host "Installed the CSharpDB migration CLI at $installRoot"
Write-Host "Run it directly as: $installRoot\csharpdb.exe"
Write-Host 'PATH was not changed. To invoke csharpdb.exe by name, add the install directory to your user PATH through Windows Settings or your shell profile.'

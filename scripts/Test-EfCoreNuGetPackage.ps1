[CmdletBinding()]
param(
    [string]$FeedPath = 'artifacts/nuget',

    [string]$Version,

    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',

    [switch]$KeepWorkingDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$fixtureRoot = Join-Path $repoRoot 'tests/package-smoke/efcore-provider'
$projectName = 'CSharpDB.EntityFrameworkCore.PackageSmoke.csproj'
$fixtureProject = Join-Path $fixtureRoot $projectName
$toolManifest = Join-Path $repoRoot '.config/dotnet-tools.json'
$rootBuildProps = Join-Path $repoRoot 'Directory.Build.props'

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path)
    }

    return [IO.Path]::GetFullPath((Join-Path $repoRoot $Path))
}

function Invoke-DotNet {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter(Mandatory)]
        [string]$Description
    )

    Write-Host $Description
    $exitCode = 0
    Push-Location -LiteralPath $repoRoot
    try {
        & dotnet @Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($exitCode -ne 0) {
        throw "$Description failed with exit code $exitCode."
    }
}

if (-not (Test-Path -LiteralPath $fixtureProject -PathType Leaf)) {
    throw "Package smoke fixture was not found: $fixtureProject"
}
if (-not (Test-Path -LiteralPath $toolManifest -PathType Leaf)) {
    throw "The repository-local dotnet tool manifest was not found: $toolManifest"
}
if (-not (Test-Path -LiteralPath $rootBuildProps -PathType Leaf)) {
    throw "The root build properties were not found: $rootBuildProps"
}

[xml]$fixtureXml = Get-Content -Raw -LiteralPath $fixtureProject
if ($null -ne $fixtureXml.SelectSingleNode('/Project/ItemGroup/ProjectReference')) {
    throw 'The package smoke fixture must not contain ProjectReference items.'
}

[xml]$rootBuildPropsXml = Get-Content -Raw -LiteralPath $rootBuildProps
$qualifiedEfCoreVersion = [string]$rootBuildPropsXml.SelectSingleNode(
    '/Project/PropertyGroup/CSharpDbQualifiedEfCoreVersion').'#text'
if ([string]::IsNullOrWhiteSpace($qualifiedEfCoreVersion)) {
    throw 'Directory.Build.props must define CSharpDbQualifiedEfCoreVersion.'
}

$resolvedFeed = Resolve-RepoPath $FeedPath
if (-not (Test-Path -LiteralPath $resolvedFeed -PathType Container)) {
    throw "The local NuGet feed was not found: $resolvedFeed"
}

$packagePrefix = 'CSharpDB.EntityFrameworkCore.'
$packageCandidates = @(
    Get-ChildItem -LiteralPath $resolvedFeed -Filter "$packagePrefix*.nupkg" -File |
        Where-Object { $_.Name -notlike '*.symbols.nupkg' }
)

if ([string]::IsNullOrWhiteSpace($Version)) {
    if ($packageCandidates.Count -ne 1) {
        $candidateNames = if ($packageCandidates.Count -eq 0) {
            '(none)'
        }
        else {
            $packageCandidates.Name -join ', '
        }
        throw "Specify -Version when the feed does not contain exactly one provider package. Found: $candidateNames"
    }

    $Version = $packageCandidates[0].Name.Substring(
        $packagePrefix.Length,
        $packageCandidates[0].Name.Length - $packagePrefix.Length - '.nupkg'.Length)
}

$providerPackage = Join-Path $resolvedFeed "$packagePrefix$Version.nupkg"
if (-not (Test-Path -LiteralPath $providerPackage -PathType Leaf)) {
    throw "The exact provider package was not found: $providerPackage"
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead($providerPackage)
try {
    $nuspecEntry = @($archive.Entries | Where-Object { $_.FullName -like '*.nuspec' })
    if ($nuspecEntry.Count -ne 1) {
        throw "Expected exactly one nuspec in $providerPackage."
    }

    $reader = [IO.StreamReader]::new($nuspecEntry[0].Open())
    try {
        [xml]$nuspec = $reader.ReadToEnd()
    }
    finally {
        $reader.Dispose()
    }
}
finally {
    $archive.Dispose()
}

$metadata = $nuspec.package.metadata
$packageId = [string]$metadata.id
$packageVersion = [string]$metadata.version
if ($packageId -cne 'CSharpDB.EntityFrameworkCore' -or
    $packageVersion -cne $Version) {
    throw "Package identity mismatch. Expected CSharpDB.EntityFrameworkCore $Version, found $packageId $packageVersion."
}

$workingRoot = Join-Path ([IO.Path]::GetTempPath()) (
    "csharpdb-ef-package-smoke-$([Guid]::NewGuid().ToString('N'))")
$workingFixture = Join-Path $workingRoot 'fixture'
$workingProject = Join-Path $workingFixture $projectName
$packagesPath = Join-Path $workingRoot 'packages'
$nugetConfig = Join-Path $workingRoot 'NuGet.Config'
$runtimeDatabase = Join-Path $workingRoot 'runtime-smoke.db'
$migrationDatabase = Join-Path $workingRoot 'migration-smoke.db'
$migrationScript = Join-Path $workingRoot 'migration.sql'

$previousPackageVersion = $env:CSharpDbPackageVersion
$previousQualifiedEfCoreVersion = $env:CSharpDbQualifiedEfCoreVersion
$previousDatabase = $env:CSHARPDB_EF_PACKAGE_SMOKE_DATABASE
$previousNuGetPackages = $env:NUGET_PACKAGES

try {
    New-Item -ItemType Directory -Path $workingFixture -Force | Out-Null
    Copy-Item -Path (Join-Path $fixtureRoot '*') `
        -Destination $workingFixture `
        -Recurse `
        -Force

    $escapedFeed = [Security.SecurityElement]::Escape($resolvedFeed)
    $escapedPackages = [Security.SecurityElement]::Escape($packagesPath)
    @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <config>
    <add key="globalPackagesFolder" value="$escapedPackages" />
  </config>
  <packageSources>
    <clear />
    <add key="csharpdb-local" value="$escapedFeed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="csharpdb-local">
      <package pattern="CSharpDB*" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
"@ | Set-Content -LiteralPath $nugetConfig -Encoding utf8

    $env:CSharpDbPackageVersion = $Version
    $env:CSharpDbQualifiedEfCoreVersion = $qualifiedEfCoreVersion
    $env:CSHARPDB_EF_PACKAGE_SMOKE_DATABASE = $migrationDatabase
    $env:NUGET_PACKAGES = $packagesPath

    Invoke-DotNet `
        -Description 'Restoring the repository-local dotnet-ef tool.' `
        -Arguments @('tool', 'restore', '--tool-manifest', $toolManifest)

    Invoke-DotNet `
        -Description "Restoring the package-only fixture against CSharpDB.EntityFrameworkCore $Version." `
        -Arguments @(
            'restore',
            $workingProject,
            '--configfile',
            $nugetConfig,
            '--no-cache',
            "-p:CSharpDbPackageVersion=$Version")

    Invoke-DotNet `
        -Description 'Running package-only CRUD and reopen qualification.' `
        -Arguments @(
            'run',
            '--project',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-restore',
            '--',
            'crud',
            $runtimeDatabase)

    Invoke-DotNet `
        -Description 'Generating a migration through the real dotnet-ef tool.' `
        -Arguments @(
            'tool',
            'run',
            'dotnet-ef',
            '--',
            'migrations',
            'add',
            'PackageSmokeInitial',
            '--project',
            $workingProject,
            '--startup-project',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-build')

    Invoke-DotNet `
        -Description 'Compiling the generated package-smoke migration.' `
        -Arguments @(
            'build',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-restore')

    Invoke-DotNet `
        -Description 'Generating SQL from the package-smoke migration.' `
        -Arguments @(
            'tool',
            'run',
            'dotnet-ef',
            '--',
            'migrations',
            'script',
            '0',
            'PackageSmokeInitial',
            '--project',
            $workingProject,
            '--startup-project',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-build',
            '--output',
            $migrationScript)

    if (-not (Test-Path -LiteralPath $migrationScript -PathType Leaf) -or
        (Get-Item -LiteralPath $migrationScript).Length -eq 0 -or
        -not (Select-String -LiteralPath $migrationScript -SimpleMatch 'CREATE TABLE' -Quiet)) {
        throw "dotnet ef produced an empty or invalid migration script: $migrationScript"
    }

    Invoke-DotNet `
        -Description 'Applying the package-smoke migration to a new CSharpDB database.' `
        -Arguments @(
            'tool',
            'run',
            'dotnet-ef',
            '--',
            'database',
            'update',
            '--project',
            $workingProject,
            '--startup-project',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-build')

    Invoke-DotNet `
        -Description 'Verifying CRUD and reopen against the migrated database.' `
        -Arguments @(
            'run',
            '--project',
            $workingProject,
            '--configuration',
            $Configuration,
            '--no-build',
            '--',
            'verify-migration',
            $migrationDatabase)

    Write-Host "EF Core package qualification passed for CSharpDB.EntityFrameworkCore $Version."
    Write-Host "Qualified package: $providerPackage"
}
finally {
    $env:CSharpDbPackageVersion = $previousPackageVersion
    $env:CSharpDbQualifiedEfCoreVersion = $previousQualifiedEfCoreVersion
    $env:CSHARPDB_EF_PACKAGE_SMOKE_DATABASE = $previousDatabase
    $env:NUGET_PACKAGES = $previousNuGetPackages

    if ($KeepWorkingDirectory) {
        Write-Host "Package qualification working directory: $workingRoot"
    }
    elseif (Test-Path -LiteralPath $workingRoot) {
        Remove-Item -LiteralPath $workingRoot -Recurse -Force
    }
}
